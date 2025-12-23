package memory

import (
	"hash/fnv"
	"inMemoryAggregation/internal/middleware"
	"inMemoryAggregation/internal/models"
	"inMemoryAggregation/internal/repository"
	"sync"
	"time"
)

const NumShards = 128

type Aggregator struct {
	shards    [NumShards]*Shard
	counter   int64
	counterMu sync.Mutex
	repo      *repository.Repository
	logger    middleware.Logger
}

func NewAggregator(repo *repository.Repository, logger middleware.Logger) *Aggregator {
	aggr := &Aggregator{
		repo:   repo,
		logger: logger,
	}

	// initialize all shards
	for i := 0; i < NumShards; i++ {
		aggr.shards[i] = NewShard()
	}

	return aggr
}

// determines which shard a key will go to
func (a *Aggregator) hashKey(key models.AggregateKey) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key.Date))
	h.Write([]byte(key.App))
	h.Write([]byte(key.Country))
	return h.Sum32() % NumShards
}

// return the shard for a given key
func (a *Aggregator) getShard(key models.AggregateKey) *Shard {
	shardIdx := a.hashKey(key)
	return a.shards[shardIdx]
}

// add a signal to the aggregator
func (a *Aggregator) Add(req models.SignalRequest) error {
	// Create aggregate key
	key := models.AggregateKey{
		Date:    time.Now().Format("2006-01-02"),
		App:     req.App,
		Country: req.Country,
	}

	// get the shard
	shard := a.getShard(key)

	// update the shard
	shard.Update(key, req.Signal, req.UserID)

	// increase global counter (for flushing to db)
	a.counterMu.Lock()
	a.counter++
	shouldFlush := a.counter >= 1000
	a.counterMu.Unlock()

	// Trigger count-based flush
	if shouldFlush {
		a.logger.Log("msg", "count-based flush triggered", "count", a.counter)
		return a.Flush()
	}

	return nil
}

// writes all shards' data to database and cleans them
func (a *Aggregator) Flush() error {
	batch := []models.AggregateRow{}

	// get data from all shards
	for i := 0; i < NumShards; i++ {
		shard := a.shards[i]
		data := shard.GetAll()

		for key, metrics := range data {
			batch = append(batch, models.AggregateRow{
				Date:         key.Date,
				App:          key.App,
				Country:      key.Country,
				AdRequest:    metrics.AdRequest,
				AdResponse:   metrics.AdResponse,
				AdImpression: metrics.AdImpression,
				AdClicks:     metrics.AdClicks,
				DAU:          int64(len(metrics.UniqueUsers)),
			})
		}
	}

	if len(batch) == 0 {
		return nil
	}

	if err := a.repo.BatchInsert(batch); err != nil {
		a.logger.Log("msg", "flush failed", "error", err)
		return err
	}

	for i := 0; i < NumShards; i++ {
		a.shards[i].Clear()
	}

	a.counterMu.Lock()
	a.counter = 0
	a.counterMu.Unlock()

	a.logger.Log("msg", "flushed to database", "records", len(batch))
	return nil
}

// total size across all shards
func (a *Aggregator) GetSize() int {
	total := 0
	for i := 0; i < NumShards; i++ {
		total += a.shards[i].Size()
	}
	return total
}

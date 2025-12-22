package memory

import (
	"hash/fnv"
	"inMemoryAggregation/internal/middleware"
	"inMemoryAggregation/internal/models"
	"sync"
	"time"
)

const NumShards = 128

type Aggregator struct {
	shards    [NumShards]*Shard
	counter   int64
	counterMu sync.Mutex
	logger    middleware.Logger
}

func NewAggregator(logger middleware.Logger) *Aggregator {
	aggr := &Aggregator{
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
		// return a.Flush()
	}

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

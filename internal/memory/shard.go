package memory

import (
	"inMemoryAggregation/internal/models"
	"sync"
)

type Shard struct {
	mu   sync.RWMutex
	data map[models.AggregateKey]*models.Metrics
}

func NewShard() *Shard {
	return &Shard{
		data: make(map[models.AggregateKey]*models.Metrics),
	}
}

// for getting metrics for a key (will create if nort exists)
func (s *Shard) Get(key models.AggregateKey) *models.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()

	metrics, exists := s.data[key]

	if !exists {
		metrics = &models.Metrics{
			UniqueUsers: make(map[string]struct{}),
		}
		s.data[key] = metrics
	}

	return metrics
}

// update a metric for a key
func (s *Shard) Update(key models.AggregateKey, signal string, userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	metrics, exists := s.data[key]
	if !exists {
		metrics = &models.Metrics{
			UniqueUsers: make(map[string]struct{}),
		}
		s.data[key] = metrics
	}

	switch signal {
	case "AdRequest":
		metrics.AdRequest++
	case "AdResponse":
		metrics.AdResponse++
	case "AdImpression":
		metrics.AdImpression++
	case "AdClicks":
		metrics.AdClicks++
	}

	// unique users
	if userID != "" {
		metrics.UniqueUsers[userID] = struct{}{}
	}
}

// returns all data in this shard (for db)
func (s *Shard) GetAll() map[models.AggregateKey]*models.Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// makinga copy to avoid race conditions
	result := make(map[models.AggregateKey]*models.Metrics, len(s.data))
	for key, metrics := range s.data {
		result[key] = metrics
	}
	return result
}

// clean all data from this shard (after writing tio db)
func (s *Shard) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[models.AggregateKey]*models.Metrics)
}

func (s *Shard) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

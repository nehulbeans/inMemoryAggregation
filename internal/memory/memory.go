package memory

import (
	"inMemoryAggregation/internal/models"
	"sync"
)

type Memory struct {
	mu   sync.RWMutex
	data map[models.AggregateKey]*models.Metrics
}

func NewMemory() *Memory {
	return &Memory{
		data: make(map[models.AggregateKey]*models.Metrics),
	}
}

// Get returns metrics for a key (creates if not exists)
func (m *Memory) Get(key models.AggregateKey) *models.Metrics {
	m.mu.RLock()
	metrics, exists := m.data[key]
	m.mu.RUnlock()

	if !exists {
		// Double-check locking pattern for performance
		m.mu.Lock()
		metrics, exists = m.data[key]
		if !exists {
			metrics = &models.Metrics{
				UniqueUsers: make(map[string]struct{}),
			}
			m.data[key] = metrics
		}
		m.mu.Unlock()
	}
	return metrics
}

// Update increments metrics for a key based on signal
func (m *Memory) Update(key models.AggregateKey, signal string, userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	metrics, exists := m.data[key]
	if !exists {
		metrics = &models.Metrics{
			UniqueUsers: make(map[string]struct{}),
		}
		m.data[key] = metrics
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

	if userID != "" {
		metrics.UniqueUsers[userID] = struct{}{}
	}
}

// GetAll returns copy of all data (thread-safe)
func (m *Memory) GetAll() map[models.AggregateKey]*models.Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[models.AggregateKey]*models.Metrics, len(m.data))
	for key, metrics := range m.data {
		result[key] = metrics
	}
	return result
}

// Clear removes all data (for DB flush)
func (m *Memory) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[models.AggregateKey]*models.Metrics)
}

// Size returns current number of keys
func (m *Memory) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

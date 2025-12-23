package memory

import (
	"inMemoryAggregation/internal/models"
	"testing"
	"time"
)

type mockLogger struct{}

func (m *mockLogger) Log(keyvals ...interface{}) error {
	return nil
}

// helper function to create test aggregator
func newTestAggregator() *Aggregator {
	logger := &mockLogger{}
	agg := &Aggregator{
		logger: logger,
	}

	// manually initializing shards
	for i := 0; i < NumShards; i++ {
		agg.shards[i] = NewShard()
	}

	return agg
}

// Test 1: Basic addition
func TestAggregator_BasicAdd(t *testing.T) {
	agg := newTestAggregator()

	req := models.SignalRequest{
		App:     "com.test",
		Country: "IND",
		Signal:  "AdRequest",
		UserID:  "user1",
	}

	err := agg.Add(req)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if agg.GetSize() != 1 {
		t.Errorf("Expected size 1, got %d", agg.GetSize())
	}
}

// Test 2: Same dimension aggregation
func TestAggregator_SameDimensionAggregation(t *testing.T) {
	agg := newTestAggregator()

	// Add 3 requests with same dimension
	for i := 1; i <= 3; i++ {
		req := models.SignalRequest{
			App:     "com.test",
			Country: "IND",
			Signal:  "AdRequest",
			UserID:  "user1",
		}
		agg.Add(req)
	}

	// Should still be 1 dimension
	if agg.GetSize() != 1 {
		t.Errorf("Expected 1 dimension, got %d", agg.GetSize())
	}

	// Check the data directly
	key := models.AggregateKey{
		Date:    time.Now().Format("2006-01-02"),
		App:     "com.test",
		Country: "IND",
	}

	shard := agg.getShard(key)
	shard.mu.RLock()
	metrics := shard.data[key]
	shard.mu.RUnlock()

	if metrics.AdRequest != 3 {
		t.Errorf("Expected AdRequest=3, got %d", metrics.AdRequest)
	}
}

// Test 3: Multiple dimensions
func TestAggregator_MultipleDimensions(t *testing.T) {
	agg := newTestAggregator()

	// Add requests with different apps
	apps := []string{"com.app1", "com.app2", "com.app3"}
	for _, app := range apps {
		req := models.SignalRequest{
			App:     app,
			Country: "IND",
			Signal:  "AdRequest",
			UserID:  "user1",
		}
		agg.Add(req)
	}

	// Should have 3 dimensions
	if agg.GetSize() != 3 {
		t.Errorf("Expected 3 dimensions, got %d", agg.GetSize())
	}
}

// Test 4: DAU counting (unique users)
func TestAggregator_DAUCounting(t *testing.T) {
	agg := newTestAggregator()

	// Add 10 requests with 3 unique users
	users := []string{"user1", "user2", "user3", "user1", "user2", "user3", "user1", "user2", "user3", "user1"}
	for _, userID := range users {
		req := models.SignalRequest{
			App:     "com.test",
			Country: "IND",
			Signal:  "AdRequest",
			UserID:  userID,
		}
		agg.Add(req)
	}

	// Check the data
	key := models.AggregateKey{
		Date:    time.Now().Format("2006-01-02"),
		App:     "com.test",
		Country: "IND",
	}

	shard := agg.getShard(key)
	shard.mu.RLock()
	metrics := shard.data[key]
	shard.mu.RUnlock()

	// Should have 10 requests
	if metrics.AdRequest != 10 {
		t.Errorf("Expected AdRequest=10, got %d", metrics.AdRequest)
	}

	// Should have 3 unique users
	if len(metrics.UniqueUsers) != 3 {
		t.Errorf("Expected DAU=3, got %d", len(metrics.UniqueUsers))
	}
}

// Test 5: All signal types
func TestAggregator_AllSignalTypes(t *testing.T) {
	agg := newTestAggregator()

	signals := []string{"AdRequest", "AdResponse", "AdImpression", "AdClicks"}
	for _, signal := range signals {
		req := models.SignalRequest{
			App:     "com.test",
			Country: "IND",
			Signal:  signal,
			UserID:  "user1",
		}
		agg.Add(req)
	}

	key := models.AggregateKey{
		Date:    time.Now().Format("2006-01-02"),
		App:     "com.test",
		Country: "IND",
	}

	shard := agg.getShard(key)
	shard.mu.RLock()
	metrics := shard.data[key]
	shard.mu.RUnlock()

	// Check all counters
	if metrics.AdRequest != 1 {
		t.Errorf("Expected AdRequest=1, got %d", metrics.AdRequest)
	}
	if metrics.AdResponse != 1 {
		t.Errorf("Expected AdResponse=1, got %d", metrics.AdResponse)
	}
	if metrics.AdImpression != 1 {
		t.Errorf("Expected AdImpression=1, got %d", metrics.AdImpression)
	}
	if metrics.AdClicks != 1 {
		t.Errorf("Expected AdClicks=1, got %d", metrics.AdClicks)
	}
}

// Test 6: Counter increments
func TestAggregator_CounterIncrement(t *testing.T) {
	agg := newTestAggregator()

	for i := 1; i <= 50; i++ {
		req := models.SignalRequest{
			App:     "com.test",
			Country: "IND",
			Signal:  "AdRequest",
			UserID:  "user1",
		}
		agg.Add(req)
	}

	if agg.counter != 50 {
		t.Errorf("Expected counter=50, got %d", agg.counter)
	}
}

// Test 7: Different countries create different dimensions
func TestAggregator_DifferentCountries(t *testing.T) {
	agg := newTestAggregator()

	countries := []string{"IND", "USA", "BRA"}
	for _, country := range countries {
		req := models.SignalRequest{
			App:     "com.test",
			Country: country,
			Signal:  "AdRequest",
			UserID:  "user1",
		}
		agg.Add(req)
	}

	if agg.GetSize() != 3 {
		t.Errorf("Expected 3 dimensions (3 countries), got %d", agg.GetSize())
	}
}

// Test 8: No user_id handling
func TestAggregator_NoUserID(t *testing.T) {
	agg := newTestAggregator()

	// Add requests without user_id
	for i := 0; i < 5; i++ {
		req := models.SignalRequest{
			App:     "com.test",
			Country: "IND",
			Signal:  "AdRequest",
			// No UserID
		}
		agg.Add(req)
	}

	key := models.AggregateKey{
		Date:    time.Now().Format("2006-01-02"),
		App:     "com.test",
		Country: "IND",
	}

	shard := agg.getShard(key)
	shard.mu.RLock()
	metrics := shard.data[key]
	shard.mu.RUnlock()

	// Should have 5 requests
	if metrics.AdRequest != 5 {
		t.Errorf("Expected AdRequest=5, got %d", metrics.AdRequest)
	}

	// Should have 0 unique users
	if len(metrics.UniqueUsers) != 0 {
		t.Errorf("Expected DAU=0 (no user_id), got %d", len(metrics.UniqueUsers))
	}
}

// Test 9: Sharding distributes data
func TestAggregator_ShardingWorks(t *testing.T) {
	agg := newTestAggregator()

	// Add many different apps - they should go to different shards
	for i := 0; i < 10; i++ {
		req := models.SignalRequest{
			App:     "com.app" + string(rune(i)),
			Country: "IND",
			Signal:  "AdRequest",
			UserID:  "user1",
		}
		agg.Add(req)
	}

	// Count non-empty shards
	nonEmptyShards := 0
	for i := 0; i < NumShards; i++ {
		if agg.shards[i].Size() > 0 {
			nonEmptyShards++
		}
	}

	// Should have multiple shards with data (likely 10, possibly less due to hash collisions)
	if nonEmptyShards < 5 {
		t.Errorf("Expected at least 5 shards with data, got %d", nonEmptyShards)
	}
}

// Test 10: Same key always goes to same shard
func TestAggregator_ConsistentHashing(t *testing.T) {
	agg := newTestAggregator()

	key := models.AggregateKey{
		Date:    "2025-12-23",
		App:     "com.test",
		Country: "IND",
	}

	// Hash same key multiple times
	shard1 := agg.hashKey(key)
	shard2 := agg.hashKey(key)
	shard3 := agg.hashKey(key)

	// Should always return same shard
	if shard1 != shard2 || shard2 != shard3 {
		t.Errorf("Hash not consistent: %d, %d, %d", shard1, shard2, shard3)
	}
}

package storage_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"diamante/storage"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockDB implements a simple in-memory database for testing
type MockDB struct {
	data map[string][]byte
}

func NewMockDB() *MockDB {
	return &MockDB{
		data: make(map[string][]byte),
	}
}

func (m *MockDB) Get(key []byte) ([]byte, error) {
	if value, ok := m.data[string(key)]; ok {
		return value, nil
	}
	return nil, errors.New("key not found")
}

func (m *MockDB) Set(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockDB) Put(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockDB) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *MockDB) Compact(start, end []byte) error {
	// Mock implementation - no actual compaction needed
	return nil
}

func (m *MockDB) GetStats() *storage.StorageStats {
	return &storage.StorageStats{
		ConnectionsActive:     1,
		ConnectionsTotal:      1,
		QueriesExecuted:       int64(len(m.data)),
		TransactionsProcessed: 0,
		CacheHitRate:          0.0,
		AverageQueryTime:      0.0,
		ErrorCount:            0,
		LastError:             "",
		UptimeSeconds:         0,
		MemoryUsageMB:         0.0,
	}
}

func (m *MockDB) NewIterator(start, end []byte) storage.StateIterator {
	return &MockIterator{
		data:  m.data,
		start: string(start),
		end:   string(end),
		keys:  m.getSortedKeys(string(start), string(end)),
		index: -1,
	}
}

func (m *MockDB) getSortedKeys(start, end string) []string {
	var keys []string
	for k := range m.data {
		if k >= start && k < end {
			keys = append(keys, k)
		}
	}
	// Simple bubble sort for test
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

type MockIterator struct {
	data  map[string][]byte
	start string
	end   string
	keys  []string
	index int
}

func (mi *MockIterator) Next() bool {
	mi.index++
	return mi.index < len(mi.keys)
}

func (mi *MockIterator) Key() []byte {
	if mi.index >= 0 && mi.index < len(mi.keys) {
		return []byte(mi.keys[mi.index])
	}
	return nil
}

func (mi *MockIterator) Value() []byte {
	if mi.index >= 0 && mi.index < len(mi.keys) {
		return mi.data[mi.keys[mi.index]]
	}
	return nil
}

func (mi *MockIterator) Error() error {
	return nil
}

func (mi *MockIterator) Close() {
	// Nothing to clean up
}

func TestStateTypeStats(t *testing.T) {
	t.Run("Struct Fields", func(t *testing.T) {
		stats := &storage.StateTypeStats{
			Type:       "account",
			Count:      100,
			TotalSize:  10240,
			MinHeight:  1,
			MaxHeight:  1000,
			AvgSize:    102.4,
			LastPruned: time.Now(),
		}

		assert.Equal(t, "account", stats.Type)
		assert.Equal(t, 100, stats.Count)
		assert.Equal(t, uint64(10240), stats.TotalSize)
		assert.Equal(t, uint64(1), stats.MinHeight)
		assert.Equal(t, uint64(1000), stats.MaxHeight)
		assert.Equal(t, 102.4, stats.AvgSize)
		assert.NotZero(t, stats.LastPruned)
	})
}

func TestStatePruningImpl(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	t.Run("GetStateTypeStats", func(t *testing.T) {
		db := NewMockDB()

		// Add test data
		// Account data with heights encoded
		db.Set([]byte("acc:user1"), encodeHeightData(100, []byte("account1")))
		db.Set([]byte("acc:user2"), encodeHeightData(200, []byte("account2")))
		db.Set([]byte("acc:user3"), encodeHeightData(300, []byte("account3")))

		// Create pruning manager
		config := &storage.PruningConfig{
			Policy:              storage.ArchivePruning,
			BlocksToKeep:        1000,
			PruningInterval:     100,
			MaxPruningBatchSize: 100,
		}

		manager := storage.NewStatePruningManager(db, config, logger)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Get stats
		stats, err := impl.GetStateTypeStats(storage.StateTypeAccount)
		require.NoError(t, err)

		assert.Equal(t, "account", stats.Type)
		assert.Equal(t, 3, stats.Count)
		assert.Greater(t, stats.TotalSize, uint64(0))
		assert.Equal(t, uint64(100), stats.MinHeight)
		assert.Equal(t, uint64(300), stats.MaxHeight)
		assert.Greater(t, stats.AvgSize, float64(0))
	})

	t.Run("GetAllStateTypeStats", func(t *testing.T) {
		db := NewMockDB()

		// Add test data for different types
		db.Set([]byte("acc:user1"), encodeHeightData(100, []byte("account1")))
		db.Set([]byte("tx:tx1"), encodeHeightData(150, []byte("transaction1")))
		db.Set([]byte("blk:block1"), encodeHeightData(200, []byte("block1")))

		// Create pruning manager
		config := &storage.PruningConfig{
			Policy:       storage.ArchivePruning,
			BlocksToKeep: 1000,
		}

		manager := storage.NewStatePruningManager(db, config, logger)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Get all stats
		allStats, err := impl.GetAllStateTypeStats()
		require.NoError(t, err)

		// Should have stats for each type with data
		assert.NotNil(t, allStats[storage.StateTypeAccount])
		assert.NotNil(t, allStats[storage.StateTypeTransaction])
		assert.NotNil(t, allStats[storage.StateTypeBlock])

		// Verify account stats
		accountStats := allStats[storage.StateTypeAccount]
		assert.Equal(t, "account", accountStats.Type)
		assert.Equal(t, 1, accountStats.Count)

		// Verify transaction stats
		txStats := allStats[storage.StateTypeTransaction]
		assert.Equal(t, "transaction", txStats.Type)
		assert.Equal(t, 1, txStats.Count)
	})

	t.Run("PruneState", func(t *testing.T) {
		db := NewMockDB()

		// Add test data with various heights
		for i := uint64(1); i <= 10; i++ {
			key := fmt.Sprintf("acc:user%d", i)
			db.Set([]byte(key), encodeHeightData(i*100, []byte(fmt.Sprintf("account%d", i))))
		}

		// Create pruning manager
		config := &storage.PruningConfig{
			Policy:              storage.FullPruning,
			BlocksToKeep:        5,
			MaxPruningBatchSize: 3,
		}

		manager := storage.NewStatePruningManager(db, config, logger)
		manager.UpdateBlockHeight(1000)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Prune up to height 500
		err := impl.PruneState(500)
		require.NoError(t, err)

		// Check that entries with height <= 500 were pruned
		for i := uint64(1); i <= 5; i++ {
			key := fmt.Sprintf("acc:user%d", i)
			_, err := db.Get([]byte(key))
			assert.Error(t, err, "Entry with height %d should be pruned", i*100)
		}

		// Check that entries with height > 500 remain
		for i := uint64(6); i <= 10; i++ {
			key := fmt.Sprintf("acc:user%d", i)
			_, err := db.Get([]byte(key))
			assert.NoError(t, err, "Entry with height %d should remain", i*100)
		}
	})

	t.Run("State Type Prefixes", func(t *testing.T) {
		db := NewMockDB()

		// Add data with different prefixes
		db.Set([]byte("acc:test"), []byte("account"))
		db.Set([]byte("tx:test"), []byte("transaction"))
		db.Set([]byte("blk:test"), []byte("block"))
		db.Set([]byte("con:test"), []byte("contract"))
		db.Set([]byte("cst:test"), []byte("contract_storage"))

		config := &storage.PruningConfig{Policy: storage.ArchivePruning, BlocksToKeep: 1000}
		manager := storage.NewStatePruningManager(db, config, logger)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Test each state type
		testCases := []struct {
			stateType storage.StateType
			expected  string
		}{
			{storage.StateTypeAccount, "account"},
			{storage.StateTypeTransaction, "transaction"},
			{storage.StateTypeBlock, "block"},
			{storage.StateTypeContract, "contract"},
			{storage.StateTypeContractStorage, "contract_storage"},
		}

		for _, tc := range testCases {
			stats, err := impl.GetStateTypeStats(tc.stateType)
			require.NoError(t, err)
			assert.Equal(t, string(tc.stateType), stats.Type)
			assert.Equal(t, 1, stats.Count)
		}
	})
}

// Helper function to encode height data for testing
func encodeHeightData(height uint64, data []byte) []byte {
	result := make([]byte, 8+len(data))
	// Encode height as big-endian
	for i := 0; i < 8; i++ {
		result[i] = byte(height >> (8 * (7 - i)))
	}
	copy(result[8:], data)
	return result
}

func TestStatePruningEdgeCases(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	t.Run("Empty Database", func(t *testing.T) {
		db := NewMockDB()
		config := &storage.PruningConfig{Policy: storage.ArchivePruning, BlocksToKeep: 1000}

		manager := storage.NewStatePruningManager(db, config, logger)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Get stats on empty DB
		stats, err := impl.GetStateTypeStats(storage.StateTypeAccount)
		require.NoError(t, err)
		assert.Equal(t, 0, stats.Count)
		assert.Equal(t, uint64(0), stats.MinHeight)
		assert.Equal(t, float64(0), stats.AvgSize)
	})

	t.Run("Invalid State Type", func(t *testing.T) {
		db := NewMockDB()
		config := &storage.PruningConfig{Policy: storage.ArchivePruning, BlocksToKeep: 1000}

		manager := storage.NewStatePruningManager(db, config, logger)
		impl := storage.NewStatePruningImpl(manager, logger)

		// Try invalid state type
		stats, err := impl.GetStateTypeStats("invalid")
		assert.Error(t, err)
		assert.Nil(t, stats)
	})

	t.Run("Retention Policy", func(t *testing.T) {
		db := NewMockDB()

		// Add data
		for i := uint64(1); i <= 100; i++ {
			key := fmt.Sprintf("acc:user%d", i)
			db.Set([]byte(key), encodeHeightData(i, []byte(fmt.Sprintf("data%d", i))))
		}

		// Set retention policy
		config := &storage.PruningConfig{
			Policy:       storage.FullPruning,
			BlocksToKeep: 50, // Keep last 50 blocks
			RetentionPolicy: map[string]uint64{
				"accounts": 30, // Keep last 30 for accounts
			},
		}

		manager := storage.NewStatePruningManager(db, config, logger)
		manager.UpdateBlockHeight(100)

		impl := storage.NewStatePruningImpl(manager, logger)

		// Prune
		err := impl.PruneState(100)
		require.NoError(t, err)

		// Check that only last 30 accounts remain
		stats, err := impl.GetStateTypeStats(storage.StateTypeAccount)
		require.NoError(t, err)
		assert.LessOrEqual(t, stats.Count, 30)
		assert.GreaterOrEqual(t, stats.MinHeight, uint64(71))
	})
}

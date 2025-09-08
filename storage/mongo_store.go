package storage

import (
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// MongoStore is now a thin wrapper around MongoAdapter for backward compatibility
type MongoStore struct {
	*MongoAdapter
}

// NewMongoStore creates a new MongoStore instance using MongoAdapter internally
func NewMongoStore(connectionString, dbName string, maxPoolSize uint64, retries int, retryDelay time.Duration) (*MongoStore, error) {
	// Create a logger if not provided
	logger := logrus.New()

	// Use a reasonable cache size
	cacheSize := 10000

	// Create MongoAdapter with retry logic
	adapter, err := NewMongoAdapter(connectionString, dbName, logger, cacheSize)
	if err != nil {
		return nil, err
	}

	return &MongoStore{
		MongoAdapter: adapter,
	}, nil
}

// SaveBlock stores a block document (wrapper for compatibility)
func (ms *MongoStore) SaveBlock(block *common.Block) error {
	return ms.MongoAdapter.SaveBlock(block)
}

// GetBlock retrieves a block by its number (wrapper for compatibility)
func (ms *MongoStore) GetBlock(blockNumber uint64) (*common.Block, error) {
	return ms.MongoAdapter.GetBlock(blockNumber)
}

// SaveReceipt stores a transaction receipt (wrapper for compatibility)
func (ms *MongoStore) SaveReceipt(receipt *Receipt) error {
	return ms.MongoAdapter.SaveReceipt(receipt)
}

// GetReceipt retrieves a transaction receipt by transaction ID (wrapper for compatibility)
func (ms *MongoStore) GetReceipt(txID string) (*Receipt, error) {
	return ms.MongoAdapter.GetReceipt(txID)
}

// Close closes the MongoDB connection (wrapper for compatibility)
func (ms *MongoStore) Close() error {
	return ms.MongoAdapter.Close()
}

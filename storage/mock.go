package storage

import (
	"sync"

	"diamante/common"
)

// MockMongoLedger is a mock implementation of the MongoLedger for testing purposes
type MockMongoLedger struct {
	accounts     map[string]*common.Account
	transactions map[string]common.Transaction
	blocks       map[int]common.Block
	mu           sync.RWMutex
}

// NewMockMongoLedger creates a new MockMongoLedger
func NewMockMongoLedger() *MongoLedger {
	return &MongoLedger{
		// Initialize with empty maps
	}
}

// MockMongoStore is a mock implementation of the MongoStore for testing purposes
type MockMongoStore struct {
	blocks map[uint64]*common.Block
	mu     sync.RWMutex
}

// NewMockMongoStore creates a new MockMongoStore
func NewMockMongoStore() *MongoStore {
	return &MongoStore{
		// Initialize with empty maps
	}
}

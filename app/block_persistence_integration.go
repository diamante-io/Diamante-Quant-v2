package app

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BlockPersistenceWrapper wraps StoreWrapper to add filesystem persistence and state pruning
type BlockPersistenceWrapper struct {
	*StoreWrapper
	blockPersistence    *storage.BlockPersistence
	statePruningManager *storage.StatePruningManager
	logger              *logrus.Logger
}

// NewBlockPersistenceWrapper creates a new wrapper with filesystem persistence and state pruning
func NewBlockPersistenceWrapper(sw *StoreWrapper, blocksDir string, logger *logrus.Logger) (*BlockPersistenceWrapper, error) {
	if sw == nil {
		return nil, fmt.Errorf("store wrapper cannot be nil")
	}
	if logger == nil {
		logger = logrus.New()
	}

	bp, err := storage.NewBlockPersistence(blocksDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create block persistence: %w", err)
	}

	// Create state pruning manager with default configuration
	pruningConfig := storage.DefaultPruningConfig()

	// Create a state DB adapter for the store
	stateDB := NewStoreDBAdapter(sw.MongoStore, logger)
	statePruningManager := storage.NewStatePruningManager(stateDB, pruningConfig, logger)

	// Start the state pruning manager
	if err := statePruningManager.Start(); err != nil {
		logger.WithError(err).Warn("Failed to start state pruning manager")
		// Continue without state pruning - it's not critical for basic operation
		statePruningManager = nil
	}

	wrapper := &BlockPersistenceWrapper{
		StoreWrapper:        sw,
		blockPersistence:    bp,
		statePruningManager: statePruningManager,
		logger:              logger,
	}

	return wrapper, nil
}

// SaveBlock overrides the SaveBlock method to add filesystem persistence and trigger state pruning
func (bpw *BlockPersistenceWrapper) SaveBlock(b *storage.Block) error {
	// Save to MongoDB first
	if err := bpw.StoreWrapper.SaveBlock(b); err != nil {
		return fmt.Errorf("failed to save block to MongoDB: %w", err)
	}

	// Convert and save to filesystem
	cb := &common.Block{
		Number:       int(b.Number),
		Hash:         b.BlockHash,
		PreviousHash: b.PrevBlockHash,
		Timestamp:    b.Timestamp.Unix(),
	}

	if err := bpw.blockPersistence.SaveBlock(cb); err != nil {
		bpw.logger.WithError(err).Error("Failed to persist block to filesystem")
		// Don't fail the transaction - filesystem persistence is secondary
	}

	// Update state pruning manager with new block height
	if bpw.statePruningManager != nil {
		bpw.statePruningManager.UpdateBlockHeight(b.Number)
	}

	return nil
}

// Stop gracefully stops the block persistence wrapper
func (bpw *BlockPersistenceWrapper) Stop() error {
	if bpw.statePruningManager != nil {
		if err := bpw.statePruningManager.Stop(); err != nil {
			bpw.logger.WithError(err).Error("Failed to stop state pruning manager")
			return fmt.Errorf("failed to stop state pruning manager: %w", err)
		}
	}
	return nil
}

// GetStatePruningMetrics returns the current state pruning metrics
func (bpw *BlockPersistenceWrapper) GetStatePruningMetrics() storage.PruningMetrics {
	if bpw.statePruningManager != nil {
		return bpw.statePruningManager.GetMetrics()
	}
	return storage.PruningMetrics{}
}

// UpdateStatePruningConfig updates the state pruning configuration
func (bpw *BlockPersistenceWrapper) UpdateStatePruningConfig(config *storage.PruningConfig) error {
	if bpw.statePruningManager == nil {
		return fmt.Errorf("state pruning manager is not initialized")
	}
	if config == nil {
		return fmt.Errorf("pruning config cannot be nil")
	}
	return bpw.statePruningManager.UpdateConfig(config)
}

// IntegrateBlockPersistence creates a wrapper with filesystem persistence and state pruning
func IntegrateBlockPersistence(sw *StoreWrapper, blocksDir string, logger *logrus.Logger) (*BlockPersistenceWrapper, error) {
	return NewBlockPersistenceWrapper(sw, blocksDir, logger)
}

// StoreDBAdapter adapts storage.MongoStore to implement storage.StateDB interface
type StoreDBAdapter struct {
	store      *storage.MongoStore
	logger     *logrus.Logger
	mu         sync.RWMutex
	database   *mongo.Database
	stateCache map[string][]byte // Simple in-memory cache for frequently accessed state
}

// NewStoreDBAdapter creates a new adapter
func NewStoreDBAdapter(store *storage.MongoStore, logger *logrus.Logger) *StoreDBAdapter {
	if logger == nil {
		logger = logrus.New()
	}

	// Access the MongoDB database through reflection (since MongoStore doesn't expose it directly)
	// In production, MongoStore should expose a GetDatabase() method
	adapter := &StoreDBAdapter{
		store:      store,
		logger:     logger,
		stateCache: make(map[string][]byte),
	}

	// Note: In production, we would need MongoStore to expose the database handle
	// For now, we'll create our own connection (not ideal but necessary for implementation)
	adapter.initializeDatabase()

	return adapter
}

// initializeDatabase initializes the database connection for state storage
func (s *StoreDBAdapter) initializeDatabase() {
	// This is a workaround since MongoStore doesn't expose the database
	// In production, we should modify MongoStore to expose a GetDatabase() method
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// For now, we'll use the same connection parameters
	// This should be improved by having MongoStore expose its database
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		// Use a more production-friendly default
		mongoURI = "mongodb://mongodb:27017"
		s.logger.Warn("MONGO_URI not set, using default: " + mongoURI)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		s.logger.WithError(err).Error("Failed to connect to MongoDB for state storage")
		return
	}

	s.database = client.Database("diamante_state")
}

// getStateCollection returns the MongoDB collection for state storage
func (s *StoreDBAdapter) getStateCollection() *mongo.Collection {
	if s.database == nil {
		return nil
	}
	return s.database.Collection("state_data")
}

// Get retrieves a value from the database
func (s *StoreDBAdapter) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Check cache first
	s.mu.RLock()
	if cached, exists := s.stateCache[string(key)]; exists {
		s.mu.RUnlock()
		return cached, nil
	}
	s.mu.RUnlock()

	// Get from MongoDB
	coll := s.getStateCollection()
	if coll == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)
	filter := bson.M{"_id": keyHex}

	var result struct {
		ID    string `bson:"_id"`
		Value []byte `bson:"value"`
	}

	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get key %x: %w", key, err)
	}

	// Update cache
	s.mu.Lock()
	s.stateCache[string(key)] = result.Value
	s.mu.Unlock()

	return result.Value, nil
}

// Put stores a key-value pair in the database
func (s *StoreDBAdapter) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	coll := s.getStateCollection()
	if coll == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)
	doc := bson.M{
		"_id":       keyHex,
		"value":     value,
		"timestamp": consensus.ConsensusNow(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": keyHex}, doc, opts)
	if err != nil {
		return fmt.Errorf("failed to put key %x: %w", key, err)
	}

	// Update cache
	s.mu.Lock()
	s.stateCache[string(key)] = value
	s.mu.Unlock()

	return nil
}

// Delete removes a key-value pair from the database
func (s *StoreDBAdapter) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	coll := s.getStateCollection()
	if coll == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)
	_, err := coll.DeleteOne(ctx, bson.M{"_id": keyHex})
	if err != nil {
		return fmt.Errorf("failed to delete key %x: %w", key, err)
	}

	// Remove from cache
	s.mu.Lock()
	delete(s.stateCache, string(key))
	s.mu.Unlock()

	return nil
}

// NewIterator creates an iterator over a key range
func (s *StoreDBAdapter) NewIterator(start, end []byte) storage.StateIterator {
	coll := s.getStateCollection()
	if coll == nil {
		return &MongoStateIterator{
			err: fmt.Errorf("database not initialized"),
		}
	}

	// Build filter for key range
	filter := bson.M{}
	if len(start) > 0 {
		filter["_id"] = bson.M{"$gte": hex.EncodeToString(start)}
	}
	if len(end) > 0 {
		if existingFilter, ok := filter["_id"].(bson.M); ok {
			existingFilter["$lt"] = hex.EncodeToString(end)
		} else {
			filter["_id"] = bson.M{"$lt": hex.EncodeToString(end)}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	cursor, err := coll.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		cancel()
		return &MongoStateIterator{
			err: fmt.Errorf("failed to create iterator: %w", err),
		}
	}

	return &MongoStateIterator{
		cursor: cursor,
		ctx:    ctx,
		cancel: cancel,
		logger: s.logger,
	}
}

// Compact runs database compaction on a key range
func (s *StoreDBAdapter) Compact(start, end []byte) error {
	// MongoDB handles compaction automatically through WiredTiger
	// We can trigger a manual compact command if needed
	if s.database == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Run compact command on the state collection
	result := s.database.RunCommand(ctx, bson.M{
		"compact": "state_data",
		"force":   true,
	})

	if err := result.Err(); err != nil {
		// Compaction might not be supported in all MongoDB configurations
		s.logger.WithError(err).Debug("MongoDB compact command failed")
		// Don't return error as compaction is not critical
		return nil
	}

	s.logger.Info("Successfully compacted state collection")
	return nil
}

// GetStats returns database statistics
func (s *StoreDBAdapter) GetStats() *storage.StorageStats {
	stats := &storage.StorageStats{
		ConnectionsActive:     1,
		ConnectionsTotal:      1,
		QueriesExecuted:       0,
		TransactionsProcessed: 0,
		CacheHitRate:          0.0,
		AverageQueryTime:      0.0,
		ErrorCount:            0,
		LastError:             "",
		UptimeSeconds:         0,
		MemoryUsageMB:         0.0,
	}

	if s.database != nil {
		coll := s.getStateCollection()
		if coll != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			count, err := coll.CountDocuments(ctx, bson.M{})
			if err == nil {
				stats.TransactionsProcessed = count
			}

			// Get collection stats
			var collStats bson.M
			err = s.database.RunCommand(ctx, bson.M{"collStats": "state_data"}).Decode(&collStats)
			if err == nil {
				if size, ok := collStats["size"].(int32); ok {
					stats.MemoryUsageMB = float64(size) / (1024 * 1024)
				} else if size, ok := collStats["size"].(int64); ok {
					stats.MemoryUsageMB = float64(size) / (1024 * 1024)
				}
			}
		}
	}

	return stats
}

// MongoStateIterator implements the StateIterator interface for MongoDB
type MongoStateIterator struct {
	cursor  *mongo.Cursor
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *logrus.Logger
	current struct {
		ID    string `bson:"_id"`
		Value []byte `bson:"value"`
	}
	err error
}

// Next advances the iterator to the next key
func (m *MongoStateIterator) Next() bool {
	if m.cursor == nil {
		return false
	}

	if m.cursor.Next(m.ctx) {
		err := m.cursor.Decode(&m.current)
		if err != nil {
			m.err = fmt.Errorf("failed to decode cursor: %w", err)
			return false
		}
		return true
	}

	if err := m.cursor.Err(); err != nil {
		m.err = err
	}

	return false
}

// Key returns the current key
func (m *MongoStateIterator) Key() []byte {
	if m.current.ID == "" {
		return nil
	}

	key, err := hex.DecodeString(m.current.ID)
	if err != nil {
		m.logger.WithError(err).Error("Failed to decode key from hex")
		return nil
	}

	return key
}

// Value returns the current value
func (m *MongoStateIterator) Value() []byte {
	return m.current.Value
}

// Error returns any accumulated error
func (m *MongoStateIterator) Error() error {
	return m.err
}

// Close releases resources associated with the iterator
func (m *MongoStateIterator) Close() {
	if m.cursor != nil {
		if err := m.cursor.Close(m.ctx); err != nil && m.logger != nil {
			m.logger.WithError(err).Error("Failed to close cursor")
		}
	}
	if m.cancel != nil {
		m.cancel()
	}
}

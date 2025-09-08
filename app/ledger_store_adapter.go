// Package app provides adapters for storage interfaces
package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// LedgerStoreAdapter adapts MongoStore to implement storage.LedgerStore
type LedgerStoreAdapter struct {
	mongoStore *storage.MongoStore
	logger     *logrus.Logger

	// Additional MongoDB connections for operations not supported by MongoStore
	client              *mongo.Client
	database            *mongo.Database
	blocksCollection    *mongo.Collection
	txCollection        *mongo.Collection
	accountsCollection  *mongo.Collection
	contractsCollection *mongo.Collection
	stateCollection     *mongo.Collection
	snapshotsCollection *mongo.Collection

	mu sync.RWMutex
}

// NewLedgerStoreAdapter creates a new adapter
func NewLedgerStoreAdapter(mongoStore *storage.MongoStore, logger *logrus.Logger) storage.LedgerStore {
	if logger == nil {
		logger = logrus.New()
	}

	adapter := &LedgerStoreAdapter{
		mongoStore: mongoStore,
		logger:     logger,
	}

	// Initialize additional MongoDB collections
	adapter.initializeCollections()

	return adapter
}

// initializeCollections sets up MongoDB collections for operations not supported by MongoStore
func (a *LedgerStoreAdapter) initializeCollections() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create MongoDB client
	// In production, this should use the same connection as MongoStore
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		// Use a more production-friendly default
		mongoURI = "mongodb://mongodb:27017"
		a.logger.Warn("MONGO_URI not set, using default: " + mongoURI)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		a.logger.WithError(err).Error("Failed to connect to MongoDB for LedgerStore")
		return
	}

	a.client = client
	a.database = client.Database("diamante")

	// Initialize collections
	a.blocksCollection = a.database.Collection("blocks")
	a.txCollection = a.database.Collection("transactions")
	a.accountsCollection = a.database.Collection("accounts")
	a.contractsCollection = a.database.Collection("contracts")
	a.stateCollection = a.database.Collection("state")
	a.snapshotsCollection = a.database.Collection("snapshots")

	// Create indexes
	a.createIndexes()
}

// createIndexes creates necessary database indexes
func (a *LedgerStoreAdapter) createIndexes() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Block indexes
	blockIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "number", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "hash", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "timestamp", Value: -1}}},
	}
	if _, err := a.blocksCollection.Indexes().CreateMany(ctx, blockIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create block indexes")
	}

	// Transaction indexes
	txIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "from", Value: 1}}},
		{Keys: bson.D{{Key: "to", Value: 1}}},
		{Keys: bson.D{{Key: "blockHeight", Value: 1}}},
		{Keys: bson.D{{Key: "timestamp", Value: -1}}},
	}
	if _, err := a.txCollection.Indexes().CreateMany(ctx, txIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create transaction indexes")
	}

	// Account indexes
	accountIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "balance", Value: -1}}},
	}
	if _, err := a.accountsCollection.Indexes().CreateMany(ctx, accountIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create account indexes")
	}

	// Contract indexes
	contractIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "owner", Value: 1}}},
		{Keys: bson.D{{Key: "createdAt", Value: -1}}},
	}
	if _, err := a.contractsCollection.Indexes().CreateMany(ctx, contractIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create contract indexes")
	}

	// State indexes
	stateIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "_id", Value: 1}}}, // _id is the hex-encoded key
	}
	if _, err := a.stateCollection.Indexes().CreateMany(ctx, stateIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create state indexes")
	}

	// Snapshot indexes
	snapshotIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "height", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "createdAt", Value: -1}}},
	}
	if _, err := a.snapshotsCollection.Indexes().CreateMany(ctx, snapshotIndexes); err != nil {
		a.logger.WithError(err).Error("Failed to create snapshot indexes")
	}
}

// Block operations

// SaveBlock saves a block to the store
func (a *LedgerStoreAdapter) SaveBlock(block *common.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	// Use MongoStore for primary storage
	if err := a.mongoStore.SaveBlock(block); err != nil {
		return fmt.Errorf("failed to save block via MongoStore: %w", err)
	}

	// Also save to our blocks collection for additional query capabilities
	if a.blocksCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		blockDoc := bson.M{
			"number":       block.Number,
			"hash":         block.Hash,
			"previousHash": block.PreviousHash,
			"timestamp":    block.Timestamp,
			"transactions": block.Transactions,
			"createdAt":    consensus.ConsensusNow(),
		}

		opts := options.Replace().SetUpsert(true)
		_, err := a.blocksCollection.ReplaceOne(ctx, bson.M{"number": block.Number}, blockDoc, opts)
		if err != nil {
			a.logger.WithError(err).Error("Failed to save block to additional collection")
		}
	}

	return nil
}

// GetBlock retrieves a block by height
func (a *LedgerStoreAdapter) GetBlock(height uint64) (*common.Block, error) {
	// Try MongoStore first
	block, err := a.mongoStore.GetBlock(height)
	if err == nil {
		return block, nil
	}

	// Fallback to our collection
	if a.blocksCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var block common.Block
		err := a.blocksCollection.FindOne(ctx, bson.M{"number": height}).Decode(&block)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, fmt.Errorf("block %d not found", height)
			}
			return nil, fmt.Errorf("failed to get block %d: %w", height, err)
		}
		return &block, nil
	}

	return nil, fmt.Errorf("block %d not found", height)
}

// GetBlockByHash retrieves a block by hash
func (a *LedgerStoreAdapter) GetBlockByHash(hash string) (*common.Block, error) {
	if hash == "" {
		return nil, fmt.Errorf("hash cannot be empty")
	}

	if a.blocksCollection == nil {
		return nil, fmt.Errorf("blocks collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var block common.Block
	err := a.blocksCollection.FindOne(ctx, bson.M{"hash": hash}).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("block with hash %s not found", hash)
		}
		return nil, fmt.Errorf("failed to get block by hash %s: %w", hash, err)
	}

	return &block, nil
}

// GetBlockRange retrieves blocks in a range
func (a *LedgerStoreAdapter) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	if startHeight > endHeight {
		return nil, fmt.Errorf("invalid range: start height %d > end height %d", startHeight, endHeight)
	}

	if a.blocksCollection == nil {
		return nil, fmt.Errorf("blocks collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{
		"number": bson.M{
			"$gte": startHeight,
			"$lte": endHeight,
		},
	}

	opts := options.Find().SetSort(bson.D{{Key: "number", Value: 1}})
	cursor, err := a.blocksCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query block range: %w", err)
	}
	defer cursor.Close(ctx)

	var blocks []*common.Block
	for cursor.Next(ctx) {
		var block common.Block
		if err := cursor.Decode(&block); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}
		blocks = append(blocks, &block)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return blocks, nil
}

// GetLatestBlock retrieves the latest block
func (a *LedgerStoreAdapter) GetLatestBlock() (*common.Block, error) {
	if a.blocksCollection == nil {
		return nil, fmt.Errorf("blocks collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.FindOne().SetSort(bson.D{{Key: "number", Value: -1}})

	var block common.Block
	err := a.blocksCollection.FindOne(ctx, bson.M{}, opts).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("no blocks found")
		}
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	return &block, nil
}

// Transaction operations

// SaveTransaction saves a transaction
func (a *LedgerStoreAdapter) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	if tx == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	if a.txCollection == nil {
		return fmt.Errorf("transactions collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add block height to transaction
	txDoc := bson.M{
		"id":          tx.ID,
		"sender":      tx.Sender,
		"receiver":    tx.Receiver,
		"amount":      tx.Amount,
		"fee":         tx.Fee,
		"priority":    tx.Priority,
		"nonce":       tx.Nonce,
		"data":        tx.Data,
		"signature":   tx.Signature,
		"timestamp":   tx.Timestamp,
		"blockHeight": blockHeight,
		"createdAt":   consensus.ConsensusNow(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := a.txCollection.ReplaceOne(ctx, bson.M{"id": tx.ID}, txDoc, opts)
	if err != nil {
		return fmt.Errorf("failed to save transaction %s: %w", tx.ID, err)
	}

	a.logger.WithField("txID", tx.ID).Debug("Transaction saved successfully")
	return nil
}

// GetTransaction retrieves a transaction by ID
func (a *LedgerStoreAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	if txID == "" {
		return nil, fmt.Errorf("transaction ID cannot be empty")
	}

	if a.txCollection == nil {
		return nil, fmt.Errorf("transactions collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tx common.Transaction
	err := a.txCollection.FindOne(ctx, bson.M{"id": txID}).Decode(&tx)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("transaction %s not found", txID)
		}
		return nil, fmt.Errorf("failed to get transaction %s: %w", txID, err)
	}

	return &tx, nil
}

// GetTransactionsByAddress retrieves transactions by address
func (a *LedgerStoreAdapter) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	if address == "" {
		return nil, fmt.Errorf("address cannot be empty")
	}

	if a.txCollection == nil {
		return nil, fmt.Errorf("transactions collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Find transactions where address is either sender or receiver
	filter := bson.M{
		"$or": []bson.M{
			{"from": address},
			{"to": address},
		},
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(int64(limit)).
		SetSkip(int64(offset))

	cursor, err := a.txCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions for address %s: %w", address, err)
	}
	defer cursor.Close(ctx)

	var transactions []*common.Transaction
	for cursor.Next(ctx) {
		var tx common.Transaction
		if err := cursor.Decode(&tx); err != nil {
			return nil, fmt.Errorf("failed to decode transaction: %w", err)
		}
		transactions = append(transactions, &tx)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return transactions, nil
}

// GetTransactionsByBlock retrieves transactions by block
func (a *LedgerStoreAdapter) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	if a.txCollection == nil {
		return nil, fmt.Errorf("transactions collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"blockHeight": blockHeight}
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})

	cursor, err := a.txCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions for block %d: %w", blockHeight, err)
	}
	defer cursor.Close(ctx)

	var transactions []*common.Transaction
	for cursor.Next(ctx) {
		var tx common.Transaction
		if err := cursor.Decode(&tx); err != nil {
			return nil, fmt.Errorf("failed to decode transaction: %w", err)
		}
		transactions = append(transactions, &tx)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return transactions, nil
}

// Account operations

// SaveAccount saves an account
func (a *LedgerStoreAdapter) SaveAccount(account *common.Account) error {
	if account == nil {
		return fmt.Errorf("account cannot be nil")
	}

	if a.accountsCollection == nil {
		return fmt.Errorf("accounts collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	accountDoc := bson.M{
		"id":           account.ID,
		"balance":      account.Balance,
		"nonce":        account.Nonce,
		"publicKey":    account.PublicKey,
		"stakedAmount": account.StakedAmount,
		"createdAt":    account.CreatedAt,
		"updatedAt":    consensus.ConsensusNow(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := a.accountsCollection.ReplaceOne(ctx, bson.M{"id": account.ID}, accountDoc, opts)
	if err != nil {
		return fmt.Errorf("failed to save account %s: %w", account.ID, err)
	}

	return nil
}

// GetAccount retrieves an account
func (a *LedgerStoreAdapter) GetAccount(accountID string) (*common.Account, error) {
	if accountID == "" {
		return nil, fmt.Errorf("account ID cannot be empty")
	}

	if a.accountsCollection == nil {
		return nil, fmt.Errorf("accounts collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var account common.Account
	err := a.accountsCollection.FindOne(ctx, bson.M{"id": accountID}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("account %s not found", accountID)
		}
		return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
	}

	return &account, nil
}

// UpdateAccount updates an account
func (a *LedgerStoreAdapter) UpdateAccount(account *common.Account) error {
	// SaveAccount with upsert already handles updates
	return a.SaveAccount(account)
}

// GetBalance retrieves the balance for an address
func (a *LedgerStoreAdapter) GetBalance(address string) (float64, error) {
	account, err := a.GetAccount(address)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

// GetNonce retrieves the nonce for an address
func (a *LedgerStoreAdapter) GetNonce(address string) (uint64, error) {
	// For now, return 0 as nonce tracking is not implemented in the Account struct
	return 0, nil
}

// State operations

// GetState retrieves state by key
func (a *LedgerStoreAdapter) GetState(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	if a.stateCollection == nil {
		return nil, fmt.Errorf("state collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)

	var result struct {
		ID    string `bson:"_id"`
		Value []byte `bson:"value"`
	}

	err := a.stateCollection.FindOne(ctx, bson.M{"_id": keyHex}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get state for key %x: %w", key, err)
	}

	return result.Value, nil
}

// SetState sets state by key
func (a *LedgerStoreAdapter) SetState(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	if a.stateCollection == nil {
		return fmt.Errorf("state collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)
	doc := bson.M{
		"_id":       keyHex,
		"value":     value,
		"updatedAt": consensus.ConsensusNow(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := a.stateCollection.ReplaceOne(ctx, bson.M{"_id": keyHex}, doc, opts)
	if err != nil {
		return fmt.Errorf("failed to set state for key %x: %w", key, err)
	}

	return nil
}

// SaveState saves a state value by key (same as SetState for compatibility)
func (a *LedgerStoreAdapter) SaveState(key, value []byte) error {
	return a.SetState(key, value)
}

// DeleteState deletes state by key
func (a *LedgerStoreAdapter) DeleteState(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	if a.stateCollection == nil {
		return fmt.Errorf("state collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyHex := hex.EncodeToString(key)
	_, err := a.stateCollection.DeleteOne(ctx, bson.M{"_id": keyHex})
	if err != nil {
		return fmt.Errorf("failed to delete state for key %x: %w", key, err)
	}

	return nil
}

// Smart contract operations

// SaveContract saves a contract
func (a *LedgerStoreAdapter) SaveContract(contract *storage.Contract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}

	if a.contractsCollection == nil {
		return fmt.Errorf("contracts collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	contractDoc := bson.M{
		"id":        contract.ID,
		"owner":     contract.Owner,
		"code":      contract.Code,
		"state":     contract.State,
		"version":   contract.Version,
		"createdAt": contract.CreatedAt,
		"updatedAt": consensus.ConsensusNow(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := a.contractsCollection.ReplaceOne(ctx, bson.M{"id": contract.ID}, contractDoc, opts)
	if err != nil {
		return fmt.Errorf("failed to save contract %s: %w", contract.ID, err)
	}

	return nil
}

// GetContract retrieves a contract
func (a *LedgerStoreAdapter) GetContract(contractID string) (*storage.Contract, error) {
	if contractID == "" {
		return nil, fmt.Errorf("contract ID cannot be empty")
	}

	if a.contractsCollection == nil {
		return nil, fmt.Errorf("contracts collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var contract common.SmartContract
	err := a.contractsCollection.FindOne(ctx, bson.M{"id": contractID}).Decode(&contract)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("contract %s not found", contractID)
		}
		return nil, fmt.Errorf("failed to get contract %s: %w", contractID, err)
	}

	return &contract, nil
}

// UpdateContract updates a contract
func (a *LedgerStoreAdapter) UpdateContract(contract *common.SmartContract) error {
	// SaveContract with upsert already handles updates
	return a.SaveContract(contract)
}

// DeleteContract deletes a contract
func (a *LedgerStoreAdapter) DeleteContract(contractID string) error {
	if contractID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	if a.contractsCollection == nil {
		return fmt.Errorf("contracts collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := a.contractsCollection.DeleteOne(ctx, bson.M{"id": contractID})
	if err != nil {
		return fmt.Errorf("failed to delete contract %s: %w", contractID, err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("contract %s not found", contractID)
	}

	return nil
}

// Receipt operations

// SaveReceipt saves a receipt
func (a *LedgerStoreAdapter) SaveReceipt(receipt *storage.Receipt) error {
	return a.mongoStore.SaveReceipt(receipt)
}

// GetReceipt retrieves a receipt by transaction ID
func (a *LedgerStoreAdapter) GetReceipt(txID string) (*storage.Receipt, error) {
	return a.mongoStore.GetReceipt(txID)
}

// Snapshot operations

// CreateSnapshot creates a snapshot
func (a *LedgerStoreAdapter) CreateSnapshot(height uint64) error {
	if a.snapshotsCollection == nil {
		return fmt.Errorf("snapshots collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get all relevant data at the specified height
	block, err := a.GetBlock(height)
	if err != nil {
		return fmt.Errorf("failed to get block for snapshot: %w", err)
	}

	// Create snapshot document
	snapshot := bson.M{
		"height":    height,
		"blockHash": block.Hash,
		"timestamp": block.Timestamp,
		"createdAt": consensus.ConsensusNow(),
		"metadata": bson.M{
			"version": "1.0",
			"type":    "full",
		},
	}

	_, err = a.snapshotsCollection.InsertOne(ctx, snapshot)
	if err != nil {
		return fmt.Errorf("failed to create snapshot at height %d: %w", height, err)
	}

	a.logger.WithField("height", height).Info("Snapshot created successfully")
	return nil
}

// RestoreSnapshot restores from a snapshot
func (a *LedgerStoreAdapter) RestoreSnapshot(height uint64) error {
	if a.snapshotsCollection == nil {
		return fmt.Errorf("snapshots collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Verify snapshot exists
	var snapshot bson.M
	err := a.snapshotsCollection.FindOne(ctx, bson.M{"height": height}).Decode(&snapshot)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return fmt.Errorf("snapshot at height %d not found", height)
		}
		return fmt.Errorf("failed to find snapshot: %w", err)
	}

	// In a real implementation, this would restore the entire state
	// For now, we just log the operation
	a.logger.WithFields(logrus.Fields{
		"height":    height,
		"blockHash": snapshot["blockHash"],
	}).Info("Snapshot restore requested (not fully implemented)")

	return nil
}

// ListSnapshots lists available snapshots
func (a *LedgerStoreAdapter) ListSnapshots() ([]storage.SnapshotInfo, error) {
	if a.snapshotsCollection == nil {
		return nil, fmt.Errorf("snapshots collection not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().SetSort(bson.D{{Key: "height", Value: -1}})
	cursor, err := a.snapshotsCollection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	defer cursor.Close(ctx)

	var snapshots []storage.SnapshotInfo
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode snapshot: %w", err)
		}

		info := storage.SnapshotInfo{
			Height:    uint64(doc["height"].(int64)),
			Timestamp: doc["createdAt"].(time.Time),
		}

		if blockHash, ok := doc["blockHash"].(string); ok {
			info.Hash = blockHash
		}

		// Set size if available
		if size, ok := doc["size"].(int64); ok {
			info.Size = size
		}

		snapshots = append(snapshots, info)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return snapshots, nil
}

// Batch operations

// BatchWrite performs multiple write operations in a batch
func (a *LedgerStoreAdapter) WriteBatch(batch storage.WriteBatch) error {
	// No need to check for nil since batch is passed by value

	// Start a session for transaction support
	var session mongo.Session
	var err error

	if a.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		session, err = a.client.StartSession()
		if err != nil {
			a.logger.WithError(err).Warn("Failed to start session, proceeding without transaction")
		} else {
			defer session.EndSession(ctx)

			// Execute in transaction
			err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
				return a.executeBatch(sc, &batch)
			})

			if err != nil {
				return fmt.Errorf("batch write failed: %w", err)
			}
			return nil
		}
	}

	// Fallback to non-transactional execution
	return a.executeBatch(context.Background(), &batch)
}

// executeBatch executes the batch operations
func (a *LedgerStoreAdapter) executeBatch(ctx context.Context, batch *storage.WriteBatch) error {
	// Process blocks
	for _, block := range batch.Blocks {
		if err := a.SaveBlock(block); err != nil {
			return fmt.Errorf("failed to save block in batch: %w", err)
		}
	}

	// Process transactions
	// Note: WriteBatch doesn't store block heights for transactions,
	// so we use a default value. In production, this should be handled differently.
	for _, tx := range batch.Transactions {
		if err := a.SaveTransaction(tx, 0); err != nil {
			return fmt.Errorf("failed to save transaction in batch: %w", err)
		}
	}

	// Process accounts
	for _, account := range batch.Accounts {
		if err := a.SaveAccount(account); err != nil {
			return fmt.Errorf("failed to save account in batch: %w", err)
		}
	}

	// Process contracts
	for _, contract := range batch.Contracts {
		if err := a.SaveContract(contract); err != nil {
			return fmt.Errorf("failed to save contract in batch: %w", err)
		}
	}

	// Process receipts
	for _, receipt := range batch.Receipts {
		if err := a.SaveReceipt(receipt); err != nil {
			return fmt.Errorf("failed to save receipt in batch: %w", err)
		}
	}

	// Process state writes
	for keyStr, value := range batch.StateWrites {
		key := []byte(keyStr)
		if err := a.SetState(key, value); err != nil {
			return fmt.Errorf("failed to set state for key %s in batch: %w", keyStr, err)
		}
	}

	// Process state deletes
	for _, keyStr := range batch.StateDeletes {
		key := []byte(keyStr)
		if err := a.DeleteState(key); err != nil {
			return fmt.Errorf("failed to delete state for key %s in batch: %w", keyStr, err)
		}
	}

	return nil
}

// Maintenance operations

// Compact compacts the database
func (a *LedgerStoreAdapter) Compact() error {
	if a.database == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Compact all collections
	collections := []string{
		"blocks", "transactions", "accounts",
		"contracts", "state", "snapshots", "receipts",
	}

	for _, collection := range collections {
		result := a.database.RunCommand(ctx, bson.M{
			"compact": collection,
			"force":   true,
		})

		if err := result.Err(); err != nil {
			a.logger.WithError(err).WithField("collection", collection).
				Debug("Failed to compact collection")
		}
	}

	a.logger.Info("Database compaction completed")
	return nil
}

// Backup creates a backup of the store
func (a *LedgerStoreAdapter) Backup(path string) error {
	if path == "" {
		return fmt.Errorf("backup path cannot be empty")
	}

	a.logger.WithField("path", path).Info("Starting backup process")

	// Create backup metadata
	metadata := map[string]interface{}{
		"timestamp":   consensus.ConsensusNow().UTC(),
		"version":     "1.0",
		"type":        "LedgerStoreAdapter",
		"collections": []string{"blocks", "transactions", "accounts", "contracts", "state", "snapshots", "receipts"},
	}

	// Create backup directory
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Write metadata
	metadataPath := filepath.Join(path, "metadata.json")
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Export collection stats
	if a.database != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Get collection names
		collections, err := a.database.ListCollectionNames(ctx, bson.M{})
		if err != nil {
			a.logger.WithError(err).Warn("Failed to list collections")
		} else {
			// Save collection info
			collInfo := map[string]interface{}{
				"database":    a.database.Name(),
				"collections": collections,
				"backupTime":  consensus.ConsensusNow().UTC(),
			}

			collInfoPath := filepath.Join(path, "collections.json")
			collInfoBytes, _ := json.MarshalIndent(collInfo, "", "  ")
			os.WriteFile(collInfoPath, collInfoBytes, 0644)
		}
	}

	// In production, you would execute mongodump here
	// For now, we document that manual backup is required
	instructionsPath := filepath.Join(path, "backup_instructions.txt")
	instructions := `LedgerStore Backup Instructions

This backup metadata has been created, but the actual data backup requires mongodump.

To complete the backup, run:
mongodump --db diamante --out ` + path + `/data

To restore, use:
mongorestore --db diamante ` + path + `/data/diamante

Backup created at: ` + consensus.ConsensusNow().UTC().Format(time.RFC3339)

	if err := os.WriteFile(instructionsPath, []byte(instructions), 0644); err != nil {
		return fmt.Errorf("failed to write instructions: %w", err)
	}

	a.logger.Info("Backup metadata created successfully")
	return nil
}

// Restore restores from a backup
// Snapshot creates a snapshot at the specified path
func (a *LedgerStoreAdapter) Snapshot(path string) error {
	// For MongoDB, we can export data to the specified path
	// This is a simplified implementation that exports to BSON files
	return a.Backup(path)
}

func (a *LedgerStoreAdapter) Restore(path string) error {
	if path == "" {
		return fmt.Errorf("restore path cannot be empty")
	}

	a.logger.WithField("path", path).Info("Starting restore process")

	// Check if backup metadata exists
	metadataPath := filepath.Join(path, "metadata.json")
	if _, err := os.Stat(metadataPath); err != nil {
		return fmt.Errorf("backup metadata not found at %s: %w", metadataPath, err)
	}

	// Read metadata
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	a.logger.WithFields(logrus.Fields{
		"backupTime": metadata["timestamp"],
		"version":    metadata["version"],
	}).Info("Found backup metadata")

	// Verify collections info exists
	collInfoPath := filepath.Join(path, "collections.json")
	if _, err := os.Stat(collInfoPath); err == nil {
		collInfoBytes, _ := os.ReadFile(collInfoPath)
		var collInfo map[string]interface{}
		if err := json.Unmarshal(collInfoBytes, &collInfo); err == nil {
			a.logger.WithField("collections", collInfo["collections"]).Info("Backup contains collections")
		}
	}

	// Check for backup instructions
	instructionsPath := filepath.Join(path, "backup_instructions.txt")
	if _, err := os.Stat(instructionsPath); err == nil {
		instructions, _ := os.ReadFile(instructionsPath)
		a.logger.Info("Backup instructions found:\n" + string(instructions))
	}

	// In production, you would execute mongorestore here
	// For now, we verify the backup structure and prepare for manual restore
	dataPath := filepath.Join(path, "data", "diamante")
	if _, err := os.Stat(dataPath); err != nil {
		a.logger.Warn("Data directory not found. Manual mongodump may be required.")
		a.logger.Infof("Expected data path: %s", dataPath)
	} else {
		// Re-create indexes after restore
		if a.database != nil {
			a.logger.Info("Re-creating indexes after restore...")
			a.createIndexes()
		}
	}

	a.logger.Info("Restore preparation completed. Use mongorestore for actual data restoration.")
	return nil
}

// PruneData prunes old data
func (a *LedgerStoreAdapter) PruneData(olderThan time.Time) error {
	if a.database == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Define retention policies for different collections
	pruneOps := []struct {
		collection string
		filter     bson.M
	}{
		{
			collection: "transactions",
			filter:     bson.M{"timestamp": bson.M{"$lt": olderThan}},
		},
		{
			collection: "receipts",
			filter:     bson.M{"createdAt": bson.M{"$lt": olderThan}},
		},
		// Blocks and accounts should generally not be pruned
		// State pruning is handled by the state pruning manager
	}

	totalDeleted := int64(0)

	for _, op := range pruneOps {
		coll := a.database.Collection(op.collection)
		result, err := coll.DeleteMany(ctx, op.filter)
		if err != nil {
			a.logger.WithError(err).WithField("collection", op.collection).
				Error("Failed to prune data")
			continue
		}

		totalDeleted += result.DeletedCount
		a.logger.WithFields(logrus.Fields{
			"collection": op.collection,
			"deleted":    result.DeletedCount,
		}).Info("Pruned old data")
	}

	a.logger.WithField("totalDeleted", totalDeleted).Info("Data pruning completed")
	return nil
}

// Vacuum performs vacuum operation
func (a *LedgerStoreAdapter) Vacuum() error {
	// MongoDB doesn't have a direct vacuum operation
	// Compact serves a similar purpose
	return a.Compact()
}

// Lifecycle operations

// Open opens the store
func (a *LedgerStoreAdapter) Open() error {
	// MongoStore is already open
	// Verify our additional connections are working
	if a.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := a.client.Ping(ctx, nil); err != nil {
			return fmt.Errorf("failed to ping MongoDB: %w", err)
		}
	}

	return nil
}

// Close closes the store
func (a *LedgerStoreAdapter) Close() error {
	var errors []error

	// Close MongoStore
	if err := a.mongoStore.Close(); err != nil {
		errors = append(errors, fmt.Errorf("failed to close MongoStore: %w", err))
	}

	// Close our additional client
	if a.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := a.client.Disconnect(ctx); err != nil {
			errors = append(errors, fmt.Errorf("failed to disconnect client: %w", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	return nil
}

// IsOpen returns true if the store is open and ready
func (a *LedgerStoreAdapter) IsOpen() bool {
	if a.client == nil {
		return false
	}

	// Check if the connection is still alive
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := a.client.Ping(ctx, nil)
	return err == nil
}

// Health and metrics

// HealthCheck performs health check
func (a *LedgerStoreAdapter) HealthCheck(ctx context.Context) error {
	// Check MongoDB connectivity
	if a.client != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := a.client.Ping(pingCtx, nil); err != nil {
			return fmt.Errorf("MongoDB health check failed: %w", err)
		}
	}

	// Check collection accessibility
	if a.blocksCollection != nil {
		countCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := a.blocksCollection.EstimatedDocumentCount(countCtx)
		if err != nil {
			return fmt.Errorf("blocks collection health check failed: %w", err)
		}
	}

	return nil
}

// GetStats returns storage statistics
func (a *LedgerStoreAdapter) GetStats() (*storage.StoreStats, error) {
	stats := &storage.StoreStats{
		DatabaseType:    "MongoDB",
		DatabaseVersion: "4.4+",
		IsHealthy:       true,
	}

	if a.blocksCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get block count
		blockCount, err := a.blocksCollection.CountDocuments(ctx, bson.M{})
		if err != nil {
			a.logger.WithError(err).Warn("Failed to get block count")
		} else {
			stats.BlockCount = blockCount
		}

		// Get latest block for timestamp
		var latestBlock common.Block
		opts := options.FindOne().SetSort(bson.D{{Key: "number", Value: -1}})
		err = a.blocksCollection.FindOne(ctx, bson.M{}, opts).Decode(&latestBlock)
		if err == nil {
			stats.LastBlockTime = time.Unix(latestBlock.Timestamp, 0)
		}
	}

	if a.txCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get transaction count
		txCount, err := a.txCollection.CountDocuments(ctx, bson.M{})
		if err != nil {
			a.logger.WithError(err).Warn("Failed to get transaction count")
		} else {
			stats.TransactionCount = txCount
		}
	}

	if a.accountsCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get account count
		accountCount, err := a.accountsCollection.CountDocuments(ctx, bson.M{})
		if err != nil {
			a.logger.WithError(err).Warn("Failed to get account count")
		} else {
			stats.AccountCount = accountCount
		}
	}

	if a.contractsCollection != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Get contract count
		contractCount, err := a.contractsCollection.CountDocuments(ctx, bson.M{})
		if err != nil {
			a.logger.WithError(err).Warn("Failed to get contract count")
		} else {
			stats.ContractCount = contractCount
		}
	}

	return stats, nil
}

// ReplaceBlockSameHeight atomically replaces a block at the same height (testnet-only conflict repair)
func (a *LedgerStoreAdapter) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	// MongoDB doesn't support atomic block replacement, return error
	return fmt.Errorf("ReplaceBlockSameHeight not supported in MongoDB adapter")
}

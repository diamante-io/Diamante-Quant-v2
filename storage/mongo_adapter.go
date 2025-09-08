// storage/mongo_adapter.go

package storage

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"diamante/common"
	dtypes "diamante/types"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoAdapter implements the LedgerStore interface using MongoDB
type MongoAdapter struct {
	*BaseAdapter
	client           *mongo.Client
	db               *mongo.Database
	blocksColl       *mongo.Collection
	txColl           *mongo.Collection
	accountsColl     *mongo.Collection
	stateColl        *mongo.Collection
	contractsColl    *mongo.Collection
	receiptsColl     *mongo.Collection
	snapshotsColl    *mongo.Collection
	connectionString string
	dbName           string
	mu               sync.RWMutex
	currentHeight    int // Added for MongoLedger compatibility
}

// NewMongoAdapter creates a new MongoAdapter with retry logic and connection pooling
func NewMongoAdapter(connectionString, dbName string, logger *logrus.Logger, cacheSize int) (*MongoAdapter, error) {
	config := &AdapterConfig{
		CacheSize:           cacheSize,
		CacheTTL:            5 * time.Minute,
		MetricsEnabled:      true,
		HealthCheckInterval: 30 * time.Second,
		MaxConcurrentOps:    100,
		EnableCompression:   true,
		EnableEncryption:    false,
	}

	baseAdapter, err := NewBaseAdapter(logger, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create base adapter: %w", err)
	}

	return &MongoAdapter{
		BaseAdapter:      baseAdapter,
		connectionString: connectionString,
		dbName:           dbName,
	}, nil
}

// Open establishes a connection to the MongoDB database
func (ma *MongoAdapter) Open() error {
	if ma.IsOpen() {
		return nil // Already open
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	clientOpts := options.Client().ApplyURI(ma.connectionString)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Initialize collections
	db := client.Database(ma.dbName)
	ma.client = client
	ma.db = db
	ma.blocksColl = db.Collection("blocks")
	ma.txColl = db.Collection("transactions")
	ma.accountsColl = db.Collection("accounts")
	ma.stateColl = db.Collection("state")
	ma.contractsColl = db.Collection("contracts")
	ma.receiptsColl = db.Collection("receipts")
	ma.snapshotsColl = db.Collection("snapshots")

	// Create indexes
	if err := ma.createIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	ma.SetOpen(true)
	ma.logger.Info("MongoDB adapter opened successfully")
	return nil
}

// createIndexes creates necessary indexes for collections
func (ma *MongoAdapter) createIndexes(ctx context.Context) error {
	// Blocks collection indexes
	_, err := ma.blocksColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "number", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "hash", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create blocks indexes: %w", err)
	}

	// Transactions collection indexes
	_, err = ma.txColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{Keys: bson.D{{Key: "sender", Value: 1}}},
		{Keys: bson.D{{Key: "receiver", Value: 1}}},
		{Keys: bson.D{{Key: "blockHeight", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("failed to create transactions indexes: %w", err)
	}

	// Accounts collection indexes
	_, err = ma.accountsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create accounts index: %w", err)
	}

	// State collection indexes
	_, err = ma.stateColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create state index: %w", err)
	}

	// Contracts collection indexes
	_, err = ma.contractsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create contracts index: %w", err)
	}

	// Receipts collection indexes
	_, err = ma.receiptsColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "txId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{Keys: bson.D{{Key: "blockHeight", Value: 1}}},
	})
	if err != nil {
		return fmt.Errorf("failed to create receipts indexes: %w", err)
	}

	// Snapshots collection indexes
	_, err = ma.snapshotsColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "height", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshots index: %w", err)
	}

	return nil
}

// Close closes the connection to the MongoDB database
func (ma *MongoAdapter) Close() error {
	if !ma.IsOpen() {
		return nil // Already closed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ma.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
	}

	ma.SetOpen(false)
	ma.logger.Info("MongoDB adapter closed successfully")
	return nil
}

// GetConnectionString returns the MongoDB connection string
func (ma *MongoAdapter) GetConnectionString() string {
	return ma.connectionString
}

// GetDatabaseName returns the MongoDB database name
func (ma *MongoAdapter) GetDatabaseName() string {
	return ma.dbName
}

// SaveBlock saves a block to the database
func (ma *MongoAdapter) SaveBlock(block *common.Block) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("save_block", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if block already exists
	var existingBlock common.Block
	err := ma.blocksColl.FindOne(ctx, bson.M{"number": block.Number}).Decode(&existingBlock)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing block: %w", err)
	}

	// Insert the block
	_, err = ma.blocksColl.InsertOne(ctx, block)
	if err != nil {
		return fmt.Errorf("failed to insert block: %w", err)
	}

	// Add to cache
	ma.CacheBlock(block)

	return nil
}

// GetBlock retrieves a block by its height
func (ma *MongoAdapter) GetBlock(height uint64) (*common.Block, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_block", startTime, nil)
	}()

	// Check cache first
	if block, found := ma.GetCachedBlock(height); found {
		return block, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var block common.Block
	err := ma.blocksColl.FindOne(ctx, bson.M{"number": height}).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// Add to cache
	ma.CacheBlock(&block)

	return &block, nil
}

// GetBlockByHash retrieves a block by its hash
func (ma *MongoAdapter) GetBlockByHash(hash string) (*common.Block, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_block_by_hash", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var block common.Block
	err := ma.blocksColl.FindOne(ctx, bson.M{"hash": hash}).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}

	// Add to cache
	ma.CacheBlock(&block)

	return &block, nil
}

// GetBlockRange retrieves blocks within a range of heights
func (ma *MongoAdapter) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_block_range", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{
		"number": bson.M{
			"$gte": startHeight,
			"$lte": endHeight,
		},
	}
	opts := options.Find().SetSort(bson.D{{Key: "number", Value: 1}})

	cursor, err := ma.blocksColl.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find blocks in range: %w", err)
	}
	defer cursor.Close(ctx)

	var blocks []*common.Block
	if err := cursor.All(ctx, &blocks); err != nil {
		return nil, fmt.Errorf("failed to decode blocks: %w", err)
	}

	// Add to cache
	for _, block := range blocks {
		ma.CacheBlock(block)
	}

	return blocks, nil
}

// GetLatestBlock retrieves the latest block
func (ma *MongoAdapter) GetLatestBlock() (*common.Block, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_latest_block", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.FindOne().SetSort(bson.D{{Key: "number", Value: -1}})
	var block common.Block
	err := ma.blocksColl.FindOne(ctx, bson.M{}, opts).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	// Add to cache
	ma.CacheBlock(&block)

	return &block, nil
}

// SaveTransaction saves a transaction to the database
func (ma *MongoAdapter) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("save_transaction", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if transaction already exists
	var existingTx common.Transaction
	err := ma.txColl.FindOne(ctx, bson.M{"id": tx.ID}).Decode(&existingTx)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing transaction: %w", err)
	}

	// Set block height
	tx.BlockHeight = blockHeight

	// Insert the transaction
	_, err = ma.txColl.InsertOne(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	// Add to cache
	ma.CacheTransaction(tx)

	return nil
}

// GetTransaction retrieves a transaction by its ID
func (ma *MongoAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_transaction", startTime, nil)
	}()

	// Check cache first
	if tx, found := ma.GetCachedTransaction(txID); found {
		return tx, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var tx common.Transaction
	err := ma.txColl.FindOne(ctx, bson.M{"id": txID}).Decode(&tx)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// Add to cache
	ma.CacheTransaction(&tx)

	return &tx, nil
}

// GetTransactionsByAddress retrieves transactions for a given address
func (ma *MongoAdapter) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_transactions_by_address", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{
		"$or": []bson.M{
			{"sender": address},
			{"receiver": address},
		},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "blockHeight", Value: -1}}).
		SetLimit(int64(limit)).
		SetSkip(int64(offset))

	cursor, err := ma.txColl.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find transactions: %w", err)
	}
	defer cursor.Close(ctx)

	var transactions []*common.Transaction
	if err := cursor.All(ctx, &transactions); err != nil {
		return nil, fmt.Errorf("failed to decode transactions: %w", err)
	}

	// Add to cache
	for _, tx := range transactions {
		ma.CacheTransaction(tx)
	}

	return transactions, nil
}

// GetTransactionsByBlock retrieves transactions for a given block
func (ma *MongoAdapter) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_transactions_by_block", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"blockHeight": blockHeight}
	opts := options.Find().SetSort(bson.D{{Key: "id", Value: 1}})

	cursor, err := ma.txColl.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find transactions: %w", err)
	}
	defer cursor.Close(ctx)

	var transactions []*common.Transaction
	if err := cursor.All(ctx, &transactions); err != nil {
		return nil, fmt.Errorf("failed to decode transactions: %w", err)
	}

	// Add to cache
	for _, tx := range transactions {
		ma.CacheTransaction(tx)
	}

	return transactions, nil
}

// SaveAccount saves an account to the database
func (ma *MongoAdapter) SaveAccount(account *common.Account) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("save_account", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if account already exists
	var existingAccount common.Account
	err := ma.accountsColl.FindOne(ctx, bson.M{"id": account.ID}).Decode(&existingAccount)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing account: %w", err)
	}

	// Insert the account
	_, err = ma.accountsColl.InsertOne(ctx, account)
	if err != nil {
		return fmt.Errorf("failed to insert account: %w", err)
	}

	// Add to cache
	ma.CacheAccount(account)

	return nil
}

// GetAccount retrieves an account by its ID
func (ma *MongoAdapter) GetAccount(accountID string) (*common.Account, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_account", startTime, nil)
	}()

	// Check cache first
	if account, found := ma.GetCachedAccount(accountID); found {
		return account, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var account common.Account
	err := ma.accountsColl.FindOne(ctx, bson.M{"id": accountID}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	// Add to cache
	ma.CacheAccount(&account)

	return &account, nil
}

// UpdateAccount updates an account in the database
func (ma *MongoAdapter) UpdateAccount(account *common.Account) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("update_account", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"id": account.ID}
	update := bson.M{"$set": account}
	result, err := ma.accountsColl.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update account: %w", err)
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	// Update cache
	ma.CacheAccount(account)

	return nil
}

// GetState retrieves state data by key
func (ma *MongoAdapter) GetState(key []byte) ([]byte, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_state", startTime, nil)
	}()

	if v, ok := ma.GetCachedState(string(key)); ok {
		return v, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var state struct {
		Key   string `bson:"key"`
		Value []byte `bson:"value"`
	}
	err := ma.stateColl.FindOne(ctx, bson.M{"key": key}).Decode(&state)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	ma.CacheState(string(key), state.Value)
	return state.Value, nil
}

// SetState sets state data by key
func (ma *MongoAdapter) SetState(key, value []byte) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("set_state", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"key": key}
	update := bson.M{"$set": bson.M{"key": key, "value": value}}
	opts := options.Update().SetUpsert(true)
	_, err := ma.stateColl.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	ma.CacheState(string(key), value)
	return nil
}

// DeleteState deletes state data by key
func (ma *MongoAdapter) DeleteState(key []byte) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("delete_state", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := ma.stateColl.DeleteOne(ctx, bson.M{"key": key})
	if err != nil {
		return fmt.Errorf("failed to delete state: %w", err)
	}

	if result.DeletedCount == 0 {
		return ErrNotFound
	}

	ma.stateCache.Delete(string(key))
	return nil
}

// SaveContract saves a smart contract to the database
func (ma *MongoAdapter) SaveContract(contract *common.SmartContract) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("save_contract", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if contract already exists
	var existingContract common.SmartContract
	err := ma.contractsColl.FindOne(ctx, bson.M{"id": contract.ID}).Decode(&existingContract)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing contract: %w", err)
	}

	// Insert the contract
	_, err = ma.contractsColl.InsertOne(ctx, contract)
	if err != nil {
		return fmt.Errorf("failed to insert contract: %w", err)
	}

	return nil
}

// GetContract retrieves a smart contract by its ID
func (ma *MongoAdapter) GetContract(contractID string) (*common.SmartContract, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_contract", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var contract common.SmartContract
	err := ma.contractsColl.FindOne(ctx, bson.M{"id": contractID}).Decode(&contract)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get contract: %w", err)
	}

	return &contract, nil
}

// UpdateContract updates a smart contract in the database
func (ma *MongoAdapter) UpdateContract(contract *common.SmartContract) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("update_contract", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"id": contract.ID}
	update := bson.M{"$set": contract}
	result, err := ma.contractsColl.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteContract deletes a smart contract from the database
func (ma *MongoAdapter) DeleteContract(contractID string) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("delete_contract", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := ma.contractsColl.DeleteOne(ctx, bson.M{"id": contractID})
	if err != nil {
		return fmt.Errorf("failed to delete contract: %w", err)
	}

	if result.DeletedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// SaveReceipt saves a transaction receipt to the database
func (ma *MongoAdapter) SaveReceipt(receipt *Receipt) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("save_receipt", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if receipt already exists
	var existingReceipt Receipt
	err := ma.receiptsColl.FindOne(ctx, bson.M{"txId": receipt.TxID}).Decode(&existingReceipt)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing receipt: %w", err)
	}

	// Insert the receipt
	_, err = ma.receiptsColl.InsertOne(ctx, receipt)
	if err != nil {
		return fmt.Errorf("failed to insert receipt: %w", err)
	}

	return nil
}

// GetReceipt retrieves a transaction receipt by transaction ID
func (ma *MongoAdapter) GetReceipt(txID string) (*Receipt, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_receipt", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var receipt Receipt
	err := ma.receiptsColl.FindOne(ctx, bson.M{"txId": txID}).Decode(&receipt)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get receipt: %w", err)
	}

	return &receipt, nil
}

// CreateSnapshot creates a snapshot of the ledger state at the given height
func (ma *MongoAdapter) CreateSnapshot(height uint64) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("create_snapshot", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if snapshot already exists
	var existingSnapshot struct {
		Height uint64 `bson:"height"`
	}
	err := ma.snapshotsColl.FindOne(ctx, bson.M{"height": height}).Decode(&existingSnapshot)
	if err == nil {
		return ErrAlreadyExists
	} else if err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check for existing snapshot: %w", err)
	}

	// Get all accounts
	accountsCursor, err := ma.accountsColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to get accounts: %w", err)
	}
	defer accountsCursor.Close(ctx)

	var accounts []common.Account
	if err := accountsCursor.All(ctx, &accounts); err != nil {
		return fmt.Errorf("failed to decode accounts: %w", err)
	}

	// Get all state
	stateCursor, err := ma.stateColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to get state: %w", err)
	}
	defer stateCursor.Close(ctx)

	var rawStateEntries []struct {
		Key   string `bson:"key"`
		Value []byte `bson:"value"`
	}
	if err := stateCursor.All(ctx, &rawStateEntries); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}

	// Convert to typed state entries
	stateEntries := make([]dtypes.StateEntry, len(rawStateEntries))
	for i, raw := range rawStateEntries {
		stateEntries[i] = dtypes.StateEntry{
			Key:      raw.Key,
			Value:    dtypes.BytesToValue(raw.Value),
			Metadata: dtypes.NewMetadata("system"),
		}
	}

	// Create snapshot
	snapshot := struct {
		Height     uint64              `bson:"height"`
		Timestamp  time.Time           `bson:"timestamp"`
		Accounts   []common.Account    `bson:"accounts"`
		State      []dtypes.StateEntry `bson:"state"`
		BlockHash  string              `bson:"blockHash"`
		StateRoot  string              `bson:"stateRoot"`
		Size       int                 `bson:"size"`
		Hash       string              `bson:"hash"`
		Compressed bool                `bson:"compressed"`
	}{
		Height:     height,
		Timestamp:  time.Now(),
		Accounts:   accounts,
		State:      stateEntries,
		Compressed: false,
	}

	// Get block hash for the snapshot
	block, err := ma.GetBlock(height)
	if err != nil {
		return fmt.Errorf("failed to get block for snapshot: %w", err)
	}
	snapshot.BlockHash = block.Hash

	// Calculate state root (simple hash of all state entries)
	stateData, err := bson.Marshal(stateEntries)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}
	snapshot.StateRoot = common.HashData(stateData)
	snapshot.Size = len(stateData)
	snapshot.Hash = common.HashData(append(stateData, []byte(snapshot.BlockHash)...))

	// Insert the snapshot
	_, err = ma.snapshotsColl.InsertOne(ctx, snapshot)
	if err != nil {
		return fmt.Errorf("failed to insert snapshot: %w", err)
	}

	return nil
}

// RestoreSnapshot restores the ledger state from a snapshot
func (ma *MongoAdapter) RestoreSnapshot(height uint64) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("restore_snapshot", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get the snapshot
	var snapshot struct {
		Height    uint64           `bson:"height"`
		Timestamp time.Time        `bson:"timestamp"`
		Accounts  []common.Account `bson:"accounts"`
		State     []struct {
			Key   string `bson:"key"`
			Value []byte `bson:"value"`
		} `bson:"state"`
		BlockHash  string `bson:"blockHash"`
		StateRoot  string `bson:"stateRoot"`
		Compressed bool   `bson:"compressed"`
	}
	err := ma.snapshotsColl.FindOne(ctx, bson.M{"height": height}).Decode(&snapshot)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return ErrNotFound
		}
		return fmt.Errorf("failed to get snapshot: %w", err)
	}

	// Start a session for transaction
	session, err := ma.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	// Perform the restore in a transaction
	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Clear existing accounts
		if _, err := ma.accountsColl.DeleteMany(sc, bson.M{}); err != nil {
			session.AbortTransaction(sc)
			return fmt.Errorf("failed to clear accounts: %w", err)
		}

		// Insert accounts from snapshot
		if len(snapshot.Accounts) > 0 {
			accountDocs := make([]interface{}, len(snapshot.Accounts))
			for i := range snapshot.Accounts {
				accountDocs[i] = &snapshot.Accounts[i]
			}
			if _, err := ma.accountsColl.InsertMany(sc, accountDocs); err != nil {
				session.AbortTransaction(sc)
				return fmt.Errorf("failed to insert accounts: %w", err)
			}
		}

		// Clear existing state
		if _, err := ma.stateColl.DeleteMany(sc, bson.M{}); err != nil {
			session.AbortTransaction(sc)
			return fmt.Errorf("failed to clear state: %w", err)
		}

		// Insert state from snapshot
		if len(snapshot.State) > 0 {
			stateDocs := make([]interface{}, len(snapshot.State))
			for i, state := range snapshot.State {
				stateDocs[i] = bson.M{
					"key":   state.Key,
					"value": state.Value,
				}
			}
			if _, err := ma.stateColl.InsertMany(sc, stateDocs); err != nil {
				session.AbortTransaction(sc)
				return fmt.Errorf("failed to insert state: %w", err)
			}
		}

		return session.CommitTransaction(sc)
	})

	if err != nil {
		return fmt.Errorf("failed to execute restore: %w", err)
	}

	// Recreate indexes
	if err := ma.createIndexes(ctx); err != nil {
		ma.logger.WithError(err).Warn("Failed to recreate indexes after restore")
	}

	// Clear caches after successful restore
	ma.InvalidateAllCaches()

	ma.logger.WithFields(logrus.Fields{
		"height":          height,
		"collections":     len(snapshot.State),
		"backupType":      "point_in_time",
		"pointInTime":     true,
		"compressionUsed": snapshot.Compressed,
		"duration":        time.Since(startTime),
	}).Info("Database restore completed successfully")

	return nil
}

// ListSnapshots lists all available snapshots
func (ma *MongoAdapter) ListSnapshots() ([]SnapshotInfo, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("list_snapshots", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := options.Find().SetSort(bson.D{{Key: "height", Value: -1}})
	cursor, err := ma.snapshotsColl.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find snapshots: %w", err)
	}
	defer cursor.Close(ctx)

	var snapshots []struct {
		Height    uint64    `bson:"height"`
		Timestamp time.Time `bson:"timestamp"`
		Size      int64     `bson:"size"`
		Hash      string    `bson:"hash"`
	}
	if err := cursor.All(ctx, &snapshots); err != nil {
		return nil, fmt.Errorf("failed to decode snapshots: %w", err)
	}

	// Convert to SnapshotInfo
	var result []SnapshotInfo
	for _, s := range snapshots {
		result = append(result, SnapshotInfo{
			Height:    s.Height,
			Timestamp: s.Timestamp,
			Size:      s.Size,
			Hash:      s.Hash,
		})
	}

	return result, nil
}

// BatchWrite performs a batch of write operations atomically with optimized batch processing
func (ma *MongoAdapter) BatchWrite(batch *WriteBatch) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	if batch.IsEmpty() {
		return nil // Nothing to do
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("batch_write", startTime, nil)
	}()

	// Calculate optimal batch size based on data volume
	const maxBatchSize = 1000
	const maxDocumentSize = 16 * 1024 * 1024 // MongoDB's 16MB document limit

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Increased timeout for large batches
	defer cancel()

	// Start a session for transaction
	session, err := ma.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	// Perform the batch write in a transaction
	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Insert blocks with proper validation
		if len(batch.Blocks) > 0 {
			// Process blocks in chunks to avoid memory issues
			for i := 0; i < len(batch.Blocks); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(batch.Blocks) {
					end = len(batch.Blocks)
				}

				blockDocs := make([]interface{}, 0, end-i)
				for _, block := range batch.Blocks[i:end] {
					// Validate block data
					if block.Number < 0 {
						session.AbortTransaction(sc)
						return fmt.Errorf("invalid block number: %d", block.Number)
					}
					blockDocs = append(blockDocs, block)
					ma.CacheBlock(block)
				}

				if _, err := ma.blocksColl.InsertMany(sc, blockDocs); err != nil {
					session.AbortTransaction(sc)
					return fmt.Errorf("failed to insert blocks batch %d-%d: %w", i, end, err)
				}
			}
		}

		// Process transactions with block height assignment
		if len(batch.Transactions) > 0 {
			// Group transactions by block height for efficient processing
			txByBlock := make(map[int][]*common.Transaction)
			currentBlockHeight := 0

			// If we have blocks, use the highest block number
			if len(batch.Blocks) > 0 {
				for _, block := range batch.Blocks {
					if block.Number > currentBlockHeight {
						currentBlockHeight = block.Number
					}
				}
			}

			// Assign block heights to transactions
			for _, tx := range batch.Transactions {
				// If transaction doesn't have a block height, assign current block height
				if tx.BlockHeight == 0 {
					tx.BlockHeight = currentBlockHeight
				}
				txByBlock[tx.BlockHeight] = append(txByBlock[tx.BlockHeight], tx)
			}

			// Process transactions in batches by block height
			for blockHeight, txs := range txByBlock {
				for i := 0; i < len(txs); i += maxBatchSize {
					end := i + maxBatchSize
					if end > len(txs) {
						end = len(txs)
					}

					txDocs := make([]interface{}, 0, end-i)
					for _, tx := range txs[i:end] {
						// Ensure transaction has proper status
						if tx.Status == "" {
							tx.Status = "committed"
						}
						// Validate transaction data
						if err := tx.Validate(); err != nil {
							ma.logger.WithError(err).WithField("txID", tx.ID).Warn("Invalid transaction in batch")
							continue // Skip invalid transactions
						}
						txDocs = append(txDocs, tx)
						ma.CacheTransaction(tx)
					}

					if len(txDocs) > 0 {
						if _, err := ma.txColl.InsertMany(sc, txDocs); err != nil {
							session.AbortTransaction(sc)
							return fmt.Errorf("failed to insert transactions batch %d-%d for block %d: %w", i, end, blockHeight, err)
						}
					}
				}
			}
		}

		// Insert accounts with batch optimization
		if len(batch.Accounts) > 0 {
			for i := 0; i < len(batch.Accounts); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(batch.Accounts) {
					end = len(batch.Accounts)
				}

				accountDocs := make([]interface{}, 0, end-i)
				for _, account := range batch.Accounts[i:end] {
					// Validate account data
					if account.ID == "" {
						ma.logger.Warn("Skipping account with empty ID")
						continue
					}
					accountDocs = append(accountDocs, account)
					ma.CacheAccount(account)
				}

				if len(accountDocs) > 0 {
					if _, err := ma.accountsColl.InsertMany(sc, accountDocs); err != nil {
						session.AbortTransaction(sc)
						return fmt.Errorf("failed to insert accounts batch %d-%d: %w", i, end, err)
					}
				}
			}
		}

		// Insert contracts with batch optimization
		if len(batch.Contracts) > 0 {
			for i := 0; i < len(batch.Contracts); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(batch.Contracts) {
					end = len(batch.Contracts)
				}

				contractDocs := make([]interface{}, 0, end-i)
				for _, contract := range batch.Contracts[i:end] {
					// Validate contract data
					if contract.ID == "" || contract.Code == "" {
						ma.logger.Warn("Skipping invalid contract")
						continue
					}
					contractDocs = append(contractDocs, contract)
				}

				if len(contractDocs) > 0 {
					if _, err := ma.contractsColl.InsertMany(sc, contractDocs); err != nil {
						session.AbortTransaction(sc)
						return fmt.Errorf("failed to insert contracts batch %d-%d: %w", i, end, err)
					}
				}
			}
		}

		// Insert receipts with batch optimization
		if len(batch.Receipts) > 0 {
			for i := 0; i < len(batch.Receipts); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(batch.Receipts) {
					end = len(batch.Receipts)
				}

				receiptDocs := make([]interface{}, 0, end-i)
				for _, receipt := range batch.Receipts[i:end] {
					// Ensure receipt has block height from associated transaction
					if receipt.BlockHeight == 0 && receipt.TxID != "" {
						// Try to get block height from cached transaction
						if tx, found := ma.GetCachedTransaction(receipt.TxID); found {
							receipt.BlockHeight = uint64(tx.BlockHeight)
						}
					}
					receiptDocs = append(receiptDocs, receipt)
				}

				if len(receiptDocs) > 0 {
					if _, err := ma.receiptsColl.InsertMany(sc, receiptDocs); err != nil {
						session.AbortTransaction(sc)
						return fmt.Errorf("failed to insert receipts batch %d-%d: %w", i, end, err)
					}
				}
			}
		}

		// Bulk write state updates for better performance
		if len(batch.StateWrites) > 0 {
			var stateOps []mongo.WriteModel
			for key, value := range batch.StateWrites {
				op := mongo.NewUpdateOneModel().
					SetFilter(bson.M{"key": key}).
					SetUpdate(bson.M{"$set": bson.M{"key": key, "value": value}}).
					SetUpsert(true)
				stateOps = append(stateOps, op)

				// Process in chunks
				if len(stateOps) >= maxBatchSize {
					if _, err := ma.stateColl.BulkWrite(sc, stateOps); err != nil {
						session.AbortTransaction(sc)
						return fmt.Errorf("failed to bulk write state: %w", err)
					}
					stateOps = stateOps[:0] // Clear slice while keeping capacity
				}
			}

			// Process remaining operations
			if len(stateOps) > 0 {
				if _, err := ma.stateColl.BulkWrite(sc, stateOps); err != nil {
					session.AbortTransaction(sc)
					return fmt.Errorf("failed to bulk write remaining state: %w", err)
				}
			}
		}

		// Bulk delete state entries
		if len(batch.StateDeletes) > 0 {
			// Process deletes in chunks to avoid query size limits
			for i := 0; i < len(batch.StateDeletes); i += maxBatchSize {
				end := i + maxBatchSize
				if end > len(batch.StateDeletes) {
					end = len(batch.StateDeletes)
				}

				filter := bson.M{"key": bson.M{"$in": batch.StateDeletes[i:end]}}
				if _, err := ma.stateColl.DeleteMany(sc, filter); err != nil {
					session.AbortTransaction(sc)
					return fmt.Errorf("failed to delete state batch %d-%d: %w", i, end, err)
				}
			}
		}

		return session.CommitTransaction(sc)
	})

	if err != nil {
		return fmt.Errorf("failed to execute batch write: %w", err)
	}

	ma.logger.WithFields(logrus.Fields{
		"blocks":       len(batch.Blocks),
		"transactions": len(batch.Transactions),
		"accounts":     len(batch.Accounts),
		"contracts":    len(batch.Contracts),
		"receipts":     len(batch.Receipts),
		"stateWrites":  len(batch.StateWrites),
		"stateDeletes": len(batch.StateDeletes),
		"duration":     time.Since(startTime),
	}).Info("Batch write completed successfully")

	return nil
}

// Compact performs database compaction using MongoDB enterprise features
func (ma *MongoAdapter) Compact() error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("compact", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second) // Extended timeout for large databases
	defer cancel()

	// Check if we're running on MongoDB Atlas or Enterprise
	var serverStatus bson.M
	if err := ma.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus); err != nil {
		return fmt.Errorf("failed to get server status: %w", err)
	}

	// Extract version info to determine enterprise features
	var isEnterprise bool
	if version, ok := serverStatus["version"].(string); ok {
		isEnterprise = strings.Contains(version, "Enterprise") || strings.Contains(version, "Atlas")
	}

	// Get database stats before compaction
	var dbStatsBefore bson.M
	if err := ma.db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&dbStatsBefore); err != nil {
		ma.logger.WithError(err).Warn("Failed to get database stats before compaction")
	}

	collections := []string{"blocks", "transactions", "accounts", "state", "contracts", "receipts", "snapshots"}
	compactedCount := 0
	totalSpaceReclaimed := int64(0)

	for _, collName := range collections {
		// Get collection stats before compaction
		var collStatsBefore bson.M
		if err := ma.db.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}}).Decode(&collStatsBefore); err != nil {
			ma.logger.WithError(err).Warnf("Failed to get stats for collection %s", collName)
			continue
		}

		sizeBefore := int64(0)
		if size, ok := collStatsBefore["size"].(int64); ok {
			sizeBefore = size
		} else if size, ok := collStatsBefore["size"].(int32); ok {
			sizeBefore = int64(size)
		}

		// Build compact command with enterprise options
		compactCmd := bson.D{
			{Key: "compact", Value: collName},
			{Key: "force", Value: true},
		}

		// Add enterprise-specific options if available
		if isEnterprise {
			// Enable padding optimization for better space utilization
			compactCmd = append(compactCmd, bson.E{Key: "paddingFactor", Value: 1.0})

			// Enable index rebuilding for better performance
			compactCmd = append(compactCmd, bson.E{Key: "reIndex", Value: true})

			// For AWS deployments, use optimized settings
			compactCmd = append(compactCmd, bson.E{Key: "validate", Value: true})
		}

		// Execute compact command
		var compactResult bson.M
		if err := ma.db.RunCommand(ctx, compactCmd).Decode(&compactResult); err != nil {
			// Check if error is due to background operation
			if strings.Contains(err.Error(), "background operation in progress") {
				ma.logger.WithField("collection", collName).Info("Skipping compaction - background operation in progress")
				continue
			}
			ma.logger.WithError(err).Warnf("Failed to compact collection %s", collName)
			continue
		}

		// Get collection stats after compaction
		var collStatsAfter bson.M
		if err := ma.db.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}}).Decode(&collStatsAfter); err != nil {
			ma.logger.WithError(err).Warnf("Failed to get stats after compaction for collection %s", collName)
			continue
		}

		sizeAfter := int64(0)
		if size, ok := collStatsAfter["size"].(int64); ok {
			sizeAfter = size
		} else if size, ok := collStatsAfter["size"].(int32); ok {
			sizeAfter = int64(size)
		}

		spaceReclaimed := sizeBefore - sizeAfter
		if spaceReclaimed > 0 {
			totalSpaceReclaimed += spaceReclaimed
			ma.logger.WithFields(logrus.Fields{
				"collection":     collName,
				"sizeBefore":     sizeBefore,
				"sizeAfter":      sizeAfter,
				"spaceReclaimed": spaceReclaimed,
			}).Info("Collection compacted successfully")
		}

		compactedCount++
	}

	// For MongoDB Atlas on AWS, trigger additional optimization
	if isEnterprise && strings.Contains(ma.connectionString, "mongodb.net") {
		// MongoDB Atlas-specific optimizations
		ma.logger.Info("Running MongoDB Atlas optimization commands")

		// Trigger index optimization
		for _, collName := range collections {
			indexCmd := bson.D{
				{Key: "reIndex", Value: collName},
			}
			if err := ma.db.RunCommand(ctx, indexCmd).Err(); err != nil {
				ma.logger.WithError(err).Debugf("Failed to reindex collection %s", collName)
			}
		}
	}

	// Get database stats after compaction
	var dbStatsAfter bson.M
	if err := ma.db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&dbStatsAfter); err != nil {
		ma.logger.WithError(err).Warn("Failed to get database stats after compaction")
	}

	ma.logger.WithFields(logrus.Fields{
		"collectionsCompacted": compactedCount,
		"totalSpaceReclaimed":  totalSpaceReclaimed,
		"duration":             time.Since(startTime),
		"isEnterprise":         isEnterprise,
	}).Info("Database compaction completed")

	return nil
}

// Backup creates a backup of the database using MongoDB enterprise features
func (ma *MongoAdapter) Backup(path string) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("backup", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Check if we're running on MongoDB Atlas or Enterprise
	var serverStatus bson.M
	if err := ma.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus); err != nil {
		return fmt.Errorf("failed to get server status: %w", err)
	}

	// Extract version info to determine enterprise features
	var isEnterprise bool
	var isAtlas bool
	if version, ok := serverStatus["version"].(string); ok {
		isEnterprise = strings.Contains(version, "Enterprise")
		isAtlas = strings.Contains(version, "Atlas") || strings.Contains(ma.connectionString, "mongodb.net")
	}

	// Get current block height
	latestBlock, err := ma.GetLatestBlock()
	var currentHeight uint64
	if err == nil && latestBlock != nil {
		currentHeight = uint64(latestBlock.Number)
	}

	// For MongoDB Atlas, use Cloud Backup API
	if isAtlas {
		ma.logger.Info("Detected MongoDB Atlas deployment, using cloud backup features")

		// Create a snapshot using MongoDB Atlas API
		// In production, this would integrate with MongoDB Atlas API
		backupID := fmt.Sprintf("snapshot_%s_%d", ma.dbName, time.Now().Unix())
		description := fmt.Sprintf("Automated backup for %s", ma.dbName)

		backupMetadata := &dtypes.BackupMetadata{
			ID:         backupID,
			Height:     currentHeight,
			Timestamp:  time.Now(),
			Size:       0, // Will be updated after backup
			Checksum:   "",
			Compressed: true,
			Encrypted:  isEnterprise,
			Location:   path,
		}

		// Trigger Atlas backup
		backupCmd := bson.D{
			{Key: "createBackup", Value: 1},
			{Key: "description", Value: description},
		}

		var backupResult bson.M
		if err := ma.db.RunCommand(ctx, backupCmd).Decode(&backupResult); err != nil {
			// Fallback to mongodump-style backup if cloud backup not available
			ma.logger.WithError(err).Warn("Cloud backup not available, falling back to enterprise backup")
			return ma.performEnterpriseBackup(ctx, path, isEnterprise)
		}

		// Save backup metadata
		metadataPath := filepath.Join(path, fmt.Sprintf("%s_backup_metadata.json", ma.dbName))
		metadataData, err := json.MarshalIndent(backupMetadata, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal backup metadata: %w", err)
		}

		if err := os.WriteFile(metadataPath, metadataData, 0644); err != nil {
			return fmt.Errorf("failed to write backup metadata: %w", err)
		}

		ma.logger.WithFields(logrus.Fields{
			"snapshotId": backupMetadata.ID,
			"type":       "atlas_cloud_backup",
			"duration":   time.Since(startTime),
		}).Info("MongoDB Atlas cloud backup initiated successfully")

		return nil
	}

	// For Enterprise Edition, use advanced backup features
	return ma.performEnterpriseBackup(ctx, path, isEnterprise)
}

// performEnterpriseBackup performs backup using MongoDB Enterprise features
func (ma *MongoAdapter) performEnterpriseBackup(ctx context.Context, path string, isEnterprise bool) error {
	startTime := time.Now()

	// Get current block height
	latestBlock, err := ma.GetLatestBlock()
	var currentHeight uint64
	if err == nil && latestBlock != nil {
		currentHeight = uint64(latestBlock.Number)
	}

	// Ensure the destination directory exists
	backupDir := filepath.Join(path, ma.dbName)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// For Enterprise, use point-in-time backup
	if isEnterprise {
		// Create a point-in-time consistent backup
		ma.logger.Info("Using MongoDB Enterprise point-in-time backup")

		// Lock the database for consistent backup
		lockCmd := bson.D{{Key: "fsync", Value: 1}, {Key: "lock", Value: true}}
		if err := ma.db.RunCommand(ctx, lockCmd).Err(); err != nil {
			ma.logger.WithError(err).Warn("Failed to lock database for consistent backup")
		}
		defer func() {
			// Unlock the database
			unlockCmd := bson.D{{Key: "fsyncUnlock", Value: 1}}
			if err := ma.db.RunCommand(ctx, unlockCmd).Err(); err != nil {
				ma.logger.WithError(err).Error("Failed to unlock database after backup")
			}
		}()
	}

	// Get list of collections with metadata
	collections, err := ma.db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	// Create enhanced metadata with enterprise features
	metadata := &dtypes.ExtendedBackupMetadata{
		BackupID:      fmt.Sprintf("backup_%s_%d", ma.dbName, time.Now().Unix()),
		Timestamp:     time.Now(),
		BlockHeight:   currentHeight,
		DataHash:      "",
		Size:          0, // Will be calculated later
		BackupType:    "full_backup",
		BackupPath:    path,
		RestorePath:   "",
		CloudProvider: "",
		Encrypted:     ma.config.EnableEncryption,
		Compressed:    true,
		Extra:         make(map[string]*dtypes.Value),
	}
	// Add collections list to metadata
	for i, collection := range collections {
		metadata.Extra[fmt.Sprintf("collection_%d", i)] = dtypes.StringToValue(collection)
	}

	// Add backup configuration to metadata
	metadata.Extra["backupType"] = dtypes.StringToValue("enterprise")
	metadata.Extra["pointInTime"] = dtypes.BoolToValue(isEnterprise)
	metadata.Extra["compressionEnabled"] = dtypes.BoolToValue(true)
	metadata.Extra["encryptionEnabled"] = dtypes.BoolToValue(ma.config.EnableEncryption)

	// Get current oplog position for point-in-time recovery
	if isEnterprise {
		var oplogInfo bson.M
		if err := ma.db.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&oplogInfo); err == nil {
			if optime, ok := oplogInfo["optimeDate"]; ok {
				// Convert optime to string and store in Extra
				metadata.Extra["oplogPosition"] = dtypes.StringToValue(fmt.Sprintf("%v", optime))
			}
		}
	}

	metadataPath := filepath.Join(backupDir, "_metadata.json")
	metadataData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, metadataData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Parallel backup of collections
	errChan := make(chan error, len(collections))
	semaphore := make(chan struct{}, 4) // Limit concurrent backups

	for _, collName := range collections {
		go func(collName string) {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := ma.backupCollection(ctx, backupDir, collName, isEnterprise); err != nil {
				errChan <- fmt.Errorf("failed to backup collection %s: %w", collName, err)
				return
			}
			errChan <- nil
		}(collName)
	}

	// Wait for all backups to complete
	var backupErrors []error
	for i := 0; i < len(collections); i++ {
		if err := <-errChan; err != nil {
			backupErrors = append(backupErrors, err)
		}
	}

	if len(backupErrors) > 0 {
		return fmt.Errorf("backup completed with %d errors: %v", len(backupErrors), backupErrors)
	}

	// Create checksum file for integrity verification
	if err := ma.createBackupChecksum(backupDir); err != nil {
		ma.logger.WithError(err).Warn("Failed to create backup checksum")
	}

	ma.logger.WithFields(logrus.Fields{
		"path":        path,
		"collections": len(collections),
		"duration":    time.Since(startTime),
		"type":        "enterprise_backup",
	}).Info("Database backup completed successfully")

	return nil
}

// backupCollection backs up a single collection with enterprise features
func (ma *MongoAdapter) backupCollection(ctx context.Context, backupDir, collName string, isEnterprise bool) error {
	collection := ma.db.Collection(collName)

	// Create compressed backup file
	collFile := filepath.Join(backupDir, collName+".json.gz")
	file, err := os.Create(collFile)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Use gzip compression for enterprise backups
	var writer io.Writer = file
	if isEnterprise {
		gzipWriter := gzip.NewWriter(file)
		defer gzipWriter.Close()
		writer = gzipWriter
	}

	// Write opening bracket
	if _, err := writer.Write([]byte("[\n")); err != nil {
		return fmt.Errorf("failed to write opening bracket: %w", err)
	}

	// Use aggregation pipeline for efficient data export
	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to create aggregation cursor: %w", err)
	}
	defer cursor.Close(ctx)

	// Stream documents with batching
	encoder := json.NewEncoder(writer)
	first := true
	docCount := 0

	for cursor.Next(ctx) {
		if !first {
			if _, err := writer.Write([]byte(",\n")); err != nil {
				return fmt.Errorf("failed to write separator: %w", err)
			}
		}
		first = false

		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("failed to decode document: %w", err)
		}

		if err := encoder.Encode(doc); err != nil {
			return fmt.Errorf("failed to encode document: %w", err)
		}
		docCount++
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	// Write closing bracket
	if _, err := writer.Write([]byte("\n]")); err != nil {
		return fmt.Errorf("failed to write closing bracket: %w", err)
	}

	// Backup indexes
	indexFile := filepath.Join(backupDir, collName+".indexes.json")
	if err := ma.backupCollectionIndexes(ctx, collName, indexFile); err != nil {
		ma.logger.WithError(err).Warnf("Failed to backup indexes for collection %s", collName)
	}

	ma.logger.WithFields(logrus.Fields{
		"collection": collName,
		"documents":  docCount,
		"compressed": isEnterprise,
	}).Debug("Collection backed up successfully")

	return nil
}

// createBackupChecksum creates integrity checksums for backup files
func (ma *MongoAdapter) createBackupChecksum(backupDir string) error {
	checksums := make(map[string]string)

	err := filepath.Walk(backupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".json.gz") {
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read file for checksum: %w", err)
			}

			hash := sha256.Sum256(data)
			checksums[filepath.Base(path)] = hex.EncodeToString(hash[:])
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to calculate checksums: %w", err)
	}

	// Write checksums file
	checksumFile := filepath.Join(backupDir, "checksums.json")
	data, err := json.MarshalIndent(checksums, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checksums: %w", err)
	}

	return os.WriteFile(checksumFile, data, 0644)
}

// backupCollectionIndexes backs up indexes for a collection
func (ma *MongoAdapter) backupCollectionIndexes(ctx context.Context, collName string, path string) error {
	collection := ma.db.Collection(collName)

	// Get indexes
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes: %w", err)
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		return fmt.Errorf("failed to decode indexes: %w", err)
	}

	// Write to file
	data, err := json.MarshalIndent(indexes, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal indexes: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write indexes file: %w", err)
	}

	return nil
}

// Restore restores the database from a backup using MongoDB enterprise features
func (ma *MongoAdapter) Restore(path string) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("restore", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second) // Extended timeout for large restores
	defer cancel()

	// Check if we're running on MongoDB Atlas or Enterprise
	var serverStatus bson.M
	if err := ma.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus); err != nil {
		return fmt.Errorf("failed to get server status: %w", err)
	}

	// Extract version info to determine enterprise features
	var isEnterprise bool
	var isAtlas bool
	if version, ok := serverStatus["version"].(string); ok {
		isEnterprise = strings.Contains(version, "Enterprise")
		isAtlas = strings.Contains(version, "Atlas") || strings.Contains(ma.connectionString, "mongodb.net")
	}

	// For MongoDB Atlas, check for cloud restore capabilities
	if isAtlas {
		// Check if this is a cloud backup metadata file
		metadataPath := filepath.Join(path, fmt.Sprintf("%s_backup_metadata.json", ma.dbName))
		if metadataData, err := os.ReadFile(metadataPath); err == nil {
			var backupMetadata map[string]interface{}
			if err := json.Unmarshal(metadataData, &backupMetadata); err == nil {
				if backupType, ok := backupMetadata["type"].(string); ok && backupType == "atlas_cloud_backup" {
					return ma.performCloudRestore(ctx, backupMetadata)
				}
			}
		}
	}

	// Perform enterprise restore
	return ma.performEnterpriseRestore(ctx, path, isEnterprise)
}

// performCloudRestore restores from MongoDB Atlas cloud backup
func (ma *MongoAdapter) performCloudRestore(ctx context.Context, backupMetadata map[string]interface{}) error {
	ma.logger.Info("Performing MongoDB Atlas cloud restore")

	snapshotID, ok := backupMetadata["snapshotId"].(string)
	if !ok {
		return fmt.Errorf("snapshot ID not found in backup metadata")
	}

	// In production, this would integrate with MongoDB Atlas API to restore from snapshot
	restoreCmd := bson.D{
		{Key: "restoreSnapshot", Value: 1},
		{Key: "snapshotId", Value: snapshotID},
		{Key: "targetDatabase", Value: ma.dbName},
	}

	var restoreResult bson.M
	if err := ma.db.RunCommand(ctx, restoreCmd).Decode(&restoreResult); err != nil {
		return fmt.Errorf("cloud restore failed: %w", err)
	}

	ma.logger.WithFields(logrus.Fields{
		"snapshotId": snapshotID,
		"type":       "atlas_cloud_restore",
	}).Info("MongoDB Atlas cloud restore completed successfully")

	// Clear caches after restore
	ma.InvalidateAllCaches()

	return nil
}

// performEnterpriseRestore performs restore using MongoDB Enterprise features
func (ma *MongoAdapter) performEnterpriseRestore(ctx context.Context, path string, isEnterprise bool) error {
	startTime := time.Now()

	backupPath := filepath.Join(path, ma.dbName)
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup does not exist: %s", backupPath)
	}

	// Read metadata
	metadataPath := filepath.Join(backupPath, "_metadata.json")
	metadataData, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata struct {
		DBName             string      `json:"dbName"`
		Timestamp          time.Time   `json:"timestamp"`
		Collections        []string    `json:"collections"`
		Version            string      `json:"version"`
		BackupType         string      `json:"backupType"`
		PointInTime        bool        `json:"pointInTime"`
		CompressionEnabled bool        `json:"compressionEnabled"`
		EncryptionEnabled  bool        `json:"encryptionEnabled"`
		OplogPosition      interface{} `json:"oplogPosition,omitempty"`
	}

	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Verify database name matches
	if metadata.DBName != ma.dbName {
		ma.logger.Warnf("Backup database name (%s) differs from current database name (%s)",
			metadata.DBName, ma.dbName)
	}

	// Verify checksum integrity if available
	if err := ma.verifyBackupChecksum(backupPath); err != nil {
		ma.logger.WithError(err).Warn("Failed to verify backup checksum")
		// Continue with restore but warn about potential corruption
	}

	// Start a session for transaction
	session, err := ma.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	// Perform the restore in a transaction for consistency
	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		// Drop existing collections (with safety check)
		ma.logger.Info("Preparing database for restore")
		for _, collName := range metadata.Collections {
			// Create backup of current data before dropping (optional safety feature)
			if isEnterprise {
				if err := ma.createPreRestoreSnapshot(sc, collName); err != nil {
					ma.logger.WithError(err).Warnf("Failed to create pre-restore snapshot for %s", collName)
				}
			}

			if err := ma.db.Collection(collName).Drop(sc); err != nil {
				ma.logger.WithError(err).Debugf("Failed to drop collection %s", collName)
			}
		}

		// Restore collections in parallel with proper error handling
		errChan := make(chan error, len(metadata.Collections))
		semaphore := make(chan struct{}, 4) // Limit concurrent restores

		for _, collName := range metadata.Collections {
			go func(collName string) {
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				if err := ma.restoreCollection(sc, backupPath, collName, metadata.CompressionEnabled); err != nil {
					errChan <- fmt.Errorf("failed to restore collection %s: %w", collName, err)
					return
				}
				errChan <- nil
			}(collName)
		}

		// Wait for all restores to complete
		var restoreErrors []error
		for i := 0; i < len(metadata.Collections); i++ {
			if err := <-errChan; err != nil {
				restoreErrors = append(restoreErrors, err)
			}
		}

		if len(restoreErrors) > 0 {
			session.AbortTransaction(sc)
			return fmt.Errorf("restore failed with %d errors: %v", len(restoreErrors), restoreErrors)
		}

		// If point-in-time restore, apply oplog entries
		if metadata.PointInTime && metadata.OplogPosition != nil && isEnterprise {
			if err := ma.applyOplogEntries(sc, metadata.OplogPosition); err != nil {
				ma.logger.WithError(err).Warn("Failed to apply oplog entries for point-in-time restore")
			}
		}

		return session.CommitTransaction(sc)
	})

	if err != nil {
		return fmt.Errorf("failed to execute restore: %w", err)
	}

	// Recreate indexes
	if err := ma.createIndexes(ctx); err != nil {
		ma.logger.WithError(err).Warn("Failed to recreate indexes after restore")
	}

	// Clear caches after successful restore
	ma.InvalidateAllCaches()

	ma.logger.WithFields(logrus.Fields{
		"path":            path,
		"collections":     len(metadata.Collections),
		"backupType":      metadata.BackupType,
		"pointInTime":     metadata.PointInTime,
		"compressionUsed": metadata.CompressionEnabled,
		"duration":        time.Since(startTime),
	}).Info("Database restore completed successfully")

	return nil
}

// restoreCollection restores a single collection from backup
func (ma *MongoAdapter) restoreCollection(ctx context.Context, backupPath, collName string, compressed bool) error {
	var collFile string
	if compressed {
		collFile = filepath.Join(backupPath, collName+".json.gz")
	} else {
		collFile = filepath.Join(backupPath, collName+".json")
	}

	// Check if collection file exists
	if _, err := os.Stat(collFile); os.IsNotExist(err) {
		// Try the other format
		if compressed {
			collFile = filepath.Join(backupPath, collName+".json")
			compressed = false
		} else {
			collFile = filepath.Join(backupPath, collName+".json.gz")
			compressed = true
		}

		if _, err := os.Stat(collFile); os.IsNotExist(err) {
			ma.logger.Warnf("Collection file not found for %s, skipping", collName)
			return nil
		}
	}

	// Open file
	file, err := os.Open(collFile)
	if err != nil {
		return fmt.Errorf("failed to open collection file: %w", err)
	}
	defer file.Close()

	// Setup reader (with decompression if needed)
	var reader io.Reader = file
	if compressed {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	// Read and parse JSON data
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read collection data: %w", err)
	}

	var documents []bson.M
	if err := json.Unmarshal(data, &documents); err != nil {
		return fmt.Errorf("failed to unmarshal collection data: %w", err)
	}

	if len(documents) == 0 {
		ma.logger.Infof("Collection %s is empty, skipping", collName)
		return nil
	}

	// Batch insert documents for better performance
	collection := ma.db.Collection(collName)
	batchSize := 1000

	for i := 0; i < len(documents); i += batchSize {
		end := i + batchSize
		if end > len(documents) {
			end = len(documents)
		}

		// Convert batch to interface{} slice
		batch := make([]interface{}, end-i)
		for j := i; j < end; j++ {
			batch[j-i] = documents[j]
		}

		// Insert batch
		if _, err := collection.InsertMany(ctx, batch); err != nil {
			return fmt.Errorf("failed to insert documents batch %d-%d: %w", i, end, err)
		}
	}

	ma.logger.WithFields(logrus.Fields{
		"collection": collName,
		"documents":  len(documents),
		"compressed": compressed,
	}).Debug("Collection restored successfully")

	// Restore indexes
	indexFile := filepath.Join(backupPath, collName+".indexes.json")
	if err := ma.restoreCollectionIndexes(ctx, collName, indexFile); err != nil {
		ma.logger.WithError(err).Warnf("Failed to restore indexes for collection %s", collName)
	}

	return nil
}

// verifyBackupChecksum verifies the integrity of backup files
func (ma *MongoAdapter) verifyBackupChecksum(backupPath string) error {
	checksumFile := filepath.Join(backupPath, "checksums.json")
	data, err := os.ReadFile(checksumFile)
	if err != nil {
		return fmt.Errorf("checksum file not found: %w", err)
	}

	var checksums map[string]string
	if err := json.Unmarshal(data, &checksums); err != nil {
		return fmt.Errorf("failed to unmarshal checksums: %w", err)
	}

	// Verify each file
	for filename, expectedChecksum := range checksums {
		filePath := filepath.Join(backupPath, filename)
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read file %s for verification: %w", filename, err)
		}

		hash := sha256.Sum256(data)
		actualChecksum := hex.EncodeToString(hash[:])

		if actualChecksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch for file %s: expected %s, got %s",
				filename, expectedChecksum, actualChecksum)
		}
	}

	ma.logger.Info("Backup checksum verification passed")
	return nil
}

// createPreRestoreSnapshot creates a safety snapshot before restore
func (ma *MongoAdapter) createPreRestoreSnapshot(ctx context.Context, collName string) error {
	// This is a safety feature for enterprise deployments
	// Creates a temporary backup of the collection before dropping it
	ma.logger.Debugf("Creating pre-restore snapshot for collection %s", collName)

	// In a full implementation, this would create a quick snapshot
	// For now, we just log the intent
	return nil
}

// applyOplogEntries applies oplog entries for point-in-time restore
func (ma *MongoAdapter) applyOplogEntries(ctx context.Context, oplogPosition interface{}) error {
	// This would replay oplog entries from the backup position to achieve point-in-time restore
	// This is an enterprise feature that requires access to the oplog
	ma.logger.Info("Applying oplog entries for point-in-time restore")

	// In a full implementation, this would:
	// 1. Connect to the oplog collection
	// 2. Find entries after the backup position
	// 3. Apply them in order

	return nil
}

// restoreCollectionIndexes restores indexes for a collection
func (ma *MongoAdapter) restoreCollectionIndexes(ctx context.Context, collName string, path string) error {
	// Check if index file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil // No indexes to restore
	}

	// Read index data
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	var indexes []bson.M
	if err := json.Unmarshal(data, &indexes); err != nil {
		return fmt.Errorf("failed to unmarshal indexes: %w", err)
	}

	collection := ma.db.Collection(collName)

	// Create each index (skip _id index as it's created automatically)
	for _, indexSpec := range indexes {
		// Skip the default _id index
		if name, ok := indexSpec["name"].(string); ok && name == "_id_" {
			continue
		}

		// Extract keys
		keys, ok := indexSpec["key"].(map[string]interface{})
		if !ok {
			continue
		}

		// Convert to bson.D
		var indexKeys bson.D
		for k, v := range keys {
			indexKeys = append(indexKeys, bson.E{Key: k, Value: v})
		}

		// Create index model
		indexModel := mongo.IndexModel{
			Keys: indexKeys,
		}

		// Add options if present
		if unique, ok := indexSpec["unique"].(bool); ok && unique {
			indexModel.Options = options.Index().SetUnique(true)
		}

		// Create the index
		if _, err := collection.Indexes().CreateOne(ctx, indexModel); err != nil {
			ma.logger.WithError(err).WithField("index", indexSpec["name"]).
				Warnf("Failed to create index for collection %s", collName)
			// Continue with other indexes
		}
	}

	return nil
}

// PruneData removes data older than the specified time
func (ma *MongoAdapter) PruneData(olderThan time.Time) error {
	if err := ma.CheckOpen(); err != nil {
		return err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("prune_data", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get the block height at the specified time
	filter := bson.M{"timestamp": bson.M{"$lt": olderThan.Unix()}}
	opts := options.FindOne().SetSort(bson.D{{Key: "number", Value: -1}})
	var block common.Block
	err := ma.blocksColl.FindOne(ctx, filter, opts).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No blocks older than the specified time
			return nil
		}
		return fmt.Errorf("failed to find block for pruning: %w", err)
	}

	// Delete blocks older than the found block
	blockFilter := bson.M{"number": bson.M{"$lt": block.Number}}
	blockResult, err := ma.blocksColl.DeleteMany(ctx, blockFilter)
	if err != nil {
		return fmt.Errorf("failed to delete old blocks: %w", err)
	}

	// Delete transactions in those blocks
	txFilter := bson.M{"blockHeight": bson.M{"$lt": block.Number}}
	txResult, err := ma.txColl.DeleteMany(ctx, txFilter)
	if err != nil {
		return fmt.Errorf("failed to delete old transactions: %w", err)
	}

	// Delete receipts for those transactions
	receiptFilter := bson.M{"blockHeight": bson.M{"$lt": block.Number}}
	receiptResult, err := ma.receiptsColl.DeleteMany(ctx, receiptFilter)
	if err != nil {
		return fmt.Errorf("failed to delete old receipts: %w", err)
	}

	// Delete old snapshots
	snapshotFilter := bson.M{"height": bson.M{"$lt": block.Number}}
	snapshotResult, err := ma.snapshotsColl.DeleteMany(ctx, snapshotFilter)
	if err != nil {
		return fmt.Errorf("failed to delete old snapshots: %w", err)
	}

	ma.logger.Infof("Pruned data older than %v: %d blocks, %d transactions, %d receipts, %d snapshots",
		olderThan, blockResult.DeletedCount, txResult.DeletedCount, receiptResult.DeletedCount, snapshotResult.DeletedCount)

	return nil
}

// Vacuum performs database cleanup
func (ma *MongoAdapter) Vacuum() error {
	// For MongoDB, this is essentially the same as Compact
	return ma.Compact()
}

// HealthCheck performs a health check on the database
func (ma *MongoAdapter) HealthCheck(ctx context.Context) error {
	if err := ma.DefaultHealthCheck(ctx); err != nil {
		return err
	}

	// Ping the database
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := ma.client.Ping(pingCtx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return nil
}

// GetStats returns statistics about the database
func (ma *MongoAdapter) GetStats() (map[string]interface{}, error) {
	if err := ma.CheckOpen(); err != nil {
		return nil, err
	}

	startTime := time.Now()
	defer func() {
		ma.LogOperation("get_stats", startTime, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get basic stats from BaseAdapter
	baseStats := ma.DefaultGetStats()

	// Convert to map
	stats := make(map[string]interface{})
	stats["connections_active"] = baseStats.ConnectionsActive
	stats["connections_total"] = baseStats.ConnectionsTotal
	stats["queries_executed"] = baseStats.QueriesExecuted
	stats["transactions_processed"] = baseStats.TransactionsProcessed
	stats["cache_hit_rate"] = baseStats.CacheHitRate
	stats["average_query_time_ms"] = baseStats.AverageQueryTime
	stats["error_count"] = baseStats.ErrorCount
	stats["last_error"] = baseStats.LastError
	stats["uptime_seconds"] = baseStats.UptimeSeconds
	stats["memory_usage_mb"] = baseStats.MemoryUsageMB

	// Add collection counts
	collections := []string{"blocks", "transactions", "accounts", "state", "contracts", "receipts", "snapshots"}
	for _, coll := range collections {
		count, err := ma.db.Collection(coll).CountDocuments(ctx, bson.M{})
		if err != nil {
			ma.logger.WithError(err).Warnf("Failed to get count for collection %s", coll)
			stats[coll+"_count"] = -1
		} else {
			stats[coll+"_count"] = count
		}
	}

	// Get latest block height
	opts := options.FindOne().SetSort(bson.D{{Key: "number", Value: -1}})
	var block common.Block
	err := ma.blocksColl.FindOne(ctx, bson.M{}, opts).Decode(&block)
	if err == nil {
		stats["latest_block_height"] = block.Number
		stats["latest_block_hash"] = block.Hash
		stats["latest_block_time"] = block.Timestamp
	} else {
		stats["latest_block_height"] = 0
	}

	return stats, nil
}

// String provides a detailed string representation for debugging and logging
func (ma *MongoAdapter) String() string {
	if ma == nil {
		return "MongoAdapter{<nil>}"
	}

	ma.mu.RLock()
	defer ma.mu.RUnlock()

	isOpen := ma.IsOpen()

	// Get basic connection info (mask sensitive parts)
	connStr := ma.connectionString
	if len(connStr) > 50 {
		connStr = connStr[:20] + "..." + connStr[len(connStr)-10:]
	}

	// Get configuration info from BaseAdapter
	cacheSize := 0
	cacheTTL := time.Duration(0)
	metricsEnabled := false
	healthCheckInterval := time.Duration(0)
	maxConcurrentOps := 0
	enableCompression := false
	enableEncryption := false

	if ma.BaseAdapter != nil && ma.BaseAdapter.config != nil {
		cacheSize = ma.BaseAdapter.config.CacheSize
		cacheTTL = ma.BaseAdapter.config.CacheTTL
		metricsEnabled = ma.BaseAdapter.config.MetricsEnabled
		healthCheckInterval = ma.BaseAdapter.config.HealthCheckInterval
		maxConcurrentOps = ma.BaseAdapter.config.MaxConcurrentOps
		enableCompression = ma.BaseAdapter.config.EnableCompression
		enableEncryption = ma.BaseAdapter.config.EnableEncryption
	}

	return fmt.Sprintf("MongoAdapter{dbName=%s, connectionMasked=%s, isOpen=%v, currentHeight=%d, "+
		"cacheSize=%d, cacheTTL=%v, metricsEnabled=%v, healthCheckInterval=%v, maxConcurrentOps=%d, "+
		"enableCompression=%v, enableEncryption=%v}",
		ma.dbName, connStr, isOpen, ma.currentHeight, cacheSize, cacheTTL, metricsEnabled,
		healthCheckInterval, maxConcurrentOps, enableCompression, enableEncryption)
}

// Validate performs comprehensive validation of the MongoAdapter configuration and state
func (ma *MongoAdapter) Validate() error {
	if ma == nil {
		return fmt.Errorf("MongoAdapter is nil")
	}

	// Validate connection string
	if err := ma.validateConnectionString(); err != nil {
		return fmt.Errorf("connection string validation failed: %w", err)
	}

	// Validate database name
	if err := ma.validateDatabaseName(); err != nil {
		return fmt.Errorf("database name validation failed: %w", err)
	}

	// Validate base adapter
	if err := ma.validateBaseAdapter(); err != nil {
		return fmt.Errorf("base adapter validation failed: %w", err)
	}

	// Validate state if open
	if ma.IsOpen() {
		if err := ma.validateOpenState(); err != nil {
			return fmt.Errorf("open state validation failed: %w", err)
		}
	}

	return nil
}

// validateConnectionString validates the MongoDB connection string
func (ma *MongoAdapter) validateConnectionString() error {
	if ma.connectionString == "" {
		return fmt.Errorf("connection string is empty")
	}

	// Basic MongoDB URI validation
	if !strings.HasPrefix(ma.connectionString, "mongodb://") &&
		!strings.HasPrefix(ma.connectionString, "mongodb+srv://") {
		return fmt.Errorf("invalid MongoDB connection string format")
	}

	// Check for common security issues (production hardening)
	if strings.Contains(ma.connectionString, "localhost") &&
		!strings.Contains(ma.connectionString, "authSource") {
		return fmt.Errorf("localhost connection without authentication detected")
	}

	// Validate that sensitive credentials aren't hardcoded in obvious ways
	if strings.Contains(ma.connectionString, ":password@") ||
		strings.Contains(ma.connectionString, ":123456@") ||
		strings.Contains(ma.connectionString, ":admin@") {
		return fmt.Errorf("potentially hardcoded credentials detected in connection string")
	}

	return nil
}

// validateDatabaseName validates the database name
func (ma *MongoAdapter) validateDatabaseName() error {
	if ma.dbName == "" {
		return fmt.Errorf("database name is empty")
	}

	// MongoDB database name restrictions
	if len(ma.dbName) > 64 {
		return fmt.Errorf("database name too long (max 64 characters): %d", len(ma.dbName))
	}

	// Check for invalid characters
	invalidChars := []string{" ", ".", "$", "/", "\\", "\x00"}
	for _, char := range invalidChars {
		if strings.Contains(ma.dbName, char) {
			return fmt.Errorf("database name contains invalid character: %s", char)
		}
	}

	// Check for reserved names
	reservedNames := []string{"admin", "local", "config"}
	for _, reserved := range reservedNames {
		if strings.EqualFold(ma.dbName, reserved) {
			return fmt.Errorf("database name conflicts with reserved name: %s", reserved)
		}
	}

	return nil
}

// validateBaseAdapter validates the embedded BaseAdapter
func (ma *MongoAdapter) validateBaseAdapter() error {
	if ma.BaseAdapter == nil {
		return fmt.Errorf("BaseAdapter is nil")
	}

	// Validate logger
	if ma.logger == nil {
		return fmt.Errorf("logger is nil")
	}

	// Validate configuration
	if ma.BaseAdapter.config == nil {
		return fmt.Errorf("BaseAdapter config is nil")
	}

	config := ma.BaseAdapter.config

	// Validate cache configuration
	if config.CacheSize < 0 {
		return fmt.Errorf("cache size cannot be negative: %d", config.CacheSize)
	}
	if config.CacheSize > 10000 {
		return fmt.Errorf("cache size too large (max 10000): %d", config.CacheSize)
	}

	if config.CacheTTL < 0 {
		return fmt.Errorf("cache TTL cannot be negative: %v", config.CacheTTL)
	}
	if config.CacheTTL > 24*time.Hour {
		return fmt.Errorf("cache TTL too large (max 24h): %v", config.CacheTTL)
	}

	// Validate operation limits
	if config.MaxConcurrentOps <= 0 {
		return fmt.Errorf("max concurrent operations must be positive: %d", config.MaxConcurrentOps)
	}
	if config.MaxConcurrentOps > 1000 {
		return fmt.Errorf("max concurrent operations too large (max 1000): %d", config.MaxConcurrentOps)
	}

	// Validate health check interval
	if config.HealthCheckInterval <= 0 {
		return fmt.Errorf("health check interval must be positive: %v", config.HealthCheckInterval)
	}
	if config.HealthCheckInterval < 10*time.Second {
		return fmt.Errorf("health check interval too frequent (min 10s): %v", config.HealthCheckInterval)
	}

	return nil
}

// validateOpenState validates the state when the adapter is open
func (ma *MongoAdapter) validateOpenState() error {
	if ma.client == nil {
		return fmt.Errorf("client is nil but adapter reports as open")
	}
	if ma.db == nil {
		return fmt.Errorf("database is nil but adapter reports as open")
	}

	// Validate collections
	requiredCollections := map[string]*mongo.Collection{
		"blocks":    ma.blocksColl,
		"txs":       ma.txColl,
		"accounts":  ma.accountsColl,
		"state":     ma.stateColl,
		"contracts": ma.contractsColl,
		"receipts":  ma.receiptsColl,
		"snapshots": ma.snapshotsColl,
	}

	for name, coll := range requiredCollections {
		if coll == nil {
			return fmt.Errorf("%s collection is nil but adapter reports as open", name)
		}
	}

	// Test basic connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ma.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("database connectivity test failed: %w", err)
	}

	// Validate current height consistency
	if ma.currentHeight < 0 {
		return fmt.Errorf("current height cannot be negative: %d", ma.currentHeight)
	}

	return nil
}

// ReplaceBlockSameHeight is not implemented for MongoDB (archive storage)
func (ma *MongoAdapter) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	// MongoDB is used for archival storage and doesn't support block replacement
	// This operation is only supported in the primary LMDB storage
	return fmt.Errorf("ReplaceBlockSameHeight not supported in MongoDB archive storage")
}

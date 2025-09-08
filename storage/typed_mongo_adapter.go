// Package storage provides typed MongoDB adapter implementation
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"diamante/types"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TypedMongoAdapter implements storage operations with typed data
type TypedMongoAdapter struct {
	client   *mongo.Client
	database *mongo.Database
	dbName   string
	logger   *logrus.Logger

	// Collections
	blocks       *mongo.Collection
	transactions *mongo.Collection
	state        *mongo.Collection
	contracts    *mongo.Collection
	validators   *mongo.Collection

	// Metrics
	metrics *types.StorageMetrics
}

// NewTypedMongoAdapter creates a new typed MongoDB adapter
func NewTypedMongoAdapter(connectionString string, dbName string, logger *logrus.Logger) (*TypedMongoAdapter, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Create client options
	clientOpts := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(100).
		SetConnectTimeout(30 * time.Second)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database := client.Database(dbName)

	adapter := &TypedMongoAdapter{
		client:       client,
		database:     database,
		dbName:       dbName,
		logger:       logger,
		blocks:       database.Collection("blocks"),
		transactions: database.Collection("transactions"),
		state:        database.Collection("state"),
		contracts:    database.Collection("contracts"),
		validators:   database.Collection("validators"),
		metrics: &types.StorageMetrics{
			ReadOps:   0,
			WriteOps:  0,
			DeleteOps: 0,
			BatchOps:  0,
			QueryOps:  0,
		},
	}

	// Create indexes
	if err := adapter.createIndexes(); err != nil {
		logger.WithError(err).Error("Failed to create indexes")
	}

	return adapter, nil
}

// StoreBlock stores a typed block
func (a *TypedMongoAdapter) StoreBlock(block *TypedBlock) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()

	// Convert to BSON document
	doc := bson.M{
		"height":          block.Height,
		"hash":            block.Hash,
		"previous_hash":   block.PreviousHash,
		"timestamp":       block.Timestamp,
		"proposer":        block.Proposer,
		"state_root":      block.StateRoot,
		"transaction_ids": block.TransactionIDs,
		"validator_set":   block.ValidatorSet,
		"metadata":        a.convertTypedMapToBSON(block.Metadata),
		"created_at":      time.Now(),
	}

	_, err := a.blocks.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}

	// Update metrics
	a.metrics.WriteOps++
	a.metrics.AvgWriteLatency = (a.metrics.AvgWriteLatency + time.Since(start)) / 2

	return nil
}

// GetBlock retrieves a typed block by height
func (a *TypedMongoAdapter) GetBlock(height uint64) (*TypedBlock, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()

	var doc bson.M
	err := a.blocks.FindOne(ctx, bson.M{"height": height}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("block not found at height %d", height)
		}
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// Convert BSON to typed block
	block := &TypedBlock{
		Height:         getUint64(doc, "height"),
		Hash:           getBytes(doc, "hash"),
		PreviousHash:   getBytes(doc, "previous_hash"),
		Timestamp:      getInt64(doc, "timestamp"),
		Proposer:       getString(doc, "proposer"),
		StateRoot:      getBytes(doc, "state_root"),
		TransactionIDs: getStringSlice(doc, "transaction_ids"),
		ValidatorSet:   getBytes(doc, "validator_set"),
		Metadata:       a.convertBSONToTypedMap(doc["metadata"].(bson.M)),
	}

	// Update metrics
	a.metrics.ReadOps++
	a.metrics.AvgReadLatency = (a.metrics.AvgReadLatency + time.Since(start)) / 2

	return block, nil
}

// StoreTransaction stores a typed transaction
func (a *TypedMongoAdapter) StoreTransaction(tx *types.TypedTransaction) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Convert transaction data to BSON
	var dataDoc bson.M
	if tx.Data != nil {
		dataDoc = a.convertTypedTransactionDataToBSON(tx.Data)
	}

	doc := bson.M{
		"id":         tx.ID,
		"type":       tx.Type,
		"from":       tx.From,
		"to":         tx.To,
		"value":      tx.Value,
		"gas_limit":  tx.GasLimit,
		"gas_price":  tx.GasPrice,
		"nonce":      tx.Nonce,
		"data":       dataDoc,
		"signature":  tx.Signature,
		"hash":       tx.Hash,
		"timestamp":  tx.Timestamp,
		"status":     tx.Status,
		"priority":   tx.Priority,
		"created_at": time.Now(),
	}

	_, err := a.transactions.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to store transaction: %w", err)
	}

	a.metrics.WriteOps++
	return nil
}

// QueryTransactions queries transactions with filters
func (a *TypedMongoAdapter) QueryTransactions(filter *types.TransactionFilter) ([]*types.TypedTransaction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build MongoDB filter
	mongoFilter := bson.M{}

	if filter.Type != nil {
		mongoFilter["type"] = *filter.Type
	}
	if filter.Status != nil {
		mongoFilter["status"] = *filter.Status
	}
	if filter.From != "" {
		mongoFilter["from"] = filter.From
	}
	if filter.To != "" {
		mongoFilter["to"] = filter.To
	}
	if filter.StartTime > 0 || filter.EndTime > 0 {
		timeFilter := bson.M{}
		if filter.StartTime > 0 {
			timeFilter["$gte"] = filter.StartTime
		}
		if filter.EndTime > 0 {
			timeFilter["$lte"] = filter.EndTime
		}
		mongoFilter["timestamp"] = timeFilter
	}
	if filter.MinValue > 0 || filter.MaxValue > 0 {
		valueFilter := bson.M{}
		if filter.MinValue > 0 {
			valueFilter["$gte"] = filter.MinValue
		}
		if filter.MaxValue > 0 {
			valueFilter["$lte"] = filter.MaxValue
		}
		mongoFilter["value"] = valueFilter
	}

	// Execute query
	cursor, err := a.transactions.Find(ctx, mongoFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer cursor.Close(ctx)

	// Parse results
	var transactions []*types.TypedTransaction
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		tx := a.convertBSONToTypedTransaction(doc)
		transactions = append(transactions, tx)
	}

	a.metrics.QueryOps++
	return transactions, nil
}

// StoreState stores typed state data
func (a *TypedMongoAdapter) StoreState(entry *types.StateEntry) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := bson.M{
		"key":        entry.Key,
		"value":      a.convertValueToBSON(entry.Value),
		"state_root": entry.StateRoot,
		"proof":      entry.Proof,
		"metadata":   a.convertMetadataToBSON(entry.Metadata),
		"updated_at": time.Now(),
	}

	// Upsert to handle updates
	opts := options.Update().SetUpsert(true)
	_, err := a.state.UpdateOne(
		ctx,
		bson.M{"key": entry.Key},
		bson.M{"$set": doc},
		opts,
	)

	if err != nil {
		return fmt.Errorf("failed to store state: %w", err)
	}

	a.metrics.WriteOps++
	return nil
}

// GetState retrieves typed state data
func (a *TypedMongoAdapter) GetState(key string) (*types.StateEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var doc bson.M
	err := a.state.FindOne(ctx, bson.M{"key": key}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("state not found for key: %s", key)
		}
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	entry := &types.StateEntry{
		Key:       getString(doc, "key"),
		Value:     a.convertBSONToValue(doc["value"].(bson.M)),
		StateRoot: getString(doc, "state_root"),
		Proof:     getBytesSlice(doc, "proof"),
		Metadata:  a.convertBSONToMetadata(doc["metadata"].(bson.M)),
	}

	a.metrics.ReadOps++
	return entry, nil
}

// ExecuteBatch executes a batch of storage operations
func (a *TypedMongoAdapter) ExecuteBatch(batch *types.StorageBatch) error {
	if !batch.Atomic {
		// Non-atomic batch - execute individually
		for _, op := range batch.Operations {
			if err := a.executeBatchOp(op); err != nil {
				a.logger.WithError(err).Error("Batch operation failed")
			}
		}
		a.metrics.BatchOps++
		return nil
	}

	// Atomic batch - use transaction
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := a.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return err
		}

		for _, op := range batch.Operations {
			if err := a.executeBatchOpInTx(sc, op); err != nil {
				return err
			}
		}

		return session.CommitTransaction(sc)
	})

	if err != nil {
		return fmt.Errorf("batch execution failed: %w", err)
	}

	a.metrics.BatchOps++
	return nil
}

// GetMetrics returns storage metrics
func (a *TypedMongoAdapter) GetMetrics() *types.StorageMetrics {
	// Get collection stats
	ctx := context.Background()

	// Count documents
	blockCount, _ := a.blocks.CountDocuments(ctx, bson.M{})
	txCount, _ := a.transactions.CountDocuments(ctx, bson.M{})
	stateCount, _ := a.state.CountDocuments(ctx, bson.M{})

	metrics := *a.metrics
	metrics.KeyCount = uint64(blockCount + txCount + stateCount)

	// Get database stats
	var result bson.M
	if err := a.database.RunCommand(ctx, bson.M{"dbStats": 1}).Decode(&result); err == nil {
		if dataSize, ok := result["dataSize"].(int64); ok {
			metrics.TotalSize = uint64(dataSize)
		}
	}

	return &metrics
}

// Helper methods

func (a *TypedMongoAdapter) createIndexes() error {
	ctx := context.Background()

	// Block indexes
	blockIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "height", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "hash", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "timestamp", Value: -1}}},
	}

	if _, err := a.blocks.Indexes().CreateMany(ctx, blockIndexes); err != nil {
		return fmt.Errorf("failed to create block indexes: %w", err)
	}

	// Transaction indexes
	txIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "hash", Value: 1}}},
		{Keys: bson.D{{Key: "from", Value: 1}}},
		{Keys: bson.D{{Key: "to", Value: 1}}},
		{Keys: bson.D{{Key: "timestamp", Value: -1}}},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	}

	if _, err := a.transactions.Indexes().CreateMany(ctx, txIndexes); err != nil {
		return fmt.Errorf("failed to create transaction indexes: %w", err)
	}

	// State indexes
	stateIndexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "key", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "updated_at", Value: -1}}},
	}

	if _, err := a.state.Indexes().CreateMany(ctx, stateIndexes); err != nil {
		return fmt.Errorf("failed to create state indexes: %w", err)
	}

	return nil
}

func (a *TypedMongoAdapter) convertValueToBSON(value *types.Value) bson.M {
	if value == nil {
		return nil
	}

	return bson.M{
		"type": value.Type,
		"data": value.Data,
	}
}

func (a *TypedMongoAdapter) convertBSONToValue(data bson.M) *types.Value {
	if data == nil {
		return nil
	}

	return &types.Value{
		Type: types.ValueType(getUint8(data, "type")),
		Data: getBytes(data, "data"),
	}
}

func (a *TypedMongoAdapter) convertTypedMapToBSON(tm *types.TypedMap) bson.M {
	if tm == nil {
		return nil
	}

	result := bson.M{}
	for _, key := range tm.Keys() {
		if value, ok := tm.Get(key); ok {
			result[key] = a.convertValueToBSON(value)
		}
	}

	return result
}

func (a *TypedMongoAdapter) convertBSONToTypedMap(data bson.M) *types.TypedMap {
	if data == nil {
		return nil
	}

	tm := types.NewTypedMap()
	for key, value := range data {
		if valueDoc, ok := value.(bson.M); ok {
			if v := a.convertBSONToValue(valueDoc); v != nil {
				tm.Set(key, v)
			}
		}
	}

	return tm
}

func (a *TypedMongoAdapter) convertMetadataToBSON(meta *types.Metadata) bson.M {
	if meta == nil {
		return nil
	}

	attrs := bson.M{}
	for k, v := range meta.Attributes {
		attrs[k] = a.convertValueToBSON(v)
	}

	return bson.M{
		"created":    meta.Created,
		"modified":   meta.Modified,
		"version":    meta.Version,
		"creator":    meta.Creator,
		"modifier":   meta.Modifier,
		"tags":       meta.Tags,
		"attributes": attrs,
	}
}

func (a *TypedMongoAdapter) convertBSONToMetadata(data bson.M) *types.Metadata {
	if data == nil {
		return nil
	}

	meta := &types.Metadata{
		Created:    getTime(data, "created"),
		Modified:   getTime(data, "modified"),
		Version:    getUint64(data, "version"),
		Creator:    getString(data, "creator"),
		Modifier:   getString(data, "modifier"),
		Tags:       getStringMap(data, "tags"),
		Attributes: make(map[string]*types.Value),
	}

	if attrs, ok := data["attributes"].(bson.M); ok {
		for k, v := range attrs {
			if valueDoc, ok := v.(bson.M); ok {
				if value := a.convertBSONToValue(valueDoc); value != nil {
					meta.Attributes[k] = value
				}
			}
		}
	}

	return meta
}

func (a *TypedMongoAdapter) convertTypedTransactionDataToBSON(data *types.TypedTransactionData) bson.M {
	if data == nil {
		return nil
	}

	doc := bson.M{}

	if data.ContractDeploy != nil {
		doc["contract_deploy"] = bson.M{
			"runtime":          data.ContractDeploy.Runtime,
			"byte_code":        data.ContractDeploy.ByteCode,
			"constructor_args": a.convertContractArgsToBSON(data.ContractDeploy.ConstructorArgs),
			"metadata":         a.convertValueMapToBSON(data.ContractDeploy.Metadata),
		}
	}

	if data.ContractCall != nil {
		doc["contract_call"] = bson.M{
			"contract_address": data.ContractCall.ContractAddress,
			"method":           data.ContractCall.Method,
			"arguments":        a.convertContractArgsToBSON(data.ContractCall.Arguments),
			"metadata":         a.convertValueMapToBSON(data.ContractCall.Metadata),
		}
	}

	if data.StakeData != nil {
		// Add stake data conversion if needed
		doc["stake_data"] = data.StakeData
	}

	if data.GovernanceData != nil {
		// Add governance data conversion if needed
		doc["governance_data"] = data.GovernanceData
	}

	if data.RawData != nil {
		doc["raw_data"] = data.RawData
	}

	if data.Metadata != nil {
		doc["metadata"] = a.convertValueMapToBSON(data.Metadata)
	}

	return doc
}

func (a *TypedMongoAdapter) convertContractArgsToBSON(args []*types.ContractArgument) []bson.M {
	if args == nil {
		return nil
	}

	result := make([]bson.M, len(args))
	for i, arg := range args {
		result[i] = bson.M{
			"name":  arg.Name,
			"type":  arg.Type,
			"value": a.convertValueToBSON(arg.Value),
		}
	}

	return result
}

func (a *TypedMongoAdapter) convertValueMapToBSON(m map[string]*types.Value) bson.M {
	if m == nil {
		return nil
	}

	result := bson.M{}
	for k, v := range m {
		result[k] = a.convertValueToBSON(v)
	}

	return result
}

func (a *TypedMongoAdapter) convertBSONToTypedTransaction(doc bson.M) *types.TypedTransaction {
	tx := &types.TypedTransaction{
		Type:      types.TransactionType(getUint8(doc, "type")),
		ID:        getString(doc, "id"),
		From:      getString(doc, "from"),
		To:        getString(doc, "to"),
		Value:     getUint64(doc, "value"),
		GasLimit:  getUint64(doc, "gas_limit"),
		GasPrice:  getUint64(doc, "gas_price"),
		Nonce:     getUint64(doc, "nonce"),
		Signature: getBytes(doc, "signature"),
		Hash:      getBytes(doc, "hash"),
		Timestamp: getInt64(doc, "timestamp"),
		Status:    types.TransactionStatus(getUint8(doc, "status")),
		Priority:  types.TransactionPriority(getUint8(doc, "priority")),
	}

	// Convert transaction data if present
	if dataDoc, ok := doc["data"].(bson.M); ok {
		tx.Data = a.convertBSONToTypedTransactionData(dataDoc)
	}

	return tx
}

func (a *TypedMongoAdapter) convertBSONToTypedTransactionData(doc bson.M) *types.TypedTransactionData {
	if doc == nil {
		return nil
	}

	data := &types.TypedTransactionData{}

	// Convert contract deploy data
	if deployDoc, ok := doc["contract_deploy"].(bson.M); ok {
		data.ContractDeploy = &types.ContractDeployData{
			Runtime:         getString(deployDoc, "runtime"),
			ByteCode:        getBytes(deployDoc, "byte_code"),
			ConstructorArgs: a.convertBSONToContractArgs(deployDoc["constructor_args"].(bson.A)),
			Metadata:        a.convertBSONToValueMap(deployDoc["metadata"].(bson.M)),
		}
	}

	// Convert contract call data
	if callDoc, ok := doc["contract_call"].(bson.M); ok {
		data.ContractCall = &types.ContractCallData{
			ContractAddress: getString(callDoc, "contract_address"),
			Method:          getString(callDoc, "method"),
			Arguments:       a.convertBSONToContractArgs(callDoc["arguments"].(bson.A)),
			Metadata:        a.convertBSONToValueMap(callDoc["metadata"].(bson.M)),
		}
	}

	// Convert stake data
	if stakeDoc, ok := doc["stake_data"]; ok {
		if sd, ok := stakeDoc.(*types.StakeData); ok {
			data.StakeData = sd
		}
	}

	// Convert governance data
	if govDoc, ok := doc["governance_data"]; ok {
		if gd, ok := govDoc.(*types.GovernanceData); ok {
			data.GovernanceData = gd
		}
	}

	// Convert raw data
	data.RawData = getBytes(doc, "raw_data")

	// Convert metadata
	data.Metadata = a.convertBSONToValueMap(doc["metadata"].(bson.M))

	return data
}

func (a *TypedMongoAdapter) convertBSONToValueMap(data bson.M) map[string]*types.Value {
	if data == nil {
		return nil
	}

	result := make(map[string]*types.Value)
	for key, val := range data {
		if valueDoc, ok := val.(bson.M); ok {
			result[key] = a.convertBSONToValue(valueDoc)
		}
	}
	return result
}

func (a *TypedMongoAdapter) convertBSONToContractArgs(data bson.A) []*types.ContractArgument {
	if data == nil {
		return nil
	}

	result := make([]*types.ContractArgument, 0, len(data))
	for _, arg := range data {
		if argDoc, ok := arg.(bson.M); ok {
			if valueDoc, ok := argDoc["value"].(bson.M); ok {
				result = append(result, &types.ContractArgument{
					Name:  getString(argDoc, "name"),
					Type:  types.ValueType(getUint8(argDoc, "type")),
					Value: a.convertBSONToValue(valueDoc),
				})
			}
		}
	}
	return result
}

func (a *TypedMongoAdapter) executeBatchOp(op *types.BatchOperation) error {
	switch op.Type {
	case types.StorageOperationPut:
		if op.Value != nil {
			return a.StoreState(&types.StateEntry{
				Key:   op.Key.ID,
				Value: &types.Value{Type: types.ValueTypeBytes, Data: op.Value.Data},
			})
		}
	case types.StorageOperationDelete:
		ctx := context.Background()
		_, err := a.state.DeleteOne(ctx, bson.M{"key": op.Key.ID})
		if err != nil {
			return err
		}
		a.metrics.DeleteOps++
	}

	return nil
}

func (a *TypedMongoAdapter) executeBatchOpInTx(ctx mongo.SessionContext, op *types.BatchOperation) error {
	// Similar to executeBatchOp but uses session context
	return a.executeBatchOp(op)
}

// Close closes the MongoDB connection
func (a *TypedMongoAdapter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return a.client.Disconnect(ctx)
}

// Backup creates a backup of the database
func (a *TypedMongoAdapter) Backup(path string) error {
	// MongoDB backup implementation using mongodump
	// 1. Check if mongodump is available
	mongodumpPath, err := exec.LookPath("mongodump")
	if err != nil {
		return fmt.Errorf("mongodump tool not found in PATH. Please install MongoDB tools: apt-get install mongodb-database-tools")
	}

	// 2. Create backup directory
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// 3. Get connection string (need to extract from client)
	// For now, we'll require environment variable or config
	connStr := os.Getenv("MONGODB_URI")
	if connStr == "" {
		connStr = "mongodb://localhost:27017" // Default
	}

	// 4. Build mongodump command
	args := []string{
		"--uri", connStr,
		"--db", a.dbName,
		"--out", path,
	}

	// Add authentication if present
	if strings.Contains(connStr, "@") {
		args = append(args, "--authenticationDatabase", "admin")
	}

	// 5. Execute mongodump
	cmd := exec.Command(mongodumpPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongodump failed: %w\nOutput: %s", err, string(output))
	}

	// 6. Create metadata file
	metadata := map[string]interface{}{
		"dbName":      a.dbName,
		"timestamp":   time.Now(),
		"collections": []string{"blocks", "transactions", "state", "contracts", "validators"},
	}

	metadataPath := filepath.Join(path, "metadata.json")
	metadataBytes, _ := json.MarshalIndent(metadata, "", "  ")
	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		a.logger.Warn("Failed to write backup metadata:", err)
	}

	a.logger.Info("Successfully created MongoDB backup at", path)
	return nil
}

// Restore restores the database from a backup
func (a *TypedMongoAdapter) Restore(path string) error {
	// MongoDB restore implementation using mongorestore
	// 1. Validate backup directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", path)
	}

	// 2. Check if mongorestore is available
	mongorestorePath, err := exec.LookPath("mongorestore")
	if err != nil {
		return fmt.Errorf("mongorestore tool not found in PATH. Please install MongoDB tools: apt-get install mongodb-database-tools")
	}

	// 3. Get connection string
	connStr := os.Getenv("MONGODB_URI")
	if connStr == "" {
		connStr = "mongodb://localhost:27017" // Default
	}

	// 4. Build mongorestore command
	args := []string{
		"--uri", connStr,
		"--db", a.dbName,
		"--drop", // Drop existing collections before restore
		"--dir", path,
	}

	// Add authentication if present
	if strings.Contains(connStr, "@") {
		args = append(args, "--authenticationDatabase", "admin")
	}

	// 5. Execute mongorestore
	cmd := exec.Command(mongorestorePath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongorestore failed: %w\nOutput: %s", err, string(output))
	}

	// 6. Validate restored data by checking block count
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	count, err := a.blocks.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to validate restored data: %w", err)
	}

	a.logger.Info("Successfully restored MongoDB database from", path, "with", count, "blocks")
	return nil
}

// Open opens the database connection (already done in constructor)
func (a *TypedMongoAdapter) Open() error {
	// Connection is already established in NewTypedMongoAdapter
	return nil
}

// Compact compacts the database
func (a *TypedMongoAdapter) Compact() error {
	// MongoDB handles compaction automatically
	// Could run compact command if needed
	return nil
}

// Vacuum vacuums the database
func (a *TypedMongoAdapter) Vacuum() error {
	// MongoDB doesn't have a vacuum operation like SQLite
	return nil
}

// HealthCheck checks the health of the database
func (a *TypedMongoAdapter) HealthCheck(ctx context.Context) error {
	return a.client.Ping(ctx, nil)
}

// GetStats returns database statistics
func (a *TypedMongoAdapter) GetStats() (*StoreStats, error) {
	stats := &StoreStats{
		DatabaseType:     "MongoDB",
		DatabaseVersion:  "Unknown",
		ConnectionPool:   100,
		BlockCount:       int64(a.getBlockCount()),
		TransactionCount: int64(a.getTransactionCount()),
		IsHealthy:        true,
		IsSyncing:        false,
	}
	return stats, nil
}

// TypedBlock represents a blockchain block with typed data
type TypedBlock struct {
	Height         uint64          `json:"height"`
	Hash           []byte          `json:"hash"`
	PreviousHash   []byte          `json:"previous_hash"`
	Timestamp      int64           `json:"timestamp"`
	Proposer       string          `json:"proposer"`
	StateRoot      []byte          `json:"state_root"`
	TransactionIDs []string        `json:"transaction_ids"`
	ValidatorSet   []byte          `json:"validator_set"`
	Metadata       *types.TypedMap `json:"metadata"`
}

// Helper functions for BSON extraction
func getString(doc bson.M, key string) string {
	if v, ok := doc[key].(string); ok {
		return v
	}
	return ""
}

func getUint64(doc bson.M, key string) uint64 {
	switch v := doc[key].(type) {
	case int64:
		return uint64(v)
	case int32:
		return uint64(v)
	case float64:
		return uint64(v)
	}
	return 0
}

func getInt64(doc bson.M, key string) int64 {
	switch v := doc[key].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}

func getUint8(doc bson.M, key string) uint8 {
	return uint8(getUint64(doc, key))
}

func getBytes(doc bson.M, key string) []byte {
	switch v := doc[key].(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case primitive.Binary:
		return v.Data
	}
	return nil
}

func getStringSlice(doc bson.M, key string) []string {
	if v, ok := doc[key].(bson.A); ok {
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

func getBytesSlice(doc bson.M, key string) [][]byte {
	if v, ok := doc[key].(bson.A); ok {
		result := make([][]byte, 0, len(v))
		for _, item := range v {
			switch b := item.(type) {
			case []byte:
				result = append(result, b)
			case string:
				result = append(result, []byte(b))
			case primitive.Binary:
				result = append(result, b.Data)
			}
		}
		return result
	}
	return nil
}

func getTime(doc bson.M, key string) time.Time {
	switch v := doc[key].(type) {
	case time.Time:
		return v
	case int64:
		return time.Unix(v, 0)
	}
	return time.Time{}
}

func getStringMap(doc bson.M, key string) map[string]string {
	result := make(map[string]string)

	if m, ok := doc[key].(bson.M); ok {
		for k, v := range m {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
	}

	return result
}

// getBlockCount returns the current block count from the database
func (a *TypedMongoAdapter) getBlockCount() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count, err := a.blocks.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0
	}
	return uint64(count)
}

// getTransactionCount returns the current transaction count from the database
func (a *TypedMongoAdapter) getTransactionCount() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count, err := a.transactions.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0
	}
	return uint64(count)
}

// GetLatestBlockHeight returns the height of the latest block in the database
func (a *TypedMongoAdapter) GetLatestBlockHeight() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Find the block with the highest height
	opts := options.FindOne().SetSort(bson.D{{Key: "height", Value: -1}})
	var doc bson.M
	err := a.blocks.FindOne(ctx, bson.M{}, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, fmt.Errorf("no blocks found")
		}
		return 0, fmt.Errorf("failed to get latest block height: %w", err)
	}

	return getUint64(doc, "height"), nil
}

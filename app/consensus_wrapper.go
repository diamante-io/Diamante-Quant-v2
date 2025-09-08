package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus/types"
	"diamante/storage"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ConsensusWrapper wraps different consensus mechanisms to satisfy the
// types.Consensus interface used across the application.
type ConsensusWrapper struct {
	lachesis types.Lachesis
	dpos     types.DPoS
	poh      types.PoH
	logger   *logrus.Logger
}

func NewConsensusWrapper(l types.Lachesis, d types.DPoS, p types.PoH, logger *logrus.Logger) *ConsensusWrapper {
	if logger == nil {
		logger = logrus.New()
	}
	return &ConsensusWrapper{lachesis: l, dpos: d, poh: p, logger: logger}
}

func (cw *ConsensusWrapper) GetNetworkLoad() float64 {
	return cw.lachesis.GetNetworkLoad()
}

func (cw *ConsensusWrapper) GetLachesis() types.Lachesis {
	return cw.lachesis
}

func (cw *ConsensusWrapper) GetDPoS() types.DPoS {
	return cw.dpos
}

func (cw *ConsensusWrapper) GetPoH() types.PoH {
	return cw.poh
}

func (cw *ConsensusWrapper) Start() error {
	if starter, ok := cw.lachesis.(interface{ Start() error }); ok {
		return starter.Start()
	}
	return nil
}

func (cw *ConsensusWrapper) Stop() error {
	if stopper, ok := cw.lachesis.(interface{ Stop() error }); ok {
		return stopper.Stop()
	}
	return nil
}

func (cw *ConsensusWrapper) ProcessBlock(blockNumber uint64) error {
	if blockNumber == 0 {
		return fmt.Errorf("invalid block number: %d", blockNumber)
	}

	// 1. Get the next validator for block production
	nextValidator := cw.dpos.GetNextValidator(blockNumber, [32]byte{})
	if nextValidator == nil {
		return fmt.Errorf("no validator available for block %d", blockNumber)
	}

	// 2. Gather pending events from Lachesis
	events := cw.GetPendingEvents()
	if events == nil {
		events = []*types.Event{} // Create empty slice if nil
	}

	// 3. Build PoH data from events and create proof
	var buf bytes.Buffer
	buf.Write([]byte(fmt.Sprintf("block_%d", blockNumber)))
	for _, ev := range events {
		buf.Write(ev.Data)
		buf.Write(ev.ID[:])
	}
	cw.poh.Record(buf.Bytes())

	// 4. Finalize events through Lachesis
	finalizedCount := 0
	for _, ev := range events {
		if cw.lachesis.ProcessEvent(ev) {
			finalizedCount++
		}
	}

	// 5. Update DPoS epoch
	if err := cw.dpos.ProcessEpoch(blockNumber); err != nil {
		return fmt.Errorf("dpos epoch processing failed: %w", err)
	}

	// 6. Log block production success (if logger available)
	if logger := cw.getLogger(); logger != nil {
		logger.WithFields(logrus.Fields{
			"blockNumber":    blockNumber,
			"producer":       fmt.Sprintf("%x", nextValidator.ID),
			"eventCount":     len(events),
			"finalizedCount": finalizedCount,
			"pohCount":       cw.poh.GetCount(),
		}).Info("Block processed successfully")
	}

	return nil
}

func (cw *ConsensusWrapper) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	return cw.lachesis.CreateEvent(creator, parentIDs, data)
}

func (cw *ConsensusWrapper) FinalizeEvent(event *types.Event) (bool, error) {
	if cw.lachesis.ProcessEvent(event) {
		return true, nil
	}
	return false, fmt.Errorf("failed to finalize event")
}

func (cw *ConsensusWrapper) SynchronizeState(targetState [32]byte, targetCount uint64) error {
	// Pause consensus by stopping Lachesis if running
	if err := cw.Stop(); err != nil {
		return err
	}
	defer cw.Start()

	// Synchronize PoH state
	if err := cw.poh.Synchronize(targetState, targetCount); err != nil {
		return fmt.Errorf("poh sync failed: %w", err)
	}

	// Attempt to restore Lachesis and DPoS states from their own snapshots
	if state, err := cw.lachesis.GetState(); err == nil {
		if restoreErr := cw.lachesis.RestoreState(state); restoreErr != nil {
			cw.logger.WithError(restoreErr).Warn("Failed to restore Lachesis state")
		}
	}
	if state, err := cw.dpos.GetState(); err == nil {
		if restoreErr := cw.dpos.RestoreState(state); restoreErr != nil {
			cw.logger.WithError(restoreErr).Warn("Failed to restore DPoS state")
		}
	}

	// Replay finalized events up to target count
	events, err := cw.lachesis.GetFinalizedEvents(0, targetCount)
	if err != nil {
		return fmt.Errorf("event fetch failed: %w", err)
	}
	for _, ev := range events {
		if _, err := cw.FinalizeEvent(ev); err != nil {
			return err
		}
	}

	return nil
}

func (cw *ConsensusWrapper) GetValidators() []*types.Validator {
	return cw.dpos.GetValidators()
}

func (cw *ConsensusWrapper) GetActiveValidators() []*types.Validator {
	return cw.dpos.GetActiveValidators()
}

func (cw *ConsensusWrapper) GetPendingEvents() []*types.Event {
	if getter, ok := cw.lachesis.(interface{ GetPendingEvents() []*types.Event }); ok {
		return getter.GetPendingEvents()
	}
	return nil
}

func (cw *ConsensusWrapper) GetFinalizedEvents(fromHeight, toHeight uint64) ([]*types.Event, error) {
	return cw.lachesis.GetFinalizedEvents(fromHeight, toHeight)
}

func (cw *ConsensusWrapper) GetState() ([]byte, error) {
	return cw.lachesis.GetState()
}

func (cw *ConsensusWrapper) RestoreState(state []byte) error {
	return cw.lachesis.RestoreState(state)
}

// getLogger attempts to get a logger from the consensus components
func (cw *ConsensusWrapper) getLogger() *logrus.Logger {
	// Check if lachesis is not nil before attempting type assertion
	if cw.lachesis != nil {
		if loggerGetter, ok := cw.lachesis.(interface{ GetLogger() *logrus.Logger }); ok {
			if logger := loggerGetter.GetLogger(); logger != nil {
				return logger
			}
		}
	}

	// Check if dpos is not nil before attempting type assertion
	if cw.dpos != nil {
		if loggerGetter, ok := cw.dpos.(interface{ GetLogger() *logrus.Logger }); ok {
			if logger := loggerGetter.GetLogger(); logger != nil {
				return logger
			}
		}
	}

	// Return nil if no logger is available
	return nil
}

// GetCurrentBlockHeight returns the current block height
func (cw *ConsensusWrapper) GetCurrentBlockHeight() (uint64, error) {
	// Check if dpos is not nil before attempting to get height
	if cw.dpos == nil {
		return 0, fmt.Errorf("dpos component not initialized")
	}

	// Try to get height from DPoS component
	if heightGetter, ok := cw.dpos.(interface{ GetCurrentHeight() uint64 }); ok {
		return heightGetter.GetCurrentHeight(), nil
	}

	// Try alternative method names
	if heightGetter, ok := cw.dpos.(interface{ GetBlockHeight() uint64 }); ok {
		return heightGetter.GetBlockHeight(), nil
	}

	// Return error if height not available
	return 0, fmt.Errorf("block height not available from consensus")
}

// MongoLedgerWrapper wraps the MongoLedger to implement any missing methods from common.LedgerAPI
// that are not provided directly by the storage package.
// Deprecated: This wrapper adds unnecessary complexity and will be removed.
// Use storage.MongoDBLedger directly instead.
type MongoLedgerWrapper struct {
	*storage.MongoDBLedger
}

func NewMongoLedgerWrapper(l *storage.MongoDBLedger) *MongoLedgerWrapper {
	return &MongoLedgerWrapper{MongoDBLedger: l}
}

func (mlw *MongoLedgerWrapper) getCollection(field string) (*mongo.Collection, error) {
	var coll *mongo.Collection
	switch field {
	case "accountsColl":
		coll = mlw.MongoDBLedger.AccountsCollection()
	case "blocksColl":
		coll = mlw.MongoDBLedger.BlocksCollection()
	case "transactionsColl":
		coll = mlw.MongoDBLedger.TransactionsCollection()
	case "snapshotsColl":
		coll = nil // snapshots not implemented
	case "contractsColl":
		coll = nil // contracts collection not directly accessible
	default:
		return nil, fmt.Errorf("%s not available", field)
	}
	if coll == nil {
		return nil, fmt.Errorf("%s not available", field)
	}
	return coll, nil
}

func (mlw *MongoLedgerWrapper) getClient() (*mongo.Client, error) {
	client := (*mongo.Client)(nil) // client not accessible due to private field
	if client == nil {
		return nil, fmt.Errorf("mongo client not available")
	}
	return client, nil
}

func (mlw *MongoLedgerWrapper) Close() error {
	return nil
}

func (mlw *MongoLedgerWrapper) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	coll, err := mlw.getCollection("transactionsColl")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"$or": []bson.M{{"from": accountID}, {"to": accountID}}}
	opts := options.Find().SetSort(bson.D{{Key: "blockHeight", Value: -1}}).SetLimit(int64(limit)).SetSkip(int64(offset))

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var txs []common.Transaction
	if err := cursor.All(ctx, &txs); err != nil {
		return nil, err
	}

	return txs, nil
}

func (mlw *MongoLedgerWrapper) GetBlockHeight() (int, error) {
	v := reflect.ValueOf(mlw.MongoDBLedger).Elem().FieldByName("currentHeight")
	if !v.IsValid() {
		return 0, fmt.Errorf("currentHeight not available")
	}
	return int(v.Int()), nil
}

func (mlw *MongoLedgerWrapper) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	coll, err := mlw.getCollection("blocksColl")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"blockHeight": bson.M{"$gte": startNum, "$lte": endNum}}
	opts := options.Find().SetSort(bson.D{{Key: "blockHeight", Value: 1}})
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var blocks []common.Block
	if err := cursor.All(ctx, &blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

func (mlw *MongoLedgerWrapper) GetTransaction(txID string) (*common.Transaction, error) {
	coll, err := mlw.getCollection("transactionsColl")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tx common.Transaction
	err = coll.FindOne(ctx, bson.M{"txHash": txID}).Decode(&tx)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("transaction not found")
	}
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (mlw *MongoLedgerWrapper) HealthCheck(ctx context.Context) error {
	client, err := mlw.getClient()
	if err != nil {
		return err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return err
	}
	return nil
}

func (mlw *MongoLedgerWrapper) GetStats() (map[string]interface{}, error) {
	accountsColl, err := mlw.getCollection("accountsColl")
	if err != nil {
		return nil, err
	}
	blocksColl, err := mlw.getCollection("blocksColl")
	if err != nil {
		return nil, err
	}
	txColl, err := mlw.getCollection("transactionsColl")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	acCount, _ := accountsColl.CountDocuments(ctx, bson.M{})
	blkCount, _ := blocksColl.CountDocuments(ctx, bson.M{})
	txCount, _ := txColl.CountDocuments(ctx, bson.M{})
	height, _ := mlw.GetBlockHeight()

	stats := map[string]interface{}{
		"accounts":       acCount,
		"blocks":         blkCount,
		"transactions":   txCount,
		"current_height": height,
	}
	return stats, nil
}

func (mlw *MongoLedgerWrapper) GetLastBlockHash() (string, error) {
	coll, err := mlw.getCollection("blocksColl")
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.FindOne().SetSort(bson.D{{Key: "blockHeight", Value: -1}})
	var result bson.M
	err = coll.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", fmt.Errorf("no blocks found")
		}
		return "", err
	}
	hash, _ := result["hash"].(string)
	return hash, nil
}

func (mlw *MongoLedgerWrapper) CreateAccount(ac *common.Account) error {
	return mlw.MongoDBLedger.CreateAccount(ac)
}

func (mlw *MongoLedgerWrapper) UpdateAccount(ac *common.Account) error {
	return mlw.MongoDBLedger.UpdateAccount(ac)
}

func (mlw *MongoLedgerWrapper) GetBalance(accountID string) (float64, error) {
	return mlw.MongoDBLedger.GetBalance(accountID)
}

func (mlw *MongoLedgerWrapper) UpdateAccountBalance(accountID string, amount float64) error {
	return mlw.UpdateAccountBalance(accountID, amount)
}

func (mlw *MongoLedgerWrapper) AddTransaction(tx common.Transaction) error {
	return mlw.MongoDBLedger.AddTransaction(tx)
}

func (mlw *MongoLedgerWrapper) IsTransactionCommitted(txID string) bool {
	return mlw.MongoDBLedger.IsTransactionCommitted(txID)
}

func (mlw *MongoLedgerWrapper) CommitBlock(block common.Block) error {
	return mlw.MongoDBLedger.CommitBlock(block)
}

func (mlw *MongoLedgerWrapper) DeploySmartContract(sc *common.SmartContract) error {
	return mlw.MongoDBLedger.DeploySmartContract(sc)
}

func (mlw *MongoLedgerWrapper) UpdateSmartContract(contractID, newCode, version string) error {
	return mlw.UpdateSmartContract(contractID, newCode, version)
}

func (mlw *MongoLedgerWrapper) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	return mlw.ExecuteSmartContract(scID, function, sender, params)
}

func (mlw *MongoLedgerWrapper) RemoveSmartContract(contractID string) error {
	return mlw.RemoveSmartContract(contractID)
}

func (mlw *MongoLedgerWrapper) IntegrityCheck() error {
	return mlw.IntegrityCheck()
}

// StoreWrapper wraps the MongoStore to implement the storage.Store interface
// used by consensus and other components.
type StoreWrapper struct {
	*storage.MongoStore
	BlockPersistence *storage.BlockPersistence
	logger           *logrus.Logger
}

func NewStoreWrapper(s *storage.MongoStore, logger *logrus.Logger) *StoreWrapper {
	return &StoreWrapper{MongoStore: s, logger: logger}
}

func (sw *StoreWrapper) GetBlock(blockNumber uint64) (*storage.Block, error) {
	commonBlock, err := sw.MongoStore.GetBlock(blockNumber)
	if err != nil {
		return nil, err
	}

	storageBlock := &storage.Block{
		Number:        blockNumber,
		Timestamp:     time.Unix(commonBlock.Timestamp, 0),
		PrevBlockHash: commonBlock.PreviousHash,
		BlockHash:     commonBlock.Hash,
	}

	return storageBlock, nil
}

func (sw *StoreWrapper) SaveBlock(block *storage.Block) error {
	commonBlock := &common.Block{
		Number:       int(block.Number),
		Hash:         block.BlockHash,
		PreviousHash: block.PrevBlockHash,
		Timestamp:    block.Timestamp.Unix(),
	}

	if err := sw.MongoStore.SaveBlock(commonBlock); err != nil {
		return err
	}

	if sw.BlockPersistence != nil {
		if err := sw.BlockPersistence.SaveBlock(commonBlock); err != nil {
			if sw.logger != nil {
				sw.logger.Errorf("failed to persist block to filesystem: %v", err)
			}
		}
	}
	return nil
}

func CreateMockLedger(logger *logrus.Logger) *storage.MockMongoLedger {
	logger.Info("Creating mock ledger for testing")
	return storage.NewMockMongoLedger()
}

func CreateMockStore(logger *logrus.Logger) *storage.MockMongoStore {
	logger.Info("Creating mock store for testing")
	return storage.NewMockMongoStore()
}

// GovernanceConsensusAdapter adapts ConsensusWrapper to the governance package.
type GovernanceConsensusAdapter struct {
	consensus         *ConsensusWrapper
	currentHeight     uint64
	scheduledUpgrades map[uint64]string
}

func NewGovernanceConsensusAdapter(consensus *ConsensusWrapper, height uint64) *GovernanceConsensusAdapter {
	return &GovernanceConsensusAdapter{consensus: consensus, currentHeight: height}
}

func (gca *GovernanceConsensusAdapter) GetDPoS() types.DPoS {
	return gca.consensus.GetDPoS()
}

func (gca *GovernanceConsensusAdapter) GetLachesis() types.Lachesis {
	return gca.consensus.GetLachesis()
}

func (gca *GovernanceConsensusAdapter) GetCurrentHeight() uint64 {
	return gca.currentHeight
}

func (gca *GovernanceConsensusAdapter) ScheduleUpgrade(version string, height uint64) error {
	if gca.scheduledUpgrades == nil {
		gca.scheduledUpgrades = make(map[uint64]string)
	}
	gca.scheduledUpgrades[height] = version
	return nil
}

// GovernanceLoggerAdapter adapts logrus.Logger to governance.Logger.
type GovernanceLoggerAdapter struct {
	logger *logrus.Logger
}

func NewGovernanceLoggerAdapter(logger *logrus.Logger) *GovernanceLoggerAdapter {
	return &GovernanceLoggerAdapter{logger: logger}
}

func (gla *GovernanceLoggerAdapter) Info(msg string, keyvals ...interface{}) {
	gla.logger.WithFields(logrusFieldsFromKeyvals(keyvals...)).Info(msg)
}

func (gla *GovernanceLoggerAdapter) Error(msg string, keyvals ...interface{}) {
	gla.logger.WithFields(logrusFieldsFromKeyvals(keyvals...)).Error(msg)
}

func logrusFieldsFromKeyvals(keyvals ...interface{}) logrus.Fields {
	fields := logrus.Fields{}
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key, ok := keyvals[i].(string)
			if !ok {
				key = fmt.Sprintf("%v", keyvals[i])
			}
			fields[key] = keyvals[i+1]
		}
	}
	return fields
}

func InitLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339})
	logger.SetLevel(logrus.InfoLevel)
	return logger
}

// ConsensusEngineAdapter adapts ConsensusWrapper to implement the ConsensusEngine interface
type ConsensusEngineAdapter struct {
	wrapper *ConsensusWrapper
	logger  *logrus.Logger
	running bool
	mu      sync.RWMutex
}

// NewConsensusEngineAdapter creates a new adapter
func NewConsensusEngineAdapter(wrapper *ConsensusWrapper, logger *logrus.Logger) *ConsensusEngineAdapter {
	if logger == nil {
		logger = logrus.New()
	}
	return &ConsensusEngineAdapter{
		wrapper: wrapper,
		logger:  logger,
		running: false,
	}
}

// ProcessBlock processes a block through consensus
func (cea *ConsensusEngineAdapter) ProcessBlock(block *common.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}
	// Convert block to block number for the wrapper
	return cea.wrapper.ProcessBlock(uint64(block.Number))
}

// ValidateBlock validates a block
func (cea *ConsensusEngineAdapter) ValidateBlock(block *common.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	// Basic validation
	if block.Number <= 0 {
		return fmt.Errorf("invalid block number: %d", block.Number)
	}

	if block.Hash == "" {
		return fmt.Errorf("block hash cannot be empty")
	}

	if block.Timestamp <= 0 {
		return fmt.Errorf("invalid block timestamp: %d", block.Timestamp)
	}

	// Validate transactions
	for i, tx := range block.Transactions {
		if err := tx.Validate(); err != nil {
			return fmt.Errorf("invalid transaction at index %d: %w", i, err)
		}
	}

	return nil
}

// GetCurrentHeight returns the current blockchain height
func (cea *ConsensusEngineAdapter) GetCurrentHeight() uint64 {
	height, err := cea.wrapper.GetCurrentBlockHeight()
	if err != nil {
		cea.logger.WithError(err).Warn("Failed to get current block height")
		return 0
	}
	return height
}

// IsRunning checks if the consensus engine is running
func (cea *ConsensusEngineAdapter) IsRunning() bool {
	cea.mu.RLock()
	defer cea.mu.RUnlock()
	return cea.running
}

// Start starts the consensus engine
func (cea *ConsensusEngineAdapter) Start() error {
	cea.mu.Lock()
	defer cea.mu.Unlock()

	if cea.running {
		return nil
	}

	if err := cea.wrapper.Start(); err != nil {
		return fmt.Errorf("failed to start consensus wrapper: %w", err)
	}

	cea.running = true
	return nil
}

// Stop stops the consensus engine
func (cea *ConsensusEngineAdapter) Stop() error {
	cea.mu.Lock()
	defer cea.mu.Unlock()

	if !cea.running {
		return nil
	}

	if err := cea.wrapper.Stop(); err != nil {
		return fmt.Errorf("failed to stop consensus wrapper: %w", err)
	}

	cea.running = false
	return nil
}

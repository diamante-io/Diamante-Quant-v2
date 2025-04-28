// main.go
package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"diamante/api"
	"diamante/common"
	finality "diamante/consensus/diamantefinality" // package is named "finality"
	"diamante/consensus/diamantepoh"
	"diamante/consensus/diamantepos"
	"diamante/consensus/types"
	"diamante/network"
	"diamante/storage"
	"diamante/transaction"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

// ConsensusWrapper Implementation
type ConsensusWrapper struct {
	lachesis *finality.Lachesis
	dpos     types.DPoS
	poh      types.PoH
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
	return cw.lachesis.Start()
}

func (cw *ConsensusWrapper) Stop() error {
	return cw.lachesis.Stop()
}

func (cw *ConsensusWrapper) ProcessBlock(blockNumber uint64) error {
	// Stub implementation.
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
	// Stub implementation.
	return nil
}

func (cw *ConsensusWrapper) GetValidators() []*types.Validator {
	return cw.dpos.GetValidators()
}

func (cw *ConsensusWrapper) GetActiveValidators() []*types.Validator {
	return cw.dpos.GetActiveValidators()
}

func (cw *ConsensusWrapper) GetPendingEvents() []*types.Event {
	return cw.lachesis.GetPendingEvents()
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

func NewConsensusWrapper(l *finality.Lachesis, d types.DPoS, p types.PoH) types.Consensus {
	return &ConsensusWrapper{
		lachesis: l,
		dpos:     d,
		poh:      p,
	}
}

// MongoLedgerWrapper wraps the MongoLedger to implement any missing methods from common.LedgerAPI
type MongoLedgerWrapper struct {
	*storage.MongoLedger
}

// Close implements the Close method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) Close() error {
	// MongoDB connections are managed by the driver, so we don't need to explicitly close them
	return nil
}

// GetAccountTransactions implements the GetAccountTransactions method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	// This is a stub implementation for the alpha testnet
	// In a production environment, this would query the ledger for transactions
	return []common.Transaction{}, nil
}

// GetBlockHeight implements the GetBlockHeight method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetBlockHeight() (int, error) {
	// This is a stub implementation for the alpha testnet
	// In a production environment, this would query the ledger for the current block height
	return 0, nil
}

// GetBlocksByRange implements the GetBlocksByRange method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	// This is a stub implementation for the alpha testnet
	// In a production environment, this would query the ledger for blocks in the given range
	return []common.Block{}, nil
}

// GetTransaction implements the GetTransaction method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetTransaction(txID string) (*common.Transaction, error) {
	// This is a stub implementation for the alpha testnet
	return nil, fmt.Errorf("transaction not found")
}

// HealthCheck implements the HealthCheck method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) HealthCheck(ctx context.Context) error {
	// This is a stub implementation for the alpha testnet
	return nil
}

// GetStats implements the GetStats method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetStats() (map[string]interface{}, error) {
	// This is a stub implementation for the alpha testnet
	return map[string]interface{}{}, nil
}

// GetLastBlockHash implements the GetLastBlockHash method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetLastBlockHash() (string, error) {
	// This is a stub implementation for the alpha testnet
	return "0000000000000000000000000000000000000000000000000000000000000000", nil
}

// CreateAccount implements the CreateAccount method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) CreateAccount(ac *common.Account) error {
	return mlw.MongoLedger.CreateAccount(ac)
}

// UpdateAccount implements the UpdateAccount method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) UpdateAccount(ac *common.Account) error {
	return mlw.MongoLedger.UpdateAccount(ac)
}

// GetBalance implements the GetBalance method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) GetBalance(accountID string) (float64, error) {
	return mlw.MongoLedger.GetBalance(accountID)
}

// UpdateAccountBalance implements the UpdateAccountBalance method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) UpdateAccountBalance(accountID string, amount float64) error {
	return mlw.MongoLedger.UpdateAccountBalance(accountID, amount)
}

// AddTransaction implements the AddTransaction method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) AddTransaction(tx common.Transaction) error {
	return mlw.MongoLedger.AddTransaction(tx)
}

// IsTransactionCommitted implements the IsTransactionCommitted method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) IsTransactionCommitted(txID string) bool {
	return mlw.MongoLedger.IsTransactionCommitted(txID)
}

// CommitBlock implements the CommitBlock method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) CommitBlock(block common.Block) error {
	return mlw.MongoLedger.CommitBlock(block)
}

// DeploySmartContract implements the DeploySmartContract method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) DeploySmartContract(sc *common.SmartContract) error {
	return mlw.MongoLedger.DeploySmartContract(sc)
}

// ExecuteSmartContract implements the ExecuteSmartContract method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	return mlw.MongoLedger.ExecuteSmartContract(scID, function, sender, params)
}

// RemoveSmartContract implements the RemoveSmartContract method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) RemoveSmartContract(contractID string) error {
	return mlw.MongoLedger.RemoveSmartContract(contractID)
}

// IntegrityCheck implements the IntegrityCheck method required by common.LedgerAPI
func (mlw *MongoLedgerWrapper) IntegrityCheck() error {
	return mlw.MongoLedger.IntegrityCheck()
}

// StoreWrapper wraps the MongoStore to implement the storage.Store interface
type StoreWrapper struct {
	*storage.MongoStore
}

// GetBlock implements the GetBlock method required by storage.Store
func (sw *StoreWrapper) GetBlock(blockNumber uint64) (*storage.Block, error) {
	commonBlock, err := sw.MongoStore.GetBlock(blockNumber)
	if err != nil {
		return nil, err
	}

	// Convert common.Block to storage.Block
	storageBlock := &storage.Block{
		Number:        blockNumber,
		Timestamp:     time.Unix(commonBlock.Timestamp, 0),
		PrevBlockHash: commonBlock.PreviousHash,
		BlockHash:     commonBlock.Hash,
	}

	return storageBlock, nil
}

// SaveBlock implements the SaveBlock method required by storage.Store
func (sw *StoreWrapper) SaveBlock(block *storage.Block) error {
	// Convert storage.Block to common.Block
	commonBlock := &common.Block{
		Number:       int(block.Number),
		Hash:         block.BlockHash,
		PreviousHash: block.PrevBlockHash,
		Timestamp:    block.Timestamp.Unix(),
	}

	return sw.MongoStore.SaveBlock(commonBlock)
}

// createMockLedger creates an in-memory mock ledger for testing purposes
func createMockLedger(logger *logrus.Logger) *storage.MongoLedger {
	// This is a simplified mock implementation that satisfies the MongoLedger interface
	logger.Info("Creating mock ledger for testing")

	// Use the mock implementation from storage package
	return storage.NewMockMongoLedger()
}

// createMockStore creates an in-memory mock store for testing purposes
func createMockStore(logger *logrus.Logger) *storage.MongoStore {
	// This is a simplified mock implementation that satisfies the MongoStore interface
	logger.Info("Creating mock store for testing")

	// Use the mock implementation from storage package
	return storage.NewMockMongoStore()
}

// Main Function
func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: Error loading .env file: %v\n", err)
	}

	// Initialize logger.
	logger := logrus.New()
	logger.Info("Starting Diamante Testnet...")

	// MongoDB connection settings
	// NOTE: Replace these with your actual MongoDB credentials
	mongoURI := "mongodb://localhost:27017"
	dbName := "diamante_testnet"

	// Initialize the MongoDB-based ledger
	logger.Info("Connecting to MongoDB ledger...")
	logger.Infof("Using MongoDB URI: %s, Database: %s", mongoURI, dbName)

	// For testing purposes, we'll create a mock ledger if MongoDB is not available
	var mongoLedger *storage.MongoLedger
	var err error

	mongoLedger, err = storage.NewMongoLedger(mongoURI, dbName)
	if err != nil {
		logger.Warnf("Failed to connect to MongoDB: %v", err)
		logger.Info("Using in-memory mock ledger for testing")
		// Create a mock ledger for testing
		mongoLedger = createMockLedger(logger)
	} else {
		logger.Info("Successfully connected to MongoDB")
	}

	// Wrap the MongoDB ledger to implement any missing methods
	ledgerWrapper := &MongoLedgerWrapper{mongoLedger}

	// Initialize the MongoDB-based storage for blocks
	logger.Info("Connecting to MongoDB store...")
	var mongoStore *storage.MongoStore

	mongoStore, err = storage.NewMongoStore(mongoURI, dbName)
	if err != nil {
		logger.Warnf("Failed to connect to MongoDB store: %v", err)
		logger.Info("Using in-memory mock store for testing")
		// Create a mock store for testing
		mongoStore = createMockStore(logger)
	} else {
		logger.Info("Successfully connected to MongoDB store")
	}

	// Wrap the MongoStore to implement the storage.Store interface
	storeWrapper := &StoreWrapper{mongoStore}

	// Create a logger adapter that implements the required Logger interfaces
	loggerAdapter := NewLoggerAdapter(logger)

	// Initialize network discovery and network manager.
	localAddr := "0.0.0.0:30303"
	seeds := []string{"127.0.0.1:30303"}
	discovery := network.NewBasicDiscovery(seeds, 10*time.Second, nil)
	nm := network.NewNetworkManager(localAddr, discovery)
	discovery.SetNetworkManager(nm)

	if err := nm.Start(); err != nil {
		logger.Fatalf("Failed to start network manager: %v", err)
	}
	defer nm.Stop()

	// Initialize transaction pool and manager.
	dummyHealthFn := func() int { return 50 }
	txPool := transaction.NewTransactionPool(
		100,            // max pool size
		10*time.Second, // transaction timeout
		1.0,            // min fee threshold
		10.0,           // max fee threshold
		60*time.Second, // expiration duration
		transaction.WithNetworkHealthFn(dummyHealthFn),
		transaction.WithConflictResolution(false),
	)
	txManager := transaction.NewTransactionManager(txPool, 1.0, false, ledgerWrapper)

	// Initialize Lachesis consensus instance with a 100ms gossip delay.
	lachesis := finality.NewLachesis(100 * time.Millisecond)

	// Initialize DPoS and PoH with production settings
	dpos := diamantepos.NewDPoS(21, 1000, loggerAdapter)                // 21 validators, 1000 blocks per epoch
	poh := diamantepoh.NewPoH([32]byte{}, 1*time.Second, loggerAdapter) // Initial state, 1 second tick delay

	// Create consensus wrapper.
	consensusWrapper := NewConsensusWrapper(lachesis, dpos, poh)

	// Initialize the API server with ledger, consensus, transaction manager, and storage.
	apiInstance := api.NewAPI(ledgerWrapper, consensusWrapper, txManager, storeWrapper)
	go func() {
		if err := apiInstance.StartServer("8080"); err != nil {
			logger.Fatalf("API server error: %v", err)
		}
	}()

	// Start consensus.
	if err := consensusWrapper.Start(); err != nil {
		logger.Fatalf("Failed to start consensus: %v", err)
	}

	// Initialize token supply with a treasury account
	treasuryID := "treasury-" + common.GenerateUniqueID()
	tokenSupply := common.GetTokenSupply()
	if err := tokenSupply.Initialize(common.DefaultTotalSupply, treasuryID); err != nil {
		logger.Warnf("Failed to initialize token supply: %v", err)
	} else {
		logger.Infof("Token supply initialized with total supply of %.2f DIAM", common.DefaultTotalSupply)
		logger.Infof("Treasury account created with ID: %s", treasuryID)
	}

	blocksDir := filepath.Join(".", "blocks")
	logger.Infof("Blocks will be stored in: %s", blocksDir)

	// Block forever.
	select {}
}

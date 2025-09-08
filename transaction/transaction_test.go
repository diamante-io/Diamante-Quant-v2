package transaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/common"
	"diamante/crypto"
)

// Constant test encryption key (64 hex characters = 32 bytes)
const testEncKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// ------------------- Mocks & Helpers ------------------- //

// MockLedger implements a minimal common.LedgerAPI for testing.
type MockLedger struct {
	mu             sync.RWMutex
	committedTxIDs map[string]bool                // Tracks committed transaction IDs
	accounts       map[string]*common.Account     // Minimal account store
	blocks         map[int]common.Block           // blockNumber -> Block
	transactions   map[string]*common.Transaction // txID -> Transaction
	lastBlockNum   int
}

func NewMockLedger() *MockLedger {
	return &MockLedger{
		committedTxIDs: make(map[string]bool),
		accounts:       make(map[string]*common.Account),
		blocks:         make(map[int]common.Block),
		transactions:   make(map[string]*common.Transaction),
	}
}

// Implement all required methods from common.LedgerAPI

// Account Management
func (ml *MockLedger) CreateAccount(ac *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if _, exists := ml.accounts[ac.ID]; exists {
		return fmt.Errorf("account %s already exists", ac.ID)
	}
	// Create a new account and copy fields individually to avoid copying mutex
	newAcc := &common.Account{
		ID:        ac.ID,
		Balance:   ac.Balance,
		PublicKey: ac.PublicKey,
		CreatedAt: ac.CreatedAt,
	}
	ml.accounts[ac.ID] = newAcc
	return nil
}

func (ml *MockLedger) UpdateAccount(ac *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if _, exists := ml.accounts[ac.ID]; !exists {
		return fmt.Errorf("account %s does not exist", ac.ID)
	}
	// Create a new account and copy fields individually to avoid copying mutex
	newAcc := &common.Account{
		ID:        ac.ID,
		Balance:   ac.Balance,
		PublicKey: ac.PublicKey,
		CreatedAt: ac.CreatedAt,
	}
	ml.accounts[ac.ID] = newAcc
	return nil
}

func (ml *MockLedger) GetBalance(accountID string) (float64, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	acc, exists := ml.accounts[accountID]
	if !exists {
		return 0, fmt.Errorf("account %s not found", accountID)
	}
	return acc.Balance, nil
}

func (ml *MockLedger) UpdateAccountBalance(accountID string, amount float64) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	acc, exists := ml.accounts[accountID]
	if !exists {
		return fmt.Errorf("account %s does not exist", accountID)
	}
	if acc.Balance+amount < 0 {
		return fmt.Errorf("insufficient funds in account %s", accountID)
	}
	acc.Balance += amount
	return nil
}

// Transaction Management
func (ml *MockLedger) AddTransaction(tx common.Transaction) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if ml.committedTxIDs[tx.ID] {
		return fmt.Errorf("transaction %s already in mock ledger", tx.ID)
	}
	ml.committedTxIDs[tx.ID] = true
	txCopy := tx
	ml.transactions[tx.ID] = &txCopy
	return nil
}

func (ml *MockLedger) IsTransactionCommitted(txID string) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.committedTxIDs[txID]
}

func (ml *MockLedger) GetTransaction(txID string) (*common.Transaction, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	tx, exists := ml.transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txID)
	}
	return tx, nil
}

func (ml *MockLedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var result []common.Transaction
	for _, tx := range ml.transactions {
		if tx.Sender == accountID || tx.Receiver == accountID {
			result = append(result, *tx)
		}
		if len(result) >= offset+limit {
			break
		}
	}

	if offset >= len(result) {
		return []common.Transaction{}, nil
	}

	end := offset + limit
	if end > len(result) {
		end = len(result)
	}

	return result[offset:end], nil
}

// Block Management
func (ml *MockLedger) CommitBlock(block common.Block) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if _, exists := ml.blocks[block.Number]; exists {
		return fmt.Errorf("block %d already committed", block.Number)
	}
	ml.blocks[block.Number] = block
	if block.Number > ml.lastBlockNum {
		ml.lastBlockNum = block.Number
	}
	// Mark each transaction as committed
	for _, tx := range block.Transactions {
		ml.committedTxIDs[tx.ID] = true
		txCopy := tx
		ml.transactions[tx.ID] = &txCopy
	}
	return nil
}

func (ml *MockLedger) GetLastBlockHash() (string, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	if ml.lastBlockNum == 0 {
		return "", errors.New("no blocks committed yet")
	}
	lastBlock := ml.blocks[ml.lastBlockNum]
	return lastBlock.Hash, nil
}

func (ml *MockLedger) GetBlockByNumber(num int) (common.Block, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	blk, exists := ml.blocks[num]
	return blk, exists
}

func (ml *MockLedger) GetBlockHeight() (int, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.lastBlockNum, nil
}

func (ml *MockLedger) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var result []common.Block
	for i := startNum; i <= endNum; i++ {
		block, exists := ml.blocks[i]
		if exists {
			result = append(result, block)
		}
	}
	return result, nil
}

// Smart Contract Management
func (ml *MockLedger) DeploySmartContract(sc *common.SmartContract) error {
	// Stub implementation
	return nil
}

func (ml *MockLedger) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	// Stub implementation
	return nil, nil
}

func (ml *MockLedger) RemoveSmartContract(contractID string) error {
	// Stub implementation
	return nil
}

// System Management
func (ml *MockLedger) CreateSnapshot(height int) error {
	// Stub implementation
	return nil
}

func (ml *MockLedger) RestoreSnapshot(height int) error {
	// Stub implementation
	return nil
}

func (ml *MockLedger) IntegrityCheck() error {
	// Stub implementation
	return nil
}

func (ml *MockLedger) GetStats() (map[string]interface{}, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	return map[string]interface{}{
		"accounts":     len(ml.accounts),
		"transactions": len(ml.transactions),
		"blocks":       len(ml.blocks),
		"lastBlock":    ml.lastBlockNum,
	}, nil
}

func (ml *MockLedger) HealthCheck(ctx context.Context) error {
	// Stub implementation
	return nil
}

func (ml *MockLedger) Close() error {
	// Stub implementation
	return nil
}

// Helper to set up test accounts and balance
func setupTestAccount(t *testing.T, id string, balance float64, priv, pub []byte) {
	acc := &common.Account{
		ID:        id,
		Balance:   balance,
		PublicKey: pub,
		CreatedAt: time.Now().Unix(),
	}

	// Register the account with common package
	if err := common.RegisterAccount(acc); err != nil {
		t.Fatalf("Failed to register account %s: %v", id, err)
	}

	// Set the private key if provided
	if priv != nil && pub != nil {
		if err := acc.SetPrivateKey(priv, testEncKey); err != nil {
			t.Fatalf("Failed to set private key for account %s: %v", id, err)
		}
	}
}

// Clean up the global account state after tests
func cleanupAccounts() {
	common.ClearAllAccounts()
}

// --------------------- Key Generation ---------------------

// generateDilithiumTestKeys creates a valid Dilithium key pair for testing.
func generateDilithiumTestKeys(t *testing.T) (priv, pub []byte) {
	kp, err := crypto.GenerateDilithiumKeyPair(crypto.DilithiumLevel3)
	if err != nil {
		t.Fatalf("Unable to generate dilithium key pair: %v", err)
	}
	return kp.PrivateKey, kp.PublicKey
}

// --------------------- Tests ---------------------

func TestNewTransactionManager(t *testing.T) {
	// Create a logger for testing
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	mockPool := NewTransactionPool(
		10,
		time.Minute,
		0.001,
		10.0,
		time.Hour,
		WithPoolLogger(logger),
	)
	mockLedger := NewMockLedger()

	tm := NewTransactionManager(
		mockPool,
		0.001,
		false,
		mockLedger,
		WithLogger(logger),
	)

	if tm == nil {
		t.Fatal("Expected a valid TransactionManager, got nil")
	}

	// Clean up
	defer cleanupAccounts()
}

func TestCreateTransaction_Success(t *testing.T) {
	// Set up logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create transaction pool and mock ledger
	mockPool := NewTransactionPool(
		5,
		time.Minute,
		0.001,
		10.0,
		time.Hour,
		WithPoolLogger(logger),
	)
	mockLedger := NewMockLedger()

	// Create transaction manager
	tm := NewTransactionManager(
		mockPool,
		0.001,
		false,
		mockLedger,
		WithLogger(logger),
	)

	// Generate real keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)

	// Set up test account
	setupTestAccount(t, "Alice", 100.0, priv, pub)

	// Attempt to create a transaction
	tx, err := tm.CreateTransaction("Alice", "Bob", 10.0, 0.01, []byte("hello"))
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Verify transaction is in pool
	if !mockPool.HasTransaction(tx.ID) {
		t.Fatalf("Expected transaction %s to be in the pool", tx.ID)
	}

	// Clean up
	defer cleanupAccounts()
}

func TestCreateTransaction_FeeBelowThreshold(t *testing.T) {
	// Set up logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create transaction pool and mock ledger
	mockPool := NewTransactionPool(
		5,
		time.Minute,
		0.001,
		10.0,
		time.Hour,
		WithPoolLogger(logger),
	)
	mockLedger := NewMockLedger()

	// Create transaction manager with higher fee threshold
	tm := NewTransactionManager(
		mockPool,
		0.1, // Minimum fee threshold
		false,
		mockLedger,
		WithLogger(logger),
	)

	// Generate keys for account
	priv, pub := generateDilithiumTestKeys(t)

	// Set up test account
	setupTestAccount(t, "Charlie", 100.0, priv, pub)

	// Attempt to create transaction with fee below threshold
	_, err := tm.CreateTransaction("Charlie", "Dave", 10.0, 0.01, nil)
	if err == nil {
		t.Fatal("Expected an error for fee below threshold, got nil")
	}

	// Verify error message contains expected text
	if errMsg := err.Error(); !errors.Is(err, ErrFeeTooLow) {
		t.Fatalf("Expected ErrFeeTooLow error, got: %v", errMsg)
	}

	// Clean up
	defer cleanupAccounts()
}

func TestProcessTransaction_Success(t *testing.T) {
	// Set up logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create transaction pool and mock ledger
	mockPool := NewTransactionPool(
		10,
		time.Minute,
		0.001,
		10.0,
		time.Hour,
		WithPoolLogger(logger),
	)
	mockLedger := NewMockLedger()

	// Create transaction manager
	tm := NewTransactionManager(
		mockPool,
		0.001,
		false,
		mockLedger,
		WithLogger(logger),
	)

	// Generate keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)

	// Set up test accounts
	setupTestAccount(t, "Alice", 100.0, priv, pub)
	setupTestAccount(t, "Bob", 0.0, nil, nil)

	// Create a transaction
	tx, err := tm.CreateTransaction("Alice", "Bob", 20.0, 0.1, nil)
	if err != nil {
		t.Fatalf("CreateTransaction error: %v", err)
	}

	// Process the transaction
	if err := tm.ProcessTransaction(tx.ID); err != nil {
		t.Fatalf("ProcessTransaction error: %v", err)
	}

	// Check pool: transaction should be removed
	if mockPool.HasTransaction(tx.ID) {
		t.Fatalf("Expected transaction %s to be removed from pool", tx.ID)
	}

	// Check ledger: transaction should be committed
	if !mockLedger.IsTransactionCommitted(tx.ID) {
		t.Fatalf("Expected transaction %s to be committed in ledger", tx.ID)
	}

	// Check balances
	aliceBalance, err := mockLedger.GetBalance("Alice")
	if err != nil {
		t.Fatalf("Failed to get Alice's balance: %v", err)
	}
	bobBalance, err := mockLedger.GetBalance("Bob")
	if err != nil {
		t.Fatalf("Failed to get Bob's balance: %v", err)
	}

	// Verify Alice's balance decreased and Bob's increased
	if aliceBalance >= 100.0 {
		t.Fatalf("Expected Alice's balance to decrease, got %.2f", aliceBalance)
	}
	if bobBalance != 20.0 {
		t.Fatalf("Expected Bob's balance to be 20.0, got %.2f", bobBalance)
	}

	// Clean up
	defer cleanupAccounts()
}

func TestTransactionPool_AddTransaction(t *testing.T) {
	// Set up logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create transaction pool
	pool := NewTransactionPool(
		2,
		time.Minute,
		0.001,
		10.0,
		time.Hour,
		WithPoolLogger(logger),
	)

	// Generate keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)

	// Set up test account
	setupTestAccount(t, "Alice", 10.0, priv, pub)

	// Create a properly signed transaction
	txID := "tx-123"
	signature, errSign := crypto.SignDataWithDilithium(priv, []byte(txID))
	if errSign != nil {
		t.Fatalf("Failed to sign tx: %v", errSign)
	}

	tx := common.Transaction{
		ID:        txID,
		Sender:    "Alice",
		Receiver:  "Bob",
		Amount:    5.0,
		Fee:       0.001,
		Timestamp: time.Now().Unix(),
		Nonce:     1,
		Signature: signature,
	}

	// Add to pool: must succeed
	err := pool.AddTransaction(tx)
	if err != nil {
		t.Fatalf("Expected AddTransaction success, got error: %v", err)
	}

	// Verify it's in the pool
	if !pool.HasTransaction(tx.ID) {
		t.Fatalf("Expected transaction %s to be in the pool", tx.ID)
	}

	// Attempt duplicate addition: should fail
	errDup := pool.AddTransaction(tx)
	if errDup == nil {
		t.Fatal("Expected error for duplicate transaction, got nil")
	}

	// Clean up
	defer cleanupAccounts()
}

func TestValidateTransaction(t *testing.T) {
	// Set up logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock ledger
	mockLedger := NewMockLedger()

	// Generate keys for "Eve"
	priv, pub := generateDilithiumTestKeys(t)

	// Set up test account with 0 balance in both common registry AND mock ledger
	setupTestAccount(t, "Eve", 0.0, priv, pub)

	// Make sure it's also in the mock ledger
	eveAccount := &common.Account{
		ID:        "Eve",
		PublicKey: pub,
		Balance:   0.0,
	}
	mockLedger.CreateAccount(eveAccount)

	tx := common.Transaction{
		ID:        "test-insufficient-balance",
		Sender:    "Eve",
		Receiver:  "Zoe",
		Amount:    100.0,
		Fee:       0.01,
		Nonce:     1,
		Signature: []byte("FakeSig"),
	}

	// Create a transaction manager with the mockLedger
	pool := NewTransactionPool(10, time.Minute, 0.001, 10.0, time.Hour, WithPoolLogger(logger))
	tm := NewTransactionManager(
		pool,
		0.001,
		false,
		mockLedger,
		WithLogger(logger),
	)

	// Use the transaction manager to validate the transaction
	err := tm.ValidateTransaction(&tx)
	if err == nil {
		t.Fatal("Expected insufficient funds error, got nil")
	}

	// Verify error message contains expected text
	if !errors.Is(err, common.ErrInsufficientFunds) {
		t.Fatalf("Expected insufficient funds error, got: %v", err)
	}

	// Clean up
	defer cleanupAccounts()
}

func TestReplayProtection(t *testing.T) {
	// Create nonce tracker and set initial nonce
	nonceTracker := NewDefaultNonceTracker()
	nonceTracker.SetNonce("Alice", 5)

	tx := common.Transaction{
		ID:       "replay-tx",
		Sender:   "Alice",
		Receiver: "Bob",
		Nonce:    5, // Should be currentNonce + 1
	}

	// Verify replay protection catches the issue
	err := ReplayProtectionMiddleware(tx, nonceTracker)
	if err == nil {
		t.Fatal("Expected replay protection error, got nil")
	}

	// Check error message with updated format
	expectedErrMsg := "invalid nonce: expected 6, got 5"
	if err.Error() != expectedErrMsg {
		t.Fatalf("Expected error '%s', got '%s'", expectedErrMsg, err.Error())
	}

	// Test with valid nonce
	tx.Nonce = 6
	err = ReplayProtectionMiddleware(tx, nonceTracker)
	if err != nil {
		t.Fatalf("Expected no error for valid nonce, got: %v", err)
	}

	// Verify nonce was updated
	newNonce := nonceTracker.GetNonce("Alice")
	if newNonce != 6 {
		t.Fatalf("Expected nonce to be updated to 6, got %d", newNonce)
	}
}

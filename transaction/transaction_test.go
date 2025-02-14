package transaction

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"diamante/common"
	"diamante/crypto"
)

// ------------------- Mocks & Helpers ------------------- //

// MockLedger implements ledger.LedgerAPI minimally for testing.
type MockLedger struct {
	mu             sync.RWMutex
	committedTxIDs map[string]bool            // Tracks committed transaction IDs
	accounts       map[string]*common.Account // Minimal account store
	blocks         map[int]common.Block       // blockNumber -> Block
	lastBlockNum   int
}

func NewMockLedger() *MockLedger {
	return &MockLedger{
		committedTxIDs: make(map[string]bool),
		accounts:       make(map[string]*common.Account),
		blocks:         make(map[int]common.Block),
	}
}

// Implement ledger.LedgerAPI methods minimally...

// 1) Account ops
func (ml *MockLedger) CreateAccount(ac *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if _, exists := ml.accounts[ac.ID]; exists {
		return fmt.Errorf("account %s already exists", ac.ID)
	}
	ml.accounts[ac.ID] = ac
	return nil
}
func (ml *MockLedger) UpdateAccount(ac *common.Account) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if _, exists := ml.accounts[ac.ID]; !exists {
		return fmt.Errorf("account %s does not exist", ac.ID)
	}
	ml.accounts[ac.ID] = ac
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
	ml.accounts[accountID] = acc
	return nil
}

// 2) Transaction ops
func (ml *MockLedger) AddTransaction(tx common.Transaction) error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if ml.committedTxIDs[tx.ID] {
		return fmt.Errorf("transaction %s already in mock ledger", tx.ID)
	}
	ml.committedTxIDs[tx.ID] = true
	return nil
}
func (ml *MockLedger) IsTransactionCommitted(txID string) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.committedTxIDs[txID]
}

// 3) Block ops
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

// 4) Snapshots / checks
func (ml *MockLedger) CreateSnapshot(height int) error  { return nil }
func (ml *MockLedger) RestoreSnapshot(height int) error { return nil }
func (ml *MockLedger) IntegrityCheck() error            { return nil }

// Helper to set balance in global `common` map
func SetAccountBalance(accountID string, balance float64) {
	common.SetAccountBalance(accountID, balance)
}

// --------------------- REAL KEY GENERATION ---------------------

// generateDilithiumTestKeys creates a valid Dilithium key pair for test
func generateDilithiumTestKeys(t *testing.T) (priv, pub []byte) {
	kp, err := crypto.GenerateDilithiumKeyPair(crypto.DilithiumLevel3)
	if err != nil {
		t.Fatalf("unable to generate dilithium key pair: %v", err)
	}
	return kp.PrivateKey, kp.PublicKey
}

// --------------------- Tests --------------------- //

// Basic test
func TestNewTransactionManager(t *testing.T) {
	mockPool := NewTransactionPool(
		10,
		time.Minute,
		0.001,
		10.0,
		false,
		time.Hour,
		nil,
		nil,
	)
	mockLedger := NewMockLedger()

	tm := NewTransactionManager(mockPool, 0.001, false, mockLedger)
	if tm == nil {
		t.Fatal("expected a valid TransactionManager, got nil")
	}
}

func TestCreateTransaction_Success(t *testing.T) {
	mockPool := NewTransactionPool(
		5,
		time.Minute,
		0.001,
		10.0,
		false,
		time.Hour,
		nil,
		nil,
	)
	mockLedger := NewMockLedger()
	tm := NewTransactionManager(mockPool, 0.001, false, mockLedger)

	// Generate real keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)

	// Create/attach them to Alice
	common.SetAccountBalance("Alice", 100.0)
	acc := common.GetAccount("Alice")
	if acc == nil {
		t.Fatal("could not retrieve Alice account")
	}
	acc.PrivateKey = priv
	acc.PublicKey = pub

	// Attempt to create a transaction
	tx, err := tm.CreateTransaction("Alice", "Bob", 10.0, 0.01, []byte("hello"))
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	// Verify in pool
	if !mockPool.HasTransaction(tx.ID) {
		t.Fatalf("expected transaction %s to be in the pool", tx.ID)
	}
}

// This test is fine because it doesn't rely on real keys
func TestCreateTransaction_FeeBelowThreshold(t *testing.T) {
	mockPool := NewTransactionPool(5, time.Minute, 0.001, 10.0, false, time.Hour, nil, nil)
	mockLedger := NewMockLedger()
	tm := NewTransactionManager(mockPool, 0.1, false, mockLedger)

	// We do not need real keys here, because we want it to fail on fee anyway
	SetAccountBalance("Charlie", 100.0)
	common.SetPublicKey("Charlie", []byte("FakePubKey")) // okay for this test

	_, err := tm.CreateTransaction("Charlie", "Dave", 10.0, 0.01, nil)
	if err == nil {
		t.Fatal("expected an error for fee below threshold, got nil")
	}
}

// In this test, we DO rely on a valid signature to pass mempool + processing
func TestProcessTransaction_Success(t *testing.T) {
	mockPool := NewTransactionPool(
		10,
		time.Minute,
		0.001,
		10.0,
		false,
		time.Hour,
		nil,
		nil,
	)
	mockLedger := NewMockLedger()
	tm := NewTransactionManager(mockPool, 0.001, false, mockLedger)

	// Generate real keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)
	SetAccountBalance("Alice", 100.0)

	// Attach to Alice’s account
	acc := common.GetAccount("Alice")
	if acc == nil {
		t.Fatal("could not retrieve Alice account")
	}
	acc.PrivateKey = priv
	acc.PublicKey = pub

	// Create a transaction
	tx, err := tm.CreateTransaction("Alice", "Bob", 20.0, 0.1, nil)
	if err != nil {
		t.Fatalf("CreateTransaction error: %v", err)
	}

	// "Finalize" it
	if err := tm.ProcessTransaction(tx.ID); err != nil {
		t.Fatalf("ProcessTransaction error: %v", err)
	}

	// Check pool => should be removed
	if mockPool.HasTransaction(tx.ID) {
		t.Fatalf("expected transaction %s to be removed from pool", tx.ID)
	}

	// Check ledger => should be committed
	if !mockLedger.IsTransactionCommitted(tx.ID) {
		t.Fatalf("expected transaction %s to be committed in ledger", tx.ID)
	}
}

// Also needs real signing to pass the pool's signature check
func TestTransactionPool_AddTransaction(t *testing.T) {
	pool := NewTransactionPool(2, time.Minute, 0.001, 10.0, false, time.Hour, nil, nil)

	// Generate real keys for "Alice"
	priv, pub := generateDilithiumTestKeys(t)
	common.SetAccountBalance("Alice", 10.0)

	// Attach to Alice’s account
	acc := common.GetAccount("Alice")
	if acc == nil {
		t.Fatal("could not retrieve 'Alice' account")
	}
	acc.PrivateKey = priv
	acc.PublicKey = pub

	// Now create a properly signed transaction
	// 1) We'll sign it using the TransactionManager sign logic or do it manually
	// For simplicity, let's call a quick "manual" approach to sign. We can do it
	// the same way your code does: SignDataWithDilithium on the tx ID.

	txID := "tx-123"
	signature, errSign := crypto.SignDataWithDilithium(priv, []byte(txID))
	if errSign != nil {
		t.Fatalf("failed to sign tx: %v", errSign)
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

	// Add to pool => must succeed
	err := pool.AddTransaction(tx)
	if err != nil {
		t.Fatalf("expected AddTransaction success, got error: %v", err)
	}

	// Verify it’s in the pool
	if !pool.HasTransaction(tx.ID) {
		t.Fatalf("expected transaction %s to be in the pool", tx.ID)
	}

	// Attempt duplicate => should fail
	errDup := pool.AddTransaction(tx)
	if errDup == nil {
		t.Fatal("expected error for duplicate transaction, got nil")
	}
}

// TestValidation_InsufficientBalance checks ValidateTransaction directly
func TestValidation_InsufficientBalance(t *testing.T) {
	// This test expects a balance fail, so real keys not necessary
	SetAccountBalance("Eve", 0.0)
	common.SetPublicKey("Eve", []byte("FakePubKey"))

	tx := common.Transaction{
		ID:        "test-insufficient-balance",
		Sender:    "Eve",
		Receiver:  "Zoe",
		Amount:    100.0,
		Fee:       0.01,
		Nonce:     1,
		Signature: []byte("FakeSig"),
	}

	err := ValidateTransaction(tx, 0.0, NewDefaultNonceTracker())
	if err == nil {
		t.Fatal("expected insufficient balance error, got nil")
	}
	if e := err.Error(); e != "Insufficient balance" {
		t.Fatalf("expected 'Insufficient balance', got %q", e)
	}
}

func TestReplayProtection(t *testing.T) {
	nonceTracker := NewDefaultNonceTracker()
	nonceTracker.SetNonce("Alice", 5)

	tx := common.Transaction{
		ID:       "replay-tx",
		Sender:   "Alice",
		Receiver: "Bob",
		Nonce:    5, // not > current => fail
	}

	err := ReplayProtectionMiddleware(tx, nonceTracker)
	if err == nil {
		t.Fatal("expected replay protection error, got nil")
	}
	if e := err.Error(); e != "nonce 5 <= current 5 for Alice" {
		t.Fatalf("unexpected replay protection error: %v", e)
	}
}

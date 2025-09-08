package wallet_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"diamante/common"
	"diamante/crypto"
	"diamante/transaction"
	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadEnvFile loads environment variables from a .env file
func loadEnvFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		os.Setenv(key, value)
	}

	return scanner.Err()
}

// getTestLogger returns a logger for testing.
func getTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

func cleanupTempFile(path string) {
	_ = os.Remove(path)
}

// setupTestEnvironment sets up the environment variables needed for testing.
func setupTestEnvironment(t *testing.T) {
	// Load environment variables from .env file
	err := loadEnvFile(".env")
	if err != nil {
		t.Logf("Warning: Failed to load .env file: %v", err)
		// If .env file doesn't exist, generate a random key
		key := make([]byte, 32)
		_, err := rand.Read(key)
		require.NoError(t, err, "Failed to generate random key")

		hexKey := hex.EncodeToString(key)
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", hexKey)
	}
}

// cleanupTestEnvironment cleans up the environment variables set for testing.
func cleanupTestEnvironment() {
	os.Unsetenv("DIAMANTE_WALLET_ENCRYPTION_KEY")
}

// --- In-Memory Mock Ledger ---
// Since the production ledger implementation (e.g. RocksDBLedger) is not ideal for unit tests,
// we implement a simple in‑memory mock ledger that satisfies the common.LedgerAPI interface.
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

// Implement the methods required by common.LedgerAPI.

// Account operations.
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

// Transaction operations.
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

// Block operations.
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
	// Mark each transaction as committed.
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

// Snapshot / integrity stubs.
func (ml *MockLedger) CreateSnapshot(height int) error  { return nil }
func (ml *MockLedger) RestoreSnapshot(height int) error { return nil }
func (ml *MockLedger) IntegrityCheck() error            { return nil }
func (ml *MockLedger) Close() error                     { return nil }

// --- Additional methods to satisfy common.LedgerAPI interface ---
// These are dummy implementations.

func (ml *MockLedger) DeploySmartContract(sc *common.SmartContract) error {
	// Dummy implementation.
	return nil
}

func (ml *MockLedger) ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error) {
	// Dummy implementation.
	return nil, nil
}

func (ml *MockLedger) RemoveSmartContract(contractID string) error {
	// Dummy implementation.
	return nil
}

func (ml *MockLedger) GetTransaction(txID string) (*common.Transaction, error) {
	// Dummy implementation.
	return nil, errors.New("transaction not found")
}

func (ml *MockLedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	// Dummy implementation.
	return []common.Transaction{}, nil
}

func (ml *MockLedger) GetBlockHeight() (int, error) {
	// Dummy implementation.
	return ml.lastBlockNum, nil
}

func (ml *MockLedger) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	// Dummy implementation.
	return []common.Block{}, nil
}

func (ml *MockLedger) GetStats() (map[string]interface{}, error) {
	// Dummy implementation.
	return map[string]interface{}{}, nil
}

func (ml *MockLedger) HealthCheck(ctx context.Context) error {
	// Dummy implementation.
	return nil
}

// --- End of Mock Ledger ---

// newDummyLedger returns a dummy ledger for testing.
func newDummyLedger() common.LedgerAPI {
	return NewMockLedger()
}

// --------------------- REAL KEY GENERATION ---------------------

// generateDilithiumTestKeys creates a valid Dilithium key pair for test.
func generateDilithiumTestKeys(t *testing.T) (priv, pub []byte) {
	kp, err := crypto.GenerateDilithiumKeyPair(crypto.DilithiumLevel3)
	if err != nil {
		t.Fatalf("unable to generate dilithium key pair: %v", err)
	}
	return kp.PrivateKey, kp.PublicKey
}

// --------------------- Tests ---------------------

func TestNewWallet(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err)
	require.NotNil(t, w)
	assert.NotEmpty(t, w.ID)
	assert.NotNil(t, w.KEMKeyPair)
	assert.NotNil(t, w.SigKeyPair)
	assert.NotNil(t, w.CryptoManager)
}

func TestRegisterAccount(t *testing.T) {
	// Setup test environment
	setupTestEnvironment(t)
	defer cleanupTestEnvironment()

	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err)

	err = w.RegisterAccount()
	require.NoError(t, err)

	ac := common.GetAccount(w.ID)
	require.NotNil(t, ac)
	assert.Equal(t, w.ID, ac.ID)
}

func TestExportImport(t *testing.T) {
	// Setup test environment
	setupTestEnvironment(t)
	defer cleanupTestEnvironment()

	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err)

	err = w.RegisterAccount()
	require.NoError(t, err)

	tmpFile, err := ioutil.TempFile("", "wallet_test_*.json")
	require.NoError(t, err)
	tmpFilePath := tmpFile.Name()
	tmpFile.Close()
	defer cleanupTempFile(tmpFilePath)

	err = w.Export(tmpFilePath)
	require.NoError(t, err)

	importedWallet, err := wallet.ImportWallet(tmpFilePath, logger)
	require.NoError(t, err)
	require.NotNil(t, importedWallet)

	assert.Equal(t, w.ID, importedWallet.ID)
	assert.Equal(t, w.Nonce, importedWallet.Nonce)

	kemOrig, err := crypto.SerializeKyberKeyPair(w.KEMKeyPair)
	require.NoError(t, err)
	kemImp, err := crypto.SerializeKyberKeyPair(importedWallet.KEMKeyPair)
	require.NoError(t, err)
	assert.Equal(t, kemOrig, kemImp)

	sigOrig, err := crypto.SerializeDilithiumKeyPair(w.SigKeyPair)
	require.NoError(t, err)
	sigImp, err := crypto.SerializeDilithiumKeyPair(importedWallet.SigKeyPair)
	require.NoError(t, err)
	assert.Equal(t, sigOrig, sigImp)
}

func TestCreateTransaction(t *testing.T) {
	// Setup test environment
	setupTestEnvironment(t)
	defer cleanupTestEnvironment()

	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err)

	err = w.RegisterAccount()
	require.NoError(t, err)

	tx, err := w.CreateTransaction("receiver_account", 15.0, 0.05, []byte("Test payload"))
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.NotEmpty(t, tx.ID)
	assert.Equal(t, w.ID, tx.Sender)
	assert.Equal(t, "receiver_account", tx.Receiver)
	assert.Equal(t, 15.0, tx.Amount)
	assert.Equal(t, 0.05, tx.Fee)
	assert.True(t, tx.Timestamp > 0)
	assert.Equal(t, w.Nonce, tx.Nonce)
	assert.NotEmpty(t, tx.Signature)
}

func TestSubmitTransaction(t *testing.T) {
	// Setup test environment
	setupTestEnvironment(t)
	defer cleanupTestEnvironment()

	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err)

	err = w.RegisterAccount()
	require.NoError(t, err)

	// Fund the wallet for testing.
	err = w.FundWallet(1000.0)
	require.NoError(t, err)

	// Use the mock transaction manager instead of the real one
	dummyPool := transaction.NewTransactionPool(10, time.Minute, 0.001, 10.0, time.Hour)
	mockTxMgr := &wallet.MockTransactionManager{
		Pool: dummyPool,
	}

	tx, err := w.SubmitTransactionWithMock("receiver_account", 20.0, 0.1, []byte("Payload for submission"), mockTxMgr)
	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, w.ID, tx.Sender)
	assert.NotEmpty(t, tx.Signature)
}

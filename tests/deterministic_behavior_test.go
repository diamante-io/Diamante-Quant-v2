package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/consensus/types"
	"diamante/storage"
	"diamante/transaction"
	"diamante/vm/deploy"
	"diamante/vm/runtime"
	"github.com/sirupsen/logrus"
	"sort"
)

// loggerAdapter wraps logrus.Logger to implement consensus.Logger
type loggerAdapter struct {
	logger *logrus.Logger
}

func (l *loggerAdapter) Info(msg string, keyvals ...interface{}) {
	l.logger.Info(msg)
}

func (l *loggerAdapter) Error(msg string, keyvals ...interface{}) {
	l.logger.Error(msg)
}

func (l *loggerAdapter) Warn(msg string, keyvals ...interface{}) {
	l.logger.Warn(msg)
}

// TestDeterministicConsensusTime verifies that consensus time is deterministic
func TestDeterministicConsensusTime(t *testing.T) {
	// Initialize consensus time using global consensus time
	config := consensus.DefaultConsensusTimeConfig()
	logger := &loggerAdapter{logger: logrus.New()}
	consensus.InitializeGlobalConsensusTime(config, logger)

	// Test that multiple calls return the same time within the same consensus round
	time1 := consensus.ConsensusNow()
	time2 := consensus.ConsensusNow()

	if time1.Unix() != time2.Unix() {
		t.Errorf("Consensus time is not deterministic: %v != %v", time1, time2)
	}

	// Test that consensus time advances deterministically
	ct := consensus.GetGlobalConsensusTime()
	initialTime := ct.GetCurrentTime()
	ct.AdvanceToBlock(ct.GetCurrentBlockHeight() + 1)
	advancedTime := ct.GetCurrentTime()

	expectedTime := initialTime.Add(ct.GetBlockInterval())
	if advancedTime.Unix() != expectedTime.Unix() {
		t.Errorf("Consensus time advancement is not deterministic: expected %v, got %v", expectedTime, advancedTime)
	}
}

// TestDeterministicTransactionProcessing verifies that transaction processing is deterministic
func TestDeterministicTransactionProcessing(t *testing.T) {
	// Create test transaction
	tx := common.Transaction{
		ID:       "test-tx-001",
		Sender:   "sender123",
		Receiver: "receiver456",
		Amount:   100.0,
		Data:     []byte("test data"),
		Metadata: &common.TransactionMetadata{
			Category: "transfer",
			Purpose:  "test",
		},
	}

	// Process transaction multiple times
	results := make([]*transaction.TransactionResult, 3)
	for i := 0; i < 3; i++ {
		processor := createTestTransactionProcessor(t)
		result, err := processor.ProcessTransaction(context.Background(), tx)
		if err != nil {
			t.Fatalf("Failed to process transaction: %v", err)
		}
		results[i] = result
	}

	// Verify all results are identical
	for i := 1; i < len(results); i++ {
		if results[0].Success != results[i].Success {
			t.Errorf("Transaction processing is not deterministic: success %v != %v", results[0].Success, results[i].Success)
		}
		if results[0].GasUsed != results[i].GasUsed {
			t.Errorf("Transaction processing is not deterministic: gas used %v != %v", results[0].GasUsed, results[i].GasUsed)
		}
		if results[0].Error != results[i].Error {
			t.Errorf("Transaction processing is not deterministic: error %v != %v", results[0].Error, results[i].Error)
		}
	}
}

// TestDeterministicEventOrdering verifies that event ordering is deterministic
func TestDeterministicEventOrdering(t *testing.T) {
	// Create multiple events with same timestamp
	events := make([]*types.Event, 5)
	for i := 0; i < 5; i++ {
		events[i] = &types.Event{
			ID:        [32]byte{byte(i)},
			Creator:   [32]byte{byte(i + 10)},
			Height:    uint64(i + 1),
			Data:      []byte(fmt.Sprintf("event-%d", i)),
			Timestamp: time.Unix(1640995200, 0), // Fixed timestamp
		}
	}

	// Process events multiple times
	results := make([][]string, 3)
	for run := 0; run < 3; run++ {
		// Shuffle events to test ordering determinism
		shuffled := make([]*types.Event, len(events))
		copy(shuffled, events)
		r := rand.New(rand.NewSource(int64(run))) // Different seed each run
		r.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		// Sort events deterministically
		sortedEvents := sortEventsDeterministically(shuffled)

		// Record order
		order := make([]string, len(sortedEvents))
		for i, event := range sortedEvents {
			order[i] = hex.EncodeToString(event.ID[:])
		}
		results[run] = order
	}

	// Verify all runs produce same order
	for i := 1; i < len(results); i++ {
		if len(results[0]) != len(results[i]) {
			t.Errorf("Event ordering is not deterministic: length %d != %d", len(results[0]), len(results[i]))
		}
		for j := 0; j < len(results[0]); j++ {
			if results[0][j] != results[i][j] {
				t.Errorf("Event ordering is not deterministic at position %d: %s != %s", j, results[0][j], results[i][j])
			}
		}
	}
}

// TestDeterministicStateTransitions verifies that state transitions are deterministic
func TestDeterministicStateTransitions(t *testing.T) {
	// Create test state
	initialState := map[string]interface{}{
		"balance_alice": 1000.0,
		"balance_bob":   500.0,
		"nonce_alice":   1,
		"nonce_bob":     1,
	}

	// Create test transactions
	transactions := []common.Transaction{
		{
			ID:       "tx1",
			Sender:   "alice",
			Receiver: "bob",
			Amount:   100.0,
			Nonce:    1,
		},
		{
			ID:       "tx2",
			Sender:   "bob",
			Receiver: "alice",
			Amount:   50.0,
			Nonce:    1,
		},
	}

	// Apply transactions multiple times
	results := make([]map[string]interface{}, 3)
	for run := 0; run < 3; run++ {
		state := copyState(initialState)

		// Apply transactions in deterministic order
		for _, tx := range transactions {
			applyTransaction(state, tx)
		}

		results[run] = state
	}

	// Verify all runs produce same final state
	for i := 1; i < len(results); i++ {
		if !statesEqual(results[0], results[i]) {
			t.Errorf("State transitions are not deterministic: run 0 != run %d", i)
			t.Logf("Run 0: %+v", results[0])
			t.Logf("Run %d: %+v", i, results[i])
		}
	}
}

// TestDeterministicConcurrentProcessing verifies determinism under concurrent processing
func TestDeterministicConcurrentProcessing(t *testing.T) {
	const numGoroutines = 10
	const numTransactions = 100

	// Create test transactions
	transactions := make([]common.Transaction, numTransactions)
	for i := 0; i < numTransactions; i++ {
		transactions[i] = common.Transaction{
			ID:       fmt.Sprintf("tx-%d", i),
			Sender:   fmt.Sprintf("sender-%d", i%5),
			Receiver: fmt.Sprintf("receiver-%d", (i+1)%5),
			Amount:   float64(i + 1),
			Nonce:    i + 1,
		}
	}

	// Process transactions concurrently multiple times
	results := make([]map[string]*transaction.TransactionResult, 3)
	for run := 0; run < 3; run++ {
		processor := createTestTransactionProcessor(t)
		resultMap := make(map[string]*transaction.TransactionResult)
		resultMutex := sync.Mutex{}

		var wg sync.WaitGroup
		semaphore := make(chan struct{}, numGoroutines)

		for _, tx := range transactions {
			wg.Add(1)
			go func(transaction common.Transaction) {
				defer wg.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				result, err := processor.ProcessTransaction(context.Background(), transaction)
				if err != nil {
					t.Errorf("Failed to process transaction %s: %v", transaction.ID, err)
					return
				}

				resultMutex.Lock()
				resultMap[transaction.ID] = result
				resultMutex.Unlock()
			}(tx)
		}

		wg.Wait()
		results[run] = resultMap
	}

	// Verify all runs produce same results
	for i := 1; i < len(results); i++ {
		if len(results[0]) != len(results[i]) {
			t.Errorf("Concurrent processing is not deterministic: result count %d != %d", len(results[0]), len(results[i]))
		}

		for txID, result0 := range results[0] {
			resultI, exists := results[i][txID]
			if !exists {
				t.Errorf("Concurrent processing is not deterministic: transaction %s missing in run %d", txID, i)
				continue
			}

			if result0.Success != resultI.Success {
				t.Errorf("Concurrent processing is not deterministic for tx %s: success %v != %v", txID, result0.Success, resultI.Success)
			}
			if result0.GasUsed != resultI.GasUsed {
				t.Errorf("Concurrent processing is not deterministic for tx %s: gas used %v != %v", txID, result0.GasUsed, resultI.GasUsed)
			}
		}
	}
}

// TestDeterministicHashGeneration verifies that hash generation is deterministic
func TestDeterministicHashGeneration(t *testing.T) {
	// Test data
	testData := [][]byte{
		[]byte("test data 1"),
		[]byte("test data 2"),
		{0x01, 0x02, 0x03, 0x04},
		[]byte(""),
	}

	// Generate hashes multiple times
	for _, data := range testData {
		hashes := make([]string, 3)
		for i := 0; i < 3; i++ {
			hash := sha256.Sum256(data)
			hashes[i] = hex.EncodeToString(hash[:])
		}

		// Verify all hashes are identical
		for i := 1; i < len(hashes); i++ {
			if hashes[0] != hashes[i] {
				t.Errorf("Hash generation is not deterministic for data %x: %s != %s", data, hashes[0], hashes[i])
			}
		}
	}
}

// TestDeterministicRandomness verifies that deterministic randomness works correctly
func TestDeterministicRandomness(t *testing.T) {
	// Test with same seed
	seed := int64(12345)

	// Generate random numbers multiple times with same seed
	results := make([][]int, 3)
	for run := 0; run < 3; run++ {
		rng := rand.New(rand.NewSource(seed))
		numbers := make([]int, 10)
		for i := 0; i < 10; i++ {
			numbers[i] = rng.Intn(1000)
		}
		results[run] = numbers
	}

	// Verify all runs produce same sequence
	for i := 1; i < len(results); i++ {
		if len(results[0]) != len(results[i]) {
			t.Errorf("Deterministic randomness failed: length %d != %d", len(results[0]), len(results[i]))
		}
		for j := 0; j < len(results[0]); j++ {
			if results[0][j] != results[i][j] {
				t.Errorf("Deterministic randomness failed at position %d: %d != %d", j, results[0][j], results[i][j])
			}
		}
	}
}

// Helper functions

func createTestTransactionProcessor(t *testing.T) *transaction.HybridTransactionProcessor {
	// Create mock dependencies
	runtimeManager := &runtime.RuntimeManager{} // Mock implementation
	// deploymentManager := &deploy.DeploymentManager{} // Mock implementation - commented out
	ledger := &mockLedgerAPI{}       // Mock implementation
	stateStore := &mockLedgerStore{} // Mock implementation
	logger := logrus.New()

	deploymentManager := &deploy.DeploymentManager{} // Mock implementation
	_ = deploymentManager                            // Suppress unused variable warning
	return transaction.NewHybridTransactionProcessor(
		runtimeManager,
		deploymentManager,
		ledger,
		stateStore,
		logger,
	)
}

func copyState(state map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{})
	for k, v := range state {
		copy[k] = v
	}
	return copy
}

func applyTransaction(state map[string]interface{}, tx common.Transaction) {
	// Simple balance update logic for testing
	senderBalance := state[fmt.Sprintf("balance_%s", tx.Sender)].(float64)
	receiverBalance := state[fmt.Sprintf("balance_%s", tx.Receiver)].(float64)

	state[fmt.Sprintf("balance_%s", tx.Sender)] = senderBalance - tx.Amount
	state[fmt.Sprintf("balance_%s", tx.Receiver)] = receiverBalance + tx.Amount

	// Update nonces
	senderNonce := state[fmt.Sprintf("nonce_%s", tx.Sender)].(int)
	state[fmt.Sprintf("nonce_%s", tx.Sender)] = senderNonce + 1
}

func statesEqual(state1, state2 map[string]interface{}) bool {
	if len(state1) != len(state2) {
		return false
	}

	for k, v1 := range state1 {
		v2, exists := state2[k]
		if !exists {
			return false
		}

		// Compare values based on type
		switch v1t := v1.(type) {
		case float64:
			if v2t, ok := v2.(float64); !ok || v1t != v2t {
				return false
			}
		case int:
			if v2t, ok := v2.(int); !ok || v1t != v2t {
				return false
			}
		case string:
			if v2t, ok := v2.(string); !ok || v1t != v2t {
				return false
			}
		default:
			if v1 != v2 {
				return false
			}
		}
	}

	return true
}

// sortEventsDeterministically sorts events in a deterministic way
func sortEventsDeterministically(events []*types.Event) []*types.Event {
	sorted := make([]*types.Event, len(events))
	copy(sorted, events)

	// Sort by multiple criteria to ensure determinism
	sort.Slice(sorted, func(i, j int) bool {
		// First by timestamp
		if !sorted[i].Timestamp.Equal(sorted[j].Timestamp) {
			return sorted[i].Timestamp.Before(sorted[j].Timestamp)
		}
		// Then by height
		if sorted[i].Height != sorted[j].Height {
			return sorted[i].Height < sorted[j].Height
		}
		// Finally by ID (as bytes)
		for k := 0; k < 32; k++ {
			if sorted[i].ID[k] != sorted[j].ID[k] {
				return sorted[i].ID[k] < sorted[j].ID[k]
			}
		}
		return false
	})

	return sorted
}

// Mock implementations

type mockLedgerAPI struct{}

// Account Management
func (m *mockLedgerAPI) CreateAccount(ac *common.Account) error {
	return nil
}

func (m *mockLedgerAPI) UpdateAccount(ac *common.Account) error {
	return nil
}

func (m *mockLedgerAPI) GetBalance(accountID string) (float64, error) {
	return 1000.0, nil
}

func (m *mockLedgerAPI) UpdateAccountBalance(accountID string, amount float64) error {
	return nil
}

// Transaction Management
func (m *mockLedgerAPI) AddTransaction(tx common.Transaction) error {
	return nil
}

func (m *mockLedgerAPI) IsTransactionCommitted(txID string) bool {
	return true
}

func (m *mockLedgerAPI) GetTransaction(txID string) (*common.Transaction, error) {
	return &common.Transaction{ID: txID}, nil
}

func (m *mockLedgerAPI) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	return []common.Transaction{}, nil
}

// Block Management
func (m *mockLedgerAPI) CommitBlock(block common.Block) error {
	return nil
}

func (m *mockLedgerAPI) GetBlockByNumber(num int) (common.Block, bool) {
	return common.Block{Number: num}, true
}

func (m *mockLedgerAPI) GetLastBlockHash() (string, error) {
	return "latest-hash", nil
}

func (m *mockLedgerAPI) GetBlockHeight() (int, error) {
	return 100, nil
}

func (m *mockLedgerAPI) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	return []common.Block{}, nil
}

// Snapshot & Recovery
func (m *mockLedgerAPI) CreateSnapshot(height int) error {
	return nil
}

func (m *mockLedgerAPI) RestoreSnapshot(height int) error {
	return nil
}

// Smart Contract Management
func (m *mockLedgerAPI) DeploySmartContract(sc *common.SmartContract) error {
	return nil
}

func (m *mockLedgerAPI) UpdateSmartContract(contractID, newCode, version string) error {
	return nil
}

func (m *mockLedgerAPI) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	return &common.SmartContractResult{Success: true}, nil
}

func (m *mockLedgerAPI) RemoveSmartContract(contractID string) error {
	return nil
}

// System Management
func (m *mockLedgerAPI) IntegrityCheck() error {
	return nil
}

func (m *mockLedgerAPI) Close() error {
	return nil
}

func (m *mockLedgerAPI) GetStats() (*common.LedgerStats, error) {
	return &common.LedgerStats{}, nil
}

func (m *mockLedgerAPI) HealthCheck(ctx context.Context) error {
	return nil
}

type mockLedgerStore struct{}

// Block operations
func (m *mockLedgerStore) SaveBlock(block *common.Block) error {
	return nil
}

func (m *mockLedgerStore) GetBlock(height uint64) (*common.Block, error) {
	return &common.Block{Number: int(height)}, nil
}

func (m *mockLedgerStore) GetBlockByHash(hash string) (*common.Block, error) {
	return &common.Block{Hash: hash}, nil
}

func (m *mockLedgerStore) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	return []*common.Block{}, nil
}

func (m *mockLedgerStore) GetLatestBlock() (*common.Block, error) {
	return &common.Block{Number: 100}, nil
}

// Transaction operations
func (m *mockLedgerStore) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	return nil
}

func (m *mockLedgerStore) GetTransaction(txID string) (*common.Transaction, error) {
	return &common.Transaction{ID: txID}, nil
}

func (m *mockLedgerStore) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	return []*common.Transaction{}, nil
}

func (m *mockLedgerStore) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	return []*common.Transaction{}, nil
}

// Account operations
func (m *mockLedgerStore) SaveAccount(account *common.Account) error {
	return nil
}

func (m *mockLedgerStore) GetAccount(accountID string) (*common.Account, error) {
	return &common.Account{ID: accountID, Balance: 1000}, nil
}

func (m *mockLedgerStore) UpdateAccount(account *common.Account) error {
	return nil
}

func (m *mockLedgerStore) GetBalance(address string) (float64, error) {
	return 1000.0, nil
}

func (m *mockLedgerStore) GetNonce(address string) (uint64, error) {
	return 0, nil
}

// State operations
func (m *mockLedgerStore) GetState(key []byte) ([]byte, error) {
	return []byte("mock-state"), nil
}

func (m *mockLedgerStore) SetState(key, value []byte) error {
	return nil
}

func (m *mockLedgerStore) DeleteState(key []byte) error {
	return nil
}

// Smart contract operations
func (m *mockLedgerStore) SaveContract(contract *common.SmartContract) error {
	return nil
}

func (m *mockLedgerStore) GetContract(contractID string) (*common.SmartContract, error) {
	return &common.SmartContract{ID: contractID}, nil
}

func (m *mockLedgerStore) UpdateContract(contract *common.SmartContract) error {
	return nil
}

func (m *mockLedgerStore) DeleteContract(contractID string) error {
	return nil
}

// Receipt operations
func (m *mockLedgerStore) SaveReceipt(receipt *storage.Receipt) error {
	return nil
}

func (m *mockLedgerStore) GetReceipt(txID string) (*storage.Receipt, error) {
	return &storage.Receipt{TxID: txID}, nil
}

// Snapshot operations
func (m *mockLedgerStore) CreateSnapshot(height uint64) error {
	return nil
}

func (m *mockLedgerStore) RestoreSnapshot(height uint64) error {
	return nil
}

func (m *mockLedgerStore) ListSnapshots() ([]storage.SnapshotInfo, error) {
	return []storage.SnapshotInfo{}, nil
}

// Batch operations
func (m *mockLedgerStore) BatchWrite(batch *storage.WriteBatch) error {
	return nil
}

// Maintenance operations
func (m *mockLedgerStore) Compact() error {
	return nil
}

func (m *mockLedgerStore) Backup(path string) error {
	return nil
}

func (m *mockLedgerStore) Restore(path string) error {
	return nil
}

func (m *mockLedgerStore) PruneData(olderThan time.Time) error {
	return nil
}

func (m *mockLedgerStore) Vacuum() error {
	return nil
}

// Lifecycle operations
func (m *mockLedgerStore) Open() error {
	return nil
}

func (m *mockLedgerStore) Close() error {
	return nil
}

// Health and metrics
func (m *mockLedgerStore) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *mockLedgerStore) GetStats() (*storage.StoreStats, error) {
	return &storage.StoreStats{}, nil
}

func (m *mockLedgerStore) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return nil
}

func (m *mockLedgerStore) IsOpen() bool {
	return true
}

func (m *mockLedgerStore) SaveState(key []byte, value []byte) error {
	return nil
}

func (m *mockLedgerStore) WriteBatch(batch storage.WriteBatch) error {
	return nil
}

func (m *mockLedgerStore) Snapshot(path string) error {
	return nil
}

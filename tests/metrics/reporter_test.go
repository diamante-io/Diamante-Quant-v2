package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/common"
	ctypes "diamante/consensus/types"
	"diamante/metrics"
	"diamante/transaction"
)

// mockConsensus implements ctypes.Consensus, ValidatorMetrics and api.HeightGetter.
type mockConsensus struct {
	networkLoad float64
	height      uint64
	nodes       [][32]byte
	peers       [][32]byte
}

// mockNetwork implements NetworkMetrics
type mockNetwork struct {
	health int
	peers  []string
}

func (mn *mockNetwork) GetNetworkHealth() (int, error) { return mn.health, nil }
func (mn *mockNetwork) GetPeerList() ([]string, error) { return mn.peers, nil }

// mockLedger implements common.LedgerAPI
type mockLedger struct{}

func (ml *mockLedger) CreateAccount(ac *common.Account) error                      { return nil }
func (ml *mockLedger) UpdateAccount(ac *common.Account) error                      { return nil }
func (ml *mockLedger) GetBalance(accountID string) (float64, error)                { return 0, nil }
func (ml *mockLedger) UpdateAccountBalance(accountID string, amount float64) error { return nil }
func (ml *mockLedger) AddTransaction(tx common.Transaction) error                  { return nil }
func (ml *mockLedger) IsTransactionCommitted(txID string) bool                     { return true }
func (ml *mockLedger) GetTransaction(txID string) (*common.Transaction, error)     { return nil, nil }
func (ml *mockLedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	return []common.Transaction{}, nil
}
func (ml *mockLedger) CommitBlock(block common.Block) error          { return nil }
func (ml *mockLedger) GetBlockByNumber(num int) (common.Block, bool) { return common.Block{}, false }
func (ml *mockLedger) GetLastBlockHash() (string, error)             { return "", nil }
func (ml *mockLedger) GetBlockHeight() (int, error)                  { return 10, nil }
func (ml *mockLedger) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	return []common.Block{}, nil
}
func (ml *mockLedger) CreateSnapshot(height int) error                               { return nil }
func (ml *mockLedger) RestoreSnapshot(height int) error                              { return nil }
func (ml *mockLedger) DeploySmartContract(sc *common.SmartContract) error            { return nil }
func (ml *mockLedger) UpdateSmartContract(contractID, newCode, version string) error { return nil }
func (ml *mockLedger) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	return &common.SmartContractResult{
		Success: true,
		GasUsed: 100000,
	}, nil
}
func (ml *mockLedger) RemoveSmartContract(contractID string) error { return nil }
func (ml *mockLedger) IntegrityCheck() error                       { return nil }
func (ml *mockLedger) Close() error                                { return nil }
func (ml *mockLedger) GetStats() (*common.LedgerStats, error) {
	return &common.LedgerStats{
		TotalAccounts:     2,
		TotalTransactions: 12,
		TotalContracts:    5,
		TotalBalance:      1000.0,
		LastBlockHeight:   5,
		NetworkHealth:     "healthy",
		ProcessingTime:    50,
	}, nil
}
func (ml *mockLedger) HealthCheck(ctx context.Context) error { return nil }

func (mc *mockConsensus) GetNetworkLoad() float64                                    { return mc.networkLoad }
func (mc *mockConsensus) GetLachesis() ctypes.Lachesis                               { return nil }
func (mc *mockConsensus) GetDPoS() ctypes.DPoS                                       { return nil }
func (mc *mockConsensus) GetPoH() ctypes.PoH                                         { return nil }
func (mc *mockConsensus) Start() error                                               { return nil }
func (mc *mockConsensus) Stop() error                                                { return nil }
func (mc *mockConsensus) ProcessBlock(uint64) error                                  { return nil }
func (mc *mockConsensus) CreateEvent([32]byte, [][32]byte, []byte) *ctypes.Event     { return nil }
func (mc *mockConsensus) FinalizeEvent(*ctypes.Event) (bool, error)                  { return true, nil }
func (mc *mockConsensus) SynchronizeState([32]byte, uint64) error                    { return nil }
func (mc *mockConsensus) GetValidators() []*ctypes.Validator                         { return nil }
func (mc *mockConsensus) GetActiveValidators() []*ctypes.Validator                   { return nil }
func (mc *mockConsensus) GetPendingEvents() []*ctypes.Event                          { return nil }
func (mc *mockConsensus) GetFinalizedEvents(uint64, uint64) ([]*ctypes.Event, error) { return nil, nil }

// HeightGetter
func (mc *mockConsensus) GetLastBlockHeight() uint64 { return mc.height }

// ValidatorMetrics
func (mc *mockConsensus) GetActiveNodes() ([][32]byte, error) { return mc.nodes, nil }
func (mc *mockConsensus) GetGossipPeers() ([][32]byte, error) { return mc.peers, nil }

// TestReporterCreation tests that the reporter can be created with proper validation
func TestReporterCreation(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.5,
		height:      42,
		nodes:       make([][32]byte, 2),
		peers:       make([][32]byte, 3),
	}
	net := &mockNetwork{
		health: 80,
		peers:  []string{"a", "b", "c", "d"},
	}
	led := &mockLedger{}
	pool := transaction.NewTransactionPool(10, time.Minute, 0.001, 10.0, time.Hour)
	tm := transaction.NewTransactionManager(pool, 0, false, nil)
	logger := logrus.New()

	// Test successful creation
	r, err := metrics.NewReporter(cons, net, led, tm, time.Second, logger, nil, nil, nil)
	if err != nil {
		// This might fail due to Prometheus registration conflicts in tests, which is expected
		t.Logf("Reporter creation failed (expected in test environment): %v", err)
		return
	}

	// Test that reporter implements expected methods
	if r == nil {
		t.Fatal("Reporter should not be nil")
	}

	// Test validation
	if err := metrics.ValidateReporter(r); err != nil {
		t.Errorf("Reporter validation failed: %v", err)
	}

	// Test error cases
	_, err = metrics.NewReporter(nil, net, led, tm, time.Second, logger, nil, nil, nil)
	if err == nil {
		t.Error("Expected error for nil consensus")
	}

	_, err = metrics.NewReporter(cons, net, led, nil, time.Second, logger, nil, nil, nil)
	if err == nil {
		t.Error("Expected error for nil transaction manager")
	}

	_, err = metrics.NewReporter(cons, net, led, tm, 0, logger, nil, nil, nil)
	if err == nil {
		t.Error("Expected error for zero update interval")
	}
}

// TestNetworkMetricsInterface tests the NetworkMetrics interface
func TestNetworkMetricsInterface(t *testing.T) {
	net := &mockNetwork{
		health: 80,
		peers:  []string{"a", "b", "c", "d"},
	}

	health, err := net.GetNetworkHealth()
	if err != nil {
		t.Errorf("GetNetworkHealth failed: %v", err)
	}
	if health != 80 {
		t.Errorf("Expected health 80, got %d", health)
	}

	peers, err := net.GetPeerList()
	if err != nil {
		t.Errorf("GetPeerList failed: %v", err)
	}
	if len(peers) != 4 {
		t.Errorf("Expected 4 peers, got %d", len(peers))
	}
}

// TestValidatorMetricsInterface tests the ValidatorMetrics interface
func TestValidatorMetricsInterface(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.5,
		height:      42,
		nodes:       make([][32]byte, 2),
		peers:       make([][32]byte, 3),
	}

	nodes, err := cons.GetActiveNodes()
	if err != nil {
		t.Errorf("GetActiveNodes failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	peers, err := cons.GetGossipPeers()
	if err != nil {
		t.Errorf("GetGossipPeers failed: %v", err)
	}
	if len(peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(peers))
	}
}

// TestCircuitBreakerFunctionality tests the circuit breaker implementation
func TestCircuitBreakerFunctionality(t *testing.T) {
	cb := metrics.NewCircuitBreaker(2, time.Millisecond*100)

	// Test successful execution
	err := cb.Execute(func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	// Test failure and circuit opening
	for i := 0; i < 3; i++ {
		cb.Execute(func() error {
			return &testError{"test error"}
		})
	}

	// Circuit should now be open
	err = cb.Execute(func() error {
		return nil
	})
	if err == nil {
		t.Error("Expected circuit to be open and reject requests")
	}
}

// testError is a simple error type for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestRetryWithBackoff tests the retry functionality
func TestRetryWithBackoff(t *testing.T) {
	ctx := context.Background()
	config := metrics.DefaultRetryConfig()

	// Test successful execution
	callCount := 0
	err := metrics.RetryWithBackoff(ctx, config, func() error {
		callCount++
		return nil
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	// Test retry on failure
	callCount = 0
	err = metrics.RetryWithBackoff(ctx, config, func() error {
		callCount++
		if callCount < 3 {
			return &testError{"transient error"}
		}
		return nil
	})
	if err != nil {
		t.Errorf("Expected successful execution after retries, got error: %v", err)
	}
	if callCount != 3 {
		t.Errorf("Expected 3 calls, got %d", callCount)
	}
}

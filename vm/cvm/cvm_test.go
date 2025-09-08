package cvm

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock VM Executor for testing
type MockVMExecutor struct {
	executeCalled    bool
	checkpointCalled bool
	gasEstimate      uint64
	executeError     error
}

func (m *MockVMExecutor) Execute(ctx context.Context, msg CVMMessage) (CVMResponse, error) {
	m.executeCalled = true
	if m.executeError != nil {
		return CVMResponse{}, m.executeError
	}

	return CVMResponse{
		MessageID: msg.ID,
		Success:   true,
		Result:    []byte("test result"),
		GasUsed:   m.gasEstimate,
	}, nil
}

func (m *MockVMExecutor) CreateCheckpoint() (string, error) {
	m.checkpointCalled = true
	return "test_checkpoint_123", nil
}

func (m *MockVMExecutor) RestoreCheckpoint(checkpointID string) error {
	return nil
}

func (m *MockVMExecutor) GetCapabilities() []string {
	return []string{"test_capability"}
}

func (m *MockVMExecutor) EstimateGas(msg CVMMessage) (uint64, error) {
	return m.gasEstimate, nil
}

// Test Protocol Creation
func TestNewProtocol(t *testing.T) {
	logger := logrus.New()
	protocol := NewProtocol(logger)

	assert.NotNil(t, protocol)
	assert.NotNil(t, protocol.registry)
	assert.NotNil(t, protocol.atomicTxMgr)
	assert.NotNil(t, protocol.assetBridge)
	assert.NotNil(t, protocol.router)
	assert.NotNil(t, protocol.gasManager)
}

// Test VM Executor Registration
func TestRegisterExecutor(t *testing.T) {
	logger := logrus.New()
	protocol := NewProtocol(logger)

	mockExecutor := &MockVMExecutor{gasEstimate: 1000}

	// Register executor
	err := protocol.RegisterExecutor(VMTypeZKEVM, mockExecutor)
	require.NoError(t, err)

	// Try to register again - should fail
	err = protocol.RegisterExecutor(VMTypeZKEVM, mockExecutor)
	assert.Error(t, err)
}

// Test Cross-VM Call
func TestCrossVMCall(t *testing.T) {
	logger := logrus.New()
	protocol := NewProtocol(logger)

	// Register executors
	zkevmExecutor := &MockVMExecutor{gasEstimate: 1000}
	chaincodeExecutor := &MockVMExecutor{gasEstimate: 500}

	err := protocol.RegisterExecutor(VMTypeZKEVM, zkevmExecutor)
	require.NoError(t, err)

	err = protocol.RegisterExecutor(VMTypeChaincode, chaincodeExecutor)
	require.NoError(t, err)

	// Register contracts in registry
	contract := &ContractMetadata{
		Address: Address{VM: VMTypeChaincode, Address: []byte("test_contract")},
		VM:      VMTypeChaincode,
		Name:    "TestContract",
		Version: "1.0.0",
		Permissions: CrossVMPermissions{
			AllowedVMs:  []VMType{VMTypeZKEVM},
			RequireAuth: false,
		},
	}

	err = protocol.registry.RegisterContract(contract)
	require.NoError(t, err)

	// Create cross-VM message
	msg := CVMMessage{
		SourceVM:   VMTypeZKEVM,
		SourceAddr: Address{VM: VMTypeZKEVM, Address: []byte("source_addr")},
		TargetVM:   VMTypeChaincode,
		TargetAddr: Address{VM: VMTypeChaincode, Address: []byte("test_contract")},
		Method:     "testMethod",
		Arguments:  []byte("test args"),
		GasLimit:   10000,
		Nonce:      1,
	}

	// Execute cross-VM call
	ctx := context.Background()
	response, err := protocol.Call(ctx, msg)

	require.NoError(t, err)
	assert.True(t, response.Success)
	assert.Equal(t, []byte("test result"), response.Result)
	assert.True(t, chaincodeExecutor.executeCalled)
	assert.True(t, chaincodeExecutor.checkpointCalled)
}

// Test Contract Registry
func TestContractRegistry(t *testing.T) {
	registry := NewContractRegistry()

	// Register a contract
	contract := &ContractMetadata{
		Address: Address{VM: VMTypeZKEVM, Address: []byte("0x1234")},
		VM:      VMTypeZKEVM,
		Name:    "TestToken",
		Version: "1.0.0",
		Permissions: CrossVMPermissions{
			AllowedVMs:  []VMType{VMTypeChaincode, VMTypeNative},
			RateLimit:   100,
			RequireAuth: true,
		},
	}

	err := registry.RegisterContract(contract)
	require.NoError(t, err)

	// Retrieve by address
	retrieved, err := registry.GetContract(contract.Address)
	require.NoError(t, err)
	assert.Equal(t, contract.Name, retrieved.Name)

	// Retrieve by alias
	retrieved, err = registry.GetContractByAlias("TestToken")
	require.NoError(t, err)
	assert.Equal(t, contract.Address, retrieved.Address)

	// Test VM routing
	vmType, err := registry.GetVMForAddress(contract.Address)
	require.NoError(t, err)
	assert.Equal(t, VMTypeZKEVM, vmType)
}

// Test Atomic Transaction Manager
func TestAtomicTransactionManager(t *testing.T) {
	logger := logrus.New()
	txMgr := NewAtomicTransactionManager(logger)

	// Begin transaction
	ctx := context.Background()
	msg := CVMMessage{
		SourceVM: VMTypeZKEVM,
		TargetVM: VMTypeChaincode,
		GasLimit: 100000,
	}

	tx, err := txMgr.BeginTransaction(ctx, msg)
	require.NoError(t, err)
	assert.NotEmpty(t, tx.ID)
	assert.Equal(t, TxStatePending, tx.State)

	// Add checkpoint
	checkpoint := Checkpoint{
		VM:           VMTypeChaincode,
		CheckpointID: "test_checkpoint",
		Timestamp:    time.Now(),
	}

	err = txMgr.AddCheckpoint(tx.ID, checkpoint)
	require.NoError(t, err)

	// Commit transaction
	err = txMgr.CommitTransaction(tx.ID)
	require.NoError(t, err)

	// Verify transaction state
	committedTx, err := txMgr.GetTransaction(tx.ID)
	require.NoError(t, err)
	assert.Equal(t, TxStateCommitted, committedTx.State)
}

// Test Asset Bridge
func TestAssetBridge(t *testing.T) {
	logger := logrus.New()
	bridge := NewAssetBridge(logger)

	// Register mock validator
	validator := &MockAssetValidator{}
	err := bridge.RegisterValidator(VMTypeZKEVM, validator)
	require.NoError(t, err)

	// Lock asset
	var assetID AssetID
	copy(assetID[:], []byte("test_asset_id"))

	from := Address{VM: VMTypeZKEVM, Address: []byte("from_addr")}
	to := Address{VM: VMTypeChaincode, Address: []byte("to_addr")}

	err = bridge.LockAsset(assetID, 1000, from, to, "test_tx_123")
	require.NoError(t, err)

	// Verify lock exists
	locks := bridge.GetLockedAssets()
	assert.Len(t, locks, 1)
	assert.Equal(t, assetID, locks[0].AssetID)

	// Transfer asset
	err = bridge.TransferAsset(assetID, 1000, from, to, "test_tx_123")
	require.NoError(t, err)

	// Verify lock removed
	locks = bridge.GetLockedAssets()
	assert.Len(t, locks, 0)
}

// Test Message Router
func TestMessageRouter(t *testing.T) {
	router := NewMessageRouter()

	// Get route
	route, err := router.GetRoute(VMTypeZKEVM, VMTypeChaincode)
	require.NoError(t, err)
	assert.True(t, route.Direct)
	assert.True(t, route.Enabled)

	// Route message
	msg := CVMMessage{
		SourceVM: VMTypeZKEVM,
		TargetVM: VMTypeChaincode,
	}

	err = router.RouteMessage(msg)
	require.NoError(t, err)

	// Get metrics
	metrics, err := router.GetRouteMetrics(VMTypeZKEVM, VMTypeChaincode)
	require.NoError(t, err)
	assert.Equal(t, int64(1), metrics.MessageCount)

	// Disable route
	err = router.DisableRoute(VMTypeZKEVM, VMTypeChaincode)
	require.NoError(t, err)

	route, err = router.GetRoute(VMTypeZKEVM, VMTypeChaincode)
	assert.Error(t, err) // Should fail because route is disabled
}

// Test Gas Manager
func TestGasManager(t *testing.T) {
	gasManager := NewGasManager()

	// Allocate gas
	err := gasManager.AllocateGas("test_tx", 100000)
	require.NoError(t, err)

	// Consume gas
	err = gasManager.ConsumeGas("test_tx", 10000)
	require.NoError(t, err)

	// Consume gas for specific VM
	err = gasManager.ConsumeGasForVM("test_tx", VMTypeZKEVM, 5000)
	require.NoError(t, err)

	// Get allocation
	allocation, err := gasManager.GetAllocation("test_tx")
	require.NoError(t, err)
	assert.Equal(t, uint64(100000), allocation.TotalLimit)
	assert.True(t, allocation.Consumed > 10000) // Should be more due to VM pricing

	// Test insufficient gas
	err = gasManager.ConsumeGas("test_tx", 200000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient gas")
}

// Mock Asset Validator for testing
type MockAssetValidator struct{}

func (v *MockAssetValidator) ValidateAsset(assetID AssetID, amount uint64) error {
	return nil
}

func (v *MockAssetValidator) GetAssetInfo(assetID AssetID) (AssetInfo, error) {
	return AssetInfo{
		ID:       assetID,
		Name:     "Test Asset",
		Symbol:   "TEST",
		Decimals: 18,
		OriginVM: VMTypeZKEVM,
	}, nil
}

func (v *MockAssetValidator) VerifyOwnership(assetID AssetID, owner Address) error {
	return nil
}

// Benchmark Cross-VM Call
func BenchmarkCrossVMCall(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce logging overhead

	protocol := NewProtocol(logger)

	// Register executors
	executor := &MockVMExecutor{gasEstimate: 1000}
	protocol.RegisterExecutor(VMTypeZKEVM, executor)
	protocol.RegisterExecutor(VMTypeChaincode, executor)

	// Register contract
	contract := &ContractMetadata{
		Address:     Address{VM: VMTypeChaincode, Address: []byte("bench_contract")},
		VM:          VMTypeChaincode,
		Permissions: CrossVMPermissions{},
	}
	protocol.registry.RegisterContract(contract)

	// Create message
	msg := CVMMessage{
		SourceVM:   VMTypeZKEVM,
		SourceAddr: Address{VM: VMTypeZKEVM, Address: []byte("source")},
		TargetVM:   VMTypeChaincode,
		TargetAddr: contract.Address,
		Method:     "benchmark",
		Arguments:  []byte("test data"),
		GasLimit:   100000,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Generate new message ID for each call
		msg.ID = MessageID{}
		copy(msg.ID[:], []byte(hex.EncodeToString([]byte{byte(i)})))

		_, err := protocol.Call(ctx, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

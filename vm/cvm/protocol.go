package cvm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Protocol implements the Cross-VM Communication Protocol
type Protocol struct {
	registry    *ContractRegistry
	atomicTxMgr *AtomicTransactionManager
	assetBridge *AssetBridge
	router      *MessageRouter
	gasManager  *GasManager

	executors map[VMType]VMExecutor
	mu        sync.RWMutex
	logger    *logrus.Logger

	// Metrics
	crossVMCalls    prometheus.Counter
	crossVMFailures prometheus.Counter
	callDuration    prometheus.Histogram
}

// VMExecutor interface for VM-specific execution
type VMExecutor interface {
	Execute(ctx context.Context, msg CVMMessage) (CVMResponse, error)
	CreateCheckpoint() (string, error)
	RestoreCheckpoint(checkpointID string) error
	GetCapabilities() []string
	EstimateGas(msg CVMMessage) (uint64, error)
}

// NewProtocol creates a new CVM protocol instance
func NewProtocol(logger *logrus.Logger) *Protocol {
	p := &Protocol{
		registry:    NewContractRegistry(),
		atomicTxMgr: NewAtomicTransactionManager(logger),
		assetBridge: NewAssetBridge(logger),
		router:      NewMessageRouter(),
		gasManager:  NewGasManager(),
		executors:   make(map[VMType]VMExecutor),
		logger:      logger,

		crossVMCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvm_cross_vm_calls_total",
			Help: "Total number of cross-VM calls",
		}),
		crossVMFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvm_cross_vm_failures_total",
			Help: "Total number of failed cross-VM calls",
		}),
		callDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cvm_call_duration_seconds",
			Help:    "Duration of cross-VM calls",
			Buckets: prometheus.DefBuckets,
		}),
	}

	// Register metrics (ignore errors in case of duplicate registration)
	prometheus.Register(p.crossVMCalls)
	prometheus.Register(p.crossVMFailures)
	prometheus.Register(p.callDuration)

	return p
}

// RegisterExecutor registers a VM executor
func (p *Protocol) RegisterExecutor(vmType VMType, executor VMExecutor) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.executors[vmType]; exists {
		return fmt.Errorf("executor for VM type %s already registered", vmType)
	}

	p.executors[vmType] = executor
	p.logger.Infof("Registered executor for VM type: %s", vmType)
	return nil
}

// Call performs a cross-VM call with atomic execution
func (p *Protocol) Call(ctx context.Context, msg CVMMessage) (CVMResponse, error) {
	timer := prometheus.NewTimer(p.callDuration)
	defer timer.ObserveDuration()

	p.crossVMCalls.Inc()

	// Generate message ID if not set
	if msg.ID == (MessageID{}) {
		if err := p.generateMessageID(&msg); err != nil {
			return CVMResponse{}, err
		}
	}

	// Validate message
	if err := p.validateMessage(msg); err != nil {
		p.crossVMFailures.Inc()
		return CVMResponse{}, err
	}

	// Check permissions
	if err := p.checkPermissions(msg); err != nil {
		p.crossVMFailures.Inc()
		return CVMResponse{}, err
	}

	// Create atomic transaction
	tx, err := p.atomicTxMgr.BeginTransaction(ctx, msg)
	if err != nil {
		p.crossVMFailures.Inc()
		return CVMResponse{}, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute with atomicity guarantees
	response, err := p.executeAtomic(ctx, tx, msg)
	if err != nil {
		p.crossVMFailures.Inc()
		// Rollback on error
		if rbErr := p.atomicTxMgr.RollbackTransaction(tx.ID); rbErr != nil {
			p.logger.Errorf("Failed to rollback transaction %s: %v", tx.ID, rbErr)
		}
		return CVMResponse{}, err
	}

	// Commit transaction
	if err := p.atomicTxMgr.CommitTransaction(tx.ID); err != nil {
		p.crossVMFailures.Inc()
		// Try to rollback
		if rbErr := p.atomicTxMgr.RollbackTransaction(tx.ID); rbErr != nil {
			p.logger.Errorf("Failed to rollback after commit error %s: %v", tx.ID, rbErr)
		}
		return CVMResponse{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return response, nil
}

// executeAtomic executes a message within an atomic transaction
func (p *Protocol) executeAtomic(ctx context.Context, tx *AtomicTransaction, msg CVMMessage) (CVMResponse, error) {
	p.mu.RLock()
	executor, exists := p.executors[msg.TargetVM]
	p.mu.RUnlock()

	if !exists {
		return CVMResponse{}, CVMError{
			Code:    ErrCodeVMNotSupported,
			Message: fmt.Sprintf("no executor registered for VM type %s", msg.TargetVM),
			VM:      msg.TargetVM,
		}
	}

	// Lock assets if needed
	for _, asset := range msg.Assets {
		if err := p.assetBridge.LockAsset(asset.AssetID, asset.Amount, msg.SourceAddr, msg.TargetAddr, tx.ID); err != nil {
			return CVMResponse{}, fmt.Errorf("failed to lock asset: %w", err)
		}
	}

	// Create checkpoint before execution
	checkpointID, err := executor.CreateCheckpoint()
	if err != nil {
		return CVMResponse{}, fmt.Errorf("failed to create checkpoint: %w", err)
	}

	checkpoint := Checkpoint{
		VM:           msg.TargetVM,
		CheckpointID: checkpointID,
		Timestamp:    time.Now(),
	}

	if err := p.atomicTxMgr.AddCheckpoint(tx.ID, checkpoint); err != nil {
		return CVMResponse{}, fmt.Errorf("failed to add checkpoint: %w", err)
	}

	// Allocate gas
	if err := p.gasManager.AllocateGas(tx.ID, msg.GasLimit); err != nil {
		return CVMResponse{}, fmt.Errorf("insufficient gas: %w", err)
	}

	// Execute the call
	response, err := executor.Execute(ctx, msg)
	if err != nil {
		return CVMResponse{}, fmt.Errorf("execution failed: %w", err)
	}

	// Track gas usage
	if err := p.gasManager.ConsumeGas(tx.ID, response.GasUsed); err != nil {
		return CVMResponse{}, fmt.Errorf("gas consumption failed: %w", err)
	}

	// Process sub-messages (recursive cross-VM calls)
	for _, subMsg := range response.SubMessages {
		subResp, err := p.Call(ctx, subMsg)
		if err != nil {
			return CVMResponse{}, fmt.Errorf("sub-message execution failed: %w", err)
		}
		response.GasUsed += subResp.GasUsed
	}

	// Transfer assets on success
	for _, asset := range msg.Assets {
		if err := p.assetBridge.TransferAsset(asset.AssetID, asset.Amount, msg.SourceAddr, msg.TargetAddr, tx.ID); err != nil {
			return CVMResponse{}, fmt.Errorf("asset transfer failed: %w", err)
		}
	}

	return response, nil
}

// validateMessage validates a CVM message
func (p *Protocol) validateMessage(msg CVMMessage) error {
	if msg.SourceVM == VMTypeUnknown || msg.TargetVM == VMTypeUnknown {
		return CVMError{
			Code:    ErrCodeInvalidArguments,
			Message: "invalid VM type",
		}
	}

	if len(msg.SourceAddr.Address) == 0 || len(msg.TargetAddr.Address) == 0 {
		return CVMError{
			Code:    ErrCodeInvalidArguments,
			Message: "invalid address",
		}
	}

	if msg.Method == "" {
		return CVMError{
			Code:    ErrCodeInvalidArguments,
			Message: "method cannot be empty",
		}
	}

	if msg.GasLimit == 0 {
		return CVMError{
			Code:    ErrCodeInvalidArguments,
			Message: "gas limit must be greater than zero",
		}
	}

	return nil
}

// checkPermissions checks if the call is allowed
func (p *Protocol) checkPermissions(msg CVMMessage) error {
	// Get target contract metadata
	contract, err := p.registry.GetContract(msg.TargetAddr)
	if err != nil {
		return CVMError{
			Code:    ErrCodeContractNotFound,
			Message: fmt.Sprintf("target contract not found: %v", err),
			VM:      msg.TargetVM,
		}
	}

	perms := contract.Permissions

	// Check if authentication is required
	if perms.RequireAuth {
		// TODO: Implement authentication check
		// For now, we'll assume authentication is handled by the VM
	}

	// Check VM type permission
	vmAllowed := false
	for _, allowedVM := range perms.AllowedVMs {
		if allowedVM == msg.SourceVM {
			vmAllowed = true
			break
		}
	}
	if len(perms.AllowedVMs) > 0 && !vmAllowed {
		return CVMError{
			Code:    ErrCodePermissionDenied,
			Message: fmt.Sprintf("VM type %s not allowed to call this contract", msg.SourceVM),
			VM:      msg.SourceVM,
		}
	}

	// Check method permission
	methodAllowed := false
	for _, allowedMethod := range perms.AllowedMethods {
		if allowedMethod == msg.Method || allowedMethod == "*" {
			methodAllowed = true
			break
		}
	}
	if len(perms.AllowedMethods) > 0 && !methodAllowed {
		return CVMError{
			Code:    ErrCodePermissionDenied,
			Message: fmt.Sprintf("method %s not allowed", msg.Method),
			VM:      msg.TargetVM,
		}
	}

	// Check caller permission
	callerAllowed := false
	for _, allowedCaller := range perms.AllowedCallers {
		if addressEquals(allowedCaller, msg.SourceAddr) {
			callerAllowed = true
			break
		}
	}
	if len(perms.AllowedCallers) > 0 && !callerAllowed {
		return CVMError{
			Code:    ErrCodePermissionDenied,
			Message: "caller not authorized",
			VM:      msg.SourceVM,
		}
	}

	// Check rate limit
	if perms.RateLimit > 0 {
		// TODO: Implement rate limiting
		// For now, we'll skip this check
	}

	return nil
}

// generateMessageID generates a unique message ID
func (p *Protocol) generateMessageID(msg *CVMMessage) error {
	_, err := rand.Read(msg.ID[:])
	return err
}

// GetRegistry returns the contract registry
func (p *Protocol) GetRegistry() *ContractRegistry {
	return p.registry
}

// GetAssetBridge returns the asset bridge
func (p *Protocol) GetAssetBridge() *AssetBridge {
	return p.assetBridge
}

// GetMetrics returns protocol metrics
func (p *Protocol) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"cross_vm_calls":      p.crossVMCalls,
		"cross_vm_failures":   p.crossVMFailures,
		"active_transactions": p.atomicTxMgr.GetActiveTransactionCount(),
		"locked_assets":       p.assetBridge.GetLockedAssetCount(),
	}
}

// addressEquals compares two addresses
func addressEquals(a, b Address) bool {
	if a.VM != b.VM {
		return false
	}
	if len(a.Address) != len(b.Address) {
		return false
	}
	for i := range a.Address {
		if a.Address[i] != b.Address[i] {
			return false
		}
	}
	return true
}

// Helper function to convert address to string
func (a Address) String() string {
	return fmt.Sprintf("%s:%s", a.VM, hex.EncodeToString(a.Address))
}

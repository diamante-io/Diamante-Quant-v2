// Package app provides hybrid VM integration with block persistence
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/transaction"
	"diamante/vm/deploy"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// ConsensusEngine defines the interface for consensus engines
type ConsensusEngine interface {
	// ProcessBlock processes a block through consensus
	ProcessBlock(block *common.Block) error
	// ValidateBlock validates a block
	ValidateBlock(block *common.Block) error
	// GetCurrentHeight returns the current blockchain height
	GetCurrentHeight() uint64
	// IsRunning checks if the consensus engine is running
	IsRunning() bool
}

// BlockPersistenceModule defines the interface for block persistence
type BlockPersistenceModule interface {
	// PersistBlock persists a block to storage
	PersistBlock(block *common.Block) error
	// LoadBlock loads a block from storage
	LoadBlock(height uint64) (*common.Block, error)
	// RegisterProcessor registers a block processor
	RegisterProcessor(processor BlockProcessor) error
}

// BlockProcessor defines the interface for block processing
type BlockProcessor interface {
	// ProcessBlock processes a block
	ProcessBlock(block *common.Block) error
}

// HybridVMIntegration integrates the hybrid VM with block persistence
type HybridVMIntegration struct {
	// Core components
	runtimeManager    *runtime.RuntimeManager
	deploymentManager *deploy.DeploymentManager
	hybridProcessor   *transaction.HybridTransactionProcessor
	eventBus          *runtime.UnifiedEventBus

	// Consensus and storage
	consensusEngine ConsensusEngine
	blockPersister  BlockPersistenceModule
	ledger          common.LedgerAPI
	stateStore      storage.LedgerStore

	// Configuration
	config *HybridVMConfig

	// State
	mu      sync.RWMutex
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Logger
	logger *logrus.Logger
}

// HybridVMConfig contains configuration for hybrid VM integration
type HybridVMConfig struct {
	// VM Configuration
	EnableEVM       bool
	EnableChaincode bool
	EnableNative    bool

	// Cross-runtime features
	EnableCrossRuntimeCalls    bool
	EnableCrossRuntimeEvents   bool
	CrossRuntimeCallGasLimit   uint64
	CrossRuntimeEventQueueSize int

	// Performance settings
	TransactionWorkers int
	EventWorkers       int
	MaxPendingTxs      int

	// Timeouts
	TransactionTimeout time.Duration
	DeploymentTimeout  time.Duration
}

// DefaultHybridVMConfig returns default configuration
func DefaultHybridVMConfig() *HybridVMConfig {
	return &HybridVMConfig{
		EnableEVM:                  true,
		EnableChaincode:            true,
		EnableNative:               true,
		EnableCrossRuntimeCalls:    true,
		EnableCrossRuntimeEvents:   true,
		CrossRuntimeCallGasLimit:   10000000,
		CrossRuntimeEventQueueSize: 10000,
		TransactionWorkers:         10,
		EventWorkers:               5,
		MaxPendingTxs:              1000,
		TransactionTimeout:         30 * time.Second,
		DeploymentTimeout:          60 * time.Second,
	}
}

// NewHybridVMIntegration creates a new hybrid VM integration
func NewHybridVMIntegration(
	runtimeManager *runtime.RuntimeManager,
	deploymentManager *deploy.DeploymentManager,
	consensusEngine ConsensusEngine,
	blockPersister BlockPersistenceModule,
	ledger common.LedgerAPI,
	stateStore storage.LedgerStore,
	config *HybridVMConfig,
	logger *logrus.Logger,
) (*HybridVMIntegration, error) {
	if config == nil {
		config = DefaultHybridVMConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	// Create event bus
	eventBus := runtime.NewUnifiedEventBus(logger)

	// Create hybrid processor
	hybridProcessor := transaction.NewHybridTransactionProcessor(
		runtimeManager,
		deploymentManager,
		ledger,
		stateStore,
		logger,
	)

	integration := &HybridVMIntegration{
		runtimeManager:    runtimeManager,
		deploymentManager: deploymentManager,
		hybridProcessor:   hybridProcessor,
		eventBus:          eventBus,
		consensusEngine:   consensusEngine,
		blockPersister:    blockPersister,
		ledger:            ledger,
		stateStore:        stateStore,
		config:            config,
		logger:            logger,
		started:           false,
	}

	return integration, nil
}

// Start starts the hybrid VM integration
func (h *HybridVMIntegration) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.started {
		return nil
	}

	h.ctx, h.cancel = context.WithCancel(context.Background())

	// Initialize runtime manager
	if err := h.runtimeManager.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize runtime manager: %w", err)
	}

	// Start runtime manager
	if err := h.runtimeManager.Start(); err != nil {
		return fmt.Errorf("failed to start runtime manager: %w", err)
	}

	// Start event bus
	if err := h.eventBus.Start(); err != nil {
		return fmt.Errorf("failed to start event bus: %w", err)
	}

	// Enable cross-runtime features if configured
	if h.config.EnableCrossRuntimeEvents {
		h.eventBus.EnableCrossRuntimeEvents(true)
		h.setupCrossRuntimeHandlers()
	}

	// Register block processor
	h.registerBlockProcessor()

	// Start transaction workers
	for i := 0; i < h.config.TransactionWorkers; i++ {
		h.wg.Add(1)
		go h.transactionWorker(i)
	}

	h.started = true
	h.logger.Info("Hybrid VM integration started")

	return nil
}

// Stop stops the hybrid VM integration
func (h *HybridVMIntegration) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.started {
		return nil
	}

	// Cancel context
	h.cancel()

	// Wait for workers
	h.wg.Wait()

	// Stop components in reverse order
	if err := h.eventBus.Stop(); err != nil {
		h.logger.WithError(err).Error("Failed to stop event bus")
	}

	if err := h.runtimeManager.Stop(); err != nil {
		h.logger.WithError(err).Error("Failed to stop runtime manager")
	}

	h.started = false
	h.logger.Info("Hybrid VM integration stopped")

	return nil
}

// ProcessBlock processes a block through the hybrid VM
func (h *HybridVMIntegration) ProcessBlock(block *common.Block) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.started {
		return errors.New("hybrid VM integration not started")
	}

	if block == nil {
		return errors.New("block cannot be nil")
	}

	startTime := consensus.ConsensusNow()

	// Process transactions in the block
	results := make([]*transaction.TransactionResult, 0, len(block.Transactions))
	receipts := make([]*storage.Receipt, 0, len(block.Transactions))
	processingErrors := make([]error, 0)

	for i := range block.Transactions {
		// Get transaction reference to modify
		tx := &block.Transactions[i]

		// Set block context in transaction
		tx.BlockHeight = block.Number
		// Note: Transaction doesn't have BlockHash field in current structure

		// Process through hybrid processor
		ctx, cancel := context.WithTimeout(h.ctx, h.config.TransactionTimeout)
		result, err := h.hybridProcessor.ProcessTransaction(ctx, *tx)
		cancel()

		if err != nil {
			h.logger.WithError(err).WithField("txID", tx.ID).Error("Failed to process transaction")
			processingErrors = append(processingErrors, fmt.Errorf("tx %s: %w", tx.ID, err))
			// Create failed result for receipt
			result = &transaction.TransactionResult{
				Success: false,
				Error:   err.Error(),
				GasUsed: 0,
			}
		}

		results = append(results, result)

		// Create receipt
		receipt := h.createReceipt(*tx, result, block)
		receipts = append(receipts, receipt)
	}

	// Store receipts
	receiptErrors := make([]error, 0)
	for _, receipt := range receipts {
		if err := h.stateStore.SaveReceipt(receipt); err != nil {
			h.logger.WithError(err).Error("Failed to save receipt")
			receiptErrors = append(receiptErrors, fmt.Errorf("receipt %s: %w", receipt.TxID, err))
		}
	}

	// Store VM state in block metadata
	vmState := h.createVMState(results)

	// Persist block if we have a block persister
	if h.blockPersister != nil {
		if err := h.blockPersister.PersistBlock(block); err != nil {
			h.logger.WithError(err).Error("Failed to persist block")
			return fmt.Errorf("failed to persist block: %w", err)
		}
	}

	h.logger.WithFields(logrus.Fields{
		"blockNumber":      block.Number,
		"transactions":     len(block.Transactions),
		"processed":        len(results),
		"processingErrors": len(processingErrors),
		"receiptErrors":    len(receiptErrors),
		"processingTime":   time.Since(startTime),
		"vmState":          vmState,
	}).Info("Block processed through hybrid VM")

	// Return error if there were critical processing errors
	if len(processingErrors) == len(block.Transactions) && len(block.Transactions) > 0 {
		return fmt.Errorf("all transactions failed to process: %v", processingErrors)
	}

	return nil
}

// Helper methods

func (h *HybridVMIntegration) setupCrossRuntimeHandlers() {
	// Create cross-runtime event handler
	crossRuntimeHandler := runtime.NewCrossRuntimeEventHandler(
		h.runtimeManager,
		h.eventBus,
		h.logger,
	)

	// Subscribe to cross-runtime events
	if err := h.eventBus.SubscribeCrossRuntime(crossRuntimeHandler); err != nil {
		h.logger.WithError(err).Error("Failed to setup cross-runtime handler")
	}

	// Setup runtime-specific event handlers
	h.runtimeManager.SetEventHandler(&hybridEventHandler{
		eventBus: h.eventBus,
		logger:   h.logger,
	})
}

func (h *HybridVMIntegration) registerBlockProcessor() {
	// Register this integration as a block processor
	if h.blockPersister != nil {
		if err := h.blockPersister.RegisterProcessor(h); err != nil {
			h.logger.WithError(err).Error("Failed to register block processor")
		} else {
			h.logger.Info("Block processor registered successfully")
		}
	} else {
		h.logger.Warn("Block persister not available, skipping processor registration")
	}
}

func (h *HybridVMIntegration) transactionWorker(id int) {
	defer h.wg.Done()

	h.logger.WithField("workerID", id).Debug("Transaction worker started")

	// This worker would handle async transaction processing if needed
	// For now, transactions are processed synchronously in ProcessBlock

	<-h.ctx.Done()
}

func (h *HybridVMIntegration) createReceipt(
	tx common.Transaction,
	result *transaction.TransactionResult,
	block *common.Block,
) *storage.Receipt {
	// Convert events to logs
	logs := make([]storage.EventLog, len(result.Events))
	for i, event := range result.Events {
		logs[i] = storage.EventLog{
			Address:     event.ContractID,
			Topics:      []string{event.Name},
			Data:        event.Data,
			BlockNumber: uint64(block.Number),
			TxHash:      tx.ID,
			Index:       uint(i),
		}
	}

	// Create metadata struct
	metadata := storage.ReceiptMetadata{
		CumulativeGasUsed: result.GasUsed,
		Type:              result.Result.Type,
	}

	// Add error if transaction failed
	if !result.Success && result.Error != "" {
		metadata.Error = result.Error
	}

	// Add deployment-specific metadata
	if result.Result.DeploymentInfo != nil {
		metadata.ContractAddress = result.Result.DeploymentInfo.ContractID
		metadata.ContractCreated = true
	}

	// Add execution-specific metadata
	if result.Result.ExecutionInfo != nil {
		// Return data is already in byte format
		if len(result.Result.ExecutionInfo.ReturnData) > 0 {
			metadata.ReturnValue = string(result.Result.ExecutionInfo.ReturnData)
		}
	}

	receipt := &storage.Receipt{
		TxID:        tx.ID,
		BlockHeight: uint64(block.Number),
		BlockHash:   block.Hash,
		Status:      result.Success,
		GasUsed:     result.GasUsed,
		Logs:        logs,
		Metadata:    metadata,
		CreatedAt:   consensus.ConsensusNow(),
	}

	return receipt
}

func (h *HybridVMIntegration) createVMState(results []*transaction.TransactionResult) map[string]interface{} {
	vmState := make(map[string]interface{})

	// Aggregate metrics
	totalGasUsed := uint64(0)
	successCount := 0
	failureCount := 0
	runtimeDistribution := make(map[string]int)

	for _, result := range results {
		totalGasUsed += result.GasUsed
		if result.Success {
			successCount++
		} else {
			failureCount++
		}

		// Track runtime distribution
		for _, event := range result.Events {
			// Check if the event has a runtime field in its parameters
			if event.Parameters.StringParams != nil {
				if runtime, ok := event.Parameters.StringParams["__runtime"]; ok {
					runtimeDistribution[runtime]++
				}
			}
		}
	}

	vmState["totalGasUsed"] = totalGasUsed
	vmState["successCount"] = successCount
	vmState["failureCount"] = failureCount
	vmState["runtimeDistribution"] = runtimeDistribution

	return vmState
}

// hybridEventHandler implements RuntimeEventHandler for the hybrid VM
type hybridEventHandler struct {
	eventBus *runtime.UnifiedEventBus
	logger   *logrus.Logger
}

func (h *hybridEventHandler) HandleEvent(event runtime.ContractEvent) error {
	// Publish to unified event bus
	return h.eventBus.PublishEvent(event)
}

// HybridVMOrchestrator orchestrates the entire hybrid VM system
type HybridVMOrchestrator struct {
	// Core components
	integration *HybridVMIntegration
	apiHandler  *HybridVMAPIHandler

	// Configuration
	config *HybridVMConfig

	// State
	mu      sync.RWMutex
	started bool

	// Logger
	logger *logrus.Logger
}

// NewHybridVMOrchestrator creates a new orchestrator
func NewHybridVMOrchestrator(
	integration *HybridVMIntegration,
	config *HybridVMConfig,
	logger *logrus.Logger,
) *HybridVMOrchestrator {
	if config == nil {
		config = DefaultHybridVMConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	return &HybridVMOrchestrator{
		integration: integration,
		config:      config,
		logger:      logger,
	}
}

// Start starts the orchestrator
func (o *HybridVMOrchestrator) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.started {
		return nil
	}

	// Start integration
	if err := o.integration.Start(); err != nil {
		return fmt.Errorf("failed to start integration: %w", err)
	}

	o.started = true
	o.logger.Info("Hybrid VM orchestrator started")

	return nil
}

// Stop stops the orchestrator
func (o *HybridVMOrchestrator) Stop() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.started {
		return nil
	}

	// Stop integration
	if err := o.integration.Stop(); err != nil {
		return fmt.Errorf("failed to stop integration: %w", err)
	}

	o.started = false
	o.logger.Info("Hybrid VM orchestrator stopped")

	return nil
}

// GetStatus returns the status of the hybrid VM system
func (o *HybridVMOrchestrator) GetStatus() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	status := make(map[string]interface{})
	status["started"] = o.started

	if o.started {
		// Get runtime statuses
		runtimes := o.integration.runtimeManager.ListRuntimes()
		runtimeStatuses := make(map[string]bool)
		for _, rt := range runtimes {
			runtimeStatuses[string(rt)] = o.integration.runtimeManager.IsRuntimeActive(rt)
		}
		status["runtimes"] = runtimeStatuses

		// Get event bus metrics
		eventMetrics := o.integration.eventBus.GetMetrics()
		status["eventBus"] = map[string]interface{}{
			"eventsReceived":  eventMetrics.EventsReceived,
			"eventsDelivered": eventMetrics.EventsDelivered,
			"eventsDropped":   eventMetrics.EventsDropped,
		}

		// Get transaction processor metrics
		txMetrics := o.integration.hybridProcessor.GetMetrics()
		status["transactions"] = map[string]interface{}{
			"totalProcessed":     txMetrics.TotalProcessed,
			"totalFailed":        txMetrics.TotalFailed,
			"averageGasUsed":     txMetrics.AverageGasUsed,
			"averageProcessTime": txMetrics.AverageProcessTime.String(),
		}
	}

	return status
}

// HybridVMAPIHandler wraps the API handler for the orchestrator
type HybridVMAPIHandler struct {
	orchestrator *HybridVMOrchestrator
}

// GetSystemStatus returns the hybrid VM system status
func (h *HybridVMAPIHandler) GetSystemStatus() map[string]interface{} {
	return h.orchestrator.GetStatus()
}

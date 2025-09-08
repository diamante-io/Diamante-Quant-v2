// Package transaction provides hybrid VM transaction processing
package transaction

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/vm/deploy"
	"diamante/vm/runtime"

	"encoding/json"
	"strconv"

	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidHybridTransaction is returned when transaction is invalid for hybrid VM
	ErrInvalidHybridTransaction = errors.New("invalid hybrid transaction")

	// ErrRuntimeNotSupported is returned when runtime is not supported
	ErrRuntimeNotSupported = errors.New("runtime not supported")

	// ErrContractNotFound is returned when contract is not found
	ErrContractNotFound = errors.New("contract not found")

	// ErrInsufficientGas is returned when gas is insufficient
	ErrInsufficientGas = errors.New("insufficient gas")

	// ErrUnauthorized is returned when user is not authorized
	ErrUnauthorized = errors.New("unauthorized operation")
)

// HybridTransactionProcessor processes transactions through the hybrid VM
type HybridTransactionProcessor struct {
	runtimeManager    *runtime.RuntimeManager
	deploymentManager *deploy.DeploymentManager
	ledger            common.LedgerAPI
	stateStore        storage.LedgerStore
	logger            *logrus.Logger

	// Transaction processing state
	mu                sync.RWMutex
	pendingTxs        map[string]*ProcessingTransaction
	processedTxs      map[string]*TransactionResult
	maxPendingTxs     int
	processingTimeout time.Duration

	// Metrics
	metrics *TransactionMetrics
}

// ProcessingTransaction tracks a transaction being processed
type ProcessingTransaction struct {
	Transaction common.Transaction
	StartTime   time.Time
	Runtime     runtime.RuntimeType
	ContractID  string
	Status      string
}

// TransactionResult stores the result of a processed transaction
type TransactionResult struct {
	TransactionID string
	Success       bool
	Result        TransactionResultData // Changed from interface{} to concrete type
	GasUsed       uint64
	Events        []runtime.ContractEvent
	StateChanges  []runtime.StateChange
	Error         string
	ProcessedAt   time.Time
}

// TransactionResultData represents the result data from different transaction types
type TransactionResultData struct {
	Type           string                `json:"type"`
	ContractID     string                `json:"contract_id,omitempty"`
	DeploymentInfo *DeploymentResultInfo `json:"deployment_info,omitempty"`
	ExecutionInfo  *ExecutionResultInfo  `json:"execution_info,omitempty"`
	UpgradeInfo    *UpgradeResultInfo    `json:"upgrade_info,omitempty"`
	TransferInfo   *TransferResultInfo   `json:"transfer_info,omitempty"`
}

// DeploymentResultInfo contains deployment-specific result information
type DeploymentResultInfo struct {
	ContractID   string            `json:"contract_id"`
	DeployedBy   string            `json:"deployed_by"`
	Language     string            `json:"language"`
	CodeHash     string            `json:"code_hash"`
	InitialValue uint64            `json:"initial_value"`
	CreatedAt    time.Time         `json:"created_at"`
	Metadata     map[string]string `json:"metadata"`
}

// ExecutionResultInfo contains execution-specific result information
type ExecutionResultInfo struct {
	ContractID    string            `json:"contract_id"`
	Function      string            `json:"function"`
	ReturnData    []byte            `json:"return_data"`
	DecodedResult string            `json:"decoded_result,omitempty"`
	Metadata      map[string]string `json:"metadata"`
}

// UpgradeResultInfo contains upgrade-specific result information
type UpgradeResultInfo struct {
	ContractID string            `json:"contract_id"`
	OldVersion string            `json:"old_version"`
	NewVersion string            `json:"new_version"`
	UpgradedBy string            `json:"upgraded_by"`
	UpgradedAt time.Time         `json:"upgraded_at"`
	Metadata   map[string]string `json:"metadata"`
}

// TransferResultInfo contains transfer-specific result information
type TransferResultInfo struct {
	From     string            `json:"from"`
	To       string            `json:"to"`
	Amount   float64           `json:"amount"`
	Fee      float64           `json:"fee"`
	Metadata map[string]string `json:"metadata"`
}

// TransactionMetrics tracks transaction processing metrics
type TransactionMetrics struct {
	TotalProcessed      uint64
	TotalFailed         uint64
	AverageGasUsed      uint64
	AverageProcessTime  time.Duration
	RuntimeDistribution map[runtime.RuntimeType]uint64
}

// ReceiptMetadata represents typed metadata for transaction receipts
type ReceiptMetadata struct {
	ContractAddress string `json:"contract_address,omitempty"`
	Result          []byte `json:"result,omitempty"`
	ReturnValue     string `json:"return_value,omitempty"`
	EventCount      int    `json:"event_count,omitempty"`
	StateChanges    int    `json:"state_changes,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

// TransactionMetadataExtractor helps extract data from transaction metadata
type TransactionMetadataExtractor struct {
	Type       string            `json:"type,omitempty"`
	Function   string            `json:"function,omitempty"`
	Method     string            `json:"method,omitempty"`
	ContractID string            `json:"contractID,omitempty"`
	Code       string            `json:"code,omitempty"`
	Language   string            `json:"language,omitempty"`
	Version    string            `json:"version,omitempty"`
	GasLimit   uint64            `json:"gasLimit,omitempty"`
	GasPrice   uint64            `json:"gasPrice,omitempty"`
	Args       []json.RawMessage `json:"args,omitempty"`
	Metadata   json.RawMessage   `json:"metadata,omitempty"`
	DryRun     bool              `json:"dryRun,omitempty"`
}

// NewHybridTransactionProcessor creates a new hybrid transaction processor
func NewHybridTransactionProcessor(
	runtimeManager *runtime.RuntimeManager,
	deploymentManager *deploy.DeploymentManager,
	ledger common.LedgerAPI,
	stateStore storage.LedgerStore,
	logger *logrus.Logger,
) *HybridTransactionProcessor {
	return &HybridTransactionProcessor{
		runtimeManager:    runtimeManager,
		deploymentManager: deploymentManager,
		ledger:            ledger,
		stateStore:        stateStore,
		logger:            logger,
		pendingTxs:        make(map[string]*ProcessingTransaction),
		processedTxs:      make(map[string]*TransactionResult),
		maxPendingTxs:     1000,
		processingTimeout: 30 * time.Second,
		metrics: &TransactionMetrics{
			RuntimeDistribution: make(map[runtime.RuntimeType]uint64),
		},
	}
}

// ProcessTransaction processes a transaction through the appropriate runtime
func (p *HybridTransactionProcessor) ProcessTransaction(ctx context.Context, tx common.Transaction) (*TransactionResult, error) {
	p.mu.Lock()

	// Check pending transaction limit
	if len(p.pendingTxs) >= p.maxPendingTxs {
		p.mu.Unlock()
		return nil, errors.New("too many pending transactions")
	}

	// Create processing transaction
	processingTx := &ProcessingTransaction{
		Transaction: tx,
		StartTime:   consensus.ConsensusNow(),
		Status:      "pending",
	}

	p.pendingTxs[tx.ID] = processingTx
	p.mu.Unlock()

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, p.processingTimeout)
	defer cancel()

	// Process transaction
	result, err := p.processTransactionInternal(timeoutCtx, tx, processingTx)

	// Update state
	p.mu.Lock()
	delete(p.pendingTxs, tx.ID)
	if result != nil {
		p.processedTxs[tx.ID] = result
		p.updateMetrics(result, processingTx)
	}
	p.mu.Unlock()

	return result, err
}

// processTransactionInternal handles the actual transaction processing
func (p *HybridTransactionProcessor) processTransactionInternal(
	ctx context.Context,
	tx common.Transaction,
	processingTx *ProcessingTransaction,
) (*TransactionResult, error) {
	// Validate transaction
	if err := p.validateTransaction(tx); err != nil {
		return &TransactionResult{
			TransactionID: tx.ID,
			Success:       false,
			Error:         err.Error(),
			ProcessedAt:   consensus.ConsensusNow(),
		}, err
	}

	// Determine transaction type
	txType := p.determineTransactionType(tx)

	switch txType {
	case "deploy":
		return p.processDeployment(ctx, tx, processingTx)
	case "execute":
		return p.processExecution(ctx, tx, processingTx)
	case "upgrade":
		return p.processUpgrade(ctx, tx, processingTx)
	default:
		// For non-contract transactions, process normally
		return p.processStandardTransaction(ctx, tx)
	}
}

// processDeployment handles contract deployment transactions
func (p *HybridTransactionProcessor) processDeployment(
	ctx context.Context,
	tx common.Transaction,
	processingTx *ProcessingTransaction,
) (*TransactionResult, error) {
	// Extract deployment data from transaction
	deployData, err := p.extractDeploymentData(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract deployment data: %w", err)
	}

	// Update processing status
	processingTx.Runtime = p.getRuntimeFromLanguage(deployData.Language)
	processingTx.Status = "deploying"

	// Create deployment request with proper type conversions
	deployReq := deploy.DeploymentRequest{
		Deployer:        tx.Sender,
		Language:        deployData.Language,
		Code:            deployData.Code,
		ConstructorArgs: p.convertToConstructorArguments(deployData.Args),
		InitialValue:    uint64(tx.Amount), // Convert float64 to uint64
		GasLimit:        p.extractGasLimit(tx),
		Metadata:        p.convertToDeploymentMetadata(deployData.Metadata),
	}

	// Deploy through deployment manager
	deployResp, err := p.deploymentManager.DeployContract(ctx, deployReq)
	if err != nil {
		return &TransactionResult{
			TransactionID: tx.ID,
			Success:       false,
			Error:         err.Error(),
			ProcessedAt:   consensus.ConsensusNow(),
		}, err
	}

	// Update transaction with contract ID
	tx.SmartContractID = deployResp.ContractID
	processingTx.ContractID = deployResp.ContractID

	// Create receipt
	receipt := &storage.Receipt{
		TxID:        tx.ID,
		BlockHeight: uint64(tx.BlockHeight),
		BlockHash:   "", // Will be set by block processing
		Status:      true,
		GasUsed:     deployResp.GasUsed,
		Logs:        p.convertDeploymentEventLogs(deployResp.Events),
		Metadata: storage.ReceiptMetadata{
			ContractAddress: deployResp.ContractID,
			ContractCreated: true,
			Type:            "deployment",
		},
		CreatedAt: consensus.ConsensusNow(),
	}

	// Store receipt
	if err := p.stateStore.SaveReceipt(receipt); err != nil {
		p.logger.WithError(err).Error("Failed to save receipt")
	}

	return &TransactionResult{
		TransactionID: tx.ID,
		Success:       true,
		Result: TransactionResultData{
			Type: "deployment",
			DeploymentInfo: &DeploymentResultInfo{
				ContractID:   deployResp.ContractID,
				DeployedBy:   tx.Sender,
				Language:     deployData.Language,
				CodeHash:     hex.EncodeToString(deployData.Code),
				InitialValue: uint64(tx.Amount),
				CreatedAt:    consensus.ConsensusNow(),
				Metadata:     deployData.Metadata,
			},
		},
		GasUsed:     deployResp.GasUsed,
		Events:      p.convertToRuntimeEvents(deployResp.Events),
		ProcessedAt: consensus.ConsensusNow(),
	}, nil
}

// processExecution handles contract execution transactions
func (p *HybridTransactionProcessor) processExecution(
	ctx context.Context,
	tx common.Transaction,
	processingTx *ProcessingTransaction,
) (*TransactionResult, error) {
	// Extract execution data from transaction
	execData, err := p.extractExecutionData(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract execution data: %w", err)
	}

	// Get contract info to determine runtime
	contractInfo, err := p.runtimeManager.GetContractInfo(execData.ContractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract info: %w", err)
	}

	// Update processing status
	processingTx.Runtime = contractInfo.Runtime
	processingTx.ContractID = execData.ContractID
	processingTx.Status = "executing"

	// Create contract call with proper type conversion
	call := runtime.ContractCall{
		ContractID: execData.ContractID,
		Caller:     tx.Sender,
		Function:   execData.Function,
		Args:       p.convertToContractParameters(execData.Args),
		Value:      uint64(tx.Amount), // Convert float64 to uint64
		GasLimit:   p.extractGasLimit(tx),
	}

	// Execute through runtime manager
	execResult, err := p.runtimeManager.ExecuteContract(ctx, call)
	if err != nil {
		return &TransactionResult{
			TransactionID: tx.ID,
			Success:       false,
			Error:         err.Error(),
			GasUsed:       p.extractGasLimit(tx), // Consume all gas on error
			ProcessedAt:   consensus.ConsensusNow(),
		}, err
	}

	// Create receipt
	receipt := &storage.Receipt{
		TxID:        tx.ID,
		BlockHeight: uint64(tx.BlockHeight),
		BlockHash:   "", // Will be set by block processing
		Status:      execResult.Success,
		GasUsed:     execResult.GasUsed,
		Logs:        p.convertRuntimeEventLogs(execResult.Events),
		Metadata: storage.ReceiptMetadata{
			ContractAddress: execData.ContractID,
			ReturnValue:     string(execResult.RawReturnData),
			Type:            "execution",
		},
		CreatedAt: consensus.ConsensusNow(),
	}

	// Store receipt
	if err := p.stateStore.SaveReceipt(receipt); err != nil {
		p.logger.WithError(err).Error("Failed to save receipt")
	}

	return &TransactionResult{
		TransactionID: tx.ID,
		Success:       execResult.Success,
		Result: TransactionResultData{
			Type: "execution",
			ExecutionInfo: &ExecutionResultInfo{
				ContractID:    execData.ContractID,
				Function:      execData.Function,
				ReturnData:    execResult.RawReturnData, // Use RawReturnData instead of ReturnData
				DecodedResult: string(execResult.RawReturnData),
				Metadata:      p.convertMetadata(tx.Metadata),
			},
		},
		GasUsed:      execResult.GasUsed,
		Events:       execResult.Events,
		StateChanges: execResult.StateChanges,
		Error:        execResult.Error,
		ProcessedAt:  consensus.ConsensusNow(),
	}, nil
}

// processUpgrade handles contract upgrade transactions
func (p *HybridTransactionProcessor) processUpgrade(
	ctx context.Context,
	tx common.Transaction,
	_ *ProcessingTransaction,
) (*TransactionResult, error) {
	// Extract upgrade data
	upgradeData, err := p.extractUpgradeData(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract upgrade data: %w", err)
	}

	// Create upgrade request with proper type conversion
	upgradeReq := deploy.UpgradeRequest{
		ContractID:    upgradeData.ContractID,
		Authorizer:    tx.Sender,
		NewVersion:    upgradeData.Version,
		NewCode:       upgradeData.Code,
		MigrationData: upgradeData.MigrationData,
		Metadata:      p.convertToDeploymentMetadata(upgradeData.Metadata),
	}

	// Upgrade through deployment manager
	upgradeResp, err := p.deploymentManager.UpgradeContract(ctx, upgradeReq)
	if err != nil {
		return &TransactionResult{
			TransactionID: tx.ID,
			Success:       false,
			Error:         err.Error(),
			ProcessedAt:   consensus.ConsensusNow(),
		}, err
	}

	return &TransactionResult{
		TransactionID: tx.ID,
		Success:       true,
		Result: TransactionResultData{
			Type: "upgrade",
			UpgradeInfo: &UpgradeResultInfo{
				ContractID: upgradeData.ContractID,
				OldVersion: upgradeData.Version, // Assuming old version is the current one before upgrade
				NewVersion: upgradeResp.NewVersion,
				UpgradedBy: tx.Sender,
				UpgradedAt: consensus.ConsensusNow(),
				Metadata:   upgradeData.Metadata,
			},
		},
		GasUsed:     50000, // Fixed gas for upgrades
		ProcessedAt: consensus.ConsensusNow(),
	}, nil
}

// processStandardTransaction handles non-contract transactions
func (p *HybridTransactionProcessor) processStandardTransaction(
	_ context.Context,
	tx common.Transaction,
) (*TransactionResult, error) {
	// For standard transfers, just update balances
	if tx.Receiver != "" {
		// Deduct from sender
		if err := p.ledger.UpdateAccountBalance(tx.Sender, -tx.Amount); err != nil {
			return nil, fmt.Errorf("failed to deduct from sender: %w", err)
		}

		// Add to receiver
		if err := p.ledger.UpdateAccountBalance(tx.Receiver, tx.Amount); err != nil {
			// Rollback sender balance
			p.ledger.UpdateAccountBalance(tx.Sender, tx.Amount)
			return nil, fmt.Errorf("failed to add to receiver: %w", err)
		}
	}

	// Record transaction
	if err := p.ledger.AddTransaction(tx); err != nil {
		return nil, fmt.Errorf("failed to record transaction: %w", err)
	}

	return &TransactionResult{
		TransactionID: tx.ID,
		Success:       true,
		Result: TransactionResultData{
			Type: "transfer",
			TransferInfo: &TransferResultInfo{
				From:     tx.Sender,
				To:       tx.Receiver,
				Amount:   tx.Amount,
				Fee:      0.0, // Standard transfer has no fee
				Metadata: p.convertMetadata(tx.Metadata),
			},
		},
		GasUsed:     21000, // Standard transfer gas
		ProcessedAt: consensus.ConsensusNow(),
	}, nil
}

// Helper methods

func (p *HybridTransactionProcessor) validateTransaction(tx common.Transaction) error {
	if tx.ID == "" {
		return fmt.Errorf("%w: missing transaction ID", ErrInvalidHybridTransaction)
	}
	if tx.Sender == "" {
		return fmt.Errorf("%w: missing sender address", ErrInvalidHybridTransaction)
	}

	// Extract gas limit from metadata
	gasLimit := p.extractGasLimit(tx)
	if gasLimit == 0 {
		return fmt.Errorf("%w: gas limit cannot be zero", ErrInvalidHybridTransaction)
	}

	// Check account balance
	balance, err := p.ledger.GetBalance(tx.Sender)
	if err != nil {
		return fmt.Errorf("failed to get balance: %w", err)
	}

	// Calculate total cost including gas
	gasPrice := p.extractGasPrice(tx)
	totalCost := tx.Amount + float64(gasLimit*gasPrice)
	if balance < totalCost {
		return fmt.Errorf("%w: insufficient balance", ErrInvalidHybridTransaction)
	}

	return nil
}

func (p *HybridTransactionProcessor) determineTransactionType(tx common.Transaction) string {
	// Check if it's a smart contract execution
	if tx.SmartContractID != "" {
		return "execute"
	}

	// Check transaction metadata for type indicators
	if tx.Metadata != nil {
		// Check specific fields in the metadata struct
		if tx.Metadata.Category == "deploy" {
			return "deploy"
		}
		if tx.Metadata.Category == "execute" {
			return "execute"
		}
		if tx.Metadata.Category == "upgrade" {
			return "upgrade"
		}

		// Check purpose field
		if strings.Contains(strings.ToLower(tx.Metadata.Purpose), "deploy") {
			return "deploy"
		}
		if strings.Contains(strings.ToLower(tx.Metadata.Purpose), "execute") ||
			strings.Contains(strings.ToLower(tx.Metadata.Purpose), "call") {
			return "execute"
		}
	}

	// If no recipient and has data, it's likely a deployment
	if tx.Receiver == "" && len(tx.Data) > 0 {
		return "deploy"
	}

	return "transfer"
}

func (p *HybridTransactionProcessor) getRuntimeFromLanguage(language string) runtime.RuntimeType {
	switch strings.ToLower(language) {
	case "solidity", "vyper", "evm":
		return runtime.RuntimeTypeEVM
	case "go", "node", "nodejs", "java", "chaincode":
		return runtime.RuntimeTypeChaincode
	case "native", "diamante", "plugin", "wasm":
		return runtime.RuntimeTypeNative
	default:
		return runtime.RuntimeTypeEVM // Default to EVM
	}
}

func (p *HybridTransactionProcessor) extractDeploymentData(tx common.Transaction) (*DeploymentData, error) {
	// Try metadata first
	if tx.Metadata == nil {
		return nil, errors.New("deployment data not found in transaction metadata")
	}

	// Try to extract structured data
	var extractor TransactionMetadataExtractor
	metadataBytes, err := json.Marshal(tx.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := json.Unmarshal(metadataBytes, &extractor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Extract code
	var code []byte
	if extractor.Code != "" {
		code, err = p.extractBytesFromString(extractor.Code)
		if err != nil {
			return nil, fmt.Errorf("invalid code format: %w", err)
		}
	}

	language := extractor.Language
	if language == "" {
		language = "solidity" // Default
	}

	// Convert args from json.RawMessage
	args := make([]interface{}, len(extractor.Args))
	for i, rawArg := range extractor.Args {
		var arg interface{}
		if err := json.Unmarshal(rawArg, &arg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal arg %d: %w", i, err)
		}
		args[i] = arg
	}

	// Convert metadata
	var metadata map[string]string
	if len(extractor.Metadata) > 0 {
		var metaMap map[string]interface{}
		if err := json.Unmarshal(extractor.Metadata, &metaMap); err == nil {
			metadata = p.convertMetadata(metaMap)
		}
	}

	return &DeploymentData{
		Language: language,
		Code:     code,
		Args:     p.convertDeploymentArguments(args),
		Metadata: metadata,
	}, nil
}

func (p *HybridTransactionProcessor) extractExecutionData(tx common.Transaction) (*ExecutionData, error) {
	// Try metadata first
	if tx.Metadata == nil {
		return nil, errors.New("execution data not found in transaction metadata")
	}

	// Try to extract structured data
	var extractor TransactionMetadataExtractor
	metadataBytes, err := json.Marshal(tx.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := json.Unmarshal(metadataBytes, &extractor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	contractID := tx.SmartContractID
	if contractID == "" {
		contractID = extractor.ContractID
	}
	if contractID == "" {
		return nil, errors.New("missing contract ID")
	}

	function := extractor.Function
	if function == "" {
		function = extractor.Method
	}
	if function == "" {
		return nil, errors.New("missing function name")
	}

	// Convert args from json.RawMessage
	args := make([]interface{}, len(extractor.Args))
	for i, rawArg := range extractor.Args {
		var arg interface{}
		if err := json.Unmarshal(rawArg, &arg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal arg %d: %w", i, err)
		}
		args[i] = arg
	}

	return &ExecutionData{
		ContractID: contractID,
		Function:   function,
		Args:       p.convertExecutionArguments(args),
	}, nil
}

func (p *HybridTransactionProcessor) extractUpgradeData(tx common.Transaction) (*UpgradeData, error) {
	// Try metadata first
	if tx.Metadata == nil {
		return nil, errors.New("upgrade data not found in transaction metadata")
	}

	// Try to extract structured data
	var extractor TransactionMetadataExtractor
	metadataBytes, err := json.Marshal(tx.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := json.Unmarshal(metadataBytes, &extractor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	contractID := extractor.ContractID
	if contractID == "" {
		return nil, errors.New("missing contract ID")
	}

	version := extractor.Version
	if version == "" {
		return nil, errors.New("missing version")
	}

	code, err := p.extractBytesFromString(extractor.Code)
	if err != nil {
		return nil, fmt.Errorf("invalid code format: %w", err)
	}

	// Convert metadata
	var metadata map[string]string
	if len(extractor.Metadata) > 0 {
		var metaMap map[string]interface{}
		if err := json.Unmarshal(extractor.Metadata, &metaMap); err == nil {
			metadata = p.convertMetadata(metaMap)
		}
	}

	return &UpgradeData{
		ContractID:    contractID,
		Version:       version,
		Code:          code,
		MigrationData: nil, // Not in current metadata structure
		Metadata:      metadata,
	}, nil
}

func (p *HybridTransactionProcessor) extractBytes(data map[string]interface{}, key string) ([]byte, error) {
	value, exists := data[key]
	if !exists {
		return nil, fmt.Errorf("key %s not found", key)
	}

	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return p.extractBytesFromString(v)
	default:
		return nil, fmt.Errorf("invalid type for %s", key)
	}
}

func (p *HybridTransactionProcessor) extractBytesFromString(s string) ([]byte, error) {
	// Try hex decoding
	if strings.HasPrefix(s, "0x") {
		return hex.DecodeString(s[2:])
	}
	return []byte(s), nil
}

func (p *HybridTransactionProcessor) updateMetrics(result *TransactionResult, processingTx *ProcessingTransaction) {
	processingTime := time.Since(processingTx.StartTime)

	p.metrics.TotalProcessed++
	if !result.Success {
		p.metrics.TotalFailed++
	}

	// Update average gas used
	p.metrics.AverageGasUsed = (p.metrics.AverageGasUsed*(p.metrics.TotalProcessed-1) + result.GasUsed) / p.metrics.TotalProcessed

	// Update average processing time
	avgTime := p.metrics.AverageProcessTime.Nanoseconds()
	avgTime = (avgTime*(int64(p.metrics.TotalProcessed)-1) + processingTime.Nanoseconds()) / int64(p.metrics.TotalProcessed)
	p.metrics.AverageProcessTime = time.Duration(avgTime)

	// Update runtime distribution
	if processingTx.Runtime != "" {
		p.metrics.RuntimeDistribution[processingTx.Runtime]++
	}
}

// GetMetrics returns current transaction processing metrics
func (p *HybridTransactionProcessor) GetMetrics() *TransactionMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Create a copy of metrics
	metrics := &TransactionMetrics{
		TotalProcessed:      p.metrics.TotalProcessed,
		TotalFailed:         p.metrics.TotalFailed,
		AverageGasUsed:      p.metrics.AverageGasUsed,
		AverageProcessTime:  p.metrics.AverageProcessTime,
		RuntimeDistribution: make(map[runtime.RuntimeType]uint64),
	}

	for rt, count := range p.metrics.RuntimeDistribution {
		metrics.RuntimeDistribution[rt] = count
	}

	return metrics
}

// Conversion helpers

func (p *HybridTransactionProcessor) convertToRuntimeEvents(events []deploy.DeploymentEvent) []runtime.ContractEvent {
	result := make([]runtime.ContractEvent, 0, len(events))
	for i, event := range events {
		// Convert EventParameters to ContractParameters
		params := runtime.ContractParameters{
			StringParams: make(map[string]string),
			IntParams:    make(map[string]int64),
			BoolParams:   make(map[string]bool),
		}

		for _, param := range event.Parameters {
			switch param.Type {
			case "string", "address":
				params.StringParams[param.Name] = param.Value
			case "int", "int64", "uint", "uint64":
				if val, err := strconv.ParseInt(param.Value, 10, 64); err == nil {
					params.IntParams[param.Name] = val
				}
			case "bool":
				params.BoolParams[param.Name] = param.Value == "true"
			default:
				// Store as string for unknown types
				params.StringParams[param.Name] = param.Value
			}
		}

		result = append(result, runtime.ContractEvent{
			Name:       event.Name,
			Parameters: params,
			Data:       event.Data,
			Index:      uint(i),
		})
	}
	return result
}

// Helper methods for extracting gas parameters
func (p *HybridTransactionProcessor) extractGasLimit(tx common.Transaction) uint64 {
	if tx.Metadata != nil {
		// Try to extract from structured metadata
		var extractor TransactionMetadataExtractor
		if metadataBytes, err := json.Marshal(tx.Metadata); err == nil {
			if err := json.Unmarshal(metadataBytes, &extractor); err == nil && extractor.GasLimit > 0 {
				return extractor.GasLimit
			}
		}
	}
	return 100000 // Default gas limit
}

func (p *HybridTransactionProcessor) extractGasPrice(tx common.Transaction) uint64 {
	if tx.Metadata != nil {
		// Try to extract from structured metadata
		var extractor TransactionMetadataExtractor
		if metadataBytes, err := json.Marshal(tx.Metadata); err == nil {
			if err := json.Unmarshal(metadataBytes, &extractor); err == nil && extractor.GasPrice > 0 {
				return extractor.GasPrice
			}
		}
	}
	return 1 // Default gas price
}

// Convert deployment events to event logs
func (p *HybridTransactionProcessor) convertDeploymentEventLogs(events []deploy.DeploymentEvent) []storage.EventLog {
	result := make([]storage.EventLog, len(events))
	for i, event := range events {
		result[i] = storage.EventLog{
			Address: "deployment",
			Topics:  []string{event.Name},
			Data:    p.serializeEventData(event.Data),
		}
	}
	return result
}

// Convert runtime events to event logs
func (p *HybridTransactionProcessor) convertRuntimeEventLogs(events []runtime.ContractEvent) []storage.EventLog {
	result := make([]storage.EventLog, len(events))
	for i, event := range events {
		result[i] = storage.EventLog{
			Address:     event.ContractID,
			Topics:      []string{event.Name},
			Data:        event.Data,
			BlockNumber: event.BlockNumber,
			TxHash:      event.TransactionHash,
			Index:       uint(event.Index),
		}
	}
	return result
}

// Serialize event data to bytes
func (p *HybridTransactionProcessor) serializeEventData(data interface{}) []byte {
	if data == nil {
		return []byte{}
	}

	switch v := data.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		// For complex types, just convert to string
		return []byte(fmt.Sprintf("%v", data))
	}
}

// Data structures

// DeploymentData represents contract deployment data with concrete types
type DeploymentData struct {
	Language string               `json:"language"`
	Code     []byte               `json:"code"`
	Args     []DeploymentArgument `json:"args"`
	Metadata map[string]string    `json:"metadata"`
}

// DeploymentArgument represents a typed argument for contract deployment
type DeploymentArgument struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"` // Keep interface{} for argument values only
}

// ExecutionData represents contract execution data with concrete types
type ExecutionData struct {
	ContractID string              `json:"contract_id"`
	Function   string              `json:"function"`
	Args       []ExecutionArgument `json:"args"`
}

// ExecutionArgument represents a typed argument for contract execution
type ExecutionArgument struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"` // Keep interface{} for argument values only
}

// UpgradeData represents contract upgrade data with concrete types
type UpgradeData struct {
	ContractID    string            `json:"contract_id"`
	Version       string            `json:"version"`
	Code          []byte            `json:"code"`
	MigrationData []byte            `json:"migration_data"`
	Metadata      map[string]string `json:"metadata"`
}

func (p *HybridTransactionProcessor) convertDeploymentArguments(args []interface{}) []DeploymentArgument {
	result := make([]DeploymentArgument, len(args))
	for i, arg := range args {
		result[i] = DeploymentArgument{
			Name:  fmt.Sprintf("arg%d", i), // Placeholder name
			Type:  "unknown",
			Value: arg,
		}
	}
	return result
}

func (p *HybridTransactionProcessor) convertExecutionArguments(args []interface{}) []ExecutionArgument {
	result := make([]ExecutionArgument, len(args))
	for i, arg := range args {
		result[i] = ExecutionArgument{
			Name:  fmt.Sprintf("arg%d", i), // Placeholder name
			Type:  "unknown",
			Value: arg,
		}
	}
	return result
}

func (p *HybridTransactionProcessor) convertMetadata(metadata interface{}) map[string]string {
	result := make(map[string]string)

	switch m := metadata.(type) {
	case map[string]interface{}:
		for k, v := range m {
			result[k] = fmt.Sprintf("%v", v)
		}
	case *common.TransactionMetadata:
		// Convert TransactionMetadata fields to string map
		if m != nil {
			result["category"] = m.Category
			result["description"] = m.Description
			result["reference"] = m.Reference
			result["source"] = m.Source
			result["destination"] = m.Destination
			result["purpose"] = m.Purpose
			if len(m.Tags) > 0 {
				result["tags"] = strings.Join(m.Tags, ",")
			}
		}
	}

	return result
}

// convertToConstructorArguments converts DeploymentArgument to deploy.ConstructorArgument
func (p *HybridTransactionProcessor) convertToConstructorArguments(args []DeploymentArgument) []deploy.ConstructorArgument {
	result := make([]deploy.ConstructorArgument, 0, len(args))
	for _, arg := range args {
		result = append(result, deploy.ConstructorArgument{
			Name:  arg.Name,
			Type:  arg.Type,
			Value: fmt.Sprintf("%v", arg.Value), // Convert to string representation
		})
	}
	return result
}

// convertToDeploymentMetadata converts map[string]string to deploy.DeploymentMetadata
func (p *HybridTransactionProcessor) convertToDeploymentMetadata(metadata map[string]string) deploy.DeploymentMetadata {
	result := deploy.DeploymentMetadata{
		Name:        metadata["name"],
		Description: metadata["description"],
		Version:     metadata["version"],
		Author:      metadata["author"],
		License:     metadata["license"],
		Repository:  metadata["repository"],
		Website:     metadata["website"],
	}

	// Parse tags if present
	if tags, ok := metadata["tags"]; ok && tags != "" {
		result.Tags = strings.Split(tags, ",")
	}

	return result
}

// convertToContractParameters converts ExecutionArgument to runtime.ContractParameters
func (p *HybridTransactionProcessor) convertToContractParameters(args []ExecutionArgument) runtime.ContractParameters {
	params := runtime.ContractParameters{
		StringParams:  make(map[string]string),
		IntParams:     make(map[string]int64),
		FloatParams:   make(map[string]float64),
		BoolParams:    make(map[string]bool),
		BytesParams:   make(map[string][]byte),
		AddressParams: make(map[string]string),
	}

	for _, arg := range args {
		switch arg.Type {
		case "string", "dynamic":
			params.StringParams[arg.Name] = fmt.Sprintf("%v", arg.Value)
		case "int", "int64", "uint", "uint64":
			switch v := arg.Value.(type) {
			case int:
				params.IntParams[arg.Name] = int64(v)
			case int64:
				params.IntParams[arg.Name] = v
			case float64:
				params.IntParams[arg.Name] = int64(v)
			default:
				// Try to parse as string
				if str, ok := arg.Value.(string); ok {
					if val, err := strconv.ParseInt(str, 10, 64); err == nil {
						params.IntParams[arg.Name] = val
					}
				}
			}
		case "float", "float64":
			switch v := arg.Value.(type) {
			case float64:
				params.FloatParams[arg.Name] = v
			case int:
				params.FloatParams[arg.Name] = float64(v)
			default:
				// Try to parse as string
				if str, ok := arg.Value.(string); ok {
					if val, err := strconv.ParseFloat(str, 64); err == nil {
						params.FloatParams[arg.Name] = val
					}
				}
			}
		case "bool":
			switch v := arg.Value.(type) {
			case bool:
				params.BoolParams[arg.Name] = v
			case string:
				params.BoolParams[arg.Name] = v == "true"
			}
		case "address":
			params.AddressParams[arg.Name] = fmt.Sprintf("%v", arg.Value)
		case "bytes":
			switch v := arg.Value.(type) {
			case []byte:
				params.BytesParams[arg.Name] = v
			case string:
				params.BytesParams[arg.Name] = []byte(v)
			}
		}
	}

	return params
}

// Package evm provides the Ethereum Virtual Machine runtime implementation
package evm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/vm/runtime"

	ethabi "github.com/ethereum/go-ethereum/accounts/abi"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

// evmRuntime implements the Runtime interface for EVM
type evmRuntime struct {
	chainID     string
	ledgerAPI   common.LedgerAPI
	stateStore  storage.LedgerStore
	logger      *logrus.Logger
	executor    *EVMExecutor
	blockHeight uint64
	mu          sync.RWMutex
	started     bool
	contracts   map[string]*contractInfo
}

// contractInfo stores information about deployed contracts
type contractInfo struct {
	Address    ethcommon.Address
	Owner      string
	DeployedAt time.Time
	Version    string
	ABI        string
	Active     bool
	Metadata   map[string]interface{}
}

// NewEVMRuntime creates a new EVM runtime instance
func NewEVMRuntime() runtime.Runtime {
	return &evmRuntime{
		contracts: make(map[string]*contractInfo),
	}
}

// Type returns the runtime type identifier
func (r *evmRuntime) Type() runtime.RuntimeType {
	return runtime.RuntimeTypeEVM
}

// Initialize sets up the runtime with necessary configuration
func (r *evmRuntime) Initialize(config runtime.RuntimeConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("runtime already initialized")
	}

	// Set basic configuration
	r.chainID = config.ChainID
	r.ledgerAPI = config.LedgerAPI
	r.stateStore = config.StateStore
	r.logger = config.Logger

	// Extract EVM-specific configuration
	evmChainID := uint64(1337) // Default chain ID
	gasLimit := uint64(30000000)
	blockHeight := uint64(0)

	if config.RuntimeSpecific.EVMConfig != nil {
		evmConfig := config.RuntimeSpecific.EVMConfig
		if evmConfig.ChainID > 0 {
			evmChainID = evmConfig.ChainID
		}
		if evmConfig.GasLimit > 0 {
			gasLimit = evmConfig.GasLimit
		}
		// Note: blockHeight should come from ledger state
	}

	r.blockHeight = blockHeight

	// Create EVM executor
	r.executor = NewEVMExecutor(r.ledgerAPI, r.stateStore, r.blockHeight, r.logger)

	r.logger.WithFields(logrus.Fields{
		"chainID":     config.ChainID,
		"evmChainID":  evmChainID,
		"gasLimit":    gasLimit,
		"blockHeight": r.blockHeight,
	}).Info("EVM runtime initialized")

	return nil
}

// Compile validates and compiles Solidity code
func (r *evmRuntime) Compile(code []byte, metadata runtime.RuntimeMetadata) (*runtime.CompiledContract, error) {
	// For EVM, we assume the code is already compiled bytecode
	// In production, you would integrate with solc or another Solidity compiler

	// Calculate source hash
	sourceHash := fmt.Sprintf("%x", crypto.Keccak256(code))

	// Extract contract name and ABI from metadata
	abi := ""

	// Convert metadata to map for processing
	metadataMap := make(map[string]interface{})
	metadataMap["name"] = metadata.Name
	metadataMap["version"] = metadata.Version
	metadataMap["description"] = metadata.Description
	metadataMap["author"] = metadata.Author
	metadataMap["license"] = metadata.License
	metadataMap["repository"] = metadata.Repository

	// Try to extract ABI if provided in description field (temporary hack)
	if strings.Contains(metadata.Description, "ABI:") {
		parts := strings.Split(metadata.Description, "ABI:")
		if len(parts) > 1 {
			abi = strings.TrimSpace(parts[1])
		}
	}

	// Calculate resource requirements based on contract size
	resources := runtime.ResourceRequirements{
		MemoryMB:             32,  // Base memory for EVM execution
		CPUCores:             0.5, // Half a core for EVM execution
		StorageMB:            len(code) / (1024 * 1024),
		NetworkBandwidthKbps: 100, // Minimal network requirements
	}

	// Adjust based on contract size
	contractSize := len(code)
	if contractSize > 24576 { // > 24KB
		resources.MemoryMB = 64
		resources.CPUCores = 1.0
	}

	return &runtime.CompiledContract{
		Runtime:              runtime.RuntimeTypeEVM,
		Code:                 code,
		ABI:                  abi,
		SourceHash:           sourceHash,
		Metadata:             metadata, // Keep the original RuntimeMetadata
		ResourceRequirements: resources,
	}, nil
}

// Deploy deploys a compiled contract to the blockchain
func (r *evmRuntime) Deploy(ctx context.Context, contract *runtime.CompiledContract, args runtime.DeploymentArgs) (*runtime.DeploymentResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return nil, errors.New("runtime not started")
	}

	if contract.Runtime != runtime.RuntimeTypeEVM {
		return nil, errors.New("invalid runtime type for EVM")
	}

	// Convert deployer to Ethereum address
	deployerAddr := ethcommon.HexToAddress(args.Deployer)

	// Prepare deployment data
	deploymentData := contract.Code

	// If constructor arguments are provided, ABI encode them when ABI is available
	if !args.ConstructorArgs.IsEmpty() && contract.ABI != "" {
		// Convert ContractParameters to map for encoding
		argsMap := make(map[string]interface{})
		for k, v := range args.ConstructorArgs.StringParams {
			argsMap[k] = v
		}
		for k, v := range args.ConstructorArgs.IntParams {
			argsMap[k] = v
		}
		for k, v := range args.ConstructorArgs.BoolParams {
			argsMap[k] = v
		}
		// Note: In production, proper ABI encoding would be needed
		r.logger.WithField("args", argsMap).Debug("Constructor arguments prepared")
	}

	// Convert value to big.Int
	value := new(big.Int).SetUint64(args.Value)

	// Set gas limit
	gasLimit := args.GasLimit
	if gasLimit == 0 {
		gasLimit = 3000000 // Default gas limit for deployment
	}

	// Prepare transaction hash for log tracking
	txHash := r.generateTxHash(deployerAddr, ethcommon.Address{}, deploymentData)
	txHashHash := ethcommon.HexToHash(txHash)
	r.executor.GetStateDB().PrepareForTx(txHashHash, 0)

	// Deploy the contract
	contractAddr, _, gasUsed, err := r.executor.DeployContract(
		deployerAddr,
		deploymentData,
		value,
		gasLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("deployment failed: %w", err)
	}
	// Update tx hash with contract address
	txHash = r.generateTxHash(deployerAddr, contractAddr, deploymentData)
	txHashHash = ethcommon.HexToHash(txHash)

	// Store contract info
	info := &contractInfo{
		Address:    contractAddr,
		Owner:      args.Deployer,
		DeployedAt: consensus.ConsensusNow(),
		Version:    "1.0.0",
		ABI:        contract.ABI,
		Active:     true,
		Metadata:   convertRuntimeMetadataToMap(contract.Metadata),
	}
	r.contracts[contractAddr.Hex()] = info

	// Capture event logs before commit
	evmLogs := r.executor.GetStateDB().GetLogs(txHashHash, ethcommon.Hash{})
	events := convertLogsToEvents(evmLogs, contractAddr.Hex(), txHash, r.blockHeight)

	// Commit state changes
	stateRoot, err := r.executor.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit state: %w", err)
	}
	// clear temporary logs
	r.executor.GetStateDB().logs = []*types.Log{}

	r.logger.WithFields(logrus.Fields{
		"contract":  contractAddr.Hex(),
		"deployer":  args.Deployer,
		"gasUsed":   gasUsed,
		"stateRoot": stateRoot.Hex(),
	}).Info("Contract deployed successfully")

	// Prepare deployment result
	result := &runtime.DeploymentResult{
		ContractID:      contractAddr.Hex(),
		TransactionHash: txHash,
		GasUsed:         gasUsed,
		Timestamp:       consensus.ConsensusNow(),
		Events:          events,
	}

	return result, nil
}

// Execute executes a function on a deployed contract
func (r *evmRuntime) Execute(ctx context.Context, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.started {
		return nil, errors.New("runtime not started")
	}

	// Validate contract exists
	contractAddr := ethcommon.HexToAddress(call.ContractID)
	if _, exists := r.contracts[call.ContractID]; !exists {
		// Check if it's a precompiled contract
		if _, isPrecompiled := PrecompiledContractsDiamante[contractAddr]; !isPrecompiled {
			return nil, fmt.Errorf("contract not found: %s", call.ContractID)
		}
	}

	// Convert caller to Ethereum address
	callerAddr := ethcommon.HexToAddress(call.Caller)

	// Prepare call data
	callData := []byte{}

	// If function name is provided, encode function selector
	if call.Function != "" {
		// Calculate function selector (first 4 bytes of keccak256 hash)
		funcSig := call.Function
		// In production, function signature would include parameter types
		hash := crypto.Keccak256([]byte(funcSig))
		callData = append(callData, hash[:4]...)
	}

	// Encode arguments if provided
	if !call.Args.IsEmpty() {
		// In production, proper ABI encoding of args would be needed
		// For now, just log that args were provided
		r.logger.WithField("function", call.Function).Debug("Function arguments provided")
	}

	// Convert value to big.Int
	value := new(big.Int).SetUint64(call.Value)

	// Set gas limit
	gasLimit := call.GasLimit
	if gasLimit == 0 {
		gasLimit = 1000000 // Default gas limit for calls
	}

	// Prepare transaction hash to tag logs
	txHash := r.generateTxHash(callerAddr, contractAddr, callData)
	txHashHash := ethcommon.HexToHash(txHash)
	r.executor.GetStateDB().PrepareForTx(txHashHash, 0)

	// Execute the contract call
	returnData, gasUsed, err := r.executor.ExecuteContract(
		callerAddr,
		contractAddr,
		callData,
		value,
		gasLimit,
	)

	// Capture state changes and events before committing
	changes := captureStateChanges(r.executor.GetStateDB())
	evmLogs := r.executor.GetStateDB().GetLogs(txHashHash, ethcommon.Hash{})
	events := convertLogsToEvents(evmLogs, call.ContractID, txHash, r.blockHeight)

	// Prepare execution result
	result := &runtime.ExecutionResult{
		RawReturnData: returnData,
		GasUsed:       gasUsed,
		Success:       err == nil,
		Events:        events,
		StateChanges:  changes,
	}

	if err != nil {
		result.Error = err.Error()
	} else if len(returnData) > 0 {
		// Convert raw bytes to ContractValue
		result.ReturnData = []runtime.ContractValue{
			{
				Type:     "bytes",
				BytesVal: returnData,
			},
		}
	}

	if _, commitErr := r.executor.Commit(); commitErr != nil {
		return nil, fmt.Errorf("failed to commit state: %w", commitErr)
	}
	r.executor.GetStateDB().logs = []*types.Log{}

	return result, nil
}

// Upgrade upgrades an existing contract to a new version
func (r *evmRuntime) Upgrade(ctx context.Context, contractID string, newCode []byte, args runtime.UpgradeArgs) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return errors.New("runtime not started")
	}

	// Check if contract exists
	info, exists := r.contracts[contractID]
	if !exists {
		return fmt.Errorf("contract not found: %s", contractID)
	}

	// Verify authorizer is the owner
	if info.Owner != args.Authorizer {
		return errors.New("unauthorized: only owner can upgrade contract")
	}

	// Perform the upgrade
	contractAddr := ethcommon.HexToAddress(contractID)
	if err := r.executor.UpgradeContract(contractAddr, newCode, args.Version); err != nil {
		return fmt.Errorf("upgrade failed: %w", err)
	}

	// Update contract info
	info.Version = args.Version
	info.Metadata["upgradedAt"] = consensus.ConsensusNow()
	info.Metadata["upgradedBy"] = args.Authorizer

	// Commit state changes
	if _, err := r.executor.Commit(); err != nil {
		return fmt.Errorf("failed to commit upgrade: %w", err)
	}

	r.logger.WithFields(logrus.Fields{
		"contract":   contractID,
		"version":    args.Version,
		"authorizer": args.Authorizer,
	}).Info("Contract upgraded successfully")

	return nil
}

// GetContractInfo retrieves information about a deployed contract
func (r *evmRuntime) GetContractInfo(contractID string) (*runtime.ContractInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.contracts[contractID]
	if !exists {
		// Check if it's a precompiled contract
		contractAddr := ethcommon.HexToAddress(contractID)
		if _, isPrecompiled := PrecompiledContractsDiamante[contractAddr]; isPrecompiled {
			return &runtime.ContractInfo{
				ContractID: contractID,
				Runtime:    runtime.RuntimeTypeEVM,
				Owner:      "0x0000000000000000000000000000000000000000",
				DeployedAt: time.Time{},
				Version:    "precompiled",
				Active:     true,
				Metadata: runtime.RuntimeMetadata{
					Name:        "Precompiled Contract",
					Description: "Built-in EVM precompiled contract",
					Version:     "1.0.0",
				},
			}, nil
		}
		return nil, fmt.Errorf("contract not found: %s", contractID)
	}

	// Calculate state hash
	stateDB := r.executor.GetStateDB()
	stateHash := stateDB.GetCommittedState(info.Address, ethcommon.Hash{})

	return &runtime.ContractInfo{
		ContractID: contractID,
		Runtime:    runtime.RuntimeTypeEVM,
		Owner:      info.Owner,
		DeployedAt: info.DeployedAt,
		Version:    info.Version,
		StateHash:  stateHash.Hex(),
		Active:     info.Active,
		Metadata:   convertMapToRuntimeMetadata(info.Metadata),
	}, nil
}

// Start starts the runtime
func (r *evmRuntime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("runtime already started")
	}

	r.started = true
	r.logger.Info("EVM runtime started")
	return nil
}

// Stop gracefully stops the runtime
func (r *evmRuntime) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return errors.New("runtime not started")
	}

	// Clear event logs
	r.executor.ClearEventLogs()

	r.started = false
	r.logger.Info("EVM runtime stopped")
	return nil
}

// HealthCheck returns the health status of the runtime
func (r *evmRuntime) HealthCheck() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.started {
		return errors.New("runtime not started")
	}

	// Check if we can access the state
	if r.executor == nil || r.executor.GetStateDB() == nil {
		return errors.New("state database not accessible")
	}

	// Try to estimate gas for a simple operation
	if _, err := r.executor.EstimateGas(
		ethcommon.Address{},
		ethcommon.Address{},
		[]byte{},
		big.NewInt(0),
	); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// ExecuteTransaction processes a raw Ethereum transaction
func (r *evmRuntime) ExecuteTransaction(tx *types.Transaction) (*types.Receipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return nil, errors.New("runtime not started")
	}

	// Extract transaction details
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract sender: %w", err)
	}

	// Create receipt
	receipt := &types.Receipt{
		Type:              tx.Type(),
		GasUsed:           0,
		CumulativeGasUsed: 0,
		TxHash:            tx.Hash(),
		BlockNumber:       big.NewInt(int64(r.blockHeight)),
		TransactionIndex:  0,
		Status:            types.ReceiptStatusSuccessful,
	}

	// Handle different transaction types
	if tx.To() == nil {
		// Contract deployment
		deploymentData := tx.Data()
		contractAddr, _, gasUsed, err := r.executor.DeployContract(
			from,
			deploymentData,
			tx.Value(),
			tx.Gas(),
		)
		if err != nil {
			receipt.Status = types.ReceiptStatusFailed
			receipt.GasUsed = tx.Gas()
			return receipt, nil
		}

		receipt.ContractAddress = contractAddr
		receipt.GasUsed = gasUsed

		// Store contract info
		info := &contractInfo{
			Address:    contractAddr,
			Owner:      from.Hex(),
			DeployedAt: consensus.ConsensusNow(),
			Version:    "1.0.0",
			Active:     true,
			Metadata:   make(map[string]interface{}),
		}
		info.Metadata["name"] = "Deployed Contract"
		info.Metadata["version"] = "1.0.0"
		info.Metadata["author"] = "System"
		info.Metadata["deployedAt"] = consensus.ConsensusNow()
		r.contracts[contractAddr.Hex()] = info

	} else {
		// Contract call or value transfer
		returnData, gasUsed, err := r.executor.ExecuteContract(
			from,
			*tx.To(),
			tx.Data(),
			tx.Value(),
			tx.Gas(),
		)
		if err != nil {
			receipt.Status = types.ReceiptStatusFailed
			receipt.GasUsed = tx.Gas()
			return receipt, nil
		}

		receipt.GasUsed = gasUsed
		if len(returnData) > 0 {
			receipt.Logs = []*types.Log{
				{
					Address: *tx.To(),
					Data:    returnData,
				},
			}
		}
	}

	// Commit state changes
	if _, err := r.executor.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit state: %w", err)
	}

	// Extract and convert event logs
	evmLogs := r.executor.GetEventLogsByTxHash(tx.Hash())
	for _, log := range evmLogs {
		receipt.Logs = append(receipt.Logs, &types.Log{
			Address:     log.Address,
			Topics:      log.Topics,
			Data:        log.Data,
			BlockNumber: r.blockHeight,
			TxHash:      tx.Hash(),
			TxIndex:     0,
			BlockHash:   ethcommon.Hash{},
			Index:       uint(log.LogIndex),
		})
	}

	return receipt, nil
}

// CallContract executes a contract call without creating a transaction
func (r *evmRuntime) CallContract(call *runtime.ContractCall) (*CallResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.started {
		return nil, errors.New("runtime not started")
	}

	// Execute the call
	result, err := r.Execute(context.Background(), *call)
	if err != nil {
		return nil, err
	}

	// Convert to CallResult
	return &CallResult{
		Success:    result.Success,
		ReturnData: result.RawReturnData,
		GasUsed:    result.GasUsed,
		Error:      result.Error,
	}, nil
}

// QueryState queries the current state of a contract
func (r *evmRuntime) QueryState(contractID string, key []byte) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.started {
		return nil, errors.New("runtime not started")
	}

	contractAddr := ethcommon.HexToAddress(contractID)
	keyHash := ethcommon.BytesToHash(key)

	stateDB := r.executor.GetStateDB()
	value := stateDB.GetState(contractAddr, keyHash)

	return value.Bytes(), nil
}

// EstimateGas estimates the gas required for a transaction
func (r *evmRuntime) EstimateGas(from, to string, data []byte, value uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.started {
		return 0, errors.New("runtime not started")
	}

	fromAddr := ethcommon.HexToAddress(from)
	toAddr := ethcommon.HexToAddress(to)
	valueBig := new(big.Int).SetUint64(value)

	return r.executor.EstimateGas(fromAddr, toAddr, data, valueBig)
}

// Helper functions

func (r *evmRuntime) generateTxHash(from, to ethcommon.Address, data []byte) string {
	// Generate a deterministic transaction hash
	hasher := sha256.New()
	hasher.Write(from.Bytes())
	hasher.Write(to.Bytes())
	hasher.Write(data)
	hasher.Write([]byte(fmt.Sprintf("%d", r.blockHeight)))
	hasher.Write([]byte(fmt.Sprintf("%d", consensus.ConsensusNow().UnixNano())))
	return hex.EncodeToString(hasher.Sum(nil))
}

// CallResult represents the result of a contract call
type CallResult struct {
	Success    bool
	ReturnData []byte
	GasUsed    uint64
	Error      string
}

func encodeConstructorArgs(abiJSON string, args []interface{}) ([]byte, error) {
	parsed, err := ethabi.JSON(bytes.NewReader([]byte(abiJSON)))
	if err != nil {
		return nil, err
	}
	return parsed.Constructor.Inputs.Pack(args...)
}

func convertLogsToEvents(logs []*types.Log, contractID, txHash string, block uint64) []runtime.ContractEvent {
	events := make([]runtime.ContractEvent, 0, len(logs))
	for i, lg := range logs {
		params := runtime.ContractParameters{
			StringParams: make(map[string]string),
		}
		for j, t := range lg.Topics {
			params.StringParams[fmt.Sprintf("topic%d", j)] = t.Hex()
		}
		events = append(events, runtime.ContractEvent{
			ContractID:      contractID,
			Name:            "LogEvent",
			Parameters:      params,
			Data:            lg.Data,
			BlockNumber:     block,
			TransactionHash: txHash,
			Index:           uint(i),
		})
	}
	return events
}

func captureStateChanges(db *StateDB) []runtime.StateChange {
	changes := []runtime.StateChange{}
	for addr, obj := range db.stateObjects {
		if _, ok := db.stateObjectsDirty[addr]; !ok {
			continue
		}
		for key := range obj.dirtyStorage {
			oldVal := obj.originStorage[key]
			newVal := obj.pendingStorage[key]
			changes = append(changes, runtime.StateChange{
				Key:        key.Bytes(),
				OldValue:   oldVal.Bytes(),
				NewValue:   newVal.Bytes(),
				ContractID: addr.Hex(),
			})
		}
	}
	return changes
}

// Helper functions to convert between RuntimeMetadata and map[string]interface{}
func convertRuntimeMetadataToMap(metadata runtime.RuntimeMetadata) map[string]interface{} {
	result := make(map[string]interface{})
	result["name"] = metadata.Name
	result["description"] = metadata.Description
	result["version"] = metadata.Version
	result["author"] = metadata.Author
	result["license"] = metadata.License
	result["repository"] = metadata.Repository
	result["createdAt"] = metadata.CreatedAt
	result["updatedAt"] = metadata.UpdatedAt

	// Convert capabilities to strings
	capabilities := make([]string, 0, len(metadata.Capabilities))
	for _, cap := range metadata.Capabilities {
		capabilities = append(capabilities, string(cap))
	}
	result["capabilities"] = capabilities

	return result
}

func convertMapToRuntimeMetadata(data map[string]interface{}) runtime.RuntimeMetadata {
	metadata := runtime.RuntimeMetadata{}

	if name, ok := data["name"].(string); ok {
		metadata.Name = name
	}
	if desc, ok := data["description"].(string); ok {
		metadata.Description = desc
	}
	if version, ok := data["version"].(string); ok {
		metadata.Version = version
	}
	if author, ok := data["author"].(string); ok {
		metadata.Author = author
	}
	if license, ok := data["license"].(string); ok {
		metadata.License = license
	}
	if repository, ok := data["repository"].(string); ok {
		metadata.Repository = repository
	}
	if createdAt, ok := data["createdAt"].(time.Time); ok {
		metadata.CreatedAt = createdAt
	}
	if updatedAt, ok := data["updatedAt"].(time.Time); ok {
		metadata.UpdatedAt = updatedAt
	}

	// Convert capabilities from strings
	if caps, ok := data["capabilities"].([]string); ok {
		capabilities := make([]runtime.RuntimeCapability, 0, len(caps))
		for _, cap := range caps {
			capabilities = append(capabilities, runtime.RuntimeCapability(cap))
		}
		metadata.Capabilities = capabilities
	} else if caps, ok := data["capabilities"].([]interface{}); ok {
		// Convert []interface{} to []RuntimeCapability
		capabilities := make([]runtime.RuntimeCapability, 0, len(caps))
		for _, cap := range caps {
			if str, ok := cap.(string); ok {
				capabilities = append(capabilities, runtime.RuntimeCapability(str))
			}
		}
		metadata.Capabilities = capabilities
	}

	return metadata
}

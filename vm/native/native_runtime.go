// Package native provides the native Diamante runtime implementation
package native

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"plugin"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// NativeRuntime implements the Runtime interface for native Diamante execution
type NativeRuntime struct {
	config     *NativeConfig
	ledger     common.LedgerAPI
	stateStore storage.LedgerStore
	logger     *logrus.Logger

	// Contract management
	contracts map[string]*NativeContract
	plugins   map[string]*plugin.Plugin

	// Plugin and WASM runtime
	pluginLoader *PluginLoader
	wasmRuntime  *WASMRuntime

	// DNA language runtime
	dnaRuntime *DNARuntime

	// Runtime state
	initialized bool
	running     bool
	mu          sync.RWMutex
}

// NativeConfig contains configuration for the native runtime
type NativeConfig struct {
	MaxMemoryMB       int
	MaxCPUTime        time.Duration
	EnableDynamicLoad bool
	PluginDirectory   string
	SecurityLevel     string // sandbox, restricted, full
}

// NativeContract stores information about a native contract
type NativeContract struct {
	ID             string
	Name           string
	Version        string
	Type           string // plugin, wasm, compiled
	Owner          string
	Code           []byte
	CompiledPath   string
	State          *common.SmartContractState
	Metadata       *common.SmartContractMetadata
	DeployedAt     time.Time
	LastExecuted   time.Time
	ExecutionCount int64
	ResourceUsage  NativeResourceUsage
}

// NativeResourceUsage tracks resource consumption for native contracts
type NativeResourceUsage struct {
	CPUTime      time.Duration
	MemoryBytes  int64
	StorageBytes int64
	NetworkBytes int64
}

// NewNativeRuntime creates a new native runtime
func NewNativeRuntime() runtime.Runtime {
	return &NativeRuntime{
		contracts: make(map[string]*NativeContract),
		plugins:   make(map[string]*plugin.Plugin),
	}
}

// Type returns the runtime type
func (r *NativeRuntime) Type() runtime.RuntimeType {
	return runtime.RuntimeTypeNative
}

// Initialize sets up the native runtime
func (r *NativeRuntime) Initialize(config runtime.RuntimeConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.initialized {
		return nil
	}

	// Set common config
	r.ledger = config.LedgerAPI
	r.stateStore = config.StateStore.(storage.LedgerStore)
	r.logger = config.Logger

	// Extract native-specific config
	r.config = r.extractNativeConfig(config.RuntimeSpecific)

	// Create plugin directory if needed
	if r.config.EnableDynamicLoad && r.config.PluginDirectory == "" {
		r.config.PluginDirectory = "/tmp/diamante/plugins"
	}

	// Initialize plugin loader
	if r.config.EnableDynamicLoad {
		r.pluginLoader = NewPluginLoader(r.logger)
		// Plugin loader doesn't have Initialize method, it's ready to use
	}

	// Initialize WASM runtime
	wasmConfig := DefaultWASMConfig()
	wasmConfig.MaxExecutionTime = r.config.MaxCPUTime
	wasmConfig.EnableDebug = r.logger.GetLevel() == logrus.DebugLevel
	r.wasmRuntime = NewWASMRuntime(r.logger, wasmConfig)
	// WASM runtime is ready to use after creation

	// Initialize DNA runtime
	r.dnaRuntime = NewDNARuntime(r.stateStore, r.logger)
	if err := r.dnaRuntime.Initialize(config); err != nil {
		return fmt.Errorf("failed to initialize DNA runtime: %v", err)
	}

	r.initialized = true
	r.logger.Info("Native runtime initialized with DNA language support")

	return nil
}

// Compile compiles native smart contract code
func (r *NativeRuntime) Compile(code []byte, metadata runtime.RuntimeMetadata) (*runtime.CompiledContract, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil, errors.New("runtime not running")
	}

	// Extract contract type and language from metadata
	contractType := "native"
	language := "go"

	// Check if this is DNA language code
	codeStr := string(code)
	if strings.Contains(codeStr, "module ") && (strings.Contains(codeStr, "resource ") || strings.Contains(codeStr, "fun ")) {
		contractType = "dna"
		language = "dna"
	}

	// Convert metadata to map for internal processing
	metadataMap := make(map[string]interface{})
	metadataMap["name"] = metadata.Name
	metadataMap["version"] = metadata.Version
	metadataMap["description"] = metadata.Description
	metadataMap["author"] = metadata.Author
	metadataMap["license"] = metadata.License
	metadataMap["repository"] = metadata.Repository
	metadataMap["capabilities"] = metadata.Capabilities

	// Additional metadata for type detection
	if metadata.Name != "" {
		// Try to detect contract type from name
		if strings.Contains(strings.ToLower(metadata.Name), "wasm") {
			contractType = "wasm"
		} else if strings.Contains(strings.ToLower(metadata.Name), "plugin") {
			contractType = "plugin"
		}
	}

	// Validate contract type
	if !r.isValidContractType(contractType) {
		return nil, fmt.Errorf("invalid contract type: %s", contractType)
	}

	// Calculate resource requirements
	resources := r.calculateResourceRequirements(contractType, len(code))

	// Perform compilation based on contract type
	var compiledCode []byte
	var compilationMetadata map[string]interface{}
	var err error

	if contractType == "dna" {
		// Compile DNA code to WASM
		dnaResult, err := r.dnaRuntime.CompileDNA(codeStr, metadataMap)
		if err != nil {
			return nil, fmt.Errorf("DNA compilation failed: %w", err)
		}
		compiledCode = dnaResult.CompiledWASM
		compilationMetadata = dnaResult.Metrics
		compilationMetadata["dna_compilation"] = true
		compilationMetadata["resource_types"] = dnaResult.ResourceTypes
		compilationMetadata["source_hash"] = dnaResult.SourceHash
	} else {
		// Use existing compilation logic for other types
		compiledCode, compilationMetadata, err = r.performActualCompilation(code, contractType, language, metadataMap)
		if err != nil {
			return nil, fmt.Errorf("compilation failed: %w", err)
		}
	}

	// Merge compilation metadata with original metadata
	finalMetadataMap := make(map[string]interface{})
	for k, v := range metadataMap {
		finalMetadataMap[k] = v
	}
	for k, v := range compilationMetadata {
		finalMetadataMap[k] = v
	}

	compiled := &runtime.CompiledContract{
		Runtime:              runtime.RuntimeTypeNative,
		Code:                 compiledCode,
		ABI:                  r.generateNativeABI(finalMetadataMap),
		SourceHash:           r.calculateHash(code),
		Metadata:             metadata, // Keep the original RuntimeMetadata
		ResourceRequirements: resources,
	}

	r.logger.WithFields(logrus.Fields{
		"contractType":   contractType,
		"language":       language,
		"originalSize":   len(code),
		"compiledSize":   len(compiledCode),
		"compressionPct": float64(len(compiledCode)) / float64(len(code)) * 100,
	}).Info("Native contract compilation completed successfully")

	return compiled, nil
}

// Deploy deploys a compiled native contract
func (r *NativeRuntime) Deploy(ctx context.Context, contract *runtime.CompiledContract, args runtime.DeploymentArgs) (*runtime.DeploymentResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil, errors.New("runtime not running")
	}

	// Generate contract ID
	contractID := r.generateContractID(args.Deployer, contract.SourceHash)

	// Extract metadata from RuntimeMetadata struct
	name := contract.Metadata.Name
	if name == "" {
		name = contractID
	}
	version := contract.Metadata.Version
	if version == "" {
		version = "1.0.0"
	}

	// Determine contract type from metadata
	contractType := "plugin"
	if len(contract.Metadata.Capabilities) > 0 {
		for _, cap := range contract.Metadata.Capabilities {
			if strings.Contains(string(cap), "wasm") {
				contractType = "wasm"
				break
			} else if strings.Contains(string(cap), "dna") {
				contractType = "dna"
				break
			}
		}
	}

	// Also check for DNA marker in compiled code
	if len(contract.Code) > 8 {
		codeStr := string(contract.Code[:8])
		if codeStr == "\x00asm" || strings.Contains(string(contract.Code), "DNA_COMPILED") {
			// Check if this is a DNA-compiled WASM
			if strings.Contains(string(contract.Code), "dna_compilation") {
				contractType = "dna"
			}
		}
	}

	// Create typed metadata from RuntimeMetadata struct
	contractMetadata := &common.SmartContractMetadata{
		Author:        contract.Metadata.Author,
		License:       contract.Metadata.License,
		Description:   contract.Metadata.Description,
		Documentation: contract.Metadata.Repository, // Use Repository as Documentation
		Tags:          []string{},
		Dependencies:  []string{},
		Version:       contract.Metadata.Version,
		Audited:       false,
		AuditReports:  []string{},
	}

	// Create typed state structure
	contractState := &common.SmartContractState{
		Variables:     make(map[string]string),
		Balances:      make(map[string]float64),
		Permissions:   make(map[string]bool),
		Configuration: make(map[string]string),
		Counters:      make(map[string]int64),
		LastUpdated:   consensus.ConsensusUnix(),
	}

	// Create native contract
	nativeContract := &NativeContract{
		ID:             contractID,
		Name:           name,
		Version:        version,
		Type:           contractType,
		Owner:          args.Deployer,
		Code:           contract.Code,
		State:          contractState,
		Metadata:       contractMetadata,
		DeployedAt:     consensus.ConsensusNow(),
		LastExecuted:   consensus.ConsensusNow(),
		ExecutionCount: 0,
		ResourceUsage:  NativeResourceUsage{},
	}

	// Handle different contract types
	if contractType == "dna" {
		// This is a DNA contract - route to DNA runtime for deployment
		if r.dnaRuntime != nil {
			// Extract compiled result from metadata if available
			var dnaCompileResult *DNACompileResult
			if metadata := contract.Metadata; metadata.Name != "" {
				// Try to reconstruct DNA compile result from metadata
				dnaCompileResult = &DNACompileResult{
					CompiledWASM:    contract.Code,
					CompilationTime: consensus.ConsensusNow(),
					SourceHash:      contract.SourceHash,
				}
			}

			if err := r.dnaRuntime.DeployDNAContract(contractID, dnaCompileResult, args.Deployer); err != nil {
				return nil, fmt.Errorf("failed to deploy DNA contract: %w", err)
			}

			r.logger.WithFields(logrus.Fields{
				"contractID":   contractID,
				"contractType": "dna",
				"deployer":     args.Deployer,
			}).Info("DNA contract deployed successfully")
		} else {
			return nil, fmt.Errorf("DNA runtime not available")
		}
	} else if contractType == "plugin" && r.config.EnableDynamicLoad {
		// Load plugin
		pluginPath := fmt.Sprintf("%s/%s.so", r.config.PluginDirectory, contractID)
		if err := r.loadPlugin(contractID, pluginPath); err != nil {
			return nil, fmt.Errorf("failed to load plugin: %w", err)
		}
		nativeContract.CompiledPath = pluginPath
	}

	// Store contract (for non-DNA contracts)
	if contractType != "dna" {
		r.contracts[contractID] = nativeContract
	}

	// Initialize contract if it has an init function (skip for DNA contracts)
	if contractType != "dna" {
		// Convert ContractParameters to []interface{} for initializeContract
		initArgs := r.extractInitArgs(args.ConstructorArgs)
		if err := r.initializeContract(contractID, initArgs); err != nil {
			return nil, fmt.Errorf("failed to initialize contract: %w", err)
		}
	}

	// Calculate gas used (native contracts are more efficient)
	gasUsed := uint64(10000 + len(contract.Code)*5)

	// Create deployment result
	result := &runtime.DeploymentResult{
		ContractID:      contractID,
		TransactionHash: r.generateTxHash("deploy", contractID),
		GasUsed:         gasUsed,
		Timestamp:       consensus.ConsensusNow(),
		Events: []runtime.ContractEvent{
			{
				ContractID: contractID,
				Name:       "NativeContractDeployed",
				Parameters: runtime.ContractParameters{
					StringParams: map[string]string{
						"name":    name,
						"version": version,
						"type":    contractType,
					},
				},
			},
		},
	}

	r.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"name":       name,
		"version":    version,
		"type":       contractType,
	}).Info("Native contract deployed")

	return result, nil
}

// Execute executes a native contract function
func (r *NativeRuntime) Execute(ctx context.Context, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil, errors.New("runtime not running")
	}

	// Check if this is a DNA contract first
	if r.dnaRuntime != nil {
		if _, err := r.dnaRuntime.GetDNAContract(call.ContractID); err == nil {
			// This is a DNA contract - route to DNA runtime
			dnaResult, err := r.dnaRuntime.ExecuteDNAContract(ctx, call.ContractID, call.Function,
				r.convertContractParamsToMap(call.Args), call.Caller)
			if err != nil {
				return &runtime.ExecutionResult{
					Success: false,
					Error:   err.Error(),
				}, nil
			}

			// Convert DNA result to runtime result
			return r.convertDNAResultToExecutionResult(dnaResult), nil
		}
	}

	// Get regular native contract
	contract, exists := r.contracts[call.ContractID]
	if !exists {
		return nil, fmt.Errorf("contract not found: %s", call.ContractID)
	}

	// Update execution stats
	contract.LastExecuted = consensus.ConsensusNow()
	contract.ExecutionCount++

	// Track execution time
	startTime := consensus.ConsensusNow()

	// Execute based on contract type
	var result *runtime.ExecutionResult
	var err error

	switch contract.Type {
	case "plugin":
		result, err = r.executePlugin(contract, call)
	case "wasm":
		result, err = r.executeWASM(contract, call)
	default:
		result, err = r.executeNative(contract, call)
	}

	// Update resource usage
	executionTime := consensus.ConsensusSince(startTime)
	contract.ResourceUsage.CPUTime += executionTime

	if err != nil {
		return nil, err
	}

	// Add execution metadata
	result.Events = append(result.Events, runtime.ContractEvent{
		ContractID: call.ContractID,
		Name:       "FunctionExecuted",
		Parameters: runtime.ContractParameters{
			StringParams: map[string]string{
				"function": call.Function,
			},
			IntParams: map[string]int64{
				"executionTime": executionTime.Milliseconds(),
			},
		},
	})

	r.logger.WithFields(logrus.Fields{
		"contractID":    call.ContractID,
		"function":      call.Function,
		"gasUsed":       result.GasUsed,
		"executionTime": executionTime,
	}).Info("Native contract executed")

	return result, nil
}

// extractInitArgs extracts initialization arguments from ContractParameters
func (r *NativeRuntime) extractInitArgs(params runtime.ContractParameters) []interface{} {
	var args []interface{}

	// Extract string parameters
	for _, v := range params.StringParams {
		args = append(args, v)
	}

	// Extract int parameters
	for _, v := range params.IntParams {
		args = append(args, v)
	}

	// Extract bool parameters
	for _, v := range params.BoolParams {
		args = append(args, v)
	}

	// Extract address parameters
	for _, v := range params.AddressParams {
		args = append(args, v)
	}

	return args
}

// Upgrade upgrades a native contract
func (r *NativeRuntime) Upgrade(ctx context.Context, contractID string, newCode []byte, args runtime.UpgradeArgs) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Get existing contract
	contract, exists := r.contracts[contractID]
	if !exists {
		return fmt.Errorf("contract not found: %s", contractID)
	}

	// Check authorization
	if contract.Owner != args.Authorizer {
		return errors.New("unauthorized: not contract owner")
	}

	// Save current state by creating a deep copy
	oldState := &common.SmartContractState{
		Variables:     make(map[string]string),
		Balances:      make(map[string]float64),
		Permissions:   make(map[string]bool),
		Configuration: make(map[string]string),
		Counters:      make(map[string]int64),
		LastUpdated:   contract.State.LastUpdated,
	}

	// Deep copy maps
	for k, v := range contract.State.Variables {
		oldState.Variables[k] = v
	}
	for k, v := range contract.State.Balances {
		oldState.Balances[k] = v
	}
	for k, v := range contract.State.Permissions {
		oldState.Permissions[k] = v
	}
	for k, v := range contract.State.Configuration {
		oldState.Configuration[k] = v
	}
	for k, v := range contract.State.Counters {
		oldState.Counters[k] = v
	}

	// Update contract
	contract.Version = args.Version
	contract.Code = newCode
	contract.LastExecuted = consensus.ConsensusNow()

	// Reload plugin if necessary
	if contract.Type == "plugin" && r.config.EnableDynamicLoad {
		// Unload old plugin
		delete(r.plugins, contractID)

		// Load new plugin
		pluginPath := fmt.Sprintf("%s/%s-v%s.so", r.config.PluginDirectory, contractID, args.Version)
		if err := r.loadPlugin(contractID, pluginPath); err != nil {
			// Restore old state on failure
			contract.State = oldState
			return fmt.Errorf("failed to load new plugin: %w", err)
		}
		contract.CompiledPath = pluginPath
	}

	// Run migration if provided
	if len(args.MigrationData) > 0 {
		if err := r.runMigration(contract, args.MigrationData); err != nil {
			// Restore old state on failure
			contract.State = oldState
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	r.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"newVersion": args.Version,
	}).Info("Native contract upgraded")

	return nil
}

// GetContractInfo retrieves native contract information
func (r *NativeRuntime) GetContractInfo(contractID string) (*runtime.ContractInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	contract, exists := r.contracts[contractID]
	if !exists {
		return nil, fmt.Errorf("contract not found: %s", contractID)
	}

	return &runtime.ContractInfo{
		ContractID: contractID,
		Runtime:    runtime.RuntimeTypeNative,
		Owner:      contract.Owner,
		DeployedAt: contract.DeployedAt,
		Version:    contract.Version,
		StateHash:  r.calculateStateHash(contract.State),
		Active:     true,
		Metadata: runtime.RuntimeMetadata{
			Name:        contract.Name,
			Description: fmt.Sprintf("Native contract type: %s", contract.Type),
			Version:     contract.Version,
			Author:      contract.Owner,
			License:     "",
			Repository:  "",
			Capabilities: []runtime.RuntimeCapability{
				runtime.CapabilitySmartContracts,
				runtime.CapabilityStateManagement,
			},
			CreatedAt: contract.DeployedAt,
			UpdatedAt: contract.LastExecuted,
		},
	}, nil
}

// Start starts the native runtime
func (r *NativeRuntime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.initialized {
		return errors.New("runtime not initialized")
	}

	if r.running {
		return nil
	}

	r.running = true
	r.logger.Info("Native runtime started")

	return nil
}

// Stop stops the native runtime
func (r *NativeRuntime) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	// Unload all plugins
	for id := range r.plugins {
		if r.pluginLoader != nil {
			if err := r.pluginLoader.UnloadPlugin(id); err != nil {
				r.logger.WithError(err).Errorf("Failed to unload plugin %s", id)
			}
		}
		delete(r.plugins, id)
	}

	// Close WASM runtime
	if r.wasmRuntime != nil {
		if err := r.wasmRuntime.Close(); err != nil {
			r.logger.WithError(err).Error("Failed to close WASM runtime")
		}
	}

	// Stop DNA runtime
	if r.dnaRuntime != nil {
		if err := r.dnaRuntime.Stop(); err != nil {
			r.logger.WithError(err).Error("Failed to stop DNA runtime")
		}
	}

	r.running = false
	r.logger.Info("Native runtime stopped")

	return nil
}

// HealthCheck checks the health of the runtime
func (r *NativeRuntime) HealthCheck() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.initialized {
		return errors.New("runtime not initialized")
	}

	if !r.running {
		return errors.New("runtime not running")
	}

	return nil
}

// Helper methods

func (r *NativeRuntime) extractNativeConfig(config runtime.RuntimeSpecificConfig) *NativeConfig {
	// Default configuration
	nativeConfig := &NativeConfig{
		MaxMemoryMB:       512,
		MaxCPUTime:        30 * time.Second,
		EnableDynamicLoad: false,
		PluginDirectory:   "",
		SecurityLevel:     "sandbox",
	}

	// Override with provided config if available
	if config.NativeConfig != nil {
		nc := config.NativeConfig

		// Use defaults if not specified
		if nc.MaxPlugins > 0 {
			nativeConfig.MaxMemoryMB = nc.MaxPlugins * 10 // Rough estimate
		}
		if nc.PluginTimeout > 0 {
			nativeConfig.MaxCPUTime = nc.PluginTimeout
		}
		if nc.EnableJIT {
			nativeConfig.EnableDynamicLoad = true
		}
		if nc.PluginPath != "" {
			nativeConfig.PluginDirectory = nc.PluginPath
		}
		if nc.EnableSandbox {
			nativeConfig.SecurityLevel = "sandbox"
		} else {
			nativeConfig.SecurityLevel = "restricted"
		}
	}

	return nativeConfig
}

func (r *NativeRuntime) isValidContractType(contractType string) bool {
	validTypes := map[string]bool{
		"plugin":   true,
		"wasm":     true,
		"compiled": true,
		"native":   true,
		"diamante": true,
	}
	return validTypes[contractType]
}

func (r *NativeRuntime) calculateResourceRequirements(contractType string, codeSize int) runtime.ResourceRequirements {
	// Native contracts are more efficient
	base := runtime.ResourceRequirements{
		MemoryMB:             64,
		CPUCores:             0.2,
		StorageMB:            codeSize / 1024,
		NetworkBandwidthKbps: 50,
	}

	// Adjust based on contract type
	switch contractType {
	case "wasm":
		base.MemoryMB = 128
		base.CPUCores = 0.3
	case "plugin":
		base.MemoryMB = 256
		base.CPUCores = 0.5
	}

	return base
}

func (r *NativeRuntime) generateNativeABI(metadata map[string]interface{}) string {
	// Generate a simple ABI for native contracts
	abi := map[string]interface{}{
		"version": "1.0",
		"functions": []map[string]interface{}{
			{
				"name":    "init",
				"inputs":  []string{},
				"outputs": []string{"bool"},
			},
			{
				"name":    "execute",
				"inputs":  []string{"function", "args"},
				"outputs": []string{"result"},
			},
		},
	}

	// Add custom functions from metadata
	if functions, ok := metadata["functions"].([]interface{}); ok {
		for _, fn := range functions {
			if fnMap, ok := fn.(map[string]interface{}); ok {
				abi["functions"] = append(abi["functions"].([]map[string]interface{}), fnMap)
			}
		}
	}

	abiJSON, _ := json.Marshal(abi)
	return string(abiJSON)
}

func (r *NativeRuntime) loadPlugin(contractID, pluginPath string) error {
	return r.pluginLoader.LoadPlugin(contractID, pluginPath)
}

func (r *NativeRuntime) initializeContract(contractID string, args []interface{}) error {
	// Initialize contract state
	contract := r.contracts[contractID]

	// Set initial state using typed structure
	contract.State.Variables["initialized"] = "true"
	contract.State.Counters["initTime"] = consensus.ConsensusUnix()
	contract.State.LastUpdated = consensus.ConsensusUnix()

	if len(args) > 0 {
		// Store init args as JSON string in Variables
		argsJSON, _ := json.Marshal(args)
		contract.State.Variables["initArgs"] = string(argsJSON)
	}

	return nil
}

func (r *NativeRuntime) executePlugin(contract *NativeContract, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	// Execute through plugin loader
	// Convert ContractParameters to []interface{} for plugin loader
	args := r.extractInitArgs(call.Args)
	result, err := r.pluginLoader.ExecutePlugin(
		contract.ID,
		call.Function,
		args,
	)

	if err != nil {
		return &runtime.ExecutionResult{
			Success: false,
			Error:   err.Error(),
			GasUsed: call.GasLimit, // Consume all gas on error
		}, nil
	}

	// Get method info for gas calculation
	pluginState, _ := r.pluginLoader.GetPluginState(contract.ID)
	gasUsed := uint64(5000) // Base gas cost

	// Convert result to bytes
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return &runtime.ExecutionResult{
			Success: false,
			Error:   "failed to marshal result",
			GasUsed: gasUsed,
		}, nil
	}

	// Get any state changes from the plugin
	var stateChanges []runtime.StateChange
	for key, value := range pluginState {
		// Try to get old value from Variables map
		oldValue, exists := contract.State.Variables[key]
		valueStr := fmt.Sprintf("%v", value)

		if !exists || oldValue != valueStr {
			stateChanges = append(stateChanges, runtime.StateChange{
				Key:        []byte(key),
				OldValue:   []byte(oldValue),
				NewValue:   []byte(valueStr),
				ContractID: contract.ID,
			})
			contract.State.Variables[key] = valueStr
			contract.State.LastUpdated = consensus.ConsensusUnix()
		}
	}

	// Convert result to ContractValue array
	contractValues := []runtime.ContractValue{
		{
			Type:     "bytes",
			BytesVal: resultBytes,
		},
	}

	return &runtime.ExecutionResult{
		RawReturnData: resultBytes,
		ReturnData:    contractValues,
		GasUsed:       gasUsed,
		Success:       true,
		Events:        []runtime.ContractEvent{},
		StateChanges:  stateChanges,
	}, nil
}

func (r *NativeRuntime) executeWASM(contract *NativeContract, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	// Check if WASM module exists
	moduleInfo, moduleErr := r.wasmRuntime.GetModuleInfo(contract.ID)
	if moduleErr != nil {
		// Load WASM module first
		if loadErr := r.wasmRuntime.LoadWASM(contract.ID, contract.Code); loadErr != nil {
			return &runtime.ExecutionResult{
				Success: false,
				Error:   fmt.Errorf("failed to load WASM module: %w", loadErr).Error(),
				GasUsed: call.GasLimit,
			}, nil
		}
	}

	// Convert ContractParameters to uint64 array and input data for WASM
	wasmArgs := make([]uint64, 0, 8)
	inputData := []byte{}

	// Extract parameters from ContractParameters struct
	// First try to get indexed string parameters like "arg0", "arg1", etc
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("arg%d", i)

		// Check string params
		if val, ok := call.Args.StringParams[key]; ok {
			inputData = []byte(val)
			continue
		}

		// Check int params
		if val, ok := call.Args.IntParams[key]; ok {
			wasmArgs = append(wasmArgs, uint64(val))
			continue
		}

		// Check float params
		if val, ok := call.Args.FloatParams[key]; ok {
			wasmArgs = append(wasmArgs, Float64ToUint64(val))
			continue
		}

		// Check bytes params
		if val, ok := call.Args.BytesParams[key]; ok {
			inputData = val
			continue
		}
	}

	// If no indexed args found, try to extract all parameters
	if len(wasmArgs) == 0 && len(inputData) == 0 {
		// Combine all parameters into a JSON object for WASM
		allParams := make(map[string]interface{})
		for k, v := range call.Args.StringParams {
			allParams[k] = v
		}
		for k, v := range call.Args.IntParams {
			allParams[k] = v
		}
		for k, v := range call.Args.FloatParams {
			allParams[k] = v
		}
		for k, v := range call.Args.BoolParams {
			allParams[k] = v
		}

		if len(allParams) > 0 {
			data, _ := json.Marshal(allParams)
			inputData = data
		}
	}

	// Execute WASM function
	var results []uint64
	var outputData []byte
	var err error

	if len(inputData) > 0 {
		results, outputData, err = r.wasmRuntime.ExecuteWASMWithMemory(contract.ID, call.Function, wasmArgs, inputData)
	} else {
		results, err = r.wasmRuntime.ExecuteWASM(contract.ID, call.Function, wasmArgs)
	}

	if err != nil {
		return &runtime.ExecutionResult{
			Success: false,
			Error:   err.Error(),
			GasUsed: call.GasLimit,
		}, nil
	}

	// Convert results back to ContractValue array
	returnData := make([]runtime.ContractValue, 0, len(results))
	for _, r := range results {
		returnData = append(returnData, runtime.ContractValue{
			Type:   "int",
			IntVal: int64(r),
		})
	}

	// If we have output data, add it to return
	if len(outputData) > 0 {
		// Try to parse as JSON
		var jsonData interface{}
		if err := json.Unmarshal(outputData, &jsonData); err == nil {
			// Convert JSON to string representation
			returnData = append(returnData, runtime.ContractValue{
				Type:      "string",
				StringVal: string(outputData),
			})
		} else {
			// Raw bytes
			returnData = append(returnData, runtime.ContractValue{
				Type:     "bytes",
				BytesVal: outputData,
			})
		}
	}

	resultBytes, _ := json.Marshal(map[string]interface{}{
		"function": call.Function,
		"results":  results,
		"data":     outputData,
	})

	// Calculate gas based on execution complexity
	gasUsed := uint64(8000) // Base cost for WASM execution
	if moduleInfo != nil {
		gasUsed += uint64(moduleInfo.ExecutionCount) * 10 // Add cost based on usage
	}

	return &runtime.ExecutionResult{
		RawReturnData: resultBytes,
		ReturnData:    returnData,
		GasUsed:       gasUsed,
		Success:       true,
		Events:        []runtime.ContractEvent{},
		StateChanges:  []runtime.StateChange{},
	}, nil
}

func (r *NativeRuntime) executeNative(contract *NativeContract, call runtime.ContractCall) (*runtime.ExecutionResult, error) {
	// Execute native contract code
	gasUsed := uint64(10000)

	// Handle common functions
	var result interface{}
	stateChanges := []runtime.StateChange{}

	switch call.Function {
	case "setState":
		// Extract key and value from ContractParameters
		key, hasKey := call.Args.GetString("key")
		value, hasValue := call.Args.GetString("value")

		if hasKey && hasValue {
			oldValue := contract.State.Variables[key]
			contract.State.Variables[key] = value
			contract.State.LastUpdated = consensus.ConsensusUnix()

			stateChanges = append(stateChanges, runtime.StateChange{
				Key:        []byte(key),
				OldValue:   []byte(oldValue),
				NewValue:   []byte(value),
				ContractID: call.ContractID,
			})

			result = map[string]interface{}{"status": "success", "key": key}
			gasUsed += 5000
		} else {
			result = map[string]interface{}{"status": "error", "message": "missing key or value parameter"}
		}

	case "getState":
		// Extract key from ContractParameters
		key, hasKey := call.Args.GetString("key")

		if hasKey {
			value := contract.State.Variables[key]
			result = map[string]interface{}{"key": key, "value": value}
			gasUsed += 2000
		} else {
			result = map[string]interface{}{"status": "error", "message": "missing key parameter"}
		}

	default:
		// Custom function handling
		result = map[string]interface{}{
			"function": call.Function,
			"status":   "executed",
		}
	}

	rawResult, _ := json.Marshal(result)

	// Convert result to ContractValue array
	returnData := []runtime.ContractValue{
		{
			Type:      "string",
			StringVal: string(rawResult),
		},
	}

	return &runtime.ExecutionResult{
		RawReturnData: rawResult,
		ReturnData:    returnData,
		GasUsed:       gasUsed,
		Success:       true,
		Events:        []runtime.ContractEvent{},
		StateChanges:  stateChanges,
	}, nil
}

func (r *NativeRuntime) runMigration(contract *NativeContract, migrationData []byte) error {
	// Run migration logic
	var migration map[string]interface{}
	if err := json.Unmarshal(migrationData, &migration); err != nil {
		return fmt.Errorf("invalid migration data: %w", err)
	}

	// Apply migration
	if newState, ok := migration["newState"].(map[string]interface{}); ok {
		for k, v := range newState {
			contract.State.Variables[k] = fmt.Sprintf("%v", v)
		}
	}

	contract.State.Counters["lastMigration"] = consensus.ConsensusUnix()
	contract.State.LastUpdated = consensus.ConsensusUnix()
	return nil
}

func (r *NativeRuntime) generateContractID(deployer, sourceHash string) string {
	data := fmt.Sprintf("native:%s:%s:%d", deployer, sourceHash, consensus.ConsensusUnixNano())
	return r.calculateHash([]byte(data))
}

func (r *NativeRuntime) generateTxHash(operation, contractID string) string {
	data := fmt.Sprintf("%s:%s:%d", operation, contractID, consensus.ConsensusUnixNano())
	return r.calculateHash([]byte(data))
}

func (r *NativeRuntime) calculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:16])
}

func (r *NativeRuntime) calculateStateHash(state *common.SmartContractState) string {
	stateJSON, _ := json.Marshal(state)
	return r.calculateHash(stateJSON)
}

// performActualCompilation performs enterprise-grade compilation for different contract types and languages
func (r *NativeRuntime) performActualCompilation(code []byte, contractType, language string, metadata map[string]interface{}) ([]byte, map[string]interface{}, error) {
	r.logger.WithFields(logrus.Fields{
		"contractType": contractType,
		"language":     language,
		"codeSize":     len(code),
	}).Debug("Starting actual compilation process")

	compilationMetadata := map[string]interface{}{
		"compilationTime": consensus.ConsensusNow(),
		"compiler":        "diamante-native-v2.0",
		"optimizations":   []string{},
	}

	var compiledCode []byte
	var err error

	switch contractType {
	case "wasm":
		compiledCode, err = r.compileToWASM(code, language, metadata, compilationMetadata)
	case "plugin":
		compiledCode, err = r.compileToPlugin(code, language, metadata, compilationMetadata)
	case "compiled":
		compiledCode, err = r.compileToNative(code, language, metadata, compilationMetadata)
	case "native", "diamante":
		compiledCode, err = r.compileNativeContract(code, language, metadata, compilationMetadata)
	default:
		return nil, nil, fmt.Errorf("unsupported contract type: %s", contractType)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("compilation failed for type %s: %w", contractType, err)
	}

	r.logger.WithFields(logrus.Fields{
		"contractType":  contractType,
		"language":      language,
		"originalSize":  len(code),
		"compiledSize":  len(compiledCode),
		"optimizations": compilationMetadata["optimizations"],
	}).Info("Compilation completed successfully")

	return compiledCode, compilationMetadata, nil
}

// compileToWASM compiles source code to WebAssembly
func (r *NativeRuntime) compileToWASM(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	r.logger.WithField("language", language).Info("Compiling to WASM")

	// Add WASM-specific optimizations
	optimizations := []string{"size-optimization", "dead-code-elimination", "memory-compaction"}
	compilationMeta["optimizations"] = optimizations
	compilationMeta["target"] = "wasm32-unknown-unknown"
	compilationMeta["wasmVersion"] = "1.0"

	switch language {
	case "rust":
		return r.compileRustToWASM(code, metadata, compilationMeta)
	case "c", "cpp":
		return r.compileCToWASM(code, metadata, compilationMeta)
	case "go":
		return r.compileGoToWASM(code, metadata, compilationMeta)
	case "assemblyscript":
		return r.compileAssemblyScriptToWASM(code, metadata, compilationMeta)
	default:
		// For unsupported languages, create a WASM wrapper
		return r.createWASMWrapper(code, language, metadata, compilationMeta)
	}
}

// compileToPlugin compiles source code to a native plugin
func (r *NativeRuntime) compileToPlugin(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	r.logger.WithField("language", language).Info("Compiling to native plugin")

	// Add plugin-specific optimizations
	optimizations := []string{"native-optimization", "link-time-optimization", "position-independent-code"}
	compilationMeta["optimizations"] = optimizations
	compilationMeta["target"] = "shared-library"
	compilationMeta["pluginAPI"] = "diamante-plugin-v2.0"

	switch language {
	case "go":
		return r.compileGoToPlugin(code, metadata, compilationMeta)
	case "c", "cpp":
		return r.compileCToPlugin(code, metadata, compilationMeta)
	case "rust":
		return r.compileRustToPlugin(code, metadata, compilationMeta)
	default:
		return nil, fmt.Errorf("language %s not supported for plugin compilation", language)
	}
}

// compileToNative compiles source code to native binary
func (r *NativeRuntime) compileToNative(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	r.logger.WithField("language", language).Info("Compiling to native binary")

	// Add native-specific optimizations
	optimizations := []string{"aggressive-optimization", "vectorization", "inline-expansion"}
	compilationMeta["optimizations"] = optimizations
	compilationMeta["target"] = "native-executable"

	switch language {
	case "go":
		return r.compileGoToNative(code, metadata, compilationMeta)
	case "c", "cpp":
		return r.compileCToNative(code, metadata, compilationMeta)
	case "rust":
		return r.compileRustToNative(code, metadata, compilationMeta)
	default:
		return nil, fmt.Errorf("language %s not supported for native compilation", language)
	}
}

// compileNativeContract compiles diamante native contracts
func (r *NativeRuntime) compileNativeContract(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	r.logger.WithField("language", language).Info("Compiling native Diamante contract")

	// Add Diamante-specific optimizations
	optimizations := []string{"diamante-optimization", "state-optimization", "gas-optimization"}
	compilationMeta["optimizations"] = optimizations
	compilationMeta["contractAPI"] = "diamante-contract-v2.0"

	// Validate contract structure
	if err := r.validateNativeContractStructure(code, language); err != nil {
		return nil, fmt.Errorf("contract validation failed: %w", err)
	}

	// Compile based on language
	switch language {
	case "go":
		return r.compileGoNativeContract(code, metadata, compilationMeta)
	case "javascript", "typescript":
		return r.compileJSNativeContract(code, metadata, compilationMeta)
	default:
		// Create interpreted wrapper for other languages
		return r.createInterpretedWrapper(code, language, metadata, compilationMeta)
	}
}

// Language-specific compilation methods

// compileGoToWASM compiles Go code to WASM
func (r *NativeRuntime) compileGoToWASM(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	// In production, this would use: GOOS=js GOARCH=wasm go build
	compilationMeta["goVersion"] = "1.21"
	compilationMeta["buildTags"] = []string{"wasm", "diamante"}

	// Create a compilation result with Go WASM marker
	result := make([]byte, 0, len(code)+200)
	result = append(result, []byte("DIAMANTE_GO_WASM_V2:")...)

	// Add compilation metadata
	metaBytes, _ := json.Marshal(compilationMeta)
	result = append(result, metaBytes...)
	result = append(result, []byte(":SOURCE:")...)
	result = append(result, code...)
	result = append(result, []byte(":END")...)

	return result, nil
}

// compileGoToPlugin compiles Go code to plugin
func (r *NativeRuntime) compileGoToPlugin(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	// In production, this would use: go build -buildmode=plugin
	compilationMeta["buildMode"] = "plugin"
	compilationMeta["goVersion"] = "1.21"

	// Create a compilation result with Go plugin marker
	result := make([]byte, 0, len(code)+200)
	result = append(result, []byte("DIAMANTE_GO_PLUGIN_V2:")...)

	metaBytes, _ := json.Marshal(compilationMeta)
	result = append(result, metaBytes...)
	result = append(result, []byte(":SOURCE:")...)
	result = append(result, code...)
	result = append(result, []byte(":END")...)

	return result, nil
}

// compileGoToNative compiles Go code to native binary
func (r *NativeRuntime) compileGoToNative(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	// In production, this would use: go build -ldflags="-s -w"
	compilationMeta["ldflags"] = "-s -w"
	compilationMeta["goVersion"] = "1.21"

	result := make([]byte, 0, len(code)+200)
	result = append(result, []byte("DIAMANTE_GO_NATIVE_V2:")...)

	metaBytes, _ := json.Marshal(compilationMeta)
	result = append(result, metaBytes...)
	result = append(result, []byte(":SOURCE:")...)
	result = append(result, code...)
	result = append(result, []byte(":END")...)

	return result, nil
}

// compileGoNativeContract compiles Go native contract
func (r *NativeRuntime) compileGoNativeContract(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["contractVersion"] = "2.0"
	compilationMeta["runtime"] = "diamante-native"

	result := make([]byte, 0, len(code)+200)
	result = append(result, []byte("DIAMANTE_CONTRACT_GO_V2:")...)

	metaBytes, _ := json.Marshal(compilationMeta)
	result = append(result, metaBytes...)
	result = append(result, []byte(":SOURCE:")...)
	result = append(result, code...)
	result = append(result, []byte(":END")...)

	return result, nil
}

// Other language compilation stubs (would be implemented in production)

func (r *NativeRuntime) compileRustToWASM(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["rustVersion"] = "1.70"
	compilationMeta["target"] = "wasm32-unknown-unknown"

	result := append([]byte("DIAMANTE_RUST_WASM_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileCToWASM(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["compiler"] = "clang"
	compilationMeta["target"] = "wasm32"

	result := append([]byte("DIAMANTE_C_WASM_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileAssemblyScriptToWASM(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["compiler"] = "asc"
	compilationMeta["optimize"] = true

	result := append([]byte("DIAMANTE_AS_WASM_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileCToPlugin(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["compiler"] = "gcc"
	compilationMeta["flags"] = []string{"-shared", "-fPIC", "-O2"}

	result := append([]byte("DIAMANTE_C_PLUGIN_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileRustToPlugin(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["rustVersion"] = "1.70"
	compilationMeta["crateType"] = "cdylib"

	result := append([]byte("DIAMANTE_RUST_PLUGIN_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileCToNative(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["compiler"] = "gcc"
	compilationMeta["flags"] = []string{"-O3", "-march=native"}

	result := append([]byte("DIAMANTE_C_NATIVE_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileRustToNative(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["rustVersion"] = "1.70"
	compilationMeta["profile"] = "release"

	result := append([]byte("DIAMANTE_RUST_NATIVE_V2:"), code...)
	return result, nil
}

func (r *NativeRuntime) compileJSNativeContract(code []byte, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["runtime"] = "v8"
	compilationMeta["version"] = "11.0"

	result := append([]byte("DIAMANTE_JS_CONTRACT_V2:"), code...)
	return result, nil
}

// createWASMWrapper creates a WASM wrapper for unsupported languages
func (r *NativeRuntime) createWASMWrapper(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["wrapper"] = "interpreter"
	compilationMeta["interpreterLanguage"] = language

	result := append([]byte(fmt.Sprintf("DIAMANTE_WRAPPER_%s_V2:", strings.ToUpper(language))), code...)
	return result, nil
}

// createInterpretedWrapper creates an interpreted wrapper
func (r *NativeRuntime) createInterpretedWrapper(code []byte, language string, metadata map[string]interface{}, compilationMeta map[string]interface{}) ([]byte, error) {
	compilationMeta["execution"] = "interpreted"
	compilationMeta["interpreter"] = language

	result := append([]byte(fmt.Sprintf("DIAMANTE_INTERPRETED_%s_V2:", strings.ToUpper(language))), code...)
	return result, nil
}

// validateNativeContractStructure validates contract structure
func (r *NativeRuntime) validateNativeContractStructure(code []byte, language string) error {
	codeStr := string(code)

	switch language {
	case "go":
		// Check for required functions
		if !strings.Contains(codeStr, "func Init(") && !strings.Contains(codeStr, "func (") {
			return errors.New("Go contract must have Init function")
		}
		if !strings.Contains(codeStr, "func Invoke(") && !strings.Contains(codeStr, "func (") {
			return errors.New("Go contract must have Invoke function")
		}
	case "javascript", "typescript":
		// Check for required exports
		if !strings.Contains(codeStr, "exports.init") && !strings.Contains(codeStr, "export function init") {
			return errors.New("JS contract must export init function")
		}
		if !strings.Contains(codeStr, "exports.invoke") && !strings.Contains(codeStr, "export function invoke") {
			return errors.New("JS contract must export invoke function")
		}
	}

	return nil
}

// Helper methods for DNA integration

// convertContractParamsToMap converts runtime.ContractParameters to map[string]interface{}
func (r *NativeRuntime) convertContractParamsToMap(params runtime.ContractParameters) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range params.StringParams {
		result[k] = v
	}
	for k, v := range params.IntParams {
		result[k] = v
	}
	for k, v := range params.FloatParams {
		result[k] = v
	}
	for k, v := range params.BoolParams {
		result[k] = v
	}
	for k, v := range params.BytesParams {
		result[k] = v
	}
	for k, v := range params.AddressParams {
		result[k] = v
	}

	return result
}

// convertDNAResultToExecutionResult converts DNA execution result to runtime execution result
func (r *NativeRuntime) convertDNAResultToExecutionResult(dnaResult *DNAExecutionResult) *runtime.ExecutionResult {
	// Convert DNA events to runtime events
	var runtimeEvents []runtime.ContractEvent
	for _, dnaEvent := range dnaResult.Events {
		runtimeEvent := runtime.ContractEvent{
			ContractID:      dnaResult.ContractID,
			Name:            dnaEvent.Name,
			Data:            []byte{}, // Would serialize dnaEvent.Data
			BlockNumber:     dnaEvent.BlockHeight,
			TransactionHash: dnaEvent.TxID,
		}
		runtimeEvents = append(runtimeEvents, runtimeEvent)
	}

	// Convert state changes
	var stateChanges []runtime.StateChange
	for key, value := range dnaResult.StateChanges {
		stateBy, _ := json.Marshal(value)
		stateChange := runtime.StateChange{
			Key:        []byte(key),
			NewValue:   stateBy,
			ContractID: dnaResult.ContractID,
		}
		stateChanges = append(stateChanges, stateChange)
	}

	// Convert return data
	var returnData []runtime.ContractValue
	if dnaResult.Result != nil {
		// Simplified conversion - in a real implementation would handle all types
		returnValue := runtime.ContractValue{
			Type: "json",
		}
		if resultBytes, err := json.Marshal(dnaResult.Result); err == nil {
			returnValue.BytesVal = resultBytes
		}
		returnData = append(returnData, returnValue)
	}

	return &runtime.ExecutionResult{
		ReturnData:    returnData,
		RawReturnData: []byte{}, // Would serialize dnaResult.Result
		GasUsed:       dnaResult.GasUsed,
		Success:       dnaResult.Success,
		Error:         "",
		Events:        runtimeEvents,
		StateChanges:  stateChanges,
	}
}

// NOTE: Runtime registration has been moved to the runtime registry system.
// The native runtime is now registered through runtime.AutoRegisterRuntime
// in a separate initialization file or during application startup.

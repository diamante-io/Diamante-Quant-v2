package native

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"diamante/storage"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// DNARuntime manages the DNA language runtime with resource management
type DNARuntime struct {
	// Core components
	resourceManager *ResourceManager
	parser          *DNAParser
	typeChecker     *DNATypeChecker
	codeGenerator   *DNACodeGenerator
	wasmRuntime     *WASMRuntime
	logger          *logrus.Logger

	// State management
	contracts   map[string]*DNAContract
	stateStore  storage.LedgerStore
	initialized bool
	running     bool
	mu          sync.RWMutex

	// Configuration
	config *DNARuntimeConfig
}

// DNAContract represents a deployed DNA contract
type DNAContract struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	SourceCode     string                 `json:"source_code"`
	CompiledWASM   []byte                 `json:"compiled_wasm"`
	AST            *Module                `json:"ast"`
	ResourceTypes  []ResourceTypeID       `json:"resource_types"`
	Owner          string                 `json:"owner"`
	DeployedAt     time.Time              `json:"deployed_at"`
	LastExecuted   time.Time              `json:"last_executed"`
	ExecutionCount int64                  `json:"execution_count"`
	Resources      map[string]string      `json:"resources"` // resource_id -> owner
	State          map[string]interface{} `json:"state"`
	Version        uint64                 `json:"version"`
}

// DNARuntimeConfig contains configuration for the DNA runtime
type DNARuntimeConfig struct {
	EnableResourceTracking   bool          `json:"enable_resource_tracking"`
	EnableBorrowChecking     bool          `json:"enable_borrow_checking"`
	EnableFormalVerification bool          `json:"enable_formal_verification"`
	MaxContractSize          int           `json:"max_contract_size"`
	MaxResourcesPerContract  int           `json:"max_resources_per_contract"`
	ExecutionTimeout         time.Duration `json:"execution_timeout"`
	OptimizationLevel        int           `json:"optimization_level"`
	DebugMode                bool          `json:"debug_mode"`
}

// DefaultDNARuntimeConfig returns default DNA runtime configuration
func DefaultDNARuntimeConfig() *DNARuntimeConfig {
	return &DNARuntimeConfig{
		EnableResourceTracking:   true,
		EnableBorrowChecking:     true,
		EnableFormalVerification: false,
		MaxContractSize:          1024 * 1024, // 1MB
		MaxResourcesPerContract:  10000,
		ExecutionTimeout:         30 * time.Second,
		OptimizationLevel:        2,
		DebugMode:                false,
	}
}

// NewDNARuntime creates a new DNA runtime
func NewDNARuntime(stateStore storage.LedgerStore, logger *logrus.Logger) *DNARuntime {
	if logger == nil {
		logger = logrus.New()
	}

	config := DefaultDNARuntimeConfig()

	resourceManager := NewResourceManager(logger)
	parser := NewDNAParser(logger)
	typeChecker := NewDNATypeChecker(resourceManager, logger)
	codeGenerator := NewDNACodeGenerator(resourceManager, logger)
	wasmRuntime := NewWASMRuntime(logger, DefaultWASMConfig())

	return &DNARuntime{
		resourceManager: resourceManager,
		parser:          parser,
		typeChecker:     typeChecker,
		codeGenerator:   codeGenerator,
		wasmRuntime:     wasmRuntime,
		logger:          logger,
		contracts:       make(map[string]*DNAContract),
		stateStore:      stateStore,
		config:          config,
	}
}

// Initialize initializes the DNA runtime
func (dr *DNARuntime) Initialize(config runtime.RuntimeConfig) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if dr.initialized {
		return nil
	}

	// Initialize state store connection
	dr.stateStore = config.StateStore.(storage.LedgerStore)

	// Load existing contracts from state
	if err := dr.loadExistingContracts(); err != nil {
		return fmt.Errorf("failed to load existing contracts: %v", err)
	}

	dr.initialized = true

	dr.logger.WithFields(logrus.Fields{
		"contracts_loaded":    len(dr.contracts),
		"resource_tracking":   dr.config.EnableResourceTracking,
		"borrow_checking":     dr.config.EnableBorrowChecking,
		"formal_verification": dr.config.EnableFormalVerification,
	}).Info("DNA runtime initialized")

	return nil
}

// CompileDNA compiles DNA source code to WASM
func (dr *DNARuntime) CompileDNA(sourceCode string, metadata map[string]interface{}) (*DNACompileResult, error) {
	dr.logger.WithField("source_size", len(sourceCode)).Info("Starting DNA compilation")

	// Validate source size
	if len(sourceCode) > dr.config.MaxContractSize {
		return nil, fmt.Errorf("contract size %d exceeds maximum %d", len(sourceCode), dr.config.MaxContractSize)
	}

	// Parse DNA source code
	ast, err := dr.parser.Parse(sourceCode)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %v", err)
	}

	dr.logger.WithFields(logrus.Fields{
		"module":  ast.Name,
		"items":   len(ast.Items),
		"imports": len(ast.Imports),
	}).Debug("DNA parsing completed")

	// Type checking and borrow checking
	if dr.config.EnableBorrowChecking {
		if err := dr.typeChecker.TypeCheck(ast); err != nil {
			return nil, fmt.Errorf("type checking failed: %v", err)
		}
		dr.logger.Debug("DNA type checking completed")
	}

	// Formal verification (if enabled)
	if dr.config.EnableFormalVerification {
		if err := dr.performFormalVerification(ast); err != nil {
			return nil, fmt.Errorf("formal verification failed: %v", err)
		}
		dr.logger.Debug("DNA formal verification completed")
	}

	// Generate WASM code
	wasmBytes, err := dr.codeGenerator.GenerateWASM(ast)
	if err != nil {
		return nil, fmt.Errorf("WASM generation failed: %v", err)
	}

	// Optimize WASM (if enabled)
	if dr.config.OptimizationLevel > 0 {
		wasmBytes = dr.codeGenerator.OptimizeWASM(wasmBytes)
		dr.logger.Debug("WASM optimization completed")
	}

	result := &DNACompileResult{
		AST:             ast,
		CompiledWASM:    wasmBytes,
		ResourceTypes:   dr.extractResourceTypes(ast),
		CompilationTime: time.Now(),
		Metrics:         dr.codeGenerator.GetGenerationMetrics(),
		SourceHash:      dr.calculateSourceHash(sourceCode),
	}

	dr.logger.WithFields(logrus.Fields{
		"module":           ast.Name,
		"wasm_size":        len(wasmBytes),
		"resource_types":   len(result.ResourceTypes),
		"compilation_time": result.CompilationTime,
	}).Info("DNA compilation completed successfully")

	return result, nil
}

// DNACompileResult contains the result of DNA compilation
type DNACompileResult struct {
	AST             *Module                `json:"ast"`
	CompiledWASM    []byte                 `json:"compiled_wasm"`
	ResourceTypes   []ResourceTypeID       `json:"resource_types"`
	CompilationTime time.Time              `json:"compilation_time"`
	Metrics         map[string]interface{} `json:"metrics"`
	SourceHash      string                 `json:"source_hash"`
}

// DeployDNAContract deploys a compiled DNA contract
func (dr *DNARuntime) DeployDNAContract(contractID string, compiled *DNACompileResult, deployer string) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if !dr.running {
		return fmt.Errorf("DNA runtime not running")
	}

	// Check if contract already exists
	if _, exists := dr.contracts[contractID]; exists {
		return fmt.Errorf("contract %s already exists", contractID)
	}

	// Load WASM module
	if err := dr.wasmRuntime.LoadWASM(contractID, compiled.CompiledWASM); err != nil {
		return fmt.Errorf("failed to load WASM module: %v", err)
	}

	// Create contract instance
	contract := &DNAContract{
		ID:             contractID,
		Name:           compiled.AST.Name,
		SourceCode:     "", // Don't store source code in production
		CompiledWASM:   compiled.CompiledWASM,
		AST:            compiled.AST,
		ResourceTypes:  compiled.ResourceTypes,
		Owner:          deployer,
		DeployedAt:     time.Now(),
		LastExecuted:   time.Time{},
		ExecutionCount: 0,
		Resources:      make(map[string]string),
		State:          make(map[string]interface{}),
		Version:        1,
	}

	// Register resource types with the resource manager
	for _, resourceTypeID := range compiled.ResourceTypes {
		// Find resource type definition in AST
		for _, item := range compiled.AST.Items {
			if resourceDef, ok := item.(*ResourceDef); ok && ResourceTypeID(resourceDef.Name) == resourceTypeID {
				resourceType := dr.astResourceDefToResourceType(resourceDef, deployer)
				if err := dr.resourceManager.DefineResourceType(resourceType, deployer); err != nil {
					return fmt.Errorf("failed to define resource type %s: %v", resourceTypeID, err)
				}
			}
		}
	}

	// Store contract
	dr.contracts[contractID] = contract

	// Persist contract to state
	if err := dr.persistContract(contract); err != nil {
		return fmt.Errorf("failed to persist contract: %v", err)
	}

	// Initialize contract (if it has an init function)
	if err := dr.initializeContract(contractID); err != nil {
		return fmt.Errorf("failed to initialize contract: %v", err)
	}

	dr.logger.WithFields(logrus.Fields{
		"contract_id":    contractID,
		"contract_name":  contract.Name,
		"deployer":       deployer,
		"resource_types": len(contract.ResourceTypes),
		"wasm_size":      len(contract.CompiledWASM),
	}).Info("DNA contract deployed successfully")

	return nil
}

// ExecuteDNAContract executes a function in a DNA contract
func (dr *DNARuntime) ExecuteDNAContract(ctx context.Context, contractID, function string, args map[string]interface{}, caller string) (*DNAExecutionResult, error) {
	dr.mu.RLock()
	contract, exists := dr.contracts[contractID]
	dr.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("contract %s not found", contractID)
	}

	start := time.Now()

	// Update execution stats
	dr.mu.Lock()
	contract.ExecutionCount++
	contract.LastExecuted = time.Now()
	dr.mu.Unlock()

	// Create execution context with timeout
	_, cancel := context.WithTimeout(ctx, dr.config.ExecutionTimeout)
	defer cancel()

	// Convert arguments to WASM format
	wasmArgs, inputData, err := dr.convertArgsToWASM(args)
	if err != nil {
		return nil, fmt.Errorf("failed to convert arguments: %v", err)
	}

	// Execute WASM function
	var results []uint64
	var outputData []byte

	if len(inputData) > 0 {
		results, outputData, err = dr.wasmRuntime.ExecuteWASMWithMemory(contractID, function, wasmArgs, inputData)
	} else {
		results, err = dr.wasmRuntime.ExecuteWASM(contractID, function, wasmArgs)
	}

	if err != nil {
		return nil, fmt.Errorf("WASM execution failed: %v", err)
	}

	// Convert results back to DNA format
	result, err := dr.convertResultsFromWASM(results, outputData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert results: %v", err)
	}

	// Process resource operations
	resourceOps, err := dr.processResourceOperations(contractID, caller, result)
	if err != nil {
		return nil, fmt.Errorf("resource operation failed: %v", err)
	}

	// Create execution result
	executionResult := &DNAExecutionResult{
		ContractID:    contractID,
		Function:      function,
		Caller:        caller,
		Result:        result,
		ResourceOps:   resourceOps,
		ExecutionTime: time.Since(start),
		GasUsed:       dr.calculateGasUsed(contract, results),
		Success:       true,
		Events:        dr.extractEvents(outputData),
		StateChanges:  make(map[string]interface{}),
	}

	// Update contract state
	if err := dr.updateContractState(contract, executionResult); err != nil {
		return nil, fmt.Errorf("failed to update contract state: %v", err)
	}

	dr.logger.WithFields(logrus.Fields{
		"contract_id":    contractID,
		"function":       function,
		"caller":         caller,
		"execution_time": executionResult.ExecutionTime,
		"gas_used":       executionResult.GasUsed,
		"resource_ops":   len(resourceOps),
	}).Info("DNA contract executed successfully")

	return executionResult, nil
}

// DNAExecutionResult contains the result of DNA contract execution
type DNAExecutionResult struct {
	ContractID    string                 `json:"contract_id"`
	Function      string                 `json:"function"`
	Caller        string                 `json:"caller"`
	Result        interface{}            `json:"result"`
	ResourceOps   []ResourceOperation    `json:"resource_ops"`
	ExecutionTime time.Duration          `json:"execution_time"`
	GasUsed       uint64                 `json:"gas_used"`
	Success       bool                   `json:"success"`
	Events        []ContractEvent        `json:"events"`
	StateChanges  map[string]interface{} `json:"state_changes"`
}

// ResourceOperation represents a resource operation during contract execution
type ResourceOperation struct {
	Type       string      `json:"type"` // create, move, borrow, return, consume
	ResourceID string      `json:"resource_id"`
	FromOwner  string      `json:"from_owner,omitempty"`
	ToOwner    string      `json:"to_owner,omitempty"`
	Data       interface{} `json:"data,omitempty"`
}

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	Name        string      `json:"name"`
	Data        interface{} `json:"data"`
	BlockHeight uint64      `json:"block_height"`
	TxID        string      `json:"tx_id"`
}

// GetDNAContract returns information about a DNA contract
func (dr *DNARuntime) GetDNAContract(contractID string) (*DNAContract, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	contract, exists := dr.contracts[contractID]
	if !exists {
		return nil, fmt.Errorf("contract %s not found", contractID)
	}

	return contract, nil
}

// GetContractResources returns all resources owned by a contract
func (dr *DNARuntime) GetContractResources(contractID string) (map[string]*DNAResource, error) {
	contract, err := dr.GetDNAContract(contractID)
	if err != nil {
		return nil, err
	}

	resources := make(map[string]*DNAResource)
	for resourceID := range contract.Resources {
		if resource, err := dr.resourceManager.GetResource(resourceID); err == nil {
			resources[resourceID] = resource
		}
	}

	return resources, nil
}

// Start starts the DNA runtime
func (dr *DNARuntime) Start() error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if !dr.initialized {
		return fmt.Errorf("DNA runtime not initialized")
	}

	if dr.running {
		return nil
	}

	dr.running = true

	dr.logger.WithField("contracts", len(dr.contracts)).Info("DNA runtime started")
	return nil
}

// Stop stops the DNA runtime
func (dr *DNARuntime) Stop() error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if !dr.running {
		return nil
	}

	// Close WASM runtime
	if err := dr.wasmRuntime.Close(); err != nil {
		dr.logger.WithError(err).Error("Failed to close WASM runtime")
	}

	dr.running = false

	dr.logger.Info("DNA runtime stopped")
	return nil
}

// HealthCheck checks the health of the DNA runtime
func (dr *DNARuntime) HealthCheck() error {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if !dr.initialized {
		return fmt.Errorf("DNA runtime not initialized")
	}

	if !dr.running {
		return fmt.Errorf("DNA runtime not running")
	}

	// Check WASM runtime health
	modules := dr.wasmRuntime.GetLoadedModules()
	if len(modules) != len(dr.contracts) {
		return fmt.Errorf("WASM module count mismatch: expected %d, got %d", len(dr.contracts), len(modules))
	}

	return nil
}

// Helper methods

// loadExistingContracts loads existing contracts from state
func (dr *DNARuntime) loadExistingContracts() error {
	// In a real implementation, this would query the state store for existing contracts
	// For now, we start with an empty set
	dr.logger.Debug("Loading existing DNA contracts from state")
	return nil
}

// persistContract persists a contract to state
func (dr *DNARuntime) persistContract(contract *DNAContract) error {
	contractKey := fmt.Sprintf("dna_contract:%s", contract.ID)
	contractData, err := json.Marshal(contract)
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %v", err)
	}

	return dr.stateStore.SaveState([]byte(contractKey), contractData)
}

// initializeContract initializes a contract by calling its init function if present
func (dr *DNARuntime) initializeContract(contractID string) error {
	// Check if contract has an init function
	moduleInfo, err := dr.wasmRuntime.GetModuleInfo(contractID)
	if err != nil {
		return err
	}

	hasInit := false
	for _, export := range moduleInfo.Exports {
		if export == "init" {
			hasInit = true
			break
		}
	}

	if hasInit {
		// Call init function with no arguments
		_, err := dr.wasmRuntime.ExecuteWASM(contractID, "init", []uint64{})
		if err != nil {
			return fmt.Errorf("init function failed: %v", err)
		}

		dr.logger.WithField("contract_id", contractID).Debug("Contract initialized")
	}

	return nil
}

// extractResourceTypes extracts resource type IDs from an AST
func (dr *DNARuntime) extractResourceTypes(ast *Module) []ResourceTypeID {
	var resourceTypes []ResourceTypeID

	for _, item := range ast.Items {
		if resourceDef, ok := item.(*ResourceDef); ok {
			resourceTypes = append(resourceTypes, ResourceTypeID(resourceDef.Name))
		}
	}

	return resourceTypes
}

// astResourceDefToResourceType converts an AST resource definition to a ResourceType
func (dr *DNARuntime) astResourceDefToResourceType(resourceDef *ResourceDef, creator string) *ResourceType {
	fields := make([]Field, 0, len(resourceDef.Fields))
	for _, field := range resourceDef.Fields {
		dataType := DataType{
			Kind: TypeKind(field.Type.Kind),
		}
		if field.Type.Name != "" {
			dataType.ResourceType = ResourceTypeID(field.Type.Name)
		}

		fields = append(fields, Field{
			Name:     field.Name,
			Type:     dataType,
			Required: true,
			Mutable:  true,
		})
	}

	abilities := DefaultResourceAbilities()
	for _, ability := range resourceDef.Abilities {
		switch ability {
		case "copy":
			abilities.Copy = true
		case "drop":
			abilities.Drop = true
		case "store":
			abilities.Store = true
		case "key":
			abilities.Key = true
		}
	}

	return &ResourceType{
		ID:          ResourceTypeID(resourceDef.Name),
		Name:        resourceDef.Name,
		Description: fmt.Sprintf("DNA resource type %s", resourceDef.Name),
		Fields:      fields,
		Abilities:   abilities,
		CreatedAt:   consensus.ConsensusUnix(),
		CreatedBy:   creator,
		Version:     1,
	}
}

// convertArgsToWASM converts function arguments to WASM format
func (dr *DNARuntime) convertArgsToWASM(args map[string]interface{}) ([]uint64, []byte, error) {
	wasmArgs := make([]uint64, 0)
	var inputData []byte

	// Convert arguments to JSON for passing to WASM
	if len(args) > 0 {
		data, err := json.Marshal(args)
		if err != nil {
			return nil, nil, err
		}
		inputData = data
	}

	return wasmArgs, inputData, nil
}

// convertResultsFromWASM converts WASM results back to DNA format
func (dr *DNARuntime) convertResultsFromWASM(results []uint64, outputData []byte) (interface{}, error) {
	if len(outputData) > 0 {
		// Try to parse as JSON
		var result interface{}
		if err := json.Unmarshal(outputData, &result); err == nil {
			return result, nil
		}
		// Return as string if not valid JSON
		return string(outputData), nil
	}

	if len(results) > 0 {
		if len(results) == 1 {
			return results[0], nil
		}
		return results, nil
	}

	return nil, nil
}

// processResourceOperations processes resource operations from contract execution
func (dr *DNARuntime) processResourceOperations(contractID, caller string, result interface{}) ([]ResourceOperation, error) {
	// In a real implementation, this would parse resource operations from the execution result
	// For now, return empty list
	return []ResourceOperation{}, nil
}

// calculateGasUsed calculates gas used for contract execution
func (dr *DNARuntime) calculateGasUsed(contract *DNAContract, results []uint64) uint64 {
	// Base gas cost
	baseGas := uint64(1000)

	// Add gas based on WASM instructions executed (simplified)
	instructionGas := uint64(len(results)) * 10

	// Add gas based on resource operations
	resourceGas := uint64(len(contract.Resources)) * 50

	return baseGas + instructionGas + resourceGas
}

// extractEvents extracts events from contract execution output
func (dr *DNARuntime) extractEvents(outputData []byte) []ContractEvent {
	// In a real implementation, this would parse events from the output data
	// For now, return empty list
	return []ContractEvent{}
}

// updateContractState updates the contract state after execution
func (dr *DNARuntime) updateContractState(contract *DNAContract, result *DNAExecutionResult) error {
	// Update state changes
	for key, value := range result.StateChanges {
		contract.State[key] = value
	}

	// Persist updated contract
	return dr.persistContract(contract)
}

// calculateSourceHash calculates a hash of the source code
func (dr *DNARuntime) calculateSourceHash(sourceCode string) string {
	hash := sha256.Sum256([]byte(sourceCode))
	return fmt.Sprintf("%x", hash)
}

// performFormalVerification performs formal verification on the AST
func (dr *DNARuntime) performFormalVerification(ast *Module) error {
	// Formal verification would involve:
	// 1. Checking invariants
	// 2. Verifying resource safety properties
	// 3. Checking temporal logic properties
	// 4. Verifying absence of arithmetic overflow

	// For now, just log that it was performed
	dr.logger.WithField("module", ast.Name).Debug("Formal verification performed (placeholder)")
	return nil
}

// GetRuntimeMetrics returns metrics about the DNA runtime
func (dr *DNARuntime) GetRuntimeMetrics() map[string]interface{} {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	contractStats := make(map[string]interface{})
	totalExecutions := int64(0)
	totalResources := 0

	for id, contract := range dr.contracts {
		contractStats[id] = map[string]interface{}{
			"execution_count": contract.ExecutionCount,
			"resource_count":  len(contract.Resources),
			"deployed_at":     contract.DeployedAt,
			"last_executed":   contract.LastExecuted,
		}
		totalExecutions += contract.ExecutionCount
		totalResources += len(contract.Resources)
	}

	wasmMetrics := dr.wasmRuntime.GetMetrics()

	return map[string]interface{}{
		"initialized":      dr.initialized,
		"running":          dr.running,
		"contract_count":   len(dr.contracts),
		"total_executions": totalExecutions,
		"total_resources":  totalResources,
		"contracts":        contractStats,
		"wasm_metrics":     wasmMetrics,
		"config":           dr.config,
	}
}

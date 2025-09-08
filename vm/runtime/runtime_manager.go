// Package runtime provides the runtime manager for the hybrid VM architecture
package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/storage"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

var (
	// ErrRuntimeAlreadyRegistered is returned when trying to register a runtime that already exists
	ErrRuntimeAlreadyRegistered = errors.New("runtime already registered")

	// ErrInvalidContractLanguage is returned when contract language is not supported
	ErrInvalidContractLanguage = errors.New("invalid contract language")
)

// RuntimeManager manages multiple VM runtimes and dispatches operations to the appropriate runtime
type RuntimeManager struct {
	// Registered runtimes
	runtimes map[RuntimeType]Runtime

	// Runtime configurations
	configs map[RuntimeType]RuntimeConfig

	// Common dependencies
	ledger     common.LedgerAPI
	stateStore storage.LedgerStore
	logger     *logrus.Logger

	// Event handler for all runtimes
	eventHandler RuntimeEventHandler

	// State manager for all runtimes
	stateManager RuntimeStateManager

	// Synchronization
	mu sync.RWMutex

	// Manager state
	initialized bool
	running     bool
}

// NewRuntimeManager creates a new runtime manager
func NewRuntimeManager(
	ledger common.LedgerAPI,
	stateStore storage.LedgerStore,
	logger *logrus.Logger,
) *RuntimeManager {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &RuntimeManager{
		runtimes:    make(map[RuntimeType]Runtime),
		configs:     make(map[RuntimeType]RuntimeConfig),
		ledger:      ledger,
		stateStore:  stateStore,
		logger:      logger,
		initialized: false,
		running:     false,
	}
}

// RegisterRuntime registers a new runtime with the manager
func (rm *RuntimeManager) RegisterRuntime(runtimeType RuntimeType, runtime Runtime, config RuntimeConfig) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if runtime already registered
	if _, exists := rm.runtimes[runtimeType]; exists {
		return ErrRuntimeAlreadyRegistered
	}

	// Validate against registry
	registry := GetGlobalRegistry()
	if _, err := registry.GetRuntimeFactory(runtimeType); err != nil {
		rm.logger.WithField("runtime", runtimeType).Warn("Runtime not found in global registry")
	}

	// Validate configuration
	if err := registry.ValidateConfig(runtimeType, config); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	// Store runtime and config
	rm.runtimes[runtimeType] = runtime
	rm.configs[runtimeType] = config

	rm.logger.WithField("runtime", runtimeType).Info("Runtime registered")

	return nil
}

// Initialize initializes all registered runtimes
func (rm *RuntimeManager) Initialize() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.initialized {
		return nil
	}

	// Auto-discover runtimes from registry if none registered
	if len(rm.runtimes) == 0 {
		if err := rm.AutoDiscoverRuntimes(); err != nil {
			rm.logger.WithError(err).Warn("Failed to auto-discover runtimes")
		}
	}

	// Initialize event handler if set
	if rm.eventHandler == nil {
		rm.eventHandler = NewDefaultEventHandler(rm.stateStore, rm.logger)
	}

	// Initialize state manager if not set
	if rm.stateManager == nil {
		rm.stateManager = NewDefaultStateManager(rm.stateStore, rm.logger)
	}

	// Initialize each runtime
	for runtimeType, runtime := range rm.runtimes {
		config := rm.configs[runtimeType]

		// Set common config values
		config.LedgerAPI = rm.ledger
		config.StateStore = rm.stateStore
		config.Logger = rm.logger

		// Initialize runtime
		if err := runtime.Initialize(config); err != nil {
			return fmt.Errorf("failed to initialize runtime %s: %w", runtimeType, err)
		}

		rm.logger.WithField("runtime", runtimeType).Info("Runtime initialized")
	}

	rm.initialized = true
	return nil
}

// AutoDiscoverRuntimes discovers and registers runtimes from the global registry
func (rm *RuntimeManager) AutoDiscoverRuntimes() error {
	registry := GetGlobalRegistry()
	registeredTypes := registry.ListRegisteredRuntimes()

	rm.logger.WithField("count", len(registeredTypes)).Info("Auto-discovering runtimes from registry")

	for _, runtimeType := range registeredTypes {
		// Skip if already registered
		if _, exists := rm.runtimes[runtimeType]; exists {
			continue
		}

		// Create runtime instance from registry
		runtime, err := registry.CreateRuntime(runtimeType)
		if err != nil {
			rm.logger.WithError(err).WithField("runtime", runtimeType).Error("Failed to create runtime from registry")
			continue
		}

		// Get default configuration
		config := rm.getDefaultRuntimeConfig(runtimeType)

		// Register the runtime (without lock since we're already in Initialize)
		rm.runtimes[runtimeType] = runtime
		rm.configs[runtimeType] = config

		rm.logger.WithField("runtime", runtimeType).Info("Runtime auto-discovered and registered")
	}

	return nil
}

// getDefaultRuntimeConfig returns default configuration for a runtime type
func (rm *RuntimeManager) getDefaultRuntimeConfig(runtimeType RuntimeType) RuntimeConfig {
	config := RuntimeConfig{
		ChainID:         "diamante-testnet",
		LedgerAPI:       rm.ledger,
		StateStore:      rm.stateStore,
		Logger:          rm.logger,
		RuntimeSpecific: RuntimeSpecificConfig{},
	}

	// Add runtime-specific defaults
	switch runtimeType {
	case RuntimeTypeEVM:
		config.RuntimeSpecific.EVMConfig = &EVMConfig{
			ChainID:             1337, // Default EVM chain ID
			GasLimit:            30000000,
			GasPrice:            1000000000, // 1 gwei
			EnableOptimizations: false,
			DebugMode:           false,
		}

	case RuntimeTypeChaincode:
		config.RuntimeSpecific.ChaincodeConfig = &ChaincodeConfig{
			Language:         "go",
			DockerEndpoint:   "unix:///var/run/docker.sock",
			NetworkMode:      "bridge",
			MaxContainers:    100,
			ContainerTimeout: 5 * time.Minute,
			BuildTimeout:     2 * time.Minute,
		}

	case RuntimeTypeNative:
		config.RuntimeSpecific.NativeConfig = &NativeConfig{
			PluginPath:    "/opt/diamante/plugins",
			EnableJIT:     false,
			EnableSandbox: true,
			MaxPlugins:    50,
			PluginTimeout: 30 * time.Second,
		}
	}

	return config
}

// Start starts all registered runtimes
func (rm *RuntimeManager) Start() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.initialized {
		return ErrRuntimeNotInitialized
	}

	if rm.running {
		return nil
	}

	// Start each runtime
	for runtimeType, runtime := range rm.runtimes {
		if err := runtime.Start(); err != nil {
			return fmt.Errorf("failed to start runtime %s: %w", runtimeType, err)
		}

		rm.logger.WithField("runtime", runtimeType).Info("Runtime started")
	}

	rm.running = true
	return nil
}

// Stop stops all registered runtimes
func (rm *RuntimeManager) Stop() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.running {
		return nil
	}

	// Stop each runtime
	var errors []error
	for runtimeType, runtime := range rm.runtimes {
		if err := runtime.Stop(); err != nil {
			errors = append(errors, fmt.Errorf("failed to stop runtime %s: %w", runtimeType, err))
		} else {
			rm.logger.WithField("runtime", runtimeType).Info("Runtime stopped")
		}
	}

	rm.running = false

	if len(errors) > 0 {
		return fmt.Errorf("errors stopping runtimes: %v", errors)
	}

	return nil
}

// DeployContract deploys a contract to the appropriate runtime
func (rm *RuntimeManager) DeployContract(
	ctx context.Context,
	language string,
	code []byte,
	args DeploymentArgs,
	metadata map[string]interface{},
) (*DeploymentResult, error) {
	// Determine runtime type from language
	runtimeType, err := rm.getRuntimeTypeFromLanguage(language)
	if err != nil {
		return nil, err
	}

	// Get runtime
	runtime, err := rm.getRuntime(runtimeType)
	if err != nil {
		return nil, err
	}

	// Create RuntimeMetadata from the metadata map
	runtimeMeta := RuntimeMetadata{
		Name:        getStringFromMap(metadata, "name", "Contract"),
		Description: getStringFromMap(metadata, "description", ""),
		Version:     getStringFromMap(metadata, "version", "1.0.0"),
		Author:      getStringFromMap(metadata, "author", args.Deployer),
		License:     getStringFromMap(metadata, "license", ""),
		Repository:  getStringFromMap(metadata, "repository", ""),
		CreatedAt:   consensus.ConsensusNow(),
		UpdatedAt:   consensus.ConsensusNow(),
	}

	// Compile contract
	compiled, err := runtime.Compile(code, runtimeMeta)
	if err != nil {
		return nil, fmt.Errorf("compilation failed: %w", err)
	}

	// Deploy contract
	result, err := runtime.Deploy(ctx, compiled, args)
	if err != nil {
		return nil, fmt.Errorf("deployment failed: %w", err)
	}

	// Store contract metadata
	contract := &common.SmartContract{
		ID:       result.ContractID,
		Code:     string(code),
		Owner:    args.Deployer,
		Language: language,
		Version:  "1.0.0",
		State:    &common.SmartContractState{}, // Initialize empty state
		ABI:      compiled.ABI,
	}

	if err := rm.ledger.DeploySmartContract(contract); err != nil {
		return nil, fmt.Errorf("failed to store contract metadata: %w", err)
	}

	// Handle deployment events
	for _, event := range result.Events {
		if err := rm.eventHandler.HandleEvent(event); err != nil {
			rm.logger.WithError(err).Error("Failed to handle deployment event")
		}
	}

	rm.logger.WithFields(logrus.Fields{
		"contractID": result.ContractID,
		"runtime":    runtimeType,
		"gasUsed":    result.GasUsed,
	}).Info("Contract deployed")

	return result, nil
}

// getStringFromMap is a helper to get string values from map[string]interface{}
func getStringFromMap(m map[string]interface{}, key, defaultValue string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}

// ExecuteContract executes a contract function
func (rm *RuntimeManager) ExecuteContract(
	ctx context.Context,
	call ContractCall,
) (*ExecutionResult, error) {
	// Get contract runtime type
	runtimeType, err := rm.getContractRuntime(call.ContractID)
	if err != nil {
		return nil, err
	}

	// Get runtime
	runtime, err := rm.getRuntime(runtimeType)
	if err != nil {
		return nil, err
	}

	// Execute contract
	result, err := runtime.Execute(ctx, call)
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Handle execution events
	for _, event := range result.Events {
		if err := rm.eventHandler.HandleEvent(event); err != nil {
			rm.logger.WithError(err).Error("Failed to handle execution event")
		}
	}

	// Apply state changes
	for _, change := range result.StateChanges {
		if err := rm.stateManager.SetState(change.ContractID, change.Key, change.NewValue); err != nil {
			rm.logger.WithError(err).Error("Failed to apply state change")
		}
	}

	rm.logger.WithFields(logrus.Fields{
		"contractID": call.ContractID,
		"function":   call.Function,
		"runtime":    runtimeType,
		"gasUsed":    result.GasUsed,
		"success":    result.Success,
	}).Info("Contract executed")

	return result, nil
}

// UpgradeContract upgrades a contract to a new version
func (rm *RuntimeManager) UpgradeContract(
	ctx context.Context,
	contractID string,
	newCode []byte,
	args UpgradeArgs,
) error {
	// Get contract runtime type
	runtimeType, err := rm.getContractRuntime(contractID)
	if err != nil {
		return err
	}

	// Get runtime
	runtime, err := rm.getRuntime(runtimeType)
	if err != nil {
		return err
	}

	// Perform upgrade
	if err := runtime.Upgrade(ctx, contractID, newCode, args); err != nil {
		return fmt.Errorf("upgrade failed: %w", err)
	}

	// Update contract metadata
	if err := rm.ledger.UpdateSmartContract(contractID, string(newCode), args.Version); err != nil {
		return fmt.Errorf("failed to update contract metadata: %w", err)
	}

	rm.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"runtime":    runtimeType,
		"version":    args.Version,
	}).Info("Contract upgraded")

	return nil
}

// GetContractInfo retrieves information about a deployed contract
func (rm *RuntimeManager) GetContractInfo(contractID string) (*ContractInfo, error) {
	// Get contract runtime type
	runtimeType, err := rm.getContractRuntime(contractID)
	if err != nil {
		return nil, err
	}

	// Get runtime
	runtime, err := rm.getRuntime(runtimeType)
	if err != nil {
		return nil, err
	}

	// Get contract info from runtime
	return runtime.GetContractInfo(contractID)
}

// SetEventHandler sets the event handler for all runtimes
func (rm *RuntimeManager) SetEventHandler(handler RuntimeEventHandler) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.eventHandler = handler
}

// SetStateManager sets the state manager for all runtimes
func (rm *RuntimeManager) SetStateManager(manager RuntimeStateManager) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.stateManager = manager
}

// HealthCheck checks the health of all runtimes
func (rm *RuntimeManager) HealthCheck() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if !rm.initialized {
		return ErrRuntimeNotInitialized
	}

	var errors []error
	for runtimeType, runtime := range rm.runtimes {
		if err := runtime.HealthCheck(); err != nil {
			errors = append(errors, fmt.Errorf("runtime %s unhealthy: %w", runtimeType, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("health check failures: %v", errors)
	}

	return nil
}

// getRuntimeTypeFromLanguage determines the runtime type from contract language
func (rm *RuntimeManager) getRuntimeTypeFromLanguage(language string) (RuntimeType, error) {
	switch language {
	case "solidity", "vyper", "EVM", "evm":
		return RuntimeTypeEVM, nil
	case "go", "node", "chaincode", "fabric":
		return RuntimeTypeChaincode, nil
	case "native", "diamante":
		return RuntimeTypeNative, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidContractLanguage, language)
	}
}

// getContractRuntime determines the runtime type for a deployed contract
func (rm *RuntimeManager) getContractRuntime(contractID string) (RuntimeType, error) {
	// Try to get contract info from each runtime
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var lastError error
	for runtimeType, runtime := range rm.runtimes {
		info, err := runtime.GetContractInfo(contractID)
		if err != nil {
			// Log the error but continue checking other runtimes
			rm.logger.WithFields(logrus.Fields{
				"contractID": contractID,
				"runtime":    runtimeType,
			}).WithError(err).Debug("Failed to get contract info from runtime")
			lastError = err
			continue
		}

		if info != nil {
			rm.logger.WithFields(logrus.Fields{
				"contractID": contractID,
				"runtime":    runtimeType,
			}).Debug("Found contract in runtime")
			return runtimeType, nil
		}
	}

	// If we had errors while checking, include them in the error message
	if lastError != nil {
		return "", fmt.Errorf("%w: %s (last error: %v)", ErrContractNotFound, contractID, lastError)
	}

	return "", fmt.Errorf("%w: %s", ErrContractNotFound, contractID)
}

// getRuntime gets a runtime by type
func (rm *RuntimeManager) getRuntime(runtimeType RuntimeType) (Runtime, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	runtime, exists := rm.runtimes[runtimeType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrRuntimeNotFound, runtimeType)
	}

	return runtime, nil
}

// GetContractState retrieves the state of a contract
func (rm *RuntimeManager) GetContractState(contractID string, key string) (interface{}, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.stateManager == nil {
		return nil, errors.New("state manager not set")
	}

	// If key is empty, get all state
	if key == "" {
		return rm.stateManager.GetAllState(contractID)
	}

	// Get specific key
	return rm.stateManager.GetState(contractID, []byte(key))
}

// ListRuntimes returns a list of all registered runtime types
func (rm *RuntimeManager) ListRuntimes() []RuntimeType {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	runtimes := make([]RuntimeType, 0, len(rm.runtimes))
	for rt := range rm.runtimes {
		runtimes = append(runtimes, rt)
	}
	return runtimes
}

// IsRuntimeActive checks if a runtime is active
func (rm *RuntimeManager) IsRuntimeActive(runtimeType RuntimeType) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	runtime, exists := rm.runtimes[runtimeType]
	if !exists {
		return false
	}

	// Check if runtime is healthy
	if err := runtime.HealthCheck(); err != nil {
		return false
	}

	return rm.running
}

// HasRuntime checks if a runtime type is registered
func (rm *RuntimeManager) HasRuntime(runtimeType RuntimeType) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	_, exists := rm.runtimes[runtimeType]
	return exists
}

// String provides a detailed string representation for debugging and logging
func (rm *RuntimeManager) String() string {
	if rm == nil {
		return "RuntimeManager{<nil>}"
	}

	rm.mu.RLock()
	defer rm.mu.RUnlock()

	runtimeCount := len(rm.runtimes)
	configCount := len(rm.configs)
	hasLedger := rm.ledger != nil
	hasStateStore := rm.stateStore != nil
	hasLogger := rm.logger != nil
	hasEventHandler := rm.eventHandler != nil
	hasStateManager := rm.stateManager != nil

	// Get list of registered runtime types
	runtimeTypes := make([]string, 0, runtimeCount)
	for rType := range rm.runtimes {
		runtimeTypes = append(runtimeTypes, string(rType))
	}

	return fmt.Sprintf("RuntimeManager{initialized=%v, running=%v, runtimeCount=%d, configCount=%d, "+
		"hasLedger=%v, hasStateStore=%v, hasLogger=%v, hasEventHandler=%v, hasStateManager=%v, "+
		"runtimeTypes=%v}",
		rm.initialized, rm.running, runtimeCount, configCount, hasLedger, hasStateStore, hasLogger,
		hasEventHandler, hasStateManager, runtimeTypes)
}

// Validate performs comprehensive validation of the RuntimeManager configuration and state
func (rm *RuntimeManager) Validate() error {
	if rm == nil {
		return fmt.Errorf("RuntimeManager is nil")
	}

	// Validate core dependencies
	if err := rm.validateCoreDependencies(); err != nil {
		return fmt.Errorf("core dependencies validation failed: %w", err)
	}

	// Validate runtime registry consistency
	if err := rm.validateRuntimeRegistry(); err != nil {
		return fmt.Errorf("runtime registry validation failed: %w", err)
	}

	// Validate configurations
	if err := rm.validateConfigurations(); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Validate state consistency
	if err := rm.validateState(); err != nil {
		return fmt.Errorf("state validation failed: %w", err)
	}

	// If initialized, validate runtime health
	if rm.initialized {
		if err := rm.validateRuntimeHealth(); err != nil {
			return fmt.Errorf("runtime health validation failed: %w", err)
		}
	}

	return nil
}

// validateCoreDependencies validates that required dependencies are present
func (rm *RuntimeManager) validateCoreDependencies() error {
	if rm.ledger == nil {
		return fmt.Errorf("ledger API is nil")
	}

	if rm.stateStore == nil {
		return fmt.Errorf("state store is nil")
	}

	if rm.logger == nil {
		return fmt.Errorf("logger is nil")
	}

	// Validate maps are initialized
	if rm.runtimes == nil {
		return fmt.Errorf("runtimes map is nil")
	}

	if rm.configs == nil {
		return fmt.Errorf("configs map is nil")
	}

	return nil
}

// validateRuntimeRegistry validates runtime registry consistency
func (rm *RuntimeManager) validateRuntimeRegistry() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Check that each runtime has a corresponding configuration
	for runtimeType := range rm.runtimes {
		if _, exists := rm.configs[runtimeType]; !exists {
			return fmt.Errorf("runtime %s has no configuration", runtimeType)
		}
	}

	// Check that each configuration has a corresponding runtime
	for runtimeType := range rm.configs {
		if _, exists := rm.runtimes[runtimeType]; !exists {
			return fmt.Errorf("configuration for %s has no corresponding runtime", runtimeType)
		}
	}

	// Validate runtime types are known
	registry := GetGlobalRegistry()
	for runtimeType := range rm.runtimes {
		if !registry.IsRuntimeRegistered(runtimeType) {
			rm.logger.WithField("runtime", runtimeType).Warn("Runtime type not registered in global registry")
		}
	}

	return nil
}

// validateConfigurations validates all runtime configurations
func (rm *RuntimeManager) validateConfigurations() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for runtimeType, config := range rm.configs {
		if err := rm.validateSingleConfig(runtimeType, config); err != nil {
			return fmt.Errorf("validation failed for %s config: %w", runtimeType, err)
		}
	}

	return nil
}

// validateSingleConfig validates a single runtime configuration
func (rm *RuntimeManager) validateSingleConfig(runtimeType RuntimeType, config RuntimeConfig) error {
	// Validate ChainID
	if config.ChainID == "" {
		return fmt.Errorf("ChainID is empty")
	}

	// Validate core dependencies in config
	if config.LedgerAPI == nil {
		return fmt.Errorf("LedgerAPI is nil in config")
	}

	if config.StateStore == nil {
		return fmt.Errorf("StateStore is nil in config")
	}

	if config.Logger == nil {
		return fmt.Errorf("Logger is nil in config")
	}

	// Validate runtime-specific configurations
	switch runtimeType {
	case RuntimeTypeEVM:
		if config.RuntimeSpecific.EVMConfig == nil {
			return fmt.Errorf("EVM config is nil for EVM runtime")
		}
		// Additional EVM-specific validation could be added here

	case RuntimeTypeWASM:
		if config.RuntimeSpecific.WASMConfig == nil {
			return fmt.Errorf("WASM config is nil for WASM runtime")
		}
		// Additional WASM-specific validation could be added here

	case RuntimeTypeNative:
		if config.RuntimeSpecific.NativeConfig == nil {
			return fmt.Errorf("Native config is nil for Native runtime")
		}
		// Additional Native-specific validation could be added here
	}

	return nil
}

// validateState validates the current state consistency
func (rm *RuntimeManager) validateState() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// If not initialized, certain conditions should hold
	if !rm.initialized {
		if rm.running {
			return fmt.Errorf("manager reports as running but not initialized")
		}
	}

	// If initialized, validate state consistency
	if rm.initialized {
		if rm.eventHandler == nil {
			return fmt.Errorf("event handler is nil but manager is initialized")
		}

		if rm.stateManager == nil {
			return fmt.Errorf("state manager is nil but manager is initialized")
		}
	}

	return nil
}

// validateRuntimeHealth validates that all initialized runtimes are healthy
func (rm *RuntimeManager) validateRuntimeHealth() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var healthErrors []string

	for runtimeType, runtime := range rm.runtimes {
		if err := runtime.HealthCheck(); err != nil {
			healthErrors = append(healthErrors, fmt.Sprintf("%s: %v", runtimeType, err))
		}
	}

	if len(healthErrors) > 0 {
		return fmt.Errorf("runtime health check failures: %s", strings.Join(healthErrors, "; "))
	}

	return nil
}

// Close properly shuts down the RuntimeManager and releases all resources
func (rm *RuntimeManager) Close() error {
	if rm == nil {
		return nil
	}

	// Stop if running
	if err := rm.Stop(); err != nil {
		return fmt.Errorf("failed to stop runtime manager: %w", err)
	}

	// Close all components in reverse dependency order
	var closeErrors []error

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Close state manager
	if rm.stateManager != nil {
		if closer, ok := rm.stateManager.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close state manager: %w", err))
			}
		}
	}

	// Close event handler
	if rm.eventHandler != nil {
		if closer, ok := rm.eventHandler.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close event handler: %w", err))
			}
		}
	}

	// Close all runtimes
	for runtimeType, runtime := range rm.runtimes {
		if err := runtime.Stop(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("failed to stop runtime %s: %w", runtimeType, err))
		}

		// Close runtime if it has a Close method
		if closer, ok := runtime.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close runtime %s: %w", runtimeType, err))
			}
		}
	}

	// Close state store if it has a Close method
	if rm.stateStore != nil {
		if closer, ok := rm.stateStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close state store: %w", err))
			}
		}
	}

	// Clear all references to help with garbage collection
	rm.runtimes = nil
	rm.configs = nil
	rm.ledger = nil
	rm.stateStore = nil
	rm.logger = nil
	rm.eventHandler = nil
	rm.stateManager = nil
	rm.initialized = false
	rm.running = false

	// If there were any errors during close, return them
	if len(closeErrors) > 0 {
		errorStrs := make([]string, len(closeErrors))
		for i, err := range closeErrors {
			errorStrs[i] = err.Error()
		}
		return fmt.Errorf("multiple close errors: %s", strings.Join(errorStrs, "; "))
	}

	return nil
}

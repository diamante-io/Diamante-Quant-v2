// Package runtime provides centralized runtime initialization for the Diamante blockchain
package runtime

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// InitializeRuntimeRegistry initializes and registers all available runtimes
// This function should be called during application startup before any runtime usage
func InitializeRuntimeRegistry(logger *logrus.Logger) error {
	if logger == nil {
		logger = logrus.New()
	}

	logger.Info("Initializing runtime registry...")

	// Register EVM Runtime
	if err := registerEVMRuntime(logger); err != nil {
		return err
	}

	// Register Native Runtime
	if err := registerNativeRuntime(logger); err != nil {
		return err
	}

	// Register Chaincode Runtime
	if err := registerChaincodeRuntime(logger); err != nil {
		return err
	}

	// Log registry statistics
	registry := GetGlobalRegistry()
	stats := registry.GetRegistryStats()
	logger.WithFields(logrus.Fields{
		"total_runtimes": stats.TotalRuntimes,
		"runtimes":       ListRegisteredRuntimes(),
	}).Info("Runtime registry initialized successfully")

	return nil
}

// registerEVMRuntime registers the EVM runtime with the global registry
func registerEVMRuntime(logger *logrus.Logger) error {
	logger.Info("Registering EVM runtime...")

	// The actual runtime factory should be registered by the EVM package itself
	// This just registers the metadata and validation rules

	// Register metadata
	RegisterRuntimeMetadata(RuntimeTypeEVM, RuntimeMetadata{
		Name:        "Ethereum Virtual Machine",
		Description: "EVM-compatible runtime for executing Ethereum smart contracts",
		Version:     "1.0.0",
		Author:      "Diamante Team",
		License:     "MIT",
		Repository:  "https://github.com/diamante/diamante",
		Capabilities: []RuntimeCapability{
			CapabilitySmartContracts,
			CapabilityStateManagement,
			CapabilityEventEmission,
			CapabilityDeterministic,
			CapabilityGasMetering,
		},
	})

	// Register validation rule for EVM
	registry := GetGlobalRegistry()
	validationRule := func(config RuntimeConfig) error {
		// EVM-specific validation
		if config.RuntimeSpecific.IsEmpty() {
			return fmt.Errorf("EVM requires runtime-specific configuration")
		}

		evmConfig := config.RuntimeSpecific.EVMConfig
		if evmConfig == nil {
			return fmt.Errorf("EVM configuration not found in runtime-specific config")
		}

		// Validate EVM config fields
		if evmConfig.ChainID == 0 {
			return fmt.Errorf("EVM requires valid chain ID")
		}

		if evmConfig.GasLimit == 0 {
			return fmt.Errorf("EVM requires gas limit to be set")
		}

		return nil
	}

	if err := registry.RegisterValidationRule(RuntimeTypeEVM, validationRule); err != nil {
		return fmt.Errorf("failed to register EVM validation rule: %w", err)
	}

	// Register health check for EVM
	healthCheck := func(runtime Runtime) error {
		// EVM-specific health check
		if runtime.Type() != RuntimeTypeEVM {
			return fmt.Errorf("invalid runtime type for EVM health check")
		}

		// Additional EVM-specific checks can be added here
		return nil
	}

	if err := registry.RegisterHealthCheck(RuntimeTypeEVM, healthCheck); err != nil {
		return fmt.Errorf("failed to register EVM health check: %w", err)
	}

	logger.Info("EVM runtime registered successfully")
	return nil
}

// registerNativeRuntime registers the Native runtime with the global registry
func registerNativeRuntime(logger *logrus.Logger) error {
	logger.Info("Registering Native runtime...")

	// The actual runtime factory should be registered by the Native package itself
	// This just registers the metadata and validation rules

	// Register metadata
	RegisterRuntimeMetadata(RuntimeTypeNative, RuntimeMetadata{
		Name:        "Diamante Native Runtime",
		Description: "High-performance native runtime for Diamante smart contracts",
		Version:     "1.0.0",
		Author:      "Diamante Team",
		License:     "MIT",
		Repository:  "https://github.com/diamante/diamante",
		Capabilities: []RuntimeCapability{
			CapabilitySmartContracts,
			CapabilityStateManagement,
			CapabilityEventEmission,
			CapabilityUpgradeable,
			CapabilityDeterministic,
			CapabilityGasMetering,
			CapabilityAsyncExecution,
		},
	})

	// Register validation rule for Native
	registry := GetGlobalRegistry()
	validationRule := func(config RuntimeConfig) error {
		// Native-specific validation
		if !config.RuntimeSpecific.IsEmpty() {
			nativeConfig := config.RuntimeSpecific.NativeConfig
			if nativeConfig != nil {
				// Validate plugin path if specified
				if nativeConfig.PluginPath == "" {
					return fmt.Errorf("Native plugin path cannot be empty when config is provided")
				}

				// Validate max plugins
				if nativeConfig.MaxPlugins < 0 {
					return fmt.Errorf("Native max plugins cannot be negative")
				}
			}
		}

		return nil
	}

	if err := registry.RegisterValidationRule(RuntimeTypeNative, validationRule); err != nil {
		return fmt.Errorf("failed to register Native validation rule: %w", err)
	}

	// Register health check for Native
	healthCheck := func(runtime Runtime) error {
		// Native-specific health check
		if runtime.Type() != RuntimeTypeNative {
			return fmt.Errorf("invalid runtime type for Native health check")
		}

		// Additional Native-specific checks can be added here
		return nil
	}

	if err := registry.RegisterHealthCheck(RuntimeTypeNative, healthCheck); err != nil {
		return fmt.Errorf("failed to register Native health check: %w", err)
	}

	logger.Info("Native runtime registered successfully")
	return nil
}

// registerChaincodeRuntime registers the Chaincode runtime with the global registry
func registerChaincodeRuntime(logger *logrus.Logger) error {
	logger.Info("Registering Chaincode runtime...")

	// The actual runtime factory should be registered by the Chaincode package itself
	// This just registers the metadata and validation rules

	// Register metadata
	RegisterRuntimeMetadata(RuntimeTypeChaincode, RuntimeMetadata{
		Name:        "Hyperledger Fabric Chaincode",
		Description: "Runtime for executing Hyperledger Fabric chaincode in Go, Node.js, and Java",
		Version:     "1.0.0",
		Author:      "Diamante Team",
		License:     "MIT",
		Repository:  "https://github.com/diamante/diamante",
		Capabilities: []RuntimeCapability{
			CapabilitySmartContracts,
			CapabilityStateManagement,
			CapabilityEventEmission,
			CapabilityUpgradeable,
			CapabilityDeterministic,
		},
	})

	// Register validation rule for Chaincode
	registry := GetGlobalRegistry()
	validationRule := func(config RuntimeConfig) error {
		// Chaincode-specific validation
		if !config.RuntimeSpecific.IsEmpty() {
			chaincodeConfig := config.RuntimeSpecific.ChaincodeConfig
			if chaincodeConfig != nil {
				// Check Docker endpoint if specified
				if chaincodeConfig.DockerEndpoint == "" {
					return fmt.Errorf("docker endpoint cannot be empty")
				}

				// Check max containers
				if chaincodeConfig.MaxContainers <= 0 {
					return fmt.Errorf("maxContainers must be positive")
				}

				// Check language
				validLanguages := map[string]bool{
					"go":         true,
					"node":       true,
					"javascript": true,
					"java":       true,
				}
				if !validLanguages[chaincodeConfig.Language] {
					return fmt.Errorf("unsupported chaincode language: %s", chaincodeConfig.Language)
				}
			}
		}

		return nil
	}

	if err := registry.RegisterValidationRule(RuntimeTypeChaincode, validationRule); err != nil {
		return fmt.Errorf("failed to register Chaincode validation rule: %w", err)
	}

	// Register health check for Chaincode
	healthCheck := func(runtime Runtime) error {
		// Chaincode-specific health check
		if runtime.Type() != RuntimeTypeChaincode {
			return fmt.Errorf("invalid runtime type for Chaincode health check")
		}

		// In production, this would check Docker daemon connectivity
		// Additional Chaincode-specific checks can be added here
		return nil
	}

	if err := registry.RegisterHealthCheck(RuntimeTypeChaincode, healthCheck); err != nil {
		return fmt.Errorf("failed to register Chaincode health check: %w", err)
	}

	logger.Info("Chaincode runtime registered successfully")
	return nil
}

// GetRuntimeCapabilities returns a map of all runtimes and their capabilities
func GetRuntimeCapabilities() map[RuntimeType][]RuntimeCapability {
	registry := GetGlobalRegistry()
	runtimes := registry.ListRegisteredRuntimes()

	capabilities := make(map[RuntimeType][]RuntimeCapability)

	for _, rt := range runtimes {
		metadata, exists := registry.GetRuntimeMetadata(rt)
		if exists {
			capabilities[rt] = metadata.Capabilities
		}
	}

	return capabilities
}

// ValidateRuntimeConfiguration validates configuration for all registered runtimes
func ValidateRuntimeConfiguration(config map[RuntimeType]RuntimeConfig) error {
	registry := GetGlobalRegistry()

	for runtimeType, runtimeConfig := range config {
		if err := registry.ValidateConfig(runtimeType, runtimeConfig); err != nil {
			return fmt.Errorf("validation failed for runtime %s: %w", runtimeType, err)
		}
	}

	return nil
}

// PerformHealthChecks performs health checks on all active runtimes
func PerformHealthChecks(manager *RuntimeManager) error {
	// Use the manager's built-in health check which handles all runtimes
	return manager.HealthCheck()
}

// RuntimeInitializationOptions provides options for runtime initialization
type RuntimeInitializationOptions struct {
	EnableEVM       bool
	EnableNative    bool
	EnableChaincode bool
	Logger          *logrus.Logger
}

// InitializeSelectedRuntimes initializes only selected runtimes based on options
func InitializeSelectedRuntimes(options RuntimeInitializationOptions) error {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}

	options.Logger.Info("Initializing selected runtimes...")

	initialized := 0

	if options.EnableEVM {
		if err := registerEVMRuntime(options.Logger); err != nil {
			return fmt.Errorf("failed to register EVM runtime: %w", err)
		}
		initialized++
	}

	if options.EnableNative {
		if err := registerNativeRuntime(options.Logger); err != nil {
			return fmt.Errorf("failed to register Native runtime: %w", err)
		}
		initialized++
	}

	if options.EnableChaincode {
		if err := registerChaincodeRuntime(options.Logger); err != nil {
			return fmt.Errorf("failed to register Chaincode runtime: %w", err)
		}
		initialized++
	}

	if initialized == 0 {
		return fmt.Errorf("no runtimes were initialized")
	}

	options.Logger.WithField("initialized", initialized).Info("Selected runtimes initialized successfully")
	return nil
}

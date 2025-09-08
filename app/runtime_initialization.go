// Package app provides application initialization and integration
package app

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"
	"diamante/ledger/evm"
	"diamante/storage"
	"diamante/transaction"
	"diamante/vm/chaincode"
	"diamante/vm/deploy"
	"diamante/vm/native"
	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// ExtendedHybridVMConfig extends the base HybridVMConfig with runtime-specific settings
type ExtendedHybridVMConfig struct {
	*HybridVMConfig // Embed the existing config

	// EVM-specific Configuration
	EVMChainID     *big.Int
	EVMGasLimit    uint64
	EVMBaseFee     *big.Int
	EVMMaxCodeSize uint64

	// Chaincode-specific Configuration
	DockerEndpoint string
	MaxContainers  int

	// Native-specific Configuration
	PluginDir       string
	WASMEnabled     bool
	MaxPluginMemory uint64

	// General Configuration
	EnableMetrics   bool
	EnableProfiling bool
	MaxConcurrency  int
}

// DefaultExtendedHybridVMConfig returns default configuration for production
func DefaultExtendedHybridVMConfig() *ExtendedHybridVMConfig {
	return &ExtendedHybridVMConfig{
		HybridVMConfig: DefaultHybridVMConfig(), // Use existing default config

		// EVM defaults
		EVMChainID:     big.NewInt(1337), // Default local chain ID
		EVMGasLimit:    30000000,
		EVMBaseFee:     big.NewInt(1000000000), // 1 Gwei
		EVMMaxCodeSize: 24576,

		// Chaincode defaults
		DockerEndpoint: "unix:///var/run/docker.sock",
		MaxContainers:  10,

		// Native defaults
		PluginDir:       "/opt/diamante/plugins",
		WASMEnabled:     true,
		MaxPluginMemory: 512 * 1024 * 1024, // 512MB

		// General defaults
		EnableMetrics:   true,
		EnableProfiling: false,
		MaxConcurrency:  100,
	}
}

// HybridVMSystem contains all components of the Hybrid VM
type HybridVMSystem struct {
	RuntimeManager    *runtime.RuntimeManager
	DeploymentManager *deploy.DeploymentManager
	TxProcessor       *transaction.HybridTransactionProcessor
	EventBus          *runtime.UnifiedEventBus
	ResourceManager   *runtime.ResourceManager
}

// InitializeHybridVM initializes the complete Hybrid VM system
func InitializeHybridVM(
	ledger common.LedgerAPI,
	stateStore storage.LedgerStore,
	logger *logrus.Logger,
	config *ExtendedHybridVMConfig,
) (*HybridVMSystem, error) {
	if ledger == nil {
		return nil, errors.New("ledger API cannot be nil")
	}
	if stateStore == nil {
		return nil, errors.New("state store cannot be nil")
	}
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}
	if config == nil {
		config = DefaultExtendedHybridVMConfig()
	}

	logger.Info("Initializing Hybrid VM system")

	// Initialize crypto time provider with consensus time
	// This fixes the critical time bug where crypto operations were using fixed epoch time
	cryptoTimeProvider := consensus.NewCryptoTimeProvider()
	crypto.SetTimeProvider(cryptoTimeProvider)
	logger.Info("Crypto time provider initialized with consensus time")

	// Create RuntimeManager
	runtimeManager := runtime.NewRuntimeManager(ledger, stateStore, logger)

	// Initialize and register EVM Runtime
	if err := registerEVMRuntime(runtimeManager, config, logger); err != nil {
		return nil, fmt.Errorf("failed to register EVM runtime: %w", err)
	}

	// Initialize and register Chaincode Runtime
	if err := registerChaincodeRuntime(runtimeManager, config, logger); err != nil {
		return nil, fmt.Errorf("failed to register Chaincode runtime: %w", err)
	}

	// Initialize and register Native Runtime
	if err := registerNativeRuntime(runtimeManager, config, logger); err != nil {
		return nil, fmt.Errorf("failed to register Native runtime: %w", err)
	}

	// Initialize the RuntimeManager with all runtimes
	if err := runtimeManager.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize runtime manager: %w", err)
	}

	// Start all runtimes
	if err := runtimeManager.Start(); err != nil {
		return nil, fmt.Errorf("failed to start runtime manager: %w", err)
	}

	// Create unified event bus for cross-runtime communication
	eventBus := runtime.NewUnifiedEventBus(logger)
	runtimeManager.SetEventHandler(eventBus)

	// Create resource manager for runtime resource control
	resourceManager := runtime.NewResourceManager(config.MaxConcurrency, logger)

	// Create deployment manager
	deploymentManager := deploy.NewDeploymentManager(runtimeManager, stateStore, logger)

	// Create transaction processor with all components
	txProcessor := transaction.NewHybridTransactionProcessor(
		runtimeManager,
		deploymentManager,
		ledger,
		stateStore,
		logger,
	)

	// Verify all components are healthy
	if err := verifySystemHealth(runtimeManager, deploymentManager); err != nil {
		return nil, fmt.Errorf("system health check failed: %w", err)
	}

	logger.Info("Hybrid VM system initialized successfully",
		"evmChainID", config.EVMChainID,
		"chaincodeTimeout", config.DeploymentTimeout,
		"nativePluginDir", config.PluginDir,
	)

	return &HybridVMSystem{
		RuntimeManager:    runtimeManager,
		DeploymentManager: deploymentManager,
		TxProcessor:       txProcessor,
		EventBus:          eventBus,
		ResourceManager:   resourceManager,
	}, nil
}

// registerEVMRuntime registers the EVM runtime with production configuration
func registerEVMRuntime(rm *runtime.RuntimeManager, config *ExtendedHybridVMConfig, logger *logrus.Logger) error {
	logger.Info("Registering EVM runtime")

	// Create EVM runtime instance
	evmRuntime := evm.NewProductionEVMRuntime()

	// Create EVM-specific configuration
	evmConfig := runtime.RuntimeConfig{
		RuntimeSpecific: runtime.RuntimeSpecificConfig{
			EVMConfig: &runtime.EVMConfig{
				ChainID:             config.EVMChainID.Uint64(),
				GasLimit:            config.EVMGasLimit,
				GasPrice:            config.EVMBaseFee.Uint64(),
				EnableOptimizations: true,
				DebugMode:           false,
			},
		},
	}

	// Register with runtime manager
	if err := rm.RegisterRuntime(runtime.RuntimeTypeEVM, evmRuntime, evmConfig); err != nil {
		return fmt.Errorf("failed to register EVM runtime: %w", err)
	}

	logger.Info("EVM runtime registered successfully")
	return nil
}

// registerChaincodeRuntime registers the Chaincode runtime with production configuration
func registerChaincodeRuntime(rm *runtime.RuntimeManager, config *ExtendedHybridVMConfig, logger *logrus.Logger) error {
	logger.Info("Registering Chaincode runtime")

	// Verify Docker is available
	if err := verifyDockerAccess(config.DockerEndpoint); err != nil {
		return fmt.Errorf("Docker verification failed: %w", err)
	}

	// Create Chaincode runtime instance
	chaincodeRuntime := chaincode.NewChaincodeRuntime()

	// Create Chaincode-specific configuration
	chaincodeConfig := runtime.RuntimeConfig{
		RuntimeSpecific: runtime.RuntimeSpecificConfig{
			ChaincodeConfig: &runtime.ChaincodeConfig{
				Language:         "golang", // Default to Go
				DockerEndpoint:   config.DockerEndpoint,
				NetworkMode:      "bridge",
				MaxContainers:    config.MaxContainers,
				ContainerTimeout: config.DeploymentTimeout,
				BuildTimeout:     config.DeploymentTimeout,
			},
		},
	}

	// Register with runtime manager
	if err := rm.RegisterRuntime(runtime.RuntimeTypeChaincode, chaincodeRuntime, chaincodeConfig); err != nil {
		return fmt.Errorf("failed to register Chaincode runtime: %w", err)
	}

	logger.Info("Chaincode runtime registered successfully")
	return nil
}

// registerNativeRuntime registers the Native runtime with production configuration
func registerNativeRuntime(rm *runtime.RuntimeManager, config *ExtendedHybridVMConfig, logger *logrus.Logger) error {
	logger.Info("Registering Native runtime")

	// Ensure plugin directory exists
	if err := os.MkdirAll(config.PluginDir, 0755); err != nil {
		return fmt.Errorf("failed to create plugin directory: %w", err)
	}

	// Create Native runtime instance
	nativeRuntime := native.NewNativeRuntime()

	// Create Native-specific configuration
	nativeConfig := runtime.RuntimeConfig{
		RuntimeSpecific: runtime.RuntimeSpecificConfig{
			NativeConfig: &runtime.NativeConfig{
				PluginPath:    config.PluginDir,
				EnableJIT:     true,
				EnableSandbox: true,
				MaxPlugins:    100,
				PluginTimeout: 30 * time.Second,
			},
		},
	}

	// Register with runtime manager
	if err := rm.RegisterRuntime(runtime.RuntimeTypeNative, nativeRuntime, nativeConfig); err != nil {
		return fmt.Errorf("failed to register Native runtime: %w", err)
	}

	logger.Info("Native runtime registered successfully")
	return nil
}

// verifyDockerAccess checks if Docker daemon is accessible
func verifyDockerAccess(endpoint string) error {
	// For Unix socket
	if endpoint == "unix:///var/run/docker.sock" {
		if _, err := os.Stat("/var/run/docker.sock"); err != nil {
			return fmt.Errorf("Docker socket not accessible: %w", err)
		}
	}
	// Additional Docker connectivity checks could be added here
	return nil
}

// getDefaultAllowedSyscalls returns the default set of allowed syscalls for native contracts
func getDefaultAllowedSyscalls() []string {
	return []string{
		"read", "write", "open", "close",
		"mmap", "munmap", "brk",
		"futex", "nanosleep", "clock_gettime",
		"getpid", "gettid", "exit_group",
	}
}

// verifySystemHealth performs health checks on all components
func verifySystemHealth(rm *runtime.RuntimeManager, dm *deploy.DeploymentManager) error {
	// Check runtime manager health
	if err := rm.HealthCheck(); err != nil {
		return fmt.Errorf("runtime manager health check failed: %w", err)
	}

	// Verify all runtimes are active
	for _, rt := range []runtime.RuntimeType{runtime.RuntimeTypeEVM, runtime.RuntimeTypeChaincode, runtime.RuntimeTypeNative} {
		if !rm.IsRuntimeActive(rt) {
			return fmt.Errorf("runtime %s is not active", rt)
		}
	}

	// Verify deployment manager is ready
	if dm == nil {
		return errors.New("deployment manager is nil")
	}

	return nil
}

// Shutdown gracefully shuts down the Hybrid VM system
func (hvs *HybridVMSystem) Shutdown(ctx context.Context) error {
	logger := logrus.WithField("component", "hybrid_vm_shutdown")
	logger.Info("Shutting down Hybrid VM system")

	// Create shutdown context with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Stop transaction processor first to prevent new transactions
	// (TxProcessor doesn't have a Stop method in current implementation, but should)

	// Stop deployment manager
	// (DeploymentManager doesn't have a Stop method in current implementation, but should)

	// Stop event bus
	if hvs.EventBus != nil {
		if err := hvs.EventBus.Stop(); err != nil {
			logger.WithError(err).Error("Failed to stop event bus")
		}
	}

	// Stop resource manager
	if hvs.ResourceManager != nil {
		if err := hvs.ResourceManager.Stop(); err != nil {
			logger.WithError(err).Error("Failed to stop resource manager")
		}
	}

	// Stop runtime manager (this stops all runtimes)
	if hvs.RuntimeManager != nil {
		if err := hvs.RuntimeManager.Stop(); err != nil {
			logger.WithError(err).Error("Failed to stop runtime manager")
			return err
		}
	}

	// Wait for context or timeout
	select {
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timeout exceeded")
	default:
		logger.Info("Hybrid VM system shutdown complete")
		return nil
	}
}

// GetMetrics returns current metrics from all components
func (hvs *HybridVMSystem) GetMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})

	// Get transaction processor metrics
	if hvs.TxProcessor != nil {
		txMetrics := hvs.TxProcessor.GetMetrics()
		metrics["transactions"] = map[string]interface{}{
			"total_processed":      txMetrics.TotalProcessed,
			"total_failed":         txMetrics.TotalFailed,
			"average_gas_used":     txMetrics.AverageGasUsed,
			"average_process_time": txMetrics.AverageProcessTime.String(),
			"runtime_distribution": txMetrics.RuntimeDistribution,
		}
	}

	// Get runtime health status
	if hvs.RuntimeManager != nil {
		runtimeStatus := make(map[string]bool)
		for _, rt := range hvs.RuntimeManager.ListRuntimes() {
			runtimeStatus[string(rt)] = hvs.RuntimeManager.IsRuntimeActive(rt)
		}
		metrics["runtime_status"] = runtimeStatus
	}

	// Get event bus metrics
	if hvs.EventBus != nil {
		metrics["event_bus"] = hvs.EventBus.GetMetrics()
	}

	// Get resource manager metrics
	if hvs.ResourceManager != nil {
		metrics["resources"] = hvs.ResourceManager.GetMetrics()
	}

	return metrics
}

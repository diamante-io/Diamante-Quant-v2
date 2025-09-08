// Package native provides WASM integration for the Diamante blockchain
package native

import (
	"fmt"
	"sync"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// WASMIntegration provides a simple interface for WASM contract execution
type WASMIntegration struct {
	runtime *WASMRuntime
	logger  *logrus.Logger
	mu      sync.RWMutex
	started bool
}

// WASMContract represents a deployed WASM contract
type WASMContract struct {
	ID         string
	Code       []byte
	DeployedAt int64
	Version    string
}

// WASMCallResult represents the result of a WASM contract call
type WASMCallResult struct {
	Success    bool
	Results    []uint64
	Error      string
	GasUsed    uint64
	ExecTimeMs int64
}

// NewWASMIntegration creates a new WASM integration instance
func NewWASMIntegration(logger *logrus.Logger) *WASMIntegration {
	if logger == nil {
		logger = logrus.New()
	}

	config := DefaultWASMConfig()
	config.EnableDebug = true // Enable debug for development

	return &WASMIntegration{
		runtime: NewWASMRuntime(logger, config),
		logger:  logger,
	}
}

// Start initializes the WASM integration
func (wi *WASMIntegration) Start() error {
	wi.mu.Lock()
	defer wi.mu.Unlock()

	if wi.started {
		return fmt.Errorf("WASM integration already started")
	}

	wi.logger.Info("Starting WASM integration")
	wi.started = true
	return nil
}

// Stop shuts down the WASM integration
func (wi *WASMIntegration) Stop() error {
	wi.mu.Lock()
	defer wi.mu.Unlock()

	if !wi.started {
		return nil
	}

	wi.logger.Info("Stopping WASM integration")
	wi.runtime.Close()
	wi.started = false
	return nil
}

// DeployContract deploys a WASM contract
func (wi *WASMIntegration) DeployContract(contractID string, wasmCode []byte) (*WASMContract, error) {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return nil, fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	wi.logger.WithFields(logrus.Fields{
		"contract_id": contractID,
		"code_size":   len(wasmCode),
	}).Info("Deploying WASM contract")

	// Load the WASM module
	err := wi.runtime.LoadWASM(contractID, wasmCode)
	if err != nil {
		return nil, fmt.Errorf("failed to deploy WASM contract: %w", err)
	}

	contract := &WASMContract{
		ID:         contractID,
		Code:       wasmCode,
		DeployedAt: consensus.ConsensusUnix(),
		Version:    "1.0.0",
	}

	wi.logger.WithField("contract_id", contractID).Info("WASM contract deployed successfully")
	return contract, nil
}

// CallContract executes a function on a deployed WASM contract
func (wi *WASMIntegration) CallContract(contractID, function string, args []uint64) (*WASMCallResult, error) {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return nil, fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	wi.logger.WithFields(logrus.Fields{
		"contract_id": contractID,
		"function":    function,
		"args_count":  len(args),
	}).Info("Calling WASM contract function")

	start := consensus.ConsensusUnixNano()

	// Execute the function
	results, err := wi.runtime.ExecuteWASM(contractID, function, args)

	execTimeMs := (consensus.ConsensusUnixNano() - start) / 1000000

	if err != nil {
		wi.logger.WithError(err).WithFields(logrus.Fields{
			"contract_id": contractID,
			"function":    function,
		}).Error("WASM contract call failed")

		return &WASMCallResult{
			Success:    false,
			Error:      err.Error(),
			GasUsed:    100, // Minimal gas for failed call
			ExecTimeMs: execTimeMs,
		}, nil
	}

	wi.logger.WithFields(logrus.Fields{
		"contract_id":   contractID,
		"function":      function,
		"results_count": len(results),
		"exec_time_ms":  execTimeMs,
	}).Info("WASM contract call completed successfully")

	return &WASMCallResult{
		Success:    true,
		Results:    results,
		GasUsed:    1000 + uint64(execTimeMs), // Simple gas calculation
		ExecTimeMs: execTimeMs,
	}, nil
}

// CallContractWithMemory executes a function with memory input/output
func (wi *WASMIntegration) CallContractWithMemory(contractID, function string, args []uint64, inputData []byte) (*WASMCallResult, []byte, error) {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return nil, nil, fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	wi.logger.WithFields(logrus.Fields{
		"contract_id":     contractID,
		"function":        function,
		"args_count":      len(args),
		"input_data_size": len(inputData),
	}).Info("Calling WASM contract function with memory")

	start := consensus.ConsensusUnixNano()

	// Execute the function with memory
	results, outputData, err := wi.runtime.ExecuteWASMWithMemory(contractID, function, args, inputData)

	execTimeMs := (consensus.ConsensusUnixNano() - start) / 1000000

	if err != nil {
		wi.logger.WithError(err).WithFields(logrus.Fields{
			"contract_id": contractID,
			"function":    function,
		}).Error("WASM contract call with memory failed")

		return &WASMCallResult{
			Success:    false,
			Error:      err.Error(),
			GasUsed:    100, // Minimal gas for failed call
			ExecTimeMs: execTimeMs,
		}, nil, nil
	}

	wi.logger.WithFields(logrus.Fields{
		"contract_id":      contractID,
		"function":         function,
		"results_count":    len(results),
		"output_data_size": len(outputData),
		"exec_time_ms":     execTimeMs,
	}).Info("WASM contract call with memory completed successfully")

	return &WASMCallResult{
		Success:    true,
		Results:    results,
		GasUsed:    1000 + uint64(execTimeMs) + uint64(len(inputData))/10, // Gas calculation including data size
		ExecTimeMs: execTimeMs,
	}, outputData, nil
}

// UnloadContract removes a contract from memory
func (wi *WASMIntegration) UnloadContract(contractID string) error {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	wi.logger.WithField("contract_id", contractID).Info("Unloading WASM contract")

	err := wi.runtime.UnloadWASM(contractID)
	if err != nil {
		return fmt.Errorf("failed to unload WASM contract: %w", err)
	}

	wi.logger.WithField("contract_id", contractID).Info("WASM contract unloaded successfully")
	return nil
}

// ListContracts returns information about loaded contracts
func (wi *WASMIntegration) ListContracts() ([]string, error) {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return nil, fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	// Get list of loaded modules
	wi.runtime.mu.RLock()
	contractIDs := make([]string, 0, len(wi.runtime.modules))
	for contractID := range wi.runtime.modules {
		contractIDs = append(contractIDs, contractID)
	}
	wi.runtime.mu.RUnlock()

	return contractIDs, nil
}

// GetContractInfo returns information about a specific contract
func (wi *WASMIntegration) GetContractInfo(contractID string) (*WASMModuleInfo, error) {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return nil, fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	return wi.runtime.GetModuleInfo(contractID)
}

// HealthCheck verifies that the WASM integration is working properly
func (wi *WASMIntegration) HealthCheck() error {
	wi.mu.RLock()
	if !wi.started {
		wi.mu.RUnlock()
		return fmt.Errorf("WASM integration not started")
	}
	wi.mu.RUnlock()

	// Create a complete minimal WASM module for testing
	minimalWASM := []byte{
		0x00, 0x61, 0x73, 0x6D, // WASM magic number
		0x01, 0x00, 0x00, 0x00, // WASM version
		0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // Type section: function type () -> ()
		0x03, 0x02, 0x01, 0x00, // Function section: one function of type 0
		0x05, 0x03, 0x01, 0x00, 0x01, // Memory section: min 1 page
		0x07, 0x08, 0x01, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, // Export section: export "test" as function 0
		0x0A, 0x04, 0x01, 0x02, 0x00, 0x0B, // Code section: function body (empty)
	}

	testID := "health-check-test"

	// Try to load and unload a minimal module
	err := wi.runtime.LoadWASM(testID, minimalWASM)
	if err != nil {
		return fmt.Errorf("WASM health check failed: %w", err)
	}

	// Clean up
	wi.runtime.UnloadWASM(testID)

	wi.logger.Info("WASM integration health check passed")
	return nil
}

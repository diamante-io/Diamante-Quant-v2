// Package native provides WASM integration tests and examples
package native

import (
	"testing"

	"github.com/sirupsen/logrus"
)

// TestWASMIntegration demonstrates basic WASM functionality
func TestWASMIntegration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Create WASM integration
	wasmIntegration := NewWASMIntegration(logger)

	// Start the integration
	err := wasmIntegration.Start()
	if err != nil {
		t.Fatalf("Failed to start WASM integration: %v", err)
	}
	defer wasmIntegration.Stop()

	// Test with a complete minimal WASM module that actually loads
	minimalWASM := []byte{
		0x00, 0x61, 0x73, 0x6D, // WASM magic number
		0x01, 0x00, 0x00, 0x00, // WASM version
		0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // Type section: function type () -> ()
		0x03, 0x02, 0x01, 0x00, // Function section: one function of type 0
		0x05, 0x03, 0x01, 0x00, 0x01, // Memory section: min 1 page
		0x07, 0x08, 0x01, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, // Export section: export "test" as function 0
		0x0A, 0x04, 0x01, 0x02, 0x00, 0x0B, // Code section: function body (empty)
	}

	contractID := "test-contract"

	// Deploy the contract
	contract, err := wasmIntegration.DeployContract(contractID, minimalWASM)
	if err != nil {
		t.Fatalf("Failed to deploy WASM contract: %v", err)
	}

	t.Logf("Contract deployed: %+v", contract)

	// Get contract info before health check
	info, err := wasmIntegration.GetContractInfo(contractID)
	if err != nil {
		t.Fatalf("Failed to get contract info: %v", err)
	}

	t.Logf("Contract info: %+v", info)

	// List contracts
	contracts, err := wasmIntegration.ListContracts()
	if err != nil {
		t.Fatalf("Failed to list contracts: %v", err)
	}

	t.Logf("Loaded contracts: %v", contracts)

	// Test that we can call the empty function now that we have a complete module
	result, err := wasmIntegration.CallContract(contractID, "test", []uint64{})
	if err != nil {
		t.Fatalf("Failed to call contract function: %v", err)
	}

	t.Logf("Function call result: %+v", result)

	// Test health check
	err = wasmIntegration.HealthCheck()
	if err != nil {
		t.Fatalf("WASM health check failed: %v", err)
	}

	// Unload the contract
	err = wasmIntegration.UnloadContract(contractID)
	if err != nil {
		t.Fatalf("Failed to unload contract: %v", err)
	}

	t.Log("WASM integration test completed successfully")
}

// TestWASMRuntimeDirect tests the underlying WASM runtime directly
func TestWASMRuntimeDirect(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	config := DefaultWASMConfig()
	config.EnableDebug = true

	runtime := NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Test loading a complete minimal WASM module
	minimalWASM := []byte{
		0x00, 0x61, 0x73, 0x6D, // WASM magic number
		0x01, 0x00, 0x00, 0x00, // WASM version
		0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // Type section: function type () -> ()
		0x03, 0x02, 0x01, 0x00, // Function section: one function of type 0
		0x05, 0x03, 0x01, 0x00, 0x01, // Memory section: min 1 page
		0x07, 0x08, 0x01, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, // Export section: export "test" as function 0
		0x0A, 0x04, 0x01, 0x02, 0x00, 0x0B, // Code section: function body (empty)
	}

	contractID := "direct-test"

	err := runtime.LoadWASM(contractID, minimalWASM)
	if err != nil {
		t.Fatalf("Failed to load WASM module: %v", err)
	}

	// Get module info
	info, err := runtime.GetModuleInfo(contractID)
	if err != nil {
		t.Fatalf("Failed to get module info: %v", err)
	}

	t.Logf("Module info: %+v", info)

	// Unload the module
	err = runtime.UnloadWASM(contractID)
	if err != nil {
		t.Fatalf("Failed to unload WASM module: %v", err)
	}

	t.Log("Direct WASM runtime test completed successfully")
}

// BenchmarkWASMExecution benchmarks WASM contract execution
func BenchmarkWASMExecution(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce logging for benchmarks

	wasmIntegration := NewWASMIntegration(logger)
	wasmIntegration.Start()
	defer wasmIntegration.Stop()

	// Simple WASM module for benchmarking (empty function)
	minimalWASM := []byte{
		0x00, 0x61, 0x73, 0x6D, // WASM magic number
		0x01, 0x00, 0x00, 0x00, // WASM version
		0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // Type section: function type () -> ()
		0x03, 0x02, 0x01, 0x00, // Function section: one function of type 0
		0x05, 0x03, 0x01, 0x00, 0x01, // Memory section: min 1 page
		0x07, 0x08, 0x01, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, // Export section: export "test" as function 0
		0x0A, 0x04, 0x01, 0x02, 0x00, 0x0B, // Code section: function body (empty)
	}

	contractID := "benchmark-contract"
	_, err := wasmIntegration.DeployContract(contractID, minimalWASM)
	if err != nil {
		b.Fatalf("Failed to deploy contract: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := wasmIntegration.CallContract(contractID, "test", []uint64{})
			if err != nil || !result.Success {
				b.Fatalf("WASM call failed: %v", err)
			}
		}
	})
}

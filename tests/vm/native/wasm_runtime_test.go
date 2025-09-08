// Package native provides tests for the WASM runtime
package native

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"diamante/vm/native"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWASMRuntimeCreation tests WASM runtime creation
func TestWASMRuntimeCreation(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)

	assert.NotNil(t, runtime)

	// Clean up
	err := runtime.Close()
	assert.NoError(t, err)
}

// TestWASMConfig tests default WASM configuration
func TestWASMConfig(t *testing.T) {
	config := native.DefaultWASMConfig()

	assert.Equal(t, uint32(256), config.MaxMemoryPages)
	assert.Equal(t, 30*time.Second, config.MaxExecutionTime)
	assert.True(t, config.EnableWASI)
	assert.False(t, config.EnableDebug)
	assert.True(t, config.CacheCompiled)
}

// TestWASMModuleLoading tests loading a WASM module
func TestWASMModuleLoading(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping WASM compilation test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Create a simple WASM module
	wasmCode := createSimpleWASM(t)

	// Load the module
	err := runtime.LoadWASM("test_module", wasmCode)
	assert.NoError(t, err)

	// Try loading again - should fail
	err = runtime.LoadWASM("test_module", wasmCode)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already loaded")

	// Get module info
	info, err := runtime.GetModuleInfo("test_module")
	assert.NoError(t, err)
	assert.Equal(t, "test_module", info.ContractID)
	assert.NotZero(t, info.LoadedAt)
	assert.Contains(t, info.Exports, "add")

	// Get loaded modules
	modules := runtime.GetLoadedModules()
	assert.Len(t, modules, 1)
	assert.Contains(t, modules, "test_module")
}

// TestWASMExecution tests executing WASM functions
func TestWASMExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping WASM execution test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Load a WASM module with add function
	wasmCode := createSimpleWASM(t)
	err := runtime.LoadWASM("math_module", wasmCode)
	require.NoError(t, err)

	// Execute add function
	results, err := runtime.ExecuteWASM("math_module", "add", []uint64{5, 3})
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, uint64(8), results[0])

	// Execute with different arguments
	results, err = runtime.ExecuteWASM("math_module", "add", []uint64{100, 200})
	assert.NoError(t, err)
	assert.Equal(t, uint64(300), results[0])

	// Try executing non-existent function
	_, err = runtime.ExecuteWASM("math_module", "nonexistent", []uint64{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Try executing on non-existent module
	_, err = runtime.ExecuteWASM("nonexistent", "add", []uint64{1, 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not loaded")
}

// TestWASMMemoryOperations tests WASM memory operations
func TestWASMMemoryOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping WASM memory test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Create WASM module with memory operations
	wasmCode := createMemoryWASM(t)
	err := runtime.LoadWASM("memory_module", wasmCode)
	require.NoError(t, err)

	// Test memory write and read
	inputData := []byte("Hello, WASM!")
	results, outputData, err := runtime.ExecuteWASMWithMemory(
		"memory_module",
		"process_data",
		[]uint64{},
		inputData,
	)

	assert.NoError(t, err)
	assert.NotNil(t, results)
	// The function should return the processed data
	if outputData != nil {
		t.Logf("Output data: %s", string(outputData))
	}
}

// TestWASMTimeout tests WASM execution timeout
func TestWASMTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping WASM timeout test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	config.MaxExecutionTime = 2 * time.Second // Short timeout
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Create WASM module with infinite loop
	wasmCode := createInfiniteLoopWASM(t)
	err := runtime.LoadWASM("timeout_module", wasmCode)
	require.NoError(t, err)

	// Execute function that loops forever
	start := time.Now()
	_, err = runtime.ExecuteWASM("timeout_module", "infinite_loop", []uint64{})
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	// Should timeout around 2 seconds
	assert.True(t, duration >= 1900*time.Millisecond && duration <= 2100*time.Millisecond)
}

// TestConcurrentWASMExecution tests concurrent WASM execution
func TestConcurrentWASMExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent WASM test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Load module
	wasmCode := createSimpleWASM(t)
	err := runtime.LoadWASM("concurrent_module", wasmCode)
	require.NoError(t, err)

	// Execute concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	results := make([]uint64, numGoroutines*numOperations)
	errors := make([]error, numGoroutines*numOperations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				idx := id*numOperations + j
				res, err := runtime.ExecuteWASM(
					"concurrent_module",
					"add",
					[]uint64{uint64(id), uint64(j)},
				)

				if err != nil {
					errors[idx] = err
				} else if len(res) > 0 {
					results[idx] = res[0]
				}
			}
		}(i)
	}

	wg.Wait()

	// Check results
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			idx := i*numOperations + j
			assert.NoError(t, errors[idx])
			assert.Equal(t, uint64(i+j), results[idx])
		}
	}

	// Check metrics
	metrics := runtime.GetMetrics()
	assert.Equal(t, int64(numGoroutines*numOperations), metrics.TotalExecutions)
}

// TestWASMUnload tests unloading WASM modules
func TestWASMUnload(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Load module
	wasmCode := createSimpleWASM(t)
	err := runtime.LoadWASM("unload_module", wasmCode)
	require.NoError(t, err)

	// Verify loaded
	modules := runtime.GetLoadedModules()
	assert.Len(t, modules, 1)

	// Execute to ensure it works
	results, err := runtime.ExecuteWASM("unload_module", "add", []uint64{1, 2})
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), results[0])

	// Unload module
	err = runtime.UnloadWASM("unload_module")
	assert.NoError(t, err)

	// Verify unloaded
	modules = runtime.GetLoadedModules()
	assert.Len(t, modules, 0)

	// Try to execute - should fail
	_, err = runtime.ExecuteWASM("unload_module", "add", []uint64{1, 2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not loaded")

	// Try to unload again - should fail
	err = runtime.UnloadWASM("unload_module")
	assert.Error(t, err)
}

// TestWASMMetrics tests WASM runtime metrics
func TestWASMMetrics(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Initial metrics
	metrics := runtime.GetMetrics()
	assert.Equal(t, 0, metrics.ModulesLoaded)
	assert.Equal(t, 0, metrics.ActiveModules)
	assert.Equal(t, int64(0), metrics.TotalExecutions)

	// Load modules
	wasmCode := createSimpleWASM(t)
	runtime.LoadWASM("module1", wasmCode)
	runtime.LoadWASM("module2", wasmCode)

	// Execute some functions
	runtime.ExecuteWASM("module1", "add", []uint64{1, 2})
	runtime.ExecuteWASM("module1", "add", []uint64{3, 4})
	runtime.ExecuteWASM("module2", "add", []uint64{5, 6})

	// Check metrics
	metrics = runtime.GetMetrics()
	assert.Equal(t, 2, metrics.ModulesLoaded)
	assert.Equal(t, 2, metrics.ActiveModules)
	assert.Equal(t, int64(3), metrics.TotalExecutions)
	assert.Greater(t, metrics.AverageExecutionMs, int64(0))

	// Unload one module
	runtime.UnloadWASM("module1")

	metrics = runtime.GetMetrics()
	assert.Equal(t, 2, metrics.ModulesLoaded) // Total loaded (historical)
	assert.Equal(t, 1, metrics.ActiveModules) // Currently active
}

// TestTypeConversion tests type conversion functions
func TestTypeConversion(t *testing.T) {
	// Test float to uint64 conversion
	tests := []struct {
		float float64
		uint  uint64
	}{
		{0.0, 0},
		{1.0, 4607182418800017408},
		{-1.0, 13830554455654793216},
		{3.14159, 4614256656552045848},
	}

	for _, tt := range tests {
		uint := native.Float64ToUint64(tt.float)
		assert.Equal(t, tt.uint, uint)

		// Convert back
		float := native.Uint64ToFloat64(uint)
		assert.Equal(t, tt.float, float)
	}
}

// Helper functions to create WASM modules

func createSimpleWASM(t *testing.T) []byte {
	// Create a simple WAT (WebAssembly Text) file
	watContent := `
(module
  (func $add (param $a i32) (param $b i32) (result i32)
    local.get $a
    local.get $b
    i32.add)
  (export "add" (func $add))
)
`

	// Write WAT to temp file
	tmpDir, err := ioutil.TempDir("", "wasm_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	watFile := filepath.Join(tmpDir, "test.wat")
	err = ioutil.WriteFile(watFile, []byte(watContent), 0644)
	require.NoError(t, err)

	// Compile WAT to WASM using wat2wasm
	wasmFile := filepath.Join(tmpDir, "test.wasm")
	cmd := exec.Command("wat2wasm", watFile, "-o", wasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("wat2wasm output: %s", output)
		// If wat2wasm is not available, use pre-compiled WASM
		return getPrecompiledSimpleWASM()
	}

	// Read compiled WASM
	wasmCode, err := ioutil.ReadFile(wasmFile)
	require.NoError(t, err)

	return wasmCode
}

func createMemoryWASM(t *testing.T) []byte {
	// WAT with memory operations
	watContent := `
(module
  (memory 1)
  (func $process_data (param $ptr i32) (param $len i32) (result i32)
    ;; Simple function that reads from memory
    local.get $ptr
    i32.load
    i32.const 1
    i32.add
    local.get $ptr
    i32.store
    local.get $ptr)
  (export "memory" (memory 0))
  (export "process_data" (func $process_data))
)
`

	tmpDir, err := ioutil.TempDir("", "wasm_memory_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	watFile := filepath.Join(tmpDir, "memory.wat")
	err = ioutil.WriteFile(watFile, []byte(watContent), 0644)
	require.NoError(t, err)

	wasmFile := filepath.Join(tmpDir, "memory.wasm")
	cmd := exec.Command("wat2wasm", watFile, "-o", wasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("wat2wasm output: %s", output)
		return getPrecompiledMemoryWASM()
	}

	wasmCode, err := ioutil.ReadFile(wasmFile)
	require.NoError(t, err)

	return wasmCode
}

func createInfiniteLoopWASM(t *testing.T) []byte {
	// WAT with infinite loop
	watContent := `
(module
  (func $infinite_loop
    (loop
      br 0))
  (export "infinite_loop" (func $infinite_loop))
)
`

	tmpDir, err := ioutil.TempDir("", "wasm_loop_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	watFile := filepath.Join(tmpDir, "loop.wat")
	err = ioutil.WriteFile(watFile, []byte(watContent), 0644)
	require.NoError(t, err)

	wasmFile := filepath.Join(tmpDir, "loop.wasm")
	cmd := exec.Command("wat2wasm", watFile, "-o", wasmFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("wat2wasm output: %s", output)
		return getPrecompiledLoopWASM()
	}

	wasmCode, err := ioutil.ReadFile(wasmFile)
	require.NoError(t, err)

	return wasmCode
}

// Pre-compiled WASM modules for systems without wat2wasm

func getPrecompiledSimpleWASM() []byte {
	// Simple add function: (func $add (param i32 i32) (result i32))
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // WASM header
		0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, // Type section
		0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, // Function section
		0x03, 0x61, 0x64, 0x64, 0x00, 0x00, 0x0a, 0x09, // Export section
		0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, // Code section
		0x0b, // End
	}
}

func getPrecompiledMemoryWASM() []byte {
	// Memory module with simple operations
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // WASM header
		0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, // Type section
		0x7f, 0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, // Function & Memory
		0x00, 0x01, 0x07, 0x11, 0x02, 0x06, 0x6d, 0x65, // Export section
		0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x04, 0x64, // Export memory
		0x61, 0x74, 0x61, 0x00, 0x00, 0x0a, 0x11, 0x01, // Export function
		0x0f, 0x00, 0x20, 0x00, 0x28, 0x02, 0x00, 0x41, // Code section
		0x01, 0x6a, 0x20, 0x00, 0x36, 0x02, 0x00, 0x20, // Code cont.
		0x00, 0x0b, // End
	}
}

func getPrecompiledLoopWASM() []byte {
	// Infinite loop module
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // WASM header
		0x01, 0x04, 0x01, 0x60, 0x00, 0x00, 0x03, 0x02, // Type section
		0x01, 0x00, 0x07, 0x11, 0x01, 0x0d, 0x69, 0x6e, // Function section
		0x66, 0x69, 0x6e, 0x69, 0x74, 0x65, 0x5f, 0x6c, // Export section
		0x6f, 0x6f, 0x70, 0x00, 0x00, 0x0a, 0x08, 0x01, // Export name
		0x06, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b, // Code (loop)
	}
}

// BenchmarkWASMExecution benchmarks WASM execution
func BenchmarkWASMExecution(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := native.DefaultWASMConfig()
	runtime := native.NewWASMRuntime(logger, config)
	defer runtime.Close()

	// Load module
	wasmCode := getPrecompiledSimpleWASM()
	err := runtime.LoadWASM("bench_module", wasmCode)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = runtime.ExecuteWASM("bench_module", "add", []uint64{uint64(i), uint64(i + 1)})
	}
}

// Package native provides WASM runtime integration for the native runtime
package native

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// WASMRuntime manages WASM module execution
type WASMRuntime struct {
	runtime   wazero.Runtime
	modules   map[string]*WASMModule
	hostFuncs map[string]api.Function
	mu        sync.RWMutex
	logger    *logrus.Logger

	// Configuration
	config WASMConfig

	// Metrics
	modulesLoaded    int
	executionCount   int64
	totalExecutionMs int64
}

// WASMModule represents a loaded WASM module
type WASMModule struct {
	Module         api.Module
	CompiledModule wazero.CompiledModule
	LoadedAt       time.Time
	ExecutionCount int64
	LastExecuted   time.Time
	TotalCPUTime   time.Duration
	MemoryPages    uint32
	Exports        []string
}

// WASMConfig contains WASM runtime configuration
type WASMConfig struct {
	MaxMemoryPages   uint32        // Maximum memory pages per module (64KB per page)
	MaxExecutionTime time.Duration // Maximum execution time per call
	EnableWASI       bool          // Enable WASI support
	EnableDebug      bool          // Enable debug logging
	CacheCompiled    bool          // Cache compiled modules
}

// DefaultWASMConfig returns default WASM configuration
func DefaultWASMConfig() WASMConfig {
	return WASMConfig{
		MaxMemoryPages:   256,              // 16MB max memory
		MaxExecutionTime: 30 * time.Second, // 30 second timeout
		EnableWASI:       true,             // Enable WASI by default
		EnableDebug:      false,
		CacheCompiled:    true,
	}
}

// NewWASMRuntime creates a new WASM runtime
func NewWASMRuntime(logger *logrus.Logger, config WASMConfig) *WASMRuntime {
	if logger == nil {
		logger = logrus.New()
	}

	ctx := context.Background()

	// Create runtime configuration
	runtimeConfig := wazero.NewRuntimeConfig()
	if config.CacheCompiled {
		// Enable compilation cache for better performance
		runtimeConfig = runtimeConfig.WithCompilationCache(wazero.NewCompilationCache())
	}

	// Create runtime
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)

	wr := &WASMRuntime{
		runtime:   r,
		modules:   make(map[string]*WASMModule),
		hostFuncs: make(map[string]api.Function),
		logger:    logger,
		config:    config,
	}

	// Initialize WASI if enabled
	if config.EnableWASI {
		if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
			logger.WithError(err).Warn("Failed to instantiate WASI")
		}
	}

	// Register host functions
	wr.registerHostFunctions(ctx)

	return wr
}

// LoadWASM loads a WASM module
func (wr *WASMRuntime) LoadWASM(contractID string, code []byte) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	// Check if already loaded
	if _, exists := wr.modules[contractID]; exists {
		return fmt.Errorf("WASM module %s already loaded", contractID)
	}

	start := consensus.ConsensusNow()

	// Compile the module
	compiled, err := wr.runtime.CompileModule(context.Background(), code)
	if err != nil {
		return fmt.Errorf("failed to compile WASM: %w", err)
	}

	// Configure module
	moduleConfig := wazero.NewModuleConfig().
		WithName(contractID)

	// Set memory limits are now set per-module basis in WASM itself
	// The wazero API no longer supports WithMemoryMax directly

	// Disable stdio unless debug is enabled
	if !wr.config.EnableDebug {
		moduleConfig = moduleConfig.
			WithStdout(nil).
			WithStderr(nil).
			WithStdin(nil)
	}

	// Instantiate module
	module, err := wr.runtime.InstantiateModule(context.Background(), compiled, moduleConfig)
	if err != nil {
		return fmt.Errorf("failed to instantiate WASM: %w", err)
	}

	// Get exports - we'll get them from the compiled module
	exportNames := make([]string, 0)
	exportedFunctions := compiled.ExportedFunctions()
	for name := range exportedFunctions {
		exportNames = append(exportNames, name)
	}

	// Get memory info - handle potential nil memory
	memory := module.Memory()
	memoryPages := uint32(0)
	if memory != nil {
		// Use a more robust approach to get memory size
		func() {
			defer func() {
				if r := recover(); r != nil {
					wr.logger.WithError(fmt.Errorf("panic getting memory size: %v", r)).Warn("Memory size panic")
					memoryPages = 0
				}
			}()

			// Try to get memory size safely
			if memSize := memory.Size(); memSize > 0 {
				memoryPages = memSize / 65536
			}
		}()
	}

	// Store module
	wr.modules[contractID] = &WASMModule{
		Module:         module,
		CompiledModule: compiled,
		LoadedAt:       consensus.ConsensusNow(),
		ExecutionCount: 0,
		LastExecuted:   time.Time{},
		TotalCPUTime:   0,
		MemoryPages:    memoryPages,
		Exports:        exportNames,
	}

	wr.modulesLoaded++

	loadDuration := consensus.ConsensusSince(start)
	wr.logger.WithFields(logrus.Fields{
		"contractID":  contractID,
		"codeSize":    len(code),
		"loadTime":    loadDuration,
		"exports":     len(exportNames),
		"memoryPages": memoryPages,
		"memoryMB":    float64(memoryPages*64) / 1024,
	}).Info("WASM module loaded")

	return nil
}

// ExecuteWASM executes a WASM function
func (wr *WASMRuntime) ExecuteWASM(contractID, function string, args []uint64) ([]uint64, error) {
	wr.mu.RLock()
	wasmModule, exists := wr.modules[contractID]
	wr.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("WASM module %s not loaded", contractID)
	}

	// Get exported function
	fn := wasmModule.Module.ExportedFunction(function)
	if fn == nil {
		return nil, fmt.Errorf("function %s not found in module %s", function, contractID)
	}

	// Create execution context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), wr.config.MaxExecutionTime)
	defer cancel()

	start := consensus.ConsensusNow()

	// Execute function in a goroutine to handle timeouts properly
	resultChan := make(chan []uint64, 1)
	errorChan := make(chan error, 1)

	go func() {
		// Recover from panics
		defer func() {
			if r := recover(); r != nil {
				errorChan <- fmt.Errorf("WASM execution panicked: %v", r)
			}
		}()

		// Execute the function
		results, err := fn.Call(ctx, args...)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- results
		}
	}()

	// Wait for result or timeout
	select {
	case results := <-resultChan:
		executionTime := consensus.ConsensusSince(start)

		// Update metrics
		wr.mu.Lock()
		wasmModule.ExecutionCount++
		wasmModule.LastExecuted = consensus.ConsensusNow()
		wasmModule.TotalCPUTime += executionTime
		wr.executionCount++
		wr.totalExecutionMs += executionTime.Milliseconds()
		wr.mu.Unlock()

		wr.logger.WithFields(logrus.Fields{
			"contractID":    contractID,
			"function":      function,
			"executionTime": executionTime,
			"resultCount":   len(results),
		}).Debug("WASM function executed")

		return results, nil

	case err := <-errorChan:
		// Check if it's a timeout error
		if err == context.DeadlineExceeded {
			return nil, fmt.Errorf("WASM execution timeout after %v", wr.config.MaxExecutionTime)
		}
		return nil, fmt.Errorf("WASM execution failed: %w", err)

	case <-ctx.Done():
		return nil, fmt.Errorf("WASM execution timeout after %v", wr.config.MaxExecutionTime)
	}
}

// ExecuteWASMWithMemory executes a WASM function with memory access
func (wr *WASMRuntime) ExecuteWASMWithMemory(contractID, function string, args []uint64, inputData []byte) ([]uint64, []byte, error) {
	wr.mu.RLock()
	wasmModule, exists := wr.modules[contractID]
	wr.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("WASM module %s not loaded", contractID)
	}

	// Get memory
	memory := wasmModule.Module.Memory()
	if memory == nil {
		return nil, nil, fmt.Errorf("module has no memory export")
	}

	// Write input data to memory if provided
	inputPtr := uint32(0)
	if len(inputData) > 0 {
		// Try to call malloc if available, otherwise use dynamic allocation
		inputPtr = wr.allocateMemory(wasmModule.Module, uint32(len(inputData)))
		if inputPtr == 0 {
			// Fallback to end of used memory
			inputPtr = memory.Size() - uint32(len(inputData))
			if inputPtr < 1024 {
				inputPtr = 1024 // Minimum safe offset
			}
		}

		if !memory.Write(inputPtr, inputData) {
			return nil, nil, fmt.Errorf("failed to write input data to memory")
		}

		// Add pointer and length to args
		args = append([]uint64{uint64(inputPtr), uint64(len(inputData))}, args...)
	}

	// Execute function
	results, err := wr.ExecuteWASM(contractID, function, args)
	if err != nil {
		return nil, nil, err
	}

	// Read output data from memory if function returns pointer and length
	if len(results) >= 2 {
		outputPtr := uint32(results[0])
		outputLen := uint32(results[1])

		if outputLen > 0 && outputLen < 1024*1024 { // Sanity check: max 1MB
			outputData, ok := memory.Read(outputPtr, outputLen)
			if !ok {
				return results, nil, fmt.Errorf("failed to read output data from memory")
			}
			return results, outputData, nil
		}
	}

	return results, nil, nil
}

// allocateMemory attempts to allocate memory in the WASM module
func (wr *WASMRuntime) allocateMemory(module api.Module, size uint32) uint32 {
	// Try to call malloc if exported
	mallocFn := module.ExportedFunction("malloc")
	if mallocFn != nil {
		ctx := context.Background()
		results, err := mallocFn.Call(ctx, uint64(size))
		if err != nil {
			wr.logger.WithError(err).WithField("size", size).Debug("Failed to call malloc function")
		} else if len(results) > 0 {
			allocatedPtr := uint32(results[0])
			wr.logger.WithFields(logrus.Fields{
				"size": size,
				"ptr":  allocatedPtr,
			}).Debug("Memory allocated via malloc")
			return allocatedPtr
		}
	}

	// Try __heap_base if available
	heapBase := module.ExportedGlobal("__heap_base")
	if heapBase != nil {
		// Get heap base value and allocate from there
		base := heapBase.Get()
		if base > 0 {
			// Simple bump allocator from heap base
			allocatedPtr := uint32(base)
			wr.logger.WithFields(logrus.Fields{
				"size": size,
				"ptr":  allocatedPtr,
			}).Debug("Memory allocated from heap base")
			return allocatedPtr
		}
	}

	// No allocation function available
	wr.logger.WithField("size", size).Debug("No memory allocation function available in WASM module")
	return 0
}

// GetModuleInfo returns information about a loaded module
func (wr *WASMRuntime) GetModuleInfo(contractID string) (*WASMModuleInfo, error) {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	wasmModule, exists := wr.modules[contractID]
	if !exists {
		return nil, fmt.Errorf("WASM module %s not loaded", contractID)
	}

	// Get current memory usage
	memory := wasmModule.Module.Memory()
	currentMemoryPages := uint32(0)
	if memory != nil {
		currentMemoryPages = memory.Size() / 65536
	}

	return &WASMModuleInfo{
		ContractID:     contractID,
		LoadedAt:       wasmModule.LoadedAt,
		ExecutionCount: wasmModule.ExecutionCount,
		LastExecuted:   wasmModule.LastExecuted,
		TotalCPUTime:   wasmModule.TotalCPUTime,
		MemoryPages:    currentMemoryPages,
		InitialMemory:  wasmModule.MemoryPages,
		Exports:        wasmModule.Exports,
	}, nil
}

// WASMModuleInfo contains information about a WASM module
type WASMModuleInfo struct {
	ContractID     string
	LoadedAt       time.Time
	ExecutionCount int64
	LastExecuted   time.Time
	TotalCPUTime   time.Duration
	MemoryPages    uint32
	InitialMemory  uint32
	Exports        []string
}

// UnloadWASM unloads a WASM module
func (wr *WASMRuntime) UnloadWASM(contractID string) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	wasmModule, exists := wr.modules[contractID]
	if !exists {
		return fmt.Errorf("WASM module %s not loaded", contractID)
	}

	// Close the module
	ctx := context.Background()
	if err := wasmModule.Module.Close(ctx); err != nil {
		return fmt.Errorf("failed to close module: %w", err)
	}

	// Remove from map
	delete(wr.modules, contractID)

	wr.logger.WithFields(logrus.Fields{
		"contractID":     contractID,
		"executionCount": wasmModule.ExecutionCount,
		"totalCPUTime":   wasmModule.TotalCPUTime,
	}).Info("WASM module unloaded")

	return nil
}

// GetLoadedModules returns information about all loaded modules
func (wr *WASMRuntime) GetLoadedModules() map[string]WASMModuleInfo {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	modules := make(map[string]WASMModuleInfo)
	for id, module := range wr.modules {
		memory := module.Module.Memory()
		currentMemoryPages := uint32(0)
		if memory != nil {
			currentMemoryPages = memory.Size() / 65536
		}

		modules[id] = WASMModuleInfo{
			ContractID:     id,
			LoadedAt:       module.LoadedAt,
			ExecutionCount: module.ExecutionCount,
			LastExecuted:   module.LastExecuted,
			TotalCPUTime:   module.TotalCPUTime,
			MemoryPages:    currentMemoryPages,
			InitialMemory:  module.MemoryPages,
			Exports:        module.Exports,
		}
	}

	return modules
}

// GetMetrics returns WASM runtime metrics
func (wr *WASMRuntime) GetMetrics() WASMRuntimeMetrics {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	avgExecutionMs := int64(0)
	if wr.executionCount > 0 {
		avgExecutionMs = wr.totalExecutionMs / wr.executionCount
	}

	// Calculate total memory usage
	totalMemoryPages := uint32(0)
	for _, module := range wr.modules {
		if memory := module.Module.Memory(); memory != nil {
			totalMemoryPages += memory.Size() / 65536
		}
	}

	return WASMRuntimeMetrics{
		ModulesLoaded:      wr.modulesLoaded,
		ActiveModules:      len(wr.modules),
		TotalExecutions:    wr.executionCount,
		AverageExecutionMs: avgExecutionMs,
		TotalMemoryPages:   totalMemoryPages,
		TotalMemoryMB:      float64(totalMemoryPages*64) / 1024,
	}
}

// WASMRuntimeMetrics contains metrics about WASM operations
type WASMRuntimeMetrics struct {
	ModulesLoaded      int
	ActiveModules      int
	TotalExecutions    int64
	AverageExecutionMs int64
	TotalMemoryPages   uint32
	TotalMemoryMB      float64
}

// Close closes the WASM runtime
func (wr *WASMRuntime) Close() error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	// Close all modules
	for id, module := range wr.modules {
		ctx := context.Background()
		if err := module.Module.Close(ctx); err != nil {
			wr.logger.WithError(err).Errorf("Failed to close module %s", id)
		}
	}

	// Clear modules map
	wr.modules = make(map[string]*WASMModule)

	// Close runtime
	ctx := context.Background()
	if err := wr.runtime.Close(ctx); err != nil {
		return fmt.Errorf("failed to close runtime: %w", err)
	}

	wr.logger.Info("WASM runtime closed")
	return nil
}

// registerHostFunctions registers host functions that WASM modules can call
func (wr *WASMRuntime) registerHostFunctions(ctx context.Context) {
	// Create host module
	hostBuilder := wr.runtime.NewHostModuleBuilder("diamante")

	// Register logging function
	hostBuilder.NewFunctionBuilder().
		WithName("log").
		WithParameterNames("level", "messagePtr", "messageLen").
		WithResultNames().
		WithFunc(func(ctx context.Context, m api.Module, level uint32, messagePtr uint32, messageLen uint32) {
			// Read message from memory
			memory := m.Memory()
			if memory == nil {
				return
			}

			message, ok := memory.Read(messagePtr, messageLen)
			if !ok {
				return
			}

			// Log based on level
			switch level {
			case 0: // Debug
				wr.logger.Debug(string(message))
			case 1: // Info
				wr.logger.Info(string(message))
			case 2: // Warn
				wr.logger.Warn(string(message))
			case 3: // Error
				wr.logger.Error(string(message))
			}
		}).
		Export("log")

	// Register time function
	hostBuilder.NewFunctionBuilder().
		WithName("getTime").
		WithParameterNames().
		WithResultNames("timestamp").
		WithFunc(func(ctx context.Context, m api.Module) uint64 {
			return uint64(consensus.ConsensusUnix())
		}).
		Export("getTime")

	// Register random number function
	hostBuilder.NewFunctionBuilder().
		WithName("getRandom").
		WithParameterNames("max").
		WithResultNames("random").
		WithFunc(func(ctx context.Context, m api.Module, max uint32) uint32 {
			if max == 0 {
				return 0
			}
			// Use consensus-based deterministic randomness
			return uint32(consensus.ConsensusUnixNano()) % max
		}).
		Export("getRandom")

	// Instantiate host module
	_, err := hostBuilder.Instantiate(ctx)
	if err != nil {
		wr.logger.WithError(err).Warn("Failed to instantiate host functions")
	}
}

// Helper functions for type conversion

// Float64ToUint64 converts float64 to uint64 for WASM
func Float64ToUint64(f float64) uint64 {
	return binary.LittleEndian.Uint64(floatToBytes(f))
}

// Uint64ToFloat64 converts uint64 to float64 from WASM
func Uint64ToFloat64(u uint64) float64 {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, u)
	return bytesToFloat(bytes)
}

func floatToBytes(f float64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(f))
	return bytes
}

func bytesToFloat(bytes []byte) float64 {
	return float64(binary.LittleEndian.Uint64(bytes))
}

// secureRandom generates a cryptographically secure random number
func (wr *WASMRuntime) secureRandom(max uint64) uint64 {
	if max == 0 {
		return 0
	}

	// Use crypto/rand for secure randomness
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to consensus-based deterministic randomness if crypto/rand fails
		wr.logger.WithError(err).Warn("Failed to generate secure random, falling back to consensus-based")
		return uint64(consensus.ConsensusUnixNano()) % max
	}

	// Convert bytes to uint64 and modulo by max
	n := binary.LittleEndian.Uint64(b[:])
	return n % max
}

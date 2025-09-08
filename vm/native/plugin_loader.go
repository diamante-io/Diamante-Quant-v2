// Package native provides plugin loading functionality for the native runtime
package native

import (
	"fmt"
	"plugin"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// PluginLoader manages Go plugin loading and execution
type PluginLoader struct {
	plugins map[string]*LoadedPlugin
	mu      sync.RWMutex
	logger  *logrus.Logger

	// Metrics
	pluginsLoaded    int
	executionCount   int64
	totalExecutionMs int64
}

// LoadedPlugin represents a loaded Go plugin
type LoadedPlugin struct {
	Path     string
	Plugin   *plugin.Plugin
	Manifest PluginManifest
	LoadedAt time.Time
	Instance NativeContractInterface

	// Execution metrics
	ExecutionCount int64
	LastExecuted   time.Time
	TotalCPUTime   time.Duration
}

// PluginManifest describes a plugin's capabilities
type PluginManifest struct {
	Name         string
	Version      string
	Author       string
	Description  string
	Methods      []MethodInfo
	MinGoVersion string
	Dependencies []string
}

// MethodInfo describes a plugin method
type MethodInfo struct {
	Name        string
	Description string
	Inputs      []string
	Outputs     []string
	GasCost     uint64
}

// NativeContractInterface that all plugins must implement
type NativeContractInterface interface {
	// Init initializes the plugin with state
	Init(state map[string]interface{}) error

	// Execute runs a method with arguments
	Execute(method string, args []interface{}) (interface{}, error)

	// GetManifest returns plugin metadata
	GetManifest() PluginManifest

	// GetState returns current plugin state
	GetState() map[string]interface{}

	// SetState updates plugin state
	SetState(key string, value interface{}) error

	// Cleanup performs cleanup before unload
	Cleanup() error
}

// NewPluginLoader creates a new plugin loader
func NewPluginLoader(logger *logrus.Logger) *PluginLoader {
	if logger == nil {
		logger = logrus.New()
	}

	return &PluginLoader{
		plugins: make(map[string]*LoadedPlugin),
		logger:  logger,
	}
}

// LoadPlugin loads a Go plugin from disk
func (pl *PluginLoader) LoadPlugin(contractID, pluginPath string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Check if already loaded
	if _, exists := pl.plugins[contractID]; exists {
		return fmt.Errorf("plugin %s already loaded", contractID)
	}

	start := consensus.ConsensusNow()

	// Open the plugin file
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to open plugin: %w", err)
	}

	// Look for the Contract symbol
	symbol, err := p.Lookup("Contract")
	if err != nil {
		return fmt.Errorf("plugin missing Contract symbol: %w", err)
	}

	// Assert to NativeContractInterface
	contract, ok := symbol.(NativeContractInterface)
	if !ok {
		// Try looking for a function that returns the interface
		if contractFn, ok := symbol.(func() NativeContractInterface); ok {
			contract = contractFn()
		} else {
			return fmt.Errorf("contract does not implement NativeContractInterface")
		}
	}

	// Get and validate manifest
	manifest := contract.GetManifest()
	if err := pl.validateManifest(manifest); err != nil {
		return fmt.Errorf("invalid manifest: %w", err)
	}

	// Initialize the contract
	if err := contract.Init(make(map[string]interface{})); err != nil {
		return fmt.Errorf("failed to initialize contract: %w", err)
	}

	// Store loaded plugin
	pl.plugins[contractID] = &LoadedPlugin{
		Path:           pluginPath,
		Plugin:         p,
		Manifest:       manifest,
		LoadedAt:       consensus.ConsensusNow(),
		Instance:       contract,
		ExecutionCount: 0,
		LastExecuted:   time.Time{},
		TotalCPUTime:   0,
	}

	pl.pluginsLoaded++

	loadDuration := consensus.ConsensusSince(start)
	pl.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"plugin":     manifest.Name,
		"version":    manifest.Version,
		"path":       pluginPath,
		"loadTime":   loadDuration,
		"methods":    len(manifest.Methods),
	}).Info("Plugin loaded successfully")

	return nil
}

// ExecutePlugin executes a method on a loaded plugin
func (pl *PluginLoader) ExecutePlugin(contractID, method string, args []interface{}) (interface{}, error) {
	pl.mu.RLock()
	loadedPlugin, exists := pl.plugins[contractID]
	pl.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("plugin %s not loaded", contractID)
	}

	// Validate method exists
	methodInfo, err := pl.findMethod(loadedPlugin.Manifest, method)
	if err != nil {
		return nil, err
	}

	// Validate argument count
	if len(args) != len(methodInfo.Inputs) {
		return nil, fmt.Errorf("method %s expects %d arguments, got %d",
			method, len(methodInfo.Inputs), len(args))
	}

	// Create execution context with timeout
	resultChan := make(chan interface{}, 1)
	errorChan := make(chan error, 1)

	start := consensus.ConsensusNow()

	go func() {
		// Recover from panics
		defer func() {
			if r := recover(); r != nil {
				errorChan <- fmt.Errorf("plugin panicked: %v", r)
			}
		}()

		// Execute the method
		result, err := loadedPlugin.Instance.Execute(method, args)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	// Wait with timeout (30 seconds)
	timeout := 30 * time.Second
	select {
	case result := <-resultChan:
		executionTime := consensus.ConsensusSince(start)

		// Update metrics
		pl.mu.Lock()
		loadedPlugin.ExecutionCount++
		loadedPlugin.LastExecuted = consensus.ConsensusNow()
		loadedPlugin.TotalCPUTime += executionTime
		pl.executionCount++
		pl.totalExecutionMs += executionTime.Milliseconds()
		pl.mu.Unlock()

		pl.logger.WithFields(logrus.Fields{
			"contractID":    contractID,
			"method":        method,
			"executionTime": executionTime,
			"gasEstimate":   methodInfo.GasCost,
		}).Debug("Plugin method executed")

		return result, nil

	case err := <-errorChan:
		return nil, fmt.Errorf("plugin execution failed: %w", err)

	case <-time.After(timeout):
		return nil, fmt.Errorf("plugin execution timeout after %v", timeout)
	}
}

// GetPluginState retrieves the current state of a plugin
func (pl *PluginLoader) GetPluginState(contractID string) (map[string]interface{}, error) {
	pl.mu.RLock()
	loadedPlugin, exists := pl.plugins[contractID]
	pl.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("plugin %s not loaded", contractID)
	}

	return loadedPlugin.Instance.GetState(), nil
}

// SetPluginState updates the state of a plugin
func (pl *PluginLoader) SetPluginState(contractID string, key string, value interface{}) error {
	pl.mu.RLock()
	loadedPlugin, exists := pl.plugins[contractID]
	pl.mu.RUnlock()

	if !exists {
		return fmt.Errorf("plugin %s not loaded", contractID)
	}

	return loadedPlugin.Instance.SetState(key, value)
}

// UnloadPlugin unloads a plugin
func (pl *PluginLoader) UnloadPlugin(contractID string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	loadedPlugin, exists := pl.plugins[contractID]
	if !exists {
		return fmt.Errorf("plugin %s not loaded", contractID)
	}

	// Call cleanup on the plugin
	if err := loadedPlugin.Instance.Cleanup(); err != nil {
		pl.logger.WithError(err).Warn("Plugin cleanup failed")
	}

	// Remove from map
	delete(pl.plugins, contractID)

	pl.logger.WithFields(logrus.Fields{
		"contractID":     contractID,
		"executionCount": loadedPlugin.ExecutionCount,
		"totalCPUTime":   loadedPlugin.TotalCPUTime,
	}).Info("Plugin unloaded")

	// Note: Go plugins cannot be truly unloaded from memory
	// This is a limitation of the Go plugin system

	return nil
}

// GetLoadedPlugins returns information about all loaded plugins
func (pl *PluginLoader) GetLoadedPlugins() map[string]PluginInfo {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	plugins := make(map[string]PluginInfo)
	for id, loaded := range pl.plugins {
		plugins[id] = PluginInfo{
			ContractID:     id,
			Name:           loaded.Manifest.Name,
			Version:        loaded.Manifest.Version,
			LoadedAt:       loaded.LoadedAt,
			ExecutionCount: loaded.ExecutionCount,
			LastExecuted:   loaded.LastExecuted,
			TotalCPUTime:   loaded.TotalCPUTime,
			Methods:        len(loaded.Manifest.Methods),
		}
	}

	return plugins
}

// PluginInfo contains runtime information about a plugin
type PluginInfo struct {
	ContractID     string
	Name           string
	Version        string
	LoadedAt       time.Time
	ExecutionCount int64
	LastExecuted   time.Time
	TotalCPUTime   time.Duration
	Methods        int
}

// GetMetrics returns plugin loader metrics
func (pl *PluginLoader) GetMetrics() PluginLoaderMetrics {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	avgExecutionMs := int64(0)
	if pl.executionCount > 0 {
		avgExecutionMs = pl.totalExecutionMs / pl.executionCount
	}

	return PluginLoaderMetrics{
		PluginsLoaded:      pl.pluginsLoaded,
		ActivePlugins:      len(pl.plugins),
		TotalExecutions:    pl.executionCount,
		AverageExecutionMs: avgExecutionMs,
	}
}

// PluginLoaderMetrics contains metrics about plugin operations
type PluginLoaderMetrics struct {
	PluginsLoaded      int
	ActivePlugins      int
	TotalExecutions    int64
	AverageExecutionMs int64
}

// Helper methods

func (pl *PluginLoader) validateManifest(manifest PluginManifest) error {
	if manifest.Name == "" {
		return fmt.Errorf("plugin name required")
	}
	if manifest.Version == "" {
		return fmt.Errorf("plugin version required")
	}
	if len(manifest.Methods) == 0 {
		return fmt.Errorf("plugin must export at least one method")
	}

	// Validate each method
	for i, method := range manifest.Methods {
		if method.Name == "" {
			return fmt.Errorf("method %d missing name", i)
		}
		if method.GasCost == 0 {
			return fmt.Errorf("method %s missing gas cost", method.Name)
		}
	}

	return nil
}

func (pl *PluginLoader) findMethod(manifest PluginManifest, methodName string) (*MethodInfo, error) {
	for _, method := range manifest.Methods {
		if method.Name == methodName {
			return &method, nil
		}
	}
	return nil, fmt.Errorf("method %s not found in plugin manifest", methodName)
}

// Example plugin implementation for testing
// This would normally be in a separate plugin file

// ExamplePlugin demonstrates a plugin implementation
type ExamplePlugin struct {
	state map[string]interface{}
	mu    sync.RWMutex
}

// Init initializes the plugin
func (p *ExamplePlugin) Init(state map[string]interface{}) error {
	p.state = state
	if p.state == nil {
		p.state = make(map[string]interface{})
	}
	return nil
}

// Execute runs a method
func (p *ExamplePlugin) Execute(method string, args []interface{}) (interface{}, error) {
	switch method {
	case "add":
		if len(args) != 2 {
			return nil, fmt.Errorf("add requires 2 arguments")
		}
		a, ok1 := args[0].(float64)
		b, ok2 := args[1].(float64)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("arguments must be numbers")
		}
		return a + b, nil

	case "store":
		if len(args) != 2 {
			return nil, fmt.Errorf("store requires 2 arguments")
		}
		key, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("key must be string")
		}
		p.mu.Lock()
		p.state[key] = args[1]
		p.mu.Unlock()
		return true, nil

	case "retrieve":
		if len(args) != 1 {
			return nil, fmt.Errorf("retrieve requires 1 argument")
		}
		key, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("key must be string")
		}
		p.mu.RLock()
		value := p.state[key]
		p.mu.RUnlock()
		return value, nil

	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

// GetManifest returns plugin metadata
func (p *ExamplePlugin) GetManifest() PluginManifest {
	return PluginManifest{
		Name:        "ExamplePlugin",
		Version:     "1.0.0",
		Author:      "Diamante Team",
		Description: "Example plugin for testing",
		Methods: []MethodInfo{
			{
				Name:        "add",
				Description: "Adds two numbers",
				Inputs:      []string{"number", "number"},
				Outputs:     []string{"number"},
				GasCost:     1000,
			},
			{
				Name:        "store",
				Description: "Stores a key-value pair",
				Inputs:      []string{"string", "any"},
				Outputs:     []string{"bool"},
				GasCost:     5000,
			},
			{
				Name:        "retrieve",
				Description: "Retrieves a value by key",
				Inputs:      []string{"string"},
				Outputs:     []string{"any"},
				GasCost:     2000,
			},
		},
		MinGoVersion: "1.16",
	}
}

// GetState returns plugin state
func (p *ExamplePlugin) GetState() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return a copy
	state := make(map[string]interface{})
	for k, v := range p.state {
		state[k] = v
	}
	return state
}

// SetState updates plugin state
func (p *ExamplePlugin) SetState(key string, value interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state[key] = value
	return nil
}

// Cleanup performs cleanup
func (p *ExamplePlugin) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state = nil
	return nil
}

// Contract is the exported symbol for the plugin
var Contract NativeContractInterface = &ExamplePlugin{}

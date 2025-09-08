// Package runtime provides the runtime registry and management
package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// RuntimeMetadata defines metadata for a runtime
type RuntimeMetadata struct {
	Name         string
	Description  string
	Version      string
	Author       string
	License      string
	Repository   string
	Capabilities []RuntimeCapability
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// RuntimeCapability defines what a runtime can do
type RuntimeCapability string

const (
	// Core capabilities
	CapabilitySmartContracts  RuntimeCapability = "smart_contracts"
	CapabilityStateManagement RuntimeCapability = "state_management"
	CapabilityEventEmission   RuntimeCapability = "event_emission"
	CapabilityUpgradeable     RuntimeCapability = "upgradeable"
	CapabilityDeterministic   RuntimeCapability = "deterministic"
	CapabilityGasMetering     RuntimeCapability = "gas_metering"

	// Advanced capabilities
	CapabilityCrossRuntime    RuntimeCapability = "cross_runtime"
	CapabilityBatchProcessing RuntimeCapability = "batch_processing"
	CapabilityAsyncExecution  RuntimeCapability = "async_execution"
	CapabilityStateProofs     RuntimeCapability = "state_proofs"
)

// RuntimeRegistry manages runtime registration and discovery
type RuntimeRegistry struct {
	mu              sync.RWMutex
	runtimes        map[RuntimeType]RuntimeFactory
	metadata        map[RuntimeType]RuntimeMetadata
	capabilities    map[RuntimeType]map[RuntimeCapability]bool
	initOrder       []RuntimeType
	logger          *logrus.Logger
	validationRules map[RuntimeType]ValidationRule
	healthChecks    map[RuntimeType]HealthCheckFunc

	// Production enhancements
	metrics          *RegistryMetrics
	circuitBreakers  *RuntimeCircuitBreakers
	configValidator  *ConfigValidator
	auditLogger      *AuditLogger
	lifecycleManager *LifecycleManager

	// Internal state
	defaultRuntime RuntimeType
	createdAt      time.Time
}

// RuntimeFactory creates runtime instances
type RuntimeFactory func() Runtime

// ValidationRule validates runtime configuration
type ValidationRule func(config RuntimeConfig) error

// HealthCheckFunc checks runtime health
type HealthCheckFunc func(runtime Runtime) error

// Global registry instance
var (
	globalRegistry     *RuntimeRegistry
	globalRegistryOnce sync.Once
)

// initGlobalRegistry initializes the global registry
func initGlobalRegistry() {
	globalRegistryOnce.Do(func() {
		logger := logrus.New()

		// Initialize audit logger
		auditLogger, err := NewAuditLogger(AuditLoggerConfig{
			LogDir:        "/var/log/diamante/audit",
			BufferSize:    1000,
			FlushSize:     100,
			FlushInterval: 10 * time.Second,
			Retention:     30 * 24 * time.Hour,
			Filters: []AuditFilter{
				NewSeverityFilter(AuditSeverityInfo),
			},
		})
		if err != nil {
			logger.WithError(err).Warn("Failed to initialize audit logger")
		}

		globalRegistry = &RuntimeRegistry{
			runtimes:        make(map[RuntimeType]RuntimeFactory),
			metadata:        make(map[RuntimeType]RuntimeMetadata),
			capabilities:    make(map[RuntimeType]map[RuntimeCapability]bool),
			initOrder:       make([]RuntimeType, 0),
			validationRules: make(map[RuntimeType]ValidationRule),
			healthChecks:    make(map[RuntimeType]HealthCheckFunc),
			logger:          logger,
			defaultRuntime:  RuntimeTypeEVM, // Default to EVM for now
			createdAt:       consensus.ConsensusNow(),

			// Production enhancements
			metrics: NewRegistryMetrics(),
			circuitBreakers: NewRuntimeCircuitBreakers(CircuitBreakerConfig{
				MaxFailures:      5,
				ResetTimeout:     60 * time.Second,
				HalfOpenRequests: 3,
			}),
			configValidator:  NewConfigValidator(),
			auditLogger:      auditLogger,
			lifecycleManager: NewLifecycleManager(logger, auditLogger),
		}

		// Register config schemas
		globalRegistry.configValidator.RegisterSchema(RuntimeTypeEVM, CreateEVMConfigSchema())
		globalRegistry.configValidator.RegisterSchema(RuntimeTypeNative, CreateNativeConfigSchema())
		globalRegistry.configValidator.RegisterSchema(RuntimeTypeChaincode, CreateChaincodeConfigSchema())

		// Register global lifecycle hooks
		globalRegistry.lifecycleManager.RegisterGlobalHook(StagePreInitialize, NewHealthCheckHook())
		globalRegistry.lifecycleManager.RegisterGlobalHook(StagePostRegister, NewMetricsHook(globalRegistry.metrics))
		globalRegistry.lifecycleManager.RegisterGlobalHook(StagePreUnregister, NewResourceCleanupHook())
	})
}

// AutoRegisterRuntime registers a runtime factory with the global registry
func AutoRegisterRuntime(runtimeType RuntimeType, factory RuntimeFactory) error {
	initGlobalRegistry()

	if err := globalRegistry.RegisterRuntime(runtimeType, factory); err != nil {
		globalRegistry.logger.WithError(err).Errorf("Failed to auto-register runtime %s", runtimeType)
		return fmt.Errorf("failed to auto-register runtime %s: %w", runtimeType, err)
	}

	return nil
}

// RegisterRuntimeMetadata registers runtime metadata with the global registry
func RegisterRuntimeMetadata(runtimeType RuntimeType, metadata RuntimeMetadata) error {
	initGlobalRegistry()

	if err := globalRegistry.RegisterMetadata(runtimeType, metadata); err != nil {
		globalRegistry.logger.WithError(err).Errorf("Failed to register metadata for runtime %s", runtimeType)
		return fmt.Errorf("failed to register metadata for runtime %s: %w", runtimeType, err)
	}

	return nil
}

// GetGlobalRegistry returns the global runtime registry
func GetGlobalRegistry() *RuntimeRegistry {
	initGlobalRegistry()
	return globalRegistry
}

// IsRuntimeRegistered checks if a runtime type is registered
func (r *RuntimeRegistry) IsRuntimeRegistered(runtimeType RuntimeType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.runtimes[runtimeType]
	return exists
}

// NewRuntimeRegistry creates a new runtime registry instance
func NewRuntimeRegistry(logger *logrus.Logger) *RuntimeRegistry {
	if logger == nil {
		logger = logrus.New()
	}

	registry := &RuntimeRegistry{
		runtimes:        make(map[RuntimeType]RuntimeFactory),
		metadata:        make(map[RuntimeType]RuntimeMetadata),
		capabilities:    make(map[RuntimeType]map[RuntimeCapability]bool),
		initOrder:       make([]RuntimeType, 0),
		validationRules: make(map[RuntimeType]ValidationRule),
		healthChecks:    make(map[RuntimeType]HealthCheckFunc),
		logger:          logger,
		defaultRuntime:  RuntimeTypeEVM, // Default to EVM for now
		createdAt:       consensus.ConsensusNow(),
	}

	// Initialize production enhancements
	cbConfig := CircuitBreakerConfig{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		HalfOpenRequests: 3,
	}
	registry.circuitBreakers = NewRuntimeCircuitBreakers(cbConfig)
	registry.configValidator = NewConfigValidator()

	auditConfig := AuditLoggerConfig{
		LogDir:        "./logs",
		LogFile:       "runtime_audit.log",
		BufferSize:    1000,
		FlushSize:     100,
		FlushInterval: 30 * time.Second,
		Retention:     7 * 24 * time.Hour,
	}
	auditLogger, err := NewAuditLogger(auditConfig)
	if err != nil {
		logger.WithError(err).Warn("Failed to create audit logger, using nil")
		auditLogger = nil
	}
	registry.auditLogger = auditLogger

	registry.lifecycleManager = NewLifecycleManager(logger, auditLogger)
	registry.metrics = NewRegistryMetrics()

	return registry
}

// RegisterRuntime registers a runtime factory
func (r *RuntimeRegistry) RegisterRuntime(runtimeType RuntimeType, factory RuntimeFactory) error {
	start := consensus.ConsensusNow()

	// Execute with circuit breaker
	err := r.circuitBreakers.Execute(runtimeType, func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		if factory == nil {
			return fmt.Errorf("runtime factory cannot be nil")
		}

		if _, exists := r.runtimes[runtimeType]; exists {
			return fmt.Errorf("runtime %s already registered", runtimeType)
		}

		// Validate the factory by creating a test instance
		testInstance := factory()
		if testInstance == nil {
			return fmt.Errorf("runtime factory for %s returned nil", runtimeType)
		}

		// Verify it implements the Runtime interface
		if testInstance.Type() != runtimeType {
			return fmt.Errorf("runtime type mismatch: expected %s, got %s", runtimeType, testInstance.Type())
		}

		// Execute pre-register lifecycle hooks
		if r.lifecycleManager != nil {
			wrappedRuntime := WrapRuntimeWithLifecycle(testInstance, r.lifecycleManager)
			if err := wrappedRuntime.OnRegister(context.Background()); err != nil {
				return fmt.Errorf("pre-register hooks failed: %w", err)
			}
		}

		r.runtimes[runtimeType] = factory
		r.initOrder = append(r.initOrder, runtimeType)

		r.logger.WithFields(logrus.Fields{
			"runtimeType": runtimeType,
			"timestamp":   consensus.ConsensusNow(),
		}).Info("Runtime registered successfully")

		// Execute post-register lifecycle hooks
		if r.lifecycleManager != nil {
			wrappedRuntime := WrapRuntimeWithLifecycle(testInstance, r.lifecycleManager)
			wrappedRuntime.OnRegister(context.Background()) // Post hooks
		}

		return nil
	})

	duration := consensus.ConsensusSince(start)

	// Update metrics
	if r.metrics != nil {
		r.metrics.ObserveRegistration(err == nil, runtimeType)
	}

	// Audit log
	if r.auditLogger != nil {
		r.auditLogger.LogRegistration(runtimeType, err == nil, err, duration)
	}

	return err
}

// RegisterMetadata registers runtime metadata
func (r *RuntimeRegistry) RegisterMetadata(runtimeType RuntimeType, metadata RuntimeMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metadata.Name == "" {
		return fmt.Errorf("runtime metadata name cannot be empty")
	}

	if metadata.Version == "" {
		return fmt.Errorf("runtime metadata version cannot be empty")
	}

	// Set timestamps using consensus time for deterministic behavior
	metadata.CreatedAt = consensus.ConsensusNow()
	metadata.UpdatedAt = consensus.ConsensusNow()

	r.metadata[runtimeType] = metadata

	// Process capabilities
	if len(metadata.Capabilities) > 0 {
		capMap := make(map[RuntimeCapability]bool)
		for _, cap := range metadata.Capabilities {
			capMap[cap] = true
		}
		r.capabilities[runtimeType] = capMap
	}

	r.logger.WithFields(logrus.Fields{
		"runtimeType":  runtimeType,
		"name":         metadata.Name,
		"version":      metadata.Version,
		"capabilities": len(metadata.Capabilities),
	}).Info("Runtime metadata registered")

	return nil
}

// RegisterValidationRule registers a validation rule for a runtime
func (r *RuntimeRegistry) RegisterValidationRule(runtimeType RuntimeType, rule ValidationRule) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rule == nil {
		return fmt.Errorf("validation rule cannot be nil")
	}

	r.validationRules[runtimeType] = rule
	return nil
}

// RegisterHealthCheck registers a health check function for a runtime
func (r *RuntimeRegistry) RegisterHealthCheck(runtimeType RuntimeType, check HealthCheckFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if check == nil {
		return fmt.Errorf("health check function cannot be nil")
	}

	r.healthChecks[runtimeType] = check
	return nil
}

// GetRuntimeFactory returns a runtime factory
func (r *RuntimeRegistry) GetRuntimeFactory(runtimeType RuntimeType) (RuntimeFactory, error) {
	start := consensus.ConsensusNow()

	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, exists := r.runtimes[runtimeType]

	// Update metrics
	if r.metrics != nil {
		r.metrics.ObserveLookup(start, exists)
	}

	if !exists {
		return nil, fmt.Errorf("runtime %s not registered", runtimeType)
	}

	return factory, nil
}

// GetRuntimeMetadata returns runtime metadata
func (r *RuntimeRegistry) GetRuntimeMetadata(runtimeType RuntimeType) (RuntimeMetadata, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metadata, exists := r.metadata[runtimeType]
	return metadata, exists
}

// HasCapability checks if a runtime has a specific capability
func (r *RuntimeRegistry) HasCapability(runtimeType RuntimeType, capability RuntimeCapability) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Update metrics
	if r.metrics != nil {
		r.metrics.ObserveCapabilityQuery(capability)
	}

	if caps, exists := r.capabilities[runtimeType]; exists {
		return caps[capability]
	}
	return false
}

// ListRegisteredRuntimes returns all registered runtime types in order
func (r *RuntimeRegistry) ListRegisteredRuntimes() []RuntimeType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]RuntimeType, len(r.initOrder))
	copy(result, r.initOrder)
	return result
}

// ListRuntimesWithCapability returns runtimes with a specific capability
func (r *RuntimeRegistry) ListRuntimesWithCapability(capability RuntimeCapability) []RuntimeType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []RuntimeType
	for rt, caps := range r.capabilities {
		if caps[capability] {
			result = append(result, rt)
		}
	}
	return result
}

// CreateRuntime creates a new runtime instance
func (r *RuntimeRegistry) CreateRuntime(runtimeType RuntimeType) (Runtime, error) {
	factory, err := r.GetRuntimeFactory(runtimeType)
	if err != nil {
		return nil, err
	}

	runtime := factory()
	if runtime == nil {
		return nil, fmt.Errorf("runtime factory returned nil for %s", runtimeType)
	}

	return runtime, nil
}

// ValidateConfig validates a runtime configuration
func (r *RuntimeRegistry) ValidateConfig(runtimeType RuntimeType, config RuntimeConfig) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if runtime is registered
	if _, exists := r.runtimes[runtimeType]; !exists {
		return fmt.Errorf("runtime %s not registered", runtimeType)
	}

	// Schema validation for RuntimeSpecific config
	if r.configValidator != nil && !config.RuntimeSpecific.IsEmpty() {
		if err := r.configValidator.ValidateConfig(runtimeType, config.RuntimeSpecific.ToMap()); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// Check custom validation rule
	if rule, exists := r.validationRules[runtimeType]; exists {
		if err := rule(config); err != nil {
			return fmt.Errorf("validation failed for runtime %s: %w", runtimeType, err)
		}
	}

	// Basic validation
	if config.ChainID == "" {
		return fmt.Errorf("chain ID cannot be empty")
	}

	if config.LedgerAPI == nil {
		return fmt.Errorf("ledger API cannot be nil")
	}

	if config.StateStore == nil {
		return fmt.Errorf("state store cannot be nil")
	}

	return nil
}

// HealthCheck performs health check on a runtime
func (r *RuntimeRegistry) HealthCheck(runtimeType RuntimeType, runtime Runtime) error {
	start := consensus.ConsensusNow()

	// Execute with circuit breaker
	err := r.circuitBreakers.Execute(runtimeType, func() error {
		r.mu.RLock()
		defer r.mu.RUnlock()

		// Basic type check
		if runtime.Type() != runtimeType {
			return fmt.Errorf("runtime type mismatch: expected %s, got %s", runtimeType, runtime.Type())
		}

		// Custom health check
		if check, exists := r.healthChecks[runtimeType]; exists {
			if err := check(runtime); err != nil {
				return fmt.Errorf("health check failed for runtime %s: %w", runtimeType, err)
			}
		}

		// Default health check
		if err := runtime.HealthCheck(); err != nil {
			return fmt.Errorf("runtime health check failed: %w", err)
		}

		return nil
	})

	duration := consensus.ConsensusSince(start)

	// Update metrics
	if r.metrics != nil {
		r.metrics.ObserveHealthCheck(start, runtimeType, err == nil)
	}

	// Audit log
	if r.auditLogger != nil {
		r.auditLogger.LogHealthCheck(runtimeType, err == nil, err, duration)
	}

	return err
}

// UnregisterRuntime removes a runtime from the registry
func (r *RuntimeRegistry) UnregisterRuntime(runtimeType RuntimeType) error {
	// Execute with circuit breaker
	err := r.circuitBreakers.Execute(runtimeType, func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		factory, exists := r.runtimes[runtimeType]
		if !exists {
			return fmt.Errorf("runtime %s not registered", runtimeType)
		}

		// Execute pre-unregister lifecycle hooks
		if r.lifecycleManager != nil && factory != nil {
			runtime := factory()
			wrappedRuntime := WrapRuntimeWithLifecycle(runtime, r.lifecycleManager)
			if err := wrappedRuntime.OnUnregister(context.Background()); err != nil {
				return fmt.Errorf("pre-unregister hooks failed: %w", err)
			}
		}

		// Remove from all maps
		delete(r.runtimes, runtimeType)
		delete(r.metadata, runtimeType)
		delete(r.capabilities, runtimeType)
		delete(r.validationRules, runtimeType)
		delete(r.healthChecks, runtimeType)

		// Remove from init order
		for i, rt := range r.initOrder {
			if rt == runtimeType {
				r.initOrder = append(r.initOrder[:i], r.initOrder[i+1:]...)
				break
			}
		}

		r.logger.WithField("runtimeType", runtimeType).Info("Runtime unregistered")

		// Execute post-unregister lifecycle hooks
		if r.lifecycleManager != nil && factory != nil {
			runtime := factory()
			wrappedRuntime := WrapRuntimeWithLifecycle(runtime, r.lifecycleManager)
			wrappedRuntime.OnUnregister(context.Background()) // Post hooks
		}

		return nil
	})

	// Update metrics
	if r.metrics != nil && err == nil {
		r.metrics.ObserveUnregistration(runtimeType)
	}

	// Audit log
	if r.auditLogger != nil {
		r.auditLogger.LogEntry(AuditEntry{
			Timestamp:   consensus.ConsensusNow(),
			Action:      AuditActionUnregisterRuntime,
			Severity:    AuditSeverityInfo,
			RuntimeType: runtimeType,
			Success:     err == nil,
			ErrorMessage: func() string {
				if err != nil {
					return err.Error()
				}
				return ""
			}(),
		})
	}

	return err
}

// RuntimeRegistryStats represents structured statistics for the runtime registry
type RuntimeRegistryStats struct {
	TotalRuntimes  int                        `json:"total_runtimes"`
	ActiveRuntimes int                        `json:"active_runtimes"`
	DefaultRuntime string                     `json:"default_runtime"`
	RuntimeDetails map[string]*RuntimeDetails `json:"runtime_details"`
	RegistryUptime int64                      `json:"registry_uptime_seconds"`
	LastUpdate     string                     `json:"last_update"`
}

// RuntimeDetails represents detailed information about a specific runtime
type RuntimeDetails struct {
	Type           string `json:"type"`
	Version        string `json:"version"`
	Status         string `json:"status"`
	ContractsCount int    `json:"contracts_count"`
	LastUsed       string `json:"last_used"`
}

// GetRegistryStats returns comprehensive statistics about the runtime registry
func (r *RuntimeRegistry) GetRegistryStats() *RuntimeRegistryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := &RuntimeRegistryStats{
		TotalRuntimes:  len(r.runtimes),
		ActiveRuntimes: r.countActiveRuntimes(),
		DefaultRuntime: string(r.defaultRuntime),
		RuntimeDetails: make(map[string]*RuntimeDetails),
		RegistryUptime: int64(consensus.ConsensusSince(r.createdAt).Seconds()),
		LastUpdate:     consensus.ConsensusNow().Format(time.RFC3339),
	}

	// Populate runtime details
	for rt := range r.runtimes {
		details := &RuntimeDetails{
			Type:           string(rt),
			Version:        "1.0.0", // This would come from the runtime
			Status:         "active",
			ContractsCount: 0, // This would be tracked
			LastUsed:       consensus.ConsensusNow().Format(time.RFC3339),
		}
		stats.RuntimeDetails[string(rt)] = details
	}

	return stats
}

// countActiveRuntimes counts the number of active runtimes
func (r *RuntimeRegistry) countActiveRuntimes() int {
	count := 0
	for _, runtime := range r.runtimes {
		if runtime != nil {
			count++
		}
	}
	return count
}

// Global helper functions

// GetRuntimeFactory returns a runtime factory from the global registry
func GetRuntimeFactory(runtimeType RuntimeType) (RuntimeFactory, error) {
	return GetGlobalRegistry().GetRuntimeFactory(runtimeType)
}

// GetRuntimeMetadata returns runtime metadata from the global registry
func GetRuntimeMetadata(runtimeType RuntimeType) (RuntimeMetadata, bool) {
	return GetGlobalRegistry().GetRuntimeMetadata(runtimeType)
}

// ListRegisteredRuntimes returns all registered runtime types from the global registry
func ListRegisteredRuntimes() []RuntimeType {
	return GetGlobalRegistry().ListRegisteredRuntimes()
}

// HasCapability checks if a runtime has a capability in the global registry
func HasCapability(runtimeType RuntimeType, capability RuntimeCapability) bool {
	return GetGlobalRegistry().HasCapability(runtimeType, capability)
}

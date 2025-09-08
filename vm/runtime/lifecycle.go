// Package runtime provides lifecycle management for VM runtimes
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// LifecycleHook represents a hook function that can be registered for lifecycle events
type LifecycleHook func(ctx context.Context, runtime Runtime, metadata interface{}) error

// LifecycleStage represents a stage in the runtime lifecycle
type LifecycleStage string

const (
	// Lifecycle stages
	StagePreRegister    LifecycleStage = "PRE_REGISTER"
	StagePostRegister   LifecycleStage = "POST_REGISTER"
	StagePreInitialize  LifecycleStage = "PRE_INITIALIZE"
	StagePostInitialize LifecycleStage = "POST_INITIALIZE"
	StagePreStart       LifecycleStage = "PRE_START"
	StagePostStart      LifecycleStage = "POST_START"
	StagePreStop        LifecycleStage = "PRE_STOP"
	StagePostStop       LifecycleStage = "POST_STOP"
	StagePreUnregister  LifecycleStage = "PRE_UNREGISTER"
	StagePostUnregister LifecycleStage = "POST_UNREGISTER"
)

// LifecycleHookResult represents the result of executing a lifecycle hook
type LifecycleHookResult struct {
	Stage    LifecycleStage
	HookName string
	Success  bool
	Error    error
	Duration time.Duration
	Metadata interface{}
}

// RuntimeLifecycle interface extends Runtime with lifecycle management
type RuntimeLifecycle interface {
	Runtime

	// Lifecycle event handlers
	OnRegister(ctx context.Context) error
	OnInitialize(ctx context.Context) error
	OnStart(ctx context.Context) error
	OnStop(ctx context.Context) error
	OnUnregister(ctx context.Context) error
}

// LifecycleManager manages lifecycle hooks for runtimes
type LifecycleManager struct {
	mu          sync.RWMutex
	hooks       map[LifecycleStage][]NamedHook
	globalHooks map[LifecycleStage][]NamedHook
	metrics     *LifecycleMetrics
	logger      interface{} // Can be *logrus.Logger
	auditLogger *AuditLogger
}

// NamedHook is a lifecycle hook with a name for identification
type NamedHook struct {
	Name     string
	Hook     LifecycleHook
	Priority int // Lower numbers execute first
	Timeout  time.Duration
}

// LifecycleMetrics tracks lifecycle metrics
type LifecycleMetrics struct {
	HookExecutions   map[string]uint64
	HookFailures     map[string]uint64
	HookDurations    map[string]time.Duration
	StageTransitions map[string]uint64
	mu               sync.RWMutex
}

// NewLifecycleManager creates a new lifecycle manager
func NewLifecycleManager(logger interface{}, auditLogger *AuditLogger) *LifecycleManager {
	return &LifecycleManager{
		hooks:       make(map[LifecycleStage][]NamedHook),
		globalHooks: make(map[LifecycleStage][]NamedHook),
		metrics:     newLifecycleMetrics(),
		logger:      logger,
		auditLogger: auditLogger,
	}
}

// RegisterHook registers a lifecycle hook for a specific runtime type and stage
func (lm *LifecycleManager) RegisterHook(runtimeType RuntimeType, stage LifecycleStage, hook NamedHook) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if hook.Hook == nil {
		return errors.New("hook function cannot be nil")
	}
	if hook.Name == "" {
		return errors.New("hook name is required")
	}
	if hook.Timeout == 0 {
		hook.Timeout = 30 * time.Second // Default timeout
	}

	key := lm.makeKey(runtimeType, stage)
	lm.hooks[key] = lm.insertHook(lm.hooks[key], hook)

	return nil
}

// RegisterGlobalHook registers a global lifecycle hook for all runtimes
func (lm *LifecycleManager) RegisterGlobalHook(stage LifecycleStage, hook NamedHook) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if hook.Hook == nil {
		return errors.New("hook function cannot be nil")
	}
	if hook.Name == "" {
		return errors.New("hook name is required")
	}
	if hook.Timeout == 0 {
		hook.Timeout = 30 * time.Second
	}

	lm.globalHooks[stage] = lm.insertHook(lm.globalHooks[stage], hook)

	return nil
}

// ExecuteHooks executes all hooks for a given runtime and stage
func (lm *LifecycleManager) ExecuteHooks(ctx context.Context, runtime Runtime, stage LifecycleStage, metadata interface{}) []LifecycleHookResult {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var results []LifecycleHookResult

	// Execute global hooks first
	globalResults := lm.executeHookList(ctx, runtime, stage, lm.globalHooks[stage], metadata)
	results = append(results, globalResults...)

	// Execute runtime-specific hooks
	key := lm.makeKey(runtime.Type(), stage)
	specificResults := lm.executeHookList(ctx, runtime, stage, lm.hooks[key], metadata)
	results = append(results, specificResults...)

	// Update metrics
	lm.metrics.recordStageTransition(string(stage))

	return results
}

// executeHookList executes a list of hooks
func (lm *LifecycleManager) executeHookList(ctx context.Context, runtime Runtime, stage LifecycleStage, hooks []NamedHook, metadata interface{}) []LifecycleHookResult {
	var results []LifecycleHookResult

	for _, hook := range hooks {
		result := lm.executeHook(ctx, runtime, stage, hook, metadata)
		results = append(results, result)

		// Stop on first failure unless it's a post-stage hook
		if !result.Success && !isPostStage(stage) {
			break
		}
	}

	return results
}

// executeHook executes a single lifecycle hook with timeout and error handling
func (lm *LifecycleManager) executeHook(ctx context.Context, runtime Runtime, stage LifecycleStage, hook NamedHook, metadata interface{}) LifecycleHookResult {
	start := consensus.ConsensusNow()

	// Create timeout context
	hookCtx, cancel := context.WithTimeout(ctx, hook.Timeout)
	defer cancel()

	// Channel for hook result
	resultChan := make(chan error, 1)

	// Execute hook in goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				resultChan <- fmt.Errorf("hook panicked: %v", r)
			}
		}()
		resultChan <- hook.Hook(hookCtx, runtime, metadata)
	}()

	// Wait for result or timeout
	var err error
	select {
	case err = <-resultChan:
		// Hook completed
	case <-hookCtx.Done():
		err = fmt.Errorf("hook timeout after %v", hook.Timeout)
	}

	duration := consensus.ConsensusSince(start)
	success := err == nil

	// Update metrics
	lm.metrics.recordHookExecution(hook.Name, success, duration)

	// Audit log
	if lm.auditLogger != nil {
		lm.logHookExecution(runtime.Type(), stage, hook.Name, success, err, duration)
	}

	return LifecycleHookResult{
		Stage:    stage,
		HookName: hook.Name,
		Success:  success,
		Error:    err,
		Duration: duration,
		Metadata: metadata,
	}
}

// RuntimeWithLifecycle wraps a runtime with lifecycle management
type RuntimeWithLifecycle struct {
	Runtime
	lifecycleManager *LifecycleManager
	state            RuntimeState
	mu               sync.RWMutex
}

// RuntimeState represents the current state of a runtime
type RuntimeState struct {
	Registered  bool
	Initialized bool
	Started     bool
	Metadata    map[string]interface{}
}

// WrapRuntimeWithLifecycle wraps a runtime with lifecycle support
func WrapRuntimeWithLifecycle(runtime Runtime, manager *LifecycleManager) *RuntimeWithLifecycle {
	return &RuntimeWithLifecycle{
		Runtime:          runtime,
		lifecycleManager: manager,
		state: RuntimeState{
			Metadata: make(map[string]interface{}),
		},
	}
}

// OnRegister is called when the runtime is registered
func (r *RuntimeWithLifecycle) OnRegister(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Registered {
		return errors.New("runtime already registered")
	}

	// Execute pre-register hooks
	results := r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePreRegister, nil)
	if err := checkHookResults(results); err != nil {
		return fmt.Errorf("pre-register hooks failed: %w", err)
	}

	// Mark as registered
	r.state.Registered = true

	// Execute post-register hooks
	r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePostRegister, nil)

	return nil
}

// OnInitialize is called when the runtime is initialized
func (r *RuntimeWithLifecycle) OnInitialize(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.state.Registered {
		return errors.New("runtime not registered")
	}
	if r.state.Initialized {
		return errors.New("runtime already initialized")
	}

	// Execute pre-initialize hooks
	results := r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePreInitialize, nil)
	if err := checkHookResults(results); err != nil {
		return fmt.Errorf("pre-initialize hooks failed: %w", err)
	}

	// Mark as initialized
	r.state.Initialized = true

	// Execute post-initialize hooks
	r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePostInitialize, nil)

	return nil
}

// OnStart is called when the runtime is started
func (r *RuntimeWithLifecycle) OnStart(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.state.Initialized {
		return errors.New("runtime not initialized")
	}
	if r.state.Started {
		return errors.New("runtime already started")
	}

	// Execute pre-start hooks
	results := r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePreStart, nil)
	if err := checkHookResults(results); err != nil {
		return fmt.Errorf("pre-start hooks failed: %w", err)
	}

	// Mark as started
	r.state.Started = true

	// Execute post-start hooks
	r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePostStart, nil)

	return nil
}

// OnStop is called when the runtime is stopped
func (r *RuntimeWithLifecycle) OnStop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.state.Started {
		return errors.New("runtime not started")
	}

	// Execute pre-stop hooks
	results := r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePreStop, nil)
	if err := checkHookResults(results); err != nil {
		return fmt.Errorf("pre-stop hooks failed: %w", err)
	}

	// Mark as stopped
	r.state.Started = false

	// Execute post-stop hooks
	r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePostStop, nil)

	return nil
}

// OnUnregister is called when the runtime is unregistered
func (r *RuntimeWithLifecycle) OnUnregister(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.state.Registered {
		return errors.New("runtime not registered")
	}
	if r.state.Started {
		return errors.New("cannot unregister started runtime")
	}

	// Execute pre-unregister hooks
	results := r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePreUnregister, nil)
	if err := checkHookResults(results); err != nil {
		return fmt.Errorf("pre-unregister hooks failed: %w", err)
	}

	// Mark as unregistered
	r.state.Registered = false
	r.state.Initialized = false

	// Execute post-unregister hooks
	r.lifecycleManager.ExecuteHooks(ctx, r.Runtime, StagePostUnregister, nil)

	return nil
}

// Helper functions

func (lm *LifecycleManager) makeKey(runtimeType RuntimeType, stage LifecycleStage) LifecycleStage {
	return LifecycleStage(fmt.Sprintf("%s:%s", runtimeType, stage))
}

func (lm *LifecycleManager) insertHook(hooks []NamedHook, hook NamedHook) []NamedHook {
	// Insert hook in priority order
	for i, h := range hooks {
		if hook.Priority < h.Priority {
			return append(hooks[:i], append([]NamedHook{hook}, hooks[i:]...)...)
		}
	}
	return append(hooks, hook)
}

// logHookExecution logs hook execution for audit purposes
func (lm *LifecycleManager) logHookExecution(runtimeType RuntimeType, stage LifecycleStage, hookName string, success bool, err error, duration time.Duration) {
	severity := AuditSeverityInfo
	if !success {
		severity = AuditSeverityError
	}

	entry := AuditEntry{
		Timestamp:   consensus.ConsensusNow(),
		Action:      AuditActionHealthCheck, // Using existing action type
		Severity:    severity,
		RuntimeType: runtimeType,
		Success:     success,
		Duration:    duration,
		Details: map[string]interface{}{
			"stage":    string(stage),
			"hookName": hookName,
		},
	}

	if err != nil {
		entry.ErrorMessage = err.Error()
	}

	lm.auditLogger.LogEntry(entry)
}

func isPostStage(stage LifecycleStage) bool {
	switch stage {
	case StagePostRegister, StagePostInitialize, StagePostStart, StagePostStop, StagePostUnregister:
		return true
	default:
		return false
	}
}

func checkHookResults(results []LifecycleHookResult) error {
	for _, result := range results {
		if !result.Success {
			return fmt.Errorf("hook '%s' failed: %v", result.HookName, result.Error)
		}
	}
	return nil
}

func newLifecycleMetrics() *LifecycleMetrics {
	return &LifecycleMetrics{
		HookExecutions:   make(map[string]uint64),
		HookFailures:     make(map[string]uint64),
		HookDurations:    make(map[string]time.Duration),
		StageTransitions: make(map[string]uint64),
	}
}

func (m *LifecycleMetrics) recordHookExecution(hookName string, success bool, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.HookExecutions[hookName]++
	m.HookDurations[hookName] += duration

	if !success {
		m.HookFailures[hookName]++
	}
}

func (m *LifecycleMetrics) recordStageTransition(stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.StageTransitions[stage]++
}

// Common lifecycle hooks

// NewValidationHook creates a hook that validates runtime configuration
func NewValidationHook(validator *ConfigValidator) NamedHook {
	return NamedHook{
		Name:     "ConfigValidation",
		Priority: 10,
		Timeout:  5 * time.Second,
		Hook: func(ctx context.Context, runtime Runtime, metadata interface{}) error {
			// Extract config from metadata
			config, ok := metadata.(map[string]interface{})
			if !ok {
				return nil // Skip validation if no config
			}

			return validator.ValidateConfig(runtime.Type(), config)
		},
	}
}

// NewHealthCheckHook creates a hook that performs health check
func NewHealthCheckHook() NamedHook {
	return NamedHook{
		Name:     "HealthCheck",
		Priority: 20,
		Timeout:  10 * time.Second,
		Hook: func(ctx context.Context, runtime Runtime, metadata interface{}) error {
			return runtime.HealthCheck()
		},
	}
}

// NewMetricsHook creates a hook that updates metrics
func NewMetricsHook(metrics *RegistryMetrics) NamedHook {
	return NamedHook{
		Name:     "MetricsUpdate",
		Priority: 100, // Low priority
		Timeout:  1 * time.Second,
		Hook: func(ctx context.Context, runtime Runtime, metadata interface{}) error {
			// Update metrics based on runtime state
			metrics.RuntimesRegistered.WithLabelValues(string(runtime.Type())).Inc()
			return nil
		},
	}
}

// NewResourceCleanupHook creates a hook that cleans up resources
func NewResourceCleanupHook() NamedHook {
	return NamedHook{
		Name:     "ResourceCleanup",
		Priority: 90,
		Timeout:  30 * time.Second,
		Hook: func(ctx context.Context, runtime Runtime, metadata interface{}) error {
			// Log cleanup completion
			if logger, ok := metadata.(*logrus.Logger); ok {
				logger.WithField("runtime", runtime.Type()).Info("Resource cleanup completed")
			}

			return nil
		},
	}
}

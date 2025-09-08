// consensus/recovery/recovery_manager.go

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"diamante/consensus"
	"diamante/consensus/types"
)

// RecoveryState represents the current state of the recovery process
type RecoveryState int

const (
	// Idle indicates no recovery is in progress
	Idle RecoveryState = iota
	// Recovering indicates a recovery is in progress
	Recovering
	// Failed indicates a recovery attempt failed
	Failed
	// Succeeded indicates a recovery attempt succeeded
	Succeeded
)

// String returns a string representation of the recovery state
func (s RecoveryState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Recovering:
		return "Recovering"
	case Failed:
		return "Failed"
	case Succeeded:
		return "Succeeded"
	default:
		return "Unknown"
	}
}

// ErrorSeverity represents the severity of an error
type ErrorSeverity int

const (
	// Minor indicates a minor error that can be recovered from automatically
	Minor ErrorSeverity = iota
	// Moderate indicates a moderate error that may require manual intervention
	Moderate
	// Severe indicates a severe error that requires manual intervention
	Severe
	// Critical indicates a critical error that requires immediate attention
	Critical
)

// String returns a string representation of the error severity
func (s ErrorSeverity) String() string {
	switch s {
	case Minor:
		return "Minor"
	case Moderate:
		return "Moderate"
	case Severe:
		return "Severe"
	case Critical:
		return "Critical"
	default:
		return "Unknown"
	}
}

// RecoveryStrategy defines the approach to recover from an error
type RecoveryStrategy int

const (
	// StateRollback rolls back to a previous known good state
	StateRollback RecoveryStrategy = iota
	// StateResync resyncs the state from peers
	StateResync
	// Restart restarts the consensus process
	Restart
	// Manual requires manual intervention
	Manual
)

// String returns a string representation of the recovery strategy
func (s RecoveryStrategy) String() string {
	switch s {
	case StateRollback:
		return "StateRollback"
	case StateResync:
		return "StateResync"
	case Restart:
		return "Restart"
	case Manual:
		return "Manual"
	default:
		return "Unknown"
	}
}

// RecoveryError represents an error that occurred during recovery
type RecoveryError struct {
	Err         error
	Severity    ErrorSeverity
	Strategy    RecoveryStrategy
	Component   string
	Description string
	Timestamp   time.Time
	Context     map[string]interface{}
}

// Error implements the error interface
func (e *RecoveryError) Error() string {
	return fmt.Sprintf("[%s] %s error in %s: %v (strategy: %s)",
		e.Timestamp.Format(time.RFC3339),
		e.Severity.String(),
		e.Component,
		e.Err,
		e.Strategy.String())
}

// Unwrap returns the underlying error
func (e *RecoveryError) Unwrap() error {
	return e.Err
}

// RecoveryManager handles error recovery for the consensus system
type RecoveryManager struct {
	// Configuration
	maxRetries        int
	retryDelay        time.Duration
	checkpointEnabled bool
	autoRecovery      bool

	// External dependencies
	checkpointManager *CheckpointManager
	consensus         types.Consensus

	// State
	mu            sync.RWMutex
	state         RecoveryState
	lastError     *RecoveryError
	recoveryCount map[string]int // Tracks recovery attempts per component
	lastRecovery  time.Time

	// Callbacks
	onRecoveryStart    func(component string, err error) error
	onRecoveryComplete func(component string, success bool) error

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Logger
	logger *logrus.Logger
}

// RecoveryOption defines functional options for RecoveryManager
type RecoveryOption func(*RecoveryManager)

// WithMaxRetries sets the maximum number of retry attempts
func WithMaxRetries(max int) RecoveryOption {
	return func(rm *RecoveryManager) {
		if max > 0 {
			rm.maxRetries = max
		}
	}
}

// WithRetryDelay sets the delay between retry attempts
func WithRetryDelay(delay time.Duration) RecoveryOption {
	return func(rm *RecoveryManager) {
		if delay > 0 {
			rm.retryDelay = delay
		}
	}
}

// WithCheckpointEnabled enables or disables checkpoint-based recovery
func WithCheckpointEnabled(enabled bool) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.checkpointEnabled = enabled
	}
}

// WithAutoRecovery enables or disables automatic recovery
func WithAutoRecovery(enabled bool) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.autoRecovery = enabled
	}
}

// WithLogger sets a custom logger
func WithLogger(logger *logrus.Logger) RecoveryOption {
	return func(rm *RecoveryManager) {
		if logger != nil {
			rm.logger = logger
		}
	}
}

// WithRecoveryCheckpointManager sets the checkpoint manager used for rollback and resync
func WithRecoveryCheckpointManager(cm *CheckpointManager) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.checkpointManager = cm
	}
}

// WithRecoveryConsensus sets the consensus instance used for resync and restart
func WithRecoveryConsensus(cons types.Consensus) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.consensus = cons
	}
}

// WithOnRecoveryStart sets a callback to be called when recovery starts
func WithOnRecoveryStart(callback func(component string, err error) error) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.onRecoveryStart = callback
	}
}

// WithOnRecoveryComplete sets a callback to be called when recovery completes
func WithOnRecoveryComplete(callback func(component string, success bool) error) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.onRecoveryComplete = callback
	}
}

// NewRecoveryManager creates a new RecoveryManager with the given options
func NewRecoveryManager(options ...RecoveryOption) *RecoveryManager {
	ctx, cancel := context.WithCancel(context.Background())

	rm := &RecoveryManager{
		maxRetries:        3,
		retryDelay:        5 * time.Second,
		checkpointEnabled: true,
		autoRecovery:      true,
		state:             Idle,
		recoveryCount:     make(map[string]int),
		ctx:               ctx,
		cancel:            cancel,
		logger:            logrus.New(),
	}

	// Apply options
	for _, option := range options {
		option(rm)
	}

	return rm
}

// GetState returns the current recovery state
func (rm *RecoveryManager) GetState() RecoveryState {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.state
}

// GetLastError returns the last error that occurred
func (rm *RecoveryManager) GetLastError() *RecoveryError {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.lastError
}

// RecoverFromError attempts to recover from an error
func (rm *RecoveryManager) RecoverFromError(component string, err error, severity ErrorSeverity) error {
	if err == nil {
		return nil
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Create recovery error
	recoveryErr := &RecoveryError{
		Err:         err,
		Severity:    severity,
		Component:   component,
		Description: err.Error(),
		Timestamp:   consensus.ConsensusNow(),
		Context:     make(map[string]interface{}),
	}

	// Determine recovery strategy based on severity
	switch severity {
	case Minor:
		recoveryErr.Strategy = StateRollback
	case Moderate:
		recoveryErr.Strategy = StateResync
	case Severe:
		recoveryErr.Strategy = Restart
	case Critical:
		recoveryErr.Strategy = Manual
	}

	// Update state
	rm.state = Recovering
	rm.lastError = recoveryErr
	rm.recoveryCount[component]++
	rm.lastRecovery = consensus.ConsensusNow()

	// Log the error
	rm.logger.WithFields(logrus.Fields{
		"component":   component,
		"severity":    severity.String(),
		"strategy":    recoveryErr.Strategy.String(),
		"error":       err.Error(),
		"recoveryNum": rm.recoveryCount[component],
	}).Error("Starting recovery process")

	// Check if we've exceeded the maximum number of retries
	if rm.recoveryCount[component] > rm.maxRetries {
		rm.state = Failed
		rm.logger.WithField("component", component).Error(
			"Exceeded maximum recovery attempts")
		return fmt.Errorf("exceeded maximum recovery attempts for %s: %w", component, err)
	}

	// If auto-recovery is disabled, just return
	if !rm.autoRecovery {
		rm.logger.WithField("component", component).Warn(
			"Auto-recovery disabled, manual intervention required")
		return fmt.Errorf("auto-recovery disabled, manual intervention required: %w", err)
	}

	// Call the onRecoveryStart callback if set
	if rm.onRecoveryStart != nil {
		if callbackErr := rm.onRecoveryStart(component, err); callbackErr != nil {
			rm.logger.WithFields(logrus.Fields{
				"component": component,
				"error":     callbackErr.Error(),
			}).Error("Recovery start callback failed")
			return fmt.Errorf("recovery start callback failed: %w", callbackErr)
		}
	}

	// Implement recovery based on strategy
	var recoveryErr2 error
	switch recoveryErr.Strategy {
	case StateRollback:
		recoveryErr2 = rm.performStateRollback(component)
	case StateResync:
		recoveryErr2 = rm.performStateResync(component)
	case Restart:
		recoveryErr2 = rm.performRestart(component)
	case Manual:
		rm.logger.WithField("component", component).Error(
			"Manual intervention required")
		recoveryErr2 = errors.New("manual intervention required")
	}

	// Update state based on recovery result
	if recoveryErr2 != nil {
		rm.state = Failed
		rm.logger.WithFields(logrus.Fields{
			"component": component,
			"error":     recoveryErr2.Error(),
			"strategy":  recoveryErr.Strategy.String(),
		}).Error("Recovery failed")

		// Call the onRecoveryComplete callback if set
		if rm.onRecoveryComplete != nil {
			if callbackErr := rm.onRecoveryComplete(component, false); callbackErr != nil {
				rm.logger.WithError(callbackErr).Warn("Recovery completion callback failed")
			}
		}

		return fmt.Errorf("recovery failed: %w", recoveryErr2)
	}

	// Recovery succeeded
	rm.state = Succeeded
	rm.logger.WithFields(logrus.Fields{
		"component": component,
		"strategy":  recoveryErr.Strategy.String(),
	}).Info("Recovery succeeded")

	// Call the onRecoveryComplete callback if set
	if rm.onRecoveryComplete != nil {
		if callbackErr := rm.onRecoveryComplete(component, true); callbackErr != nil {
			rm.logger.WithFields(logrus.Fields{
				"component": component,
				"error":     callbackErr.Error(),
			}).Error("Recovery complete callback failed")
		}
	}

	return nil
}

// performStateRollback implements state rollback recovery
func (rm *RecoveryManager) performStateRollback(component string) error {
	if rm.checkpointManager == nil {
		return errors.New("checkpoint manager not configured")
	}

	cp, err := rm.checkpointManager.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	rm.logger.WithFields(logrus.Fields{
		"component": component,
		"height":    cp.Metadata.Height,
	}).Info("Performing state rollback recovery")

	if err := rm.checkpointManager.RestoreFromCheckpoint(cp.Metadata.Height); err != nil {
		return fmt.Errorf("failed to restore from checkpoint: %w", err)
	}

	return nil
}

// performStateResync implements state resync recovery
func (rm *RecoveryManager) performStateResync(component string) error {
	if rm.checkpointManager == nil {
		return errors.New("checkpoint manager not configured")
	}
	if rm.consensus == nil {
		return errors.New("consensus not configured")
	}

	cp, err := rm.checkpointManager.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	// Load PoH state from checkpoint
	data, ok := cp.ComponentStates["poh"]
	if !ok {
		stateFile := filepath.Join(cp.Path, "poh.state")
		data, err = os.ReadFile(stateFile)
		if err != nil {
			return fmt.Errorf("failed to read PoH state: %w", err)
		}
	}

	var pohState struct {
		State [32]byte `json:"state"`
		Count uint64   `json:"count"`
	}
	if err := json.Unmarshal(data, &pohState); err != nil {
		return fmt.Errorf("failed to decode PoH state: %w", err)
	}

	rm.logger.WithFields(logrus.Fields{
		"component": component,
		"height":    cp.Metadata.Height,
	}).Info("Performing state resync recovery")

	if err := rm.consensus.SynchronizeState(pohState.State, pohState.Count); err != nil {
		return fmt.Errorf("synchronize state failed: %w", err)
	}

	return nil
}

// performRestart implements restart recovery
func (rm *RecoveryManager) performRestart(component string) error {
	if rm.consensus == nil {
		return errors.New("consensus not configured")
	}

	rm.logger.WithField("component", component).Info("Performing restart recovery")

	if err := rm.consensus.Stop(); err != nil {
		return fmt.Errorf("failed to stop component: %w", err)
	}

	// Use context-aware timer instead of time.Sleep
	select {
	case <-rm.ctx.Done():
		return fmt.Errorf("context cancelled during restart delay")
	case <-time.After(rm.retryDelay):
		// Continue with restart
	}

	if err := rm.consensus.Start(); err != nil {
		return fmt.Errorf("failed to start component: %w", err)
	}

	return nil
}

// ResetRecoveryCount resets the recovery count for a component
func (rm *RecoveryManager) ResetRecoveryCount(component string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.recoveryCount[component] = 0
}

// Close releases resources used by the RecoveryManager
func (rm *RecoveryManager) Close() {
	rm.cancel()
}

// IsRecovering returns true if a recovery is in progress
func (rm *RecoveryManager) IsRecovering() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.state == Recovering
}

// GetRecoveryStats returns statistics about recovery attempts
func (rm *RecoveryManager) GetRecoveryStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["state"] = rm.state.String()
	stats["recoveryCount"] = rm.recoveryCount
	stats["lastRecovery"] = rm.lastRecovery

	if rm.lastError != nil {
		stats["lastError"] = map[string]interface{}{
			"component":   rm.lastError.Component,
			"severity":    rm.lastError.Severity.String(),
			"strategy":    rm.lastError.Strategy.String(),
			"description": rm.lastError.Description,
			"timestamp":   rm.lastError.Timestamp,
		}
	}

	return stats
}

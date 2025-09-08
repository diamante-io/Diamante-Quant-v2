// consensus/common/errors.go

package common

import (
	"fmt"
	"time"

	"diamante/consensus"
)

// ErrorCategory defines the general category of a consensus error
type ErrorCategory int

const (
	// ErrorCategoryTemporary indicates a temporary error that can be retried
	ErrorCategoryTemporary ErrorCategory = iota
	// ErrorCategoryPermanent indicates a permanent error that requires manual intervention
	ErrorCategoryPermanent
	// ErrorCategoryByzantine indicates a potential Byzantine behavior
	ErrorCategoryByzantine
	// ErrorCategoryNetwork indicates a network-related error
	ErrorCategoryNetwork
	// ErrorCategoryState indicates a state inconsistency error
	ErrorCategoryState
	// ErrorCategoryConfiguration indicates a configuration error
	ErrorCategoryConfiguration
)

// String returns a string representation of the error category
func (c ErrorCategory) String() string {
	switch c {
	case ErrorCategoryTemporary:
		return "Temporary"
	case ErrorCategoryPermanent:
		return "Permanent"
	case ErrorCategoryByzantine:
		return "Byzantine"
	case ErrorCategoryNetwork:
		return "Network"
	case ErrorCategoryState:
		return "State"
	case ErrorCategoryConfiguration:
		return "Configuration"
	default:
		return "Unknown"
	}
}

// ConsensusErrorCode defines specific error codes for consensus errors
type ConsensusErrorCode int

const (
	// General errors
	ErrUnknown ConsensusErrorCode = iota
	ErrTimeout
	ErrCanceled

	// Validator errors
	ErrInvalidValidator
	ErrValidatorNotFound
	ErrInsufficientStake
	ErrValidatorSetInconsistency

	// Event errors
	ErrEventCreationFailed
	ErrEventValidationFailed
	ErrEventFinalizationFailed
	ErrEventDuplicate
	ErrEventTimeout
	ErrEventRejected

	// Block errors
	ErrBlockCreationFailed
	ErrBlockValidationFailed
	ErrBlockFinalizationFailed
	ErrInvalidBlockNumber
	ErrInvalidBlockProducer

	// PoH errors
	ErrPoHVerificationFailed
	ErrPoHDriftExceeded
	ErrPoHSynchronizationFailed

	// State errors
	ErrStateCorruption
	ErrStateInconsistency
	ErrStateSynchronizationFailed

	// Checkpoint errors
	ErrCheckpointCreationFailed
	ErrCheckpointRestorationFailed
	ErrCheckpointNotFound
	ErrInvalidCheckpoint

	// Network errors
	ErrNetworkPartition
	ErrNetworkOverload
	ErrMessagePropagationFailed

	// Configuration errors
	ErrInvalidConfiguration
	ErrIncompatibleConfiguration
)

// String returns a string representation of the error code
func (c ConsensusErrorCode) String() string {
	switch c {
	// General errors
	case ErrUnknown:
		return "UnknownError"
	case ErrTimeout:
		return "Timeout"
	case ErrCanceled:
		return "Canceled"

	// Validator errors
	case ErrInvalidValidator:
		return "InvalidValidator"
	case ErrValidatorNotFound:
		return "ValidatorNotFound"
	case ErrInsufficientStake:
		return "InsufficientStake"
	case ErrValidatorSetInconsistency:
		return "ValidatorSetInconsistency"

	// Event errors
	case ErrEventCreationFailed:
		return "EventCreationFailed"
	case ErrEventValidationFailed:
		return "EventValidationFailed"
	case ErrEventFinalizationFailed:
		return "EventFinalizationFailed"
	case ErrEventDuplicate:
		return "EventDuplicate"
	case ErrEventTimeout:
		return "EventTimeout"
	case ErrEventRejected:
		return "EventRejected"

	// Block errors
	case ErrBlockCreationFailed:
		return "BlockCreationFailed"
	case ErrBlockValidationFailed:
		return "BlockValidationFailed"
	case ErrBlockFinalizationFailed:
		return "BlockFinalizationFailed"
	case ErrInvalidBlockNumber:
		return "InvalidBlockNumber"
	case ErrInvalidBlockProducer:
		return "InvalidBlockProducer"

	// PoH errors
	case ErrPoHVerificationFailed:
		return "PoHVerificationFailed"
	case ErrPoHDriftExceeded:
		return "PoHDriftExceeded"
	case ErrPoHSynchronizationFailed:
		return "PoHSynchronizationFailed"

	// State errors
	case ErrStateCorruption:
		return "StateCorruption"
	case ErrStateInconsistency:
		return "StateInconsistency"
	case ErrStateSynchronizationFailed:
		return "StateSynchronizationFailed"

	// Checkpoint errors
	case ErrCheckpointCreationFailed:
		return "CheckpointCreationFailed"
	case ErrCheckpointRestorationFailed:
		return "CheckpointRestorationFailed"
	case ErrCheckpointNotFound:
		return "CheckpointNotFound"
	case ErrInvalidCheckpoint:
		return "InvalidCheckpoint"

	// Network errors
	case ErrNetworkPartition:
		return "NetworkPartition"
	case ErrNetworkOverload:
		return "NetworkOverload"
	case ErrMessagePropagationFailed:
		return "MessagePropagationFailed"

	// Configuration errors
	case ErrInvalidConfiguration:
		return "InvalidConfiguration"
	case ErrIncompatibleConfiguration:
		return "IncompatibleConfiguration"

	default:
		return fmt.Sprintf("UnknownErrorCode(%d)", c)
	}
}

// ConsensusError represents a structured error in the consensus system
type ConsensusError struct {
	// Error code and category
	Code     ConsensusErrorCode
	Category ErrorCategory

	// Error details
	Message string
	Cause   error

	// Context information
	BlockNumber uint64
	EventID     [32]byte
	ValidatorID [32]byte
	Timestamp   time.Time

	// Recovery information
	Retryable      bool
	RetryAfter     time.Duration
	RecoveryAction string

	// Additional context
	Context *ErrorContext
}

// Error implements the error interface
func (e *ConsensusError) Error() string {
	baseMsg := fmt.Sprintf("[%s:%s] %s", e.Category, e.Code, e.Message)

	if e.Cause != nil {
		baseMsg += fmt.Sprintf(" (caused by: %v)", e.Cause)
	}

	if e.BlockNumber > 0 {
		baseMsg += fmt.Sprintf(" [block=%d]", e.BlockNumber)
	}

	if e.Retryable {
		if e.RetryAfter > 0 {
			baseMsg += fmt.Sprintf(" (retryable after %s)", e.RetryAfter)
		} else {
			baseMsg += " (retryable)"
		}
	}

	return baseMsg
}

// Unwrap returns the underlying cause of the error
func (e *ConsensusError) Unwrap() error {
	return e.Cause
}

// WithContext adds context information to the error
func (e *ConsensusError) WithContext(key string, value string) *ConsensusError {
	if e.Context == nil {
		e.Context = NewErrorContext()
	}
	e.Context.AddString(key, value)
	return e
}

// WithBlockNumber adds block number information to the error
func (e *ConsensusError) WithBlockNumber(blockNumber uint64) *ConsensusError {
	e.BlockNumber = blockNumber
	return e
}

// WithEventID adds event ID information to the error
func (e *ConsensusError) WithEventID(eventID [32]byte) *ConsensusError {
	e.EventID = eventID
	return e
}

// WithValidatorID adds validator ID information to the error
func (e *ConsensusError) WithValidatorID(validatorID [32]byte) *ConsensusError {
	e.ValidatorID = validatorID
	return e
}

// WithRetryInfo adds retry information to the error
func (e *ConsensusError) WithRetryInfo(retryable bool, retryAfter time.Duration) *ConsensusError {
	e.Retryable = retryable
	e.RetryAfter = retryAfter
	return e
}

// WithRecoveryAction adds recovery action information to the error
func (e *ConsensusError) WithRecoveryAction(action string) *ConsensusError {
	e.RecoveryAction = action
	return e
}

// ErrorFramework provides centralized error handling for consensus operations
type ErrorFramework struct {
	recoveryManager RecoveryManager
	logger          Logger
	errorTracker    *ErrorTracker
}

// RecoveryManager interface for error recovery operations
type RecoveryManager interface {
	HandleError(err error) error
}

// ErrorTracker tracks error statistics and patterns
type ErrorTracker struct {
	errorCounts map[ConsensusErrorCode]uint64
	lastErrors  map[ConsensusErrorCode]*ConsensusError
	errorRates  map[ConsensusErrorCode]float64
	totalErrors uint64
	startTime   time.Time
}

// NewErrorTracker creates a new error tracker
func NewErrorTracker() *ErrorTracker {
	return &ErrorTracker{
		errorCounts: make(map[ConsensusErrorCode]uint64),
		lastErrors:  make(map[ConsensusErrorCode]*ConsensusError),
		errorRates:  make(map[ConsensusErrorCode]float64),
		startTime:   consensus.ConsensusNow(),
	}
}

// TrackError records an error occurrence
func (et *ErrorTracker) TrackError(err *ConsensusError) {
	et.errorCounts[err.Code]++
	et.lastErrors[err.Code] = err
	et.totalErrors++

	// Calculate error rate (errors per hour)
	duration := consensus.ConsensusSince(et.startTime).Hours()
	if duration > 0 {
		et.errorRates[err.Code] = float64(et.errorCounts[err.Code]) / duration
	}
}

// GetErrorStats returns error statistics
func (et *ErrorTracker) GetErrorStats() *ErrorStats {
	stats := NewErrorStats()
	stats.TotalErrors = et.totalErrors
	stats.Uptime = consensus.ConsensusSince(et.startTime)
	stats.ErrorCounts = et.errorCounts
	stats.ErrorRates = et.errorRates

	// Add most frequent errors
	for code, count := range et.errorCounts {
		if count > 0 {
			stats.MostFrequentErrors[code.String()] = count
		}
	}

	return stats
}

// NewErrorFramework creates a new error framework
func NewErrorFramework(recoveryManager RecoveryManager, logger Logger) *ErrorFramework {
	return &ErrorFramework{
		recoveryManager: recoveryManager,
		logger:          logger,
		errorTracker:    NewErrorTracker(),
	}
}

// CreateError creates a new ConsensusError with the given code, category, and message
func (ef *ErrorFramework) CreateError(code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	err := &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Timestamp: consensus.ConsensusNow(),
		Context:   NewErrorContext(),
	}

	// Track the error
	ef.errorTracker.TrackError(err)

	// Log the error
	ef.logger.Error("Consensus error created",
		"code", code.String(),
		"category", category.String(),
		"message", message)

	return err
}

// WrapError wraps an existing error in a ConsensusError
func (ef *ErrorFramework) WrapError(err error, code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	consensusErr := &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Cause:     err,
		Timestamp: consensus.ConsensusNow(),
		Context:   NewErrorContext(),
	}

	// Track the error
	ef.errorTracker.TrackError(consensusErr)

	// Log the error
	ef.logger.Error("Error wrapped in consensus error",
		"code", code.String(),
		"category", category.String(),
		"message", message,
		"cause", err.Error())

	return consensusErr
}

// HandleError processes an error through the recovery manager
func (ef *ErrorFramework) HandleError(err error) error {
	// If it's already a ConsensusError, track it
	if consensusErr, ok := err.(*ConsensusError); ok {
		ef.errorTracker.TrackError(consensusErr)
	}

	// Attempt recovery
	if ef.recoveryManager != nil {
		return ef.recoveryManager.HandleError(err)
	}

	// No recovery manager available
	ef.logger.Error("No recovery manager available for error", "error", err.Error())
	return err
}

// TrackError manually tracks an error
func (ef *ErrorFramework) TrackError(err error) {
	if consensusErr, ok := err.(*ConsensusError); ok {
		ef.errorTracker.TrackError(consensusErr)
	}
}

// GetErrorStats returns error statistics
func (ef *ErrorFramework) GetErrorStats() *ErrorStats {
	return ef.errorTracker.GetErrorStats()
}

// IsTemporary returns true if the error is temporary and can be retried
func IsTemporary(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryTemporary || cerr.Retryable
	}
	return false
}

// IsPermanent returns true if the error is permanent and requires manual intervention
func IsPermanent(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryPermanent && !cerr.Retryable
	}
	return false
}

// IsByzantine returns true if the error indicates potential Byzantine behavior
func IsByzantine(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryByzantine
	}
	return false
}

// IsNetworkError returns true if the error is related to network issues
func IsNetworkError(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryNetwork
	}
	return false
}

// IsStateError returns true if the error is related to state inconsistency
func IsStateError(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryState
	}
	return false
}

// IsConfigurationError returns true if the error is related to configuration issues
func IsConfigurationError(err error) bool {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category == ErrorCategoryConfiguration
	}
	return false
}

// GetErrorCode returns the error code of a ConsensusError, or ErrUnknown if not a ConsensusError
func GetErrorCode(err error) ConsensusErrorCode {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Code
	}
	return ErrUnknown
}

// GetErrorCategory returns the error category of a ConsensusError, or ErrorCategoryTemporary if not a ConsensusError
func GetErrorCategory(err error) ErrorCategory {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Category
	}
	return ErrorCategoryTemporary
}

// GetRecoveryAction returns the recommended recovery action for an error, or an empty string if none
func GetRecoveryAction(err error) string {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.RecoveryAction
	}
	return ""
}

// ShouldRetry returns true if the error is retryable, along with the recommended retry delay
func ShouldRetry(err error) (bool, time.Duration) {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Retryable, cerr.RetryAfter
	}
	return false, 0
}

// GetErrorContext returns the context of a ConsensusError, or nil if not a ConsensusError
func GetErrorContext(err error) *ErrorContext {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Context
	}
	return nil
}

// NewConsensusError creates a new ConsensusError with the given code, category, and message
// This is a convenience function that creates an ErrorFramework and uses it
func NewConsensusError(code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	return &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Timestamp: consensus.ConsensusNow(),
		Context:   NewErrorContext(),
	}
}

// WrapError wraps an existing error in a ConsensusError
// This is a convenience function that creates an ErrorFramework and uses it
func WrapError(err error, code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	return &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Cause:     err,
		Timestamp: consensus.ConsensusNow(),
		Context:   NewErrorContext(),
	}
}

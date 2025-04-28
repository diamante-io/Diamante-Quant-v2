// consensus/errors.go

package consensus

import (
	"fmt"
	"time"
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
	Context map[string]interface{}
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
func (e *ConsensusError) WithContext(key string, value interface{}) *ConsensusError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
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

// NewConsensusError creates a new ConsensusError with the given code, category, and message
func NewConsensusError(code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	return &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Timestamp: time.Now(),
		Context:   make(map[string]interface{}),
	}
}

// WrapError wraps an existing error in a ConsensusError
func WrapError(err error, code ConsensusErrorCode, category ErrorCategory, message string) *ConsensusError {
	return &ConsensusError{
		Code:      code,
		Category:  category,
		Message:   message,
		Cause:     err,
		Timestamp: time.Now(),
		Context:   make(map[string]interface{}),
	}
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

// GetErrorContext returns the context map of a ConsensusError, or nil if not a ConsensusError
func GetErrorContext(err error) map[string]interface{} {
	if cerr, ok := err.(*ConsensusError); ok {
		return cerr.Context
	}
	return nil
}

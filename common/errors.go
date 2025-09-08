// common/errors.go
package common

import (
	"fmt"
	"runtime"
	"time"

	"diamante/apperrors"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity int

const (
	SeverityLow ErrorSeverity = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

func (s ErrorSeverity) String() string {
	switch s {
	case SeverityLow:
		return "LOW"
	case SeverityMedium:
		return "MEDIUM"
	case SeverityHigh:
		return "HIGH"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// ErrorContext represents structured context for errors
type ErrorContext struct {
	ValidatorID   string `json:"validator_id,omitempty"`
	BlockHash     string `json:"block_hash,omitempty"`
	TransactionID string `json:"transaction_id,omitempty"`
	PeerID        string `json:"peer_id,omitempty"`
	ContractID    string `json:"contract_id,omitempty"`
	Operation     string `json:"operation,omitempty"`
	RequestID     string `json:"request_id,omitempty"`
	UserID        string `json:"user_id,omitempty"`
	NetworkID     string `json:"network_id,omitempty"`
	Additional    string `json:"additional,omitempty"`
}

// ContextualError provides rich error context for production debugging
type ContextualError struct {
	Err         error
	Module      apperrors.Module
	Code        apperrors.Code
	Severity    ErrorSeverity
	Context     *ErrorContext
	Stack       string
	Timestamp   time.Time
	Retryable   bool
	UserMessage string // User-friendly message for API responses
}

// Error implements the error interface
func (ce *ContextualError) Error() string {
	return fmt.Sprintf("[%s] %s.%s: %v", ce.Severity, ce.Module, ce.Code, ce.Err)
}

// Unwrap returns the underlying error
func (ce *ContextualError) Unwrap() error {
	return ce.Err
}

// NewContextualError creates a new contextual error with stack trace
func NewContextualError(err error, module apperrors.Module, code apperrors.Code, severity ErrorSeverity, message string) *ContextualError {
	// Capture stack trace
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])

	return &ContextualError{
		Err:         err,
		Module:      module,
		Code:        code,
		Severity:    severity,
		Context:     &ErrorContext{},
		Stack:       stack,
		Timestamp:   ConsensusNow(),
		Retryable:   isRetryable(code),
		UserMessage: message,
	}
}

// AddContext adds additional context to the error
func (ce *ContextualError) AddContext(key string, value string) *ContextualError {
	if ce.Context == nil {
		ce.Context = &ErrorContext{}
	}

	switch key {
	case "validator_id":
		ce.Context.ValidatorID = value
	case "block_hash":
		ce.Context.BlockHash = value
	case "transaction_id":
		ce.Context.TransactionID = value
	case "peer_id":
		ce.Context.PeerID = value
	case "contract_id":
		ce.Context.ContractID = value
	case "operation":
		ce.Context.Operation = value
	case "request_id":
		ce.Context.RequestID = value
	case "user_id":
		ce.Context.UserID = value
	case "network_id":
		ce.Context.NetworkID = value
	default:
		ce.Context.Additional = value
	}

	return ce
}

// SetRetryable marks the error as retryable or not
func (ce *ContextualError) SetRetryable(retryable bool) *ContextualError {
	ce.Retryable = retryable
	return ce
}

// IsRetryable returns whether the error can be retried
func (ce *ContextualError) IsRetryable() bool {
	return ce.Retryable
}

// GetSeverity returns the error severity
func (ce *ContextualError) GetSeverity() ErrorSeverity {
	return ce.Severity
}

// isRetryable determines if an error code represents a retryable condition
func isRetryable(code apperrors.Code) bool {
	switch code {
	case apperrors.CodeTimeout, apperrors.CodeInternal, apperrors.CodeUnavailable:
		return true
	case apperrors.CodeInvalid, apperrors.CodeNotFound, apperrors.CodeForbidden:
		return false
	default:
		return false
	}
}

// Common error constructors for consistency across modules

// NetworkError creates a standardized network error
func NetworkError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleNetwork, apperrors.CodeInternal, SeverityHigh, message)
}

// ConsensusError creates a standardized consensus error
func ConsensusError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleConsensus, apperrors.CodeInternal, SeverityHigh, message)
}

// StorageError creates a standardized storage error
func StorageError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleStorage, apperrors.CodeInternal, SeverityMedium, message)
}

// TransactionError creates a standardized transaction error
func TransactionError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleTransaction, apperrors.CodeInvalid, SeverityMedium, message)
}

// CryptoError creates a standardized crypto error
func CryptoError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleCrypto, apperrors.CodeInternal, SeverityCritical, message)
}

// ValidationError creates a standardized validation error
func ValidationError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleAPI, apperrors.CodeInvalid, SeverityLow, message)
}

// TimeoutError creates a standardized timeout error
func TimeoutError(err error, operation string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleNetwork, apperrors.CodeTimeout, SeverityMedium,
		fmt.Sprintf("Operation '%s' timed out", operation)).
		AddContext("operation", operation).
		SetRetryable(true)
}

// WrapError wraps an existing error with additional context
func WrapError(err error, module apperrors.Module, code apperrors.Code, message string) error {
	if err == nil {
		return nil
	}

	// If it's already a ContextualError, preserve the original context
	if ce, ok := err.(*ContextualError); ok {
		wrapped := *ce // Create a copy
		wrapped.Err = fmt.Errorf("%s: %w", message, ce.Err)
		return &wrapped
	}

	return NewContextualError(err, module, code, SeverityMedium, message)
}

// SecurityError creates a standardized security error
func SecurityError(err error, message string) *ContextualError {
	return NewContextualError(err, apperrors.ModuleSecurity, apperrors.CodeForbidden, SeverityCritical, message)
}

// ErrorMetrics provides structured data for monitoring systems
type ErrorMetrics struct {
	Module    string        `json:"module"`
	Code      string        `json:"code"`
	Severity  string        `json:"severity"`
	Count     int64         `json:"count"`
	LastSeen  time.Time     `json:"last_seen"`
	Context   *ErrorContext `json:"context,omitempty"`
	Retryable bool          `json:"retryable"`
}

// ToMetrics converts a ContextualError to ErrorMetrics for monitoring
func (ce *ContextualError) ToMetrics() ErrorMetrics {
	return ErrorMetrics{
		Module:    string(ce.Module),
		Code:      string(ce.Code),
		Severity:  ce.Severity.String(),
		Count:     1,
		LastSeen:  ce.Timestamp,
		Context:   ce.Context,
		Retryable: ce.Retryable,
	}
}

// RecoverableError indicates an error that can be recovered from
type RecoverableError struct {
	*ContextualError
	RecoveryActions []string
}

// NewRecoverableError creates a new recoverable error
func NewRecoverableError(err error, module apperrors.Module, actions []string, message string) *RecoverableError {
	return &RecoverableError{
		ContextualError: NewContextualError(err, module, apperrors.CodeInternal, SeverityMedium, message).SetRetryable(true),
		RecoveryActions: actions,
	}
}

// GetRecoveryActions returns suggested recovery actions
func (re *RecoverableError) GetRecoveryActions() []string {
	return re.RecoveryActions
}

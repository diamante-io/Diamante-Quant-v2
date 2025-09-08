// Package runtime provides error definitions for the runtime system
package runtime

import (
	"errors"
	"fmt"
	"time"

	"diamante/common"
)

// Common runtime errors
var (
	// ErrRuntimeNotFound is returned when a runtime type is not registered
	ErrRuntimeNotFound = errors.New("runtime not found")

	// ErrRuntimeNotInitialized is returned when a runtime is not initialized
	ErrRuntimeNotInitialized = errors.New("runtime not initialized")

	// ErrRuntimeNotStarted is returned when a runtime is not started
	ErrRuntimeNotStarted = errors.New("runtime not started")

	// ErrRuntimeAlreadyStarted is returned when trying to start an already running runtime
	ErrRuntimeAlreadyStarted = errors.New("runtime already started")

	// ErrContractNotFound is returned when a contract is not found
	ErrContractNotFound = errors.New("contract not found")

	// ErrInvalidContractType is returned when a contract type is invalid
	ErrInvalidContractType = errors.New("invalid contract type")

	// ErrInvalidRuntimeType is returned when a runtime type is invalid for an operation
	ErrInvalidRuntimeType = errors.New("invalid runtime type")

	// ErrUnauthorized is returned when an operation is not authorized
	ErrUnauthorized = errors.New("unauthorized")

	// ErrOutOfGas is returned when a contract execution runs out of gas
	ErrOutOfGas = errors.New("out of gas")

	// ErrExecutionFailed is returned when contract execution fails
	ErrExecutionFailed = errors.New("execution failed")

	// ErrInvalidArguments is returned when invalid arguments are provided
	ErrInvalidArguments = errors.New("invalid arguments")

	// ErrContractAlreadyExists is returned when trying to deploy a contract that already exists
	ErrContractAlreadyExists = errors.New("contract already exists")

	// ErrUpgradeFailed is returned when contract upgrade fails
	ErrUpgradeFailed = errors.New("upgrade failed")

	// ErrResourceExhausted is returned when runtime resources are exhausted
	ErrResourceExhausted = errors.New("resource exhausted")

	// ErrInvalidState is returned when the runtime is in an invalid state
	ErrInvalidState = errors.New("invalid state")

	// ErrNotImplemented is returned when a feature is not implemented
	ErrNotImplemented = errors.New("not implemented")
)

// RuntimeErrorCode defines error codes for runtime errors
type RuntimeErrorCode string

const (
	RuntimeErrorCodeNotFound       RuntimeErrorCode = "NOT_FOUND"
	RuntimeErrorCodeNotStarted     RuntimeErrorCode = "NOT_STARTED"
	RuntimeErrorCodeOutOfGas       RuntimeErrorCode = "OUT_OF_GAS"
	RuntimeErrorCodeExecFailed     RuntimeErrorCode = "EXEC_FAILED"
	RuntimeErrorCodeInvalidArgs    RuntimeErrorCode = "INVALID_ARGS"
	RuntimeErrorCodeUnauthorized   RuntimeErrorCode = "UNAUTHORIZED"
	RuntimeErrorCodeResourceLimit  RuntimeErrorCode = "RESOURCE_LIMIT"
	RuntimeErrorCodeInvalidState   RuntimeErrorCode = "INVALID_STATE"
	RuntimeErrorCodeNotImplemented RuntimeErrorCode = "NOT_IMPLEMENTED"
)

// RuntimeErrorDetails represents structured details for runtime errors
type RuntimeErrorDetails struct {
	ContractID     string `json:"contract_id,omitempty"`
	FunctionName   string `json:"function_name,omitempty"`
	ErrorCode      string `json:"error_code,omitempty"`
	StackTrace     string `json:"stack_trace,omitempty"`
	GasUsed        uint64 `json:"gas_used,omitempty"`
	ExecutionTime  int64  `json:"execution_time_ms,omitempty"`
	RuntimeType    string `json:"runtime_type,omitempty"`
	AdditionalInfo string `json:"additional_info,omitempty"`
}

// RuntimeError represents a comprehensive runtime error with context
type RuntimeError struct {
	Code      RuntimeErrorCode
	Message   string
	Cause     error
	Timestamp time.Time
	Details   *RuntimeErrorDetails
}

// Error implements the error interface
func (e *RuntimeError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("runtime error [%s]: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("runtime error [%s]: %s", e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *RuntimeError) Unwrap() error {
	return e.Cause
}

// NewRuntimeError creates a new runtime error
func NewRuntimeError(code RuntimeErrorCode, message string, cause error) *RuntimeError {
	return &RuntimeError{
		Code:      code,
		Message:   message,
		Cause:     cause,
		Timestamp: common.ConsensusNow(),
		Details:   &RuntimeErrorDetails{},
	}
}

// WithDetails adds additional context to the error
func (e *RuntimeError) WithDetails(key string, value string) *RuntimeError {
	if e.Details == nil {
		e.Details = &RuntimeErrorDetails{}
	}

	switch key {
	case "contract_id":
		e.Details.ContractID = value
	case "function_name":
		e.Details.FunctionName = value
	case "error_code":
		e.Details.ErrorCode = value
	case "stack_trace":
		e.Details.StackTrace = value
	case "runtime_type":
		e.Details.RuntimeType = value
	default:
		e.Details.AdditionalInfo = value
	}

	return e
}

// WithNumericDetails adds numeric context to the error
func (e *RuntimeError) WithNumericDetails(key string, value uint64) *RuntimeError {
	if e.Details == nil {
		e.Details = &RuntimeErrorDetails{}
	}

	switch key {
	case "gas_used":
		e.Details.GasUsed = value
	case "execution_time":
		e.Details.ExecutionTime = int64(value)
	}

	return e
}

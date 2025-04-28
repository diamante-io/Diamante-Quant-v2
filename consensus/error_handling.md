# Error Handling and Recovery System

This document describes the error handling and recovery system implemented in the Diamante consensus engine.

## Overview

The error handling and recovery system provides a structured approach to handling errors in the consensus system. It includes:

1. **Structured Error Types**: A hierarchy of error types with rich context information
2. **Error Categories**: Classification of errors by their nature (temporary, permanent, etc.)
3. **Recovery Strategies**: Different strategies for recovering from errors
4. **Circuit Breakers**: Protection against cascading failures
5. **Error Tracking**: Metrics for monitoring error patterns

## Structured Error Types

The core of the system is the `ConsensusError` type, which provides rich context for errors:

```go
type ConsensusError struct {
    // Error code and category
    Code     ConsensusErrorCode
    Category ErrorCategory
    
    // Error details
    Message  string
    Cause    error
    
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
```

### Creating Errors

There are two main ways to create consensus errors:

1. **Creating a new error**:

```go
err := NewConsensusError(
    ErrTimeout,
    ErrorCategoryTemporary,
    "operation timed out",
)
```

2. **Wrapping an existing error**:

```go
originalErr := errors.New("underlying error")
wrappedErr := WrapError(
    originalErr,
    ErrBlockCreationFailed,
    ErrorCategoryTemporary,
    "failed to create block",
)
```

### Adding Context

You can add additional context to errors using the fluent interface:

```go
err = err.WithBlockNumber(42)
    .WithValidatorID(validatorID)
    .WithRetryInfo(true, 5*time.Second)
    .WithContext("attempt", 3)
    .WithRecoveryAction("retry after backoff")
```

## Error Categories

Errors are categorized by their nature:

- **Temporary**: Transient errors that may resolve on retry
- **Permanent**: Errors that require manual intervention
- **Byzantine**: Errors indicating potential malicious behavior
- **Network**: Errors related to network issues
- **State**: Errors related to state inconsistency
- **Configuration**: Errors related to configuration issues

## Error Codes

Error codes provide specific information about the error:

```go
// General errors
ErrUnknown
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
```

## Recovery Manager

The `RecoveryManager` handles error recovery:

```go
type RecoveryManager struct {
    // Recovery state
    recoveryInProgress bool
    lastRecoveryTime   time.Time
    recoveryAttempts   map[ConsensusErrorCode]int
    circuitBreakers    map[ConsensusErrorCode]time.Time

    // Configuration
    maxRecoveryAttempts     int
    recoveryBackoff         time.Duration
    circuitBreakerDuration  time.Duration
    enableAutomaticRecovery bool
    recoveryStrategies      map[ConsensusErrorCode]RecoveryStrategy
}
```

### Recovery Strategies

Different recovery strategies are available:

- **None**: No recovery should be attempted
- **Retry**: The operation should be retried
- **Checkpoint**: Recovery from checkpoint should be attempted
- **Reset**: The system should be reset to a known good state
- **Restart**: The system should be restarted
- **Manual**: Manual intervention is required

### Circuit Breakers

Circuit breakers prevent cascading failures by temporarily disabling recovery attempts for specific error codes after multiple failures.

## Using the Error Handling System

### Returning Errors

When an error occurs, create a structured error with appropriate context:

```go
if !hc.IsRunning() {
    return NewConsensusError(
        ErrStateInconsistency,
        ErrorCategoryTemporary,
        "consensus is not running",
    ).WithBlockNumber(blockNumber).
        WithRetryInfo(true, 1*time.Second).
        WithContext("state", "stopped")
}
```

### Handling Errors

When handling errors, use the recovery manager:

```go
if err := hc.validatorManager.ProcessEpoch(blockNumber); err != nil {
    // Create a structured error for epoch processing failure
    cerr := WrapError(
        err,
        ErrStateInconsistency,
        ErrorCategoryState,
        "failed to process epoch",
    ).WithBlockNumber(blockNumber).
        WithRetryInfo(true, 2*time.Second)
    
    // Try to recover using the RecoveryManager
    if recoveryErr := hc.recoveryManager.HandleError(cerr); recoveryErr != nil {
        // If recovery failed, return the error
        return recoveryErr
    }
    
    // If recovery succeeded, continue processing
}
```

### Tracking Errors

Track errors for metrics and debugging:

```go
func (hc *HybridConsensus) trackError(err error) {
    if cerr, ok := err.(*ConsensusError); ok {
        // Update last error
        hc.lastError = cerr
        
        // Increment error count
        hc.errorCount[cerr.Code]++
    }
}
```

## Best Practices

1. **Use Structured Errors**: Always use the structured error types for better context and recovery.
2. **Categorize Errors Correctly**: Properly categorize errors to enable appropriate recovery strategies.
3. **Add Context**: Add relevant context to errors to aid in debugging and recovery.
4. **Use Recovery Manager**: Let the recovery manager handle error recovery.
5. **Monitor Error Patterns**: Track error patterns to identify systemic issues.
6. **Set Appropriate Recovery Strategies**: Configure recovery strategies based on the nature of the error.
7. **Use Circuit Breakers**: Enable circuit breakers to prevent cascading failures.

## Example: Handling a Block Production Error

```go
// Produce block
block, err := hc.produceBlock(blockNumber, validator.ID)
if err != nil {
    // Create a structured error for block production failure
    cerr := WrapError(
        err,
        ErrBlockCreationFailed,
        ErrorCategoryTemporary,
        "failed to produce block",
    ).WithBlockNumber(blockNumber).
        WithValidatorID(validator.ID).
        WithRetryInfo(true, 2*time.Second)
    
    // Try to recover using the RecoveryManager
    if recoveryErr := hc.recoveryManager.HandleError(cerr); recoveryErr != nil {
        // If recovery failed, return the error
        return recoveryErr
    }
    
    // If recovery succeeded, try again
    block, err = hc.produceBlock(blockNumber, validator.ID)
    if err != nil {
        cerr := WrapError(
            err,
            ErrBlockCreationFailed,
            ErrorCategoryPermanent,
            "failed to produce block after recovery",
        ).WithBlockNumber(blockNumber).
            WithValidatorID(validator.ID).
            WithRetryInfo(false, 0)
        
        return cerr
    }
}
```

## Conclusion

The error handling and recovery system provides a robust framework for handling errors in the consensus system. By using structured errors, appropriate recovery strategies, and circuit breakers, the system can recover from many types of failures automatically, improving the overall reliability of the consensus engine.

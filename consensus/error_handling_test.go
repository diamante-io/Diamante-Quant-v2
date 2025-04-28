// consensus/error_handling_test.go

package consensus

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsensusError(t *testing.T) {
	// Create a basic error
	err := NewConsensusError(
		ErrTimeout,
		ErrorCategoryTemporary,
		"operation timed out",
	)

	// Test basic properties
	assert.Equal(t, ErrTimeout, err.Code)
	assert.Equal(t, ErrorCategoryTemporary, err.Category)
	assert.Equal(t, "operation timed out", err.Message)
	assert.Contains(t, err.Error(), "operation timed out")
	assert.Contains(t, err.Error(), "Temporary")
	assert.Contains(t, err.Error(), "Timeout")

	// Test with context
	err = err.WithContext("attempt", 3).WithContext("operation", "block_creation")
	assert.Equal(t, 3, err.Context["attempt"])
	assert.Equal(t, "block_creation", err.Context["operation"])

	// Test with block number
	err = err.WithBlockNumber(42)
	assert.Equal(t, uint64(42), err.BlockNumber)
	assert.Contains(t, err.Error(), "block=42")

	// Test with retry info
	err = err.WithRetryInfo(true, 5*time.Second)
	assert.True(t, err.Retryable)
	assert.Equal(t, 5*time.Second, err.RetryAfter)
	assert.Contains(t, err.Error(), "retryable after 5s")

	// Test wrapping an error
	originalErr := errors.New("underlying error")
	wrappedErr := WrapError(
		originalErr,
		ErrBlockCreationFailed,
		ErrorCategoryTemporary,
		"failed to create block",
	)
	assert.Equal(t, ErrBlockCreationFailed, wrappedErr.Code)
	assert.Equal(t, originalErr, wrappedErr.Cause)
	assert.Contains(t, wrappedErr.Error(), "underlying error")
}

func TestErrorHelperFunctions(t *testing.T) {
	// Create errors of different categories
	tempErr := NewConsensusError(ErrTimeout, ErrorCategoryTemporary, "temp error")
	permErr := NewConsensusError(ErrStateCorruption, ErrorCategoryPermanent, "perm error")
	byzErr := NewConsensusError(ErrPoHVerificationFailed, ErrorCategoryByzantine, "byz error")
	netErr := NewConsensusError(ErrNetworkPartition, ErrorCategoryNetwork, "net error")
	stateErr := NewConsensusError(ErrStateInconsistency, ErrorCategoryState, "state error")
	configErr := NewConsensusError(ErrInvalidConfiguration, ErrorCategoryConfiguration, "config error")

	// Test category checks
	assert.True(t, IsTemporary(tempErr))
	assert.False(t, IsTemporary(permErr))

	assert.True(t, IsPermanent(permErr))
	assert.False(t, IsPermanent(tempErr))

	assert.True(t, IsByzantine(byzErr))
	assert.False(t, IsByzantine(tempErr))

	assert.True(t, IsNetworkError(netErr))
	assert.False(t, IsNetworkError(tempErr))

	assert.True(t, IsStateError(stateErr))
	assert.False(t, IsStateError(tempErr))

	assert.True(t, IsConfigurationError(configErr))
	assert.False(t, IsConfigurationError(tempErr))

	// Test GetErrorCode
	assert.Equal(t, ErrTimeout, GetErrorCode(tempErr))
	assert.Equal(t, ErrStateCorruption, GetErrorCode(permErr))
	assert.Equal(t, ErrUnknown, GetErrorCode(errors.New("regular error")))

	// Test GetErrorCategory
	assert.Equal(t, ErrorCategoryTemporary, GetErrorCategory(tempErr))
	assert.Equal(t, ErrorCategoryPermanent, GetErrorCategory(permErr))
	assert.Equal(t, ErrorCategoryTemporary, GetErrorCategory(errors.New("regular error")))

	// Test ShouldRetry
	tempErr = tempErr.WithRetryInfo(true, 2*time.Second)
	shouldRetry, retryAfter := ShouldRetry(tempErr)
	assert.True(t, shouldRetry)
	assert.Equal(t, 2*time.Second, retryAfter)

	permErr = permErr.WithRetryInfo(false, 0)
	shouldRetry, retryAfter = ShouldRetry(permErr)
	assert.False(t, shouldRetry)
	assert.Equal(t, time.Duration(0), retryAfter)
}

func TestRecoveryManager(t *testing.T) {
	// Create a test HybridConsensus
	hc := NewHybridConsensusWithConfig(TestHybridConfig())

	// Create a recovery manager
	rm := NewRecoveryManager(hc)

	// Test setting recovery strategies
	rm.SetRecoveryStrategy(ErrTimeout, RecoveryStrategyRetry)
	assert.Equal(t, RecoveryStrategyRetry, rm.GetRecoveryStrategy(ErrTimeout))

	rm.SetRecoveryStrategy(ErrStateCorruption, RecoveryStrategyCheckpoint)
	assert.Equal(t, RecoveryStrategyCheckpoint, rm.GetRecoveryStrategy(ErrStateCorruption))

	// Test configuration methods
	rm.SetMaxRecoveryAttempts(5)
	rm.SetRecoveryBackoff(3 * time.Second)
	rm.SetCircuitBreakerDuration(10 * time.Minute)
	rm.EnableAutomaticRecovery(true)

	// Test handling a retryable error
	err := NewConsensusError(
		ErrTimeout,
		ErrorCategoryTemporary,
		"operation timed out",
	).WithRetryInfo(true, 1*time.Millisecond)

	// Handle the error - this should apply the retry strategy
	result := rm.HandleError(err)

	// Since we're using a test configuration without actual components,
	// we expect the error to be returned (retry would be simulated in a real environment)
	assert.NotNil(t, result)

	// Test circuit breaker activation
	for i := 0; i < rm.maxRecoveryAttempts; i++ {
		rm.HandleError(err)
	}

	// Get circuit breaker status
	status := rm.GetCircuitBreakerStatus()
	assert.NotEmpty(t, status)

	// Reset circuit breakers
	rm.ResetCircuitBreakers()
	status = rm.GetCircuitBreakerStatus()
	assert.Empty(t, status)

	// Reset recovery attempts
	rm.ResetRecoveryAttempts()
	attempts := rm.GetRecoveryAttemptStatus()
	assert.Empty(t, attempts)
}

func TestErrorHandlingIntegration(t *testing.T) {
	// Create a test HybridConsensus
	hc := NewHybridConsensusWithConfig(TestHybridConfig())

	// Track an error
	err := NewConsensusError(
		ErrBlockCreationFailed,
		ErrorCategoryTemporary,
		"failed to create block",
	).WithBlockNumber(42).WithRetryInfo(true, 2*time.Second)

	hc.trackError(err)

	// Check error tracking
	assert.Equal(t, err, hc.lastError)
	assert.Equal(t, 1, hc.errorCount[ErrBlockCreationFailed])

	// Track another error of the same type
	hc.trackError(err)
	assert.Equal(t, 2, hc.errorCount[ErrBlockCreationFailed])

	// Track a different error
	err2 := NewConsensusError(
		ErrTimeout,
		ErrorCategoryTemporary,
		"operation timed out",
	)

	hc.trackError(err2)
	assert.Equal(t, err2, hc.lastError)
	assert.Equal(t, 1, hc.errorCount[ErrTimeout])
}

func TestRecoveryStrategies(t *testing.T) {
	// Create a test HybridConsensus
	hc := NewHybridConsensusWithConfig(TestHybridConfig())

	// Create a recovery manager
	rm := NewRecoveryManager(hc)

	// Test retry strategy
	err := rm.applyRetryStrategy(nil)
	assert.Nil(t, err, "Retry strategy should not return an error")

	// Test checkpoint strategy
	// This will fail because we don't have a checkpoint
	err = rm.applyCheckpointStrategy(nil)
	assert.NotNil(t, err, "Checkpoint strategy should return an error when no checkpoint is available")

	// Test reset strategy
	// This will fail because the consensus is not running
	err = rm.applyResetStrategy(nil)
	assert.NotNil(t, err, "Reset strategy should return an error when consensus is not running")

	// Test restart strategy
	// This will fail because the consensus is not running
	err = rm.applyRestartStrategy(nil)
	assert.NotNil(t, err, "Restart strategy should return an error when consensus is not running")

	// Test manual strategy
	err = rm.applyManualStrategy(nil)
	assert.NotNil(t, err, "Manual strategy should return an error")
	assert.Contains(t, err.Error(), "manual intervention required")
}

func TestRecoveryStrategyString(t *testing.T) {
	assert.Equal(t, "None", RecoveryStrategyNone.String())
	assert.Equal(t, "Retry", RecoveryStrategyRetry.String())
	assert.Equal(t, "Checkpoint", RecoveryStrategyCheckpoint.String())
	assert.Equal(t, "Reset", RecoveryStrategyReset.String())
	assert.Equal(t, "Restart", RecoveryStrategyRestart.String())
	assert.Equal(t, "Manual", RecoveryStrategyManual.String())

	// Test unknown strategy
	unknownStrategy := RecoveryStrategy(99)
	assert.Contains(t, unknownStrategy.String(), "Unknown")
}

func TestErrorCategoryString(t *testing.T) {
	assert.Equal(t, "Temporary", ErrorCategoryTemporary.String())
	assert.Equal(t, "Permanent", ErrorCategoryPermanent.String())
	assert.Equal(t, "Byzantine", ErrorCategoryByzantine.String())
	assert.Equal(t, "Network", ErrorCategoryNetwork.String())
	assert.Equal(t, "State", ErrorCategoryState.String())
	assert.Equal(t, "Configuration", ErrorCategoryConfiguration.String())

	// Test unknown category
	unknownCategory := ErrorCategory(99)
	assert.Equal(t, "Unknown", unknownCategory.String())
}

func TestConsensusErrorCodeString(t *testing.T) {
	// Test a few error codes
	assert.Equal(t, "Timeout", ErrTimeout.String())
	assert.Equal(t, "StateCorruption", ErrStateCorruption.String())
	assert.Equal(t, "BlockCreationFailed", ErrBlockCreationFailed.String())

	// Test unknown error code
	unknownCode := ConsensusErrorCode(999)
	assert.Contains(t, unknownCode.String(), "UnknownErrorCode")
}

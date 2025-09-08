// consensus/recovery/recovery_manager_test.go

package recovery

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockLogger is a simple logger for testing
type mockLogger struct {
	*logrus.Logger
	infoMessages  []string
	errorMessages []string
}

func newMockLogger() *mockLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return &mockLogger{
		Logger:        logger,
		infoMessages:  make([]string, 0),
		errorMessages: make([]string, 0),
	}
}

func TestRecoveryManager_NewRecoveryManager(t *testing.T) {
	// Test with default options
	rm := NewRecoveryManager()
	assert.NotNil(t, rm)
	assert.Equal(t, 3, rm.maxRetries)
	assert.Equal(t, 5*time.Second, rm.retryDelay)
	assert.True(t, rm.checkpointEnabled)
	assert.True(t, rm.autoRecovery)
	assert.Equal(t, Idle, rm.state)
	assert.NotNil(t, rm.recoveryCount)
	assert.NotNil(t, rm.logger)

	// Test with custom options
	logger := logrus.New()
	rm = NewRecoveryManager(
		WithMaxRetries(5),
		WithRetryDelay(10*time.Second),
		WithCheckpointEnabled(false),
		WithAutoRecovery(false),
		WithLogger(logger),
	)
	assert.NotNil(t, rm)
	assert.Equal(t, 5, rm.maxRetries)
	assert.Equal(t, 10*time.Second, rm.retryDelay)
	assert.False(t, rm.checkpointEnabled)
	assert.False(t, rm.autoRecovery)
	assert.Equal(t, logger, rm.logger)
}

func TestRecoveryManager_GetState(t *testing.T) {
	rm := NewRecoveryManager()
	assert.Equal(t, Idle, rm.GetState())

	// Change state and verify
	rm.mu.Lock()
	rm.state = Recovering
	rm.mu.Unlock()
	assert.Equal(t, Recovering, rm.GetState())
}

func TestRecoveryManager_GetLastError(t *testing.T) {
	rm := NewRecoveryManager()
	assert.Nil(t, rm.GetLastError())

	// Set last error and verify
	testErr := &RecoveryError{
		Err:         errors.New("test error"),
		Severity:    Minor,
		Strategy:    StateRollback,
		Component:   "test",
		Description: "test error",
		Timestamp:   time.Now(),
		Context:     make(map[string]interface{}),
	}
	rm.mu.Lock()
	rm.lastError = testErr
	rm.mu.Unlock()
	assert.Equal(t, testErr, rm.GetLastError())
}

func TestRecoveryManager_RecoverFromError_NilError(t *testing.T) {
	rm := NewRecoveryManager()
	err := rm.RecoverFromError("test", nil, Minor)
	assert.Nil(t, err)
	assert.Equal(t, Idle, rm.GetState())
}

func TestRecoveryManager_RecoverFromError_AutoRecoveryDisabled(t *testing.T) {
	rm := NewRecoveryManager(WithAutoRecovery(false))
	err := rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.NotNil(t, err)
	assert.Equal(t, Recovering, rm.GetState())
}

func TestRecoveryManager_RecoverFromError_MaxRetriesExceeded(t *testing.T) {
	rm := NewRecoveryManager(WithMaxRetries(1))

	// First attempt
	err := rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.Nil(t, err)
	assert.Equal(t, Succeeded, rm.GetState())

	// Second attempt (should exceed max retries)
	err = rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.NotNil(t, err)
	assert.Equal(t, Failed, rm.GetState())
}

func TestRecoveryManager_RecoverFromError_SuccessfulRecovery(t *testing.T) {
	rm := NewRecoveryManager()
	err := rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.Nil(t, err)
	assert.Equal(t, Succeeded, rm.GetState())
}

func TestRecoveryManager_RecoverFromError_ManualIntervention(t *testing.T) {
	rm := NewRecoveryManager()
	err := rm.RecoverFromError("test", errors.New("critical error"), Critical)
	assert.NotNil(t, err)
	assert.Equal(t, Failed, rm.GetState())
}

func TestRecoveryManager_RecoverFromError_WithCallbacks(t *testing.T) {
	startCalled := false
	completeCalled := false

	rm := NewRecoveryManager(
		WithOnRecoveryStart(func(component string, err error) error {
			startCalled = true
			return nil
		}),
		WithOnRecoveryComplete(func(component string, success bool) error {
			completeCalled = true
			assert.True(t, success)
			return nil
		}),
	)

	err := rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.Nil(t, err)
	assert.True(t, startCalled)
	assert.True(t, completeCalled)
}

func TestRecoveryManager_RecoverFromError_CallbackError(t *testing.T) {
	rm := NewRecoveryManager(
		WithOnRecoveryStart(func(component string, err error) error {
			return errors.New("callback error")
		}),
	)

	err := rm.RecoverFromError("test", errors.New("test error"), Minor)
	assert.NotNil(t, err)
	assert.Equal(t, Recovering, rm.GetState())
}

func TestRecoveryManager_ResetRecoveryCount(t *testing.T) {
	rm := NewRecoveryManager()

	// Set recovery count
	rm.mu.Lock()
	rm.recoveryCount["test"] = 5
	rm.mu.Unlock()

	// Reset and verify
	rm.ResetRecoveryCount("test")
	rm.mu.RLock()
	count := rm.recoveryCount["test"]
	rm.mu.RUnlock()
	assert.Equal(t, 0, count)
}

func TestRecoveryManager_IsRecovering(t *testing.T) {
	rm := NewRecoveryManager()
	assert.False(t, rm.IsRecovering())

	// Set state to Recovering
	rm.mu.Lock()
	rm.state = Recovering
	rm.mu.Unlock()
	assert.True(t, rm.IsRecovering())
}

func TestRecoveryManager_GetRecoveryStats(t *testing.T) {
	rm := NewRecoveryManager()

	// Set up some state
	rm.mu.Lock()
	rm.state = Succeeded
	rm.recoveryCount["test"] = 3
	rm.lastRecovery = time.Now()
	rm.lastError = &RecoveryError{
		Err:         errors.New("test error"),
		Severity:    Minor,
		Strategy:    StateRollback,
		Component:   "test",
		Description: "test error",
		Timestamp:   time.Now(),
		Context:     make(map[string]interface{}),
	}
	rm.mu.Unlock()

	// Get stats and verify
	stats := rm.GetRecoveryStats()
	assert.Equal(t, "Succeeded", stats["state"])
	assert.NotNil(t, stats["recoveryCount"])
	assert.NotNil(t, stats["lastRecovery"])
	assert.NotNil(t, stats["lastError"])

	// Verify lastError fields
	lastError := stats["lastError"].(map[string]interface{})
	assert.Equal(t, "test", lastError["component"])
	assert.Equal(t, "Minor", lastError["severity"])
	assert.Equal(t, "StateRollback", lastError["strategy"])
	assert.Equal(t, "test error", lastError["description"])
	assert.NotNil(t, lastError["timestamp"])
}

func TestRecoveryError_Error(t *testing.T) {
	err := &RecoveryError{
		Err:         errors.New("test error"),
		Severity:    Minor,
		Strategy:    StateRollback,
		Component:   "test",
		Description: "test error",
		Timestamp:   time.Now(),
		Context:     make(map[string]interface{}),
	}

	errStr := err.Error()
	assert.Contains(t, errStr, "Minor")
	assert.Contains(t, errStr, "test")
	assert.Contains(t, errStr, "test error")
	assert.Contains(t, errStr, "StateRollback")
}

func TestRecoveryError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	err := &RecoveryError{
		Err:         originalErr,
		Severity:    Minor,
		Strategy:    StateRollback,
		Component:   "test",
		Description: "test error",
		Timestamp:   time.Now(),
		Context:     make(map[string]interface{}),
	}

	unwrappedErr := err.Unwrap()
	assert.Equal(t, originalErr, unwrappedErr)
}

func TestRecoveryState_String(t *testing.T) {
	assert.Equal(t, "Idle", Idle.String())
	assert.Equal(t, "Recovering", Recovering.String())
	assert.Equal(t, "Failed", Failed.String())
	assert.Equal(t, "Succeeded", Succeeded.String())
	assert.Equal(t, "Unknown", RecoveryState(99).String())
}

func TestErrorSeverity_String(t *testing.T) {
	assert.Equal(t, "Minor", Minor.String())
	assert.Equal(t, "Moderate", Moderate.String())
	assert.Equal(t, "Severe", Severe.String())
	assert.Equal(t, "Critical", Critical.String())
	assert.Equal(t, "Unknown", ErrorSeverity(99).String())
}

func TestRecoveryStrategy_String(t *testing.T) {
	assert.Equal(t, "StateRollback", StateRollback.String())
	assert.Equal(t, "StateResync", StateResync.String())
	assert.Equal(t, "Restart", Restart.String())
	assert.Equal(t, "Manual", Manual.String())
	assert.Equal(t, "Unknown", RecoveryStrategy(99).String())
}

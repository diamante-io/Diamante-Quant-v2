// consensus/diamantehybrid_test.go

package consensus

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"diamante/consensus/diamantepos"
	"diamante/consensus/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eventFinalizationTimeout = 5 * time.Second
	finalizationRetryDelay   = 200 * time.Millisecond
	maxFinalizationRetries   = 5

	maxWaitRetries   = 50
	waitRetryDelay   = 100 * time.Millisecond
	consensusTimeout = 5 * time.Second
	eventTimeout     = 5 * time.Second

	testGossipDelay    = 50 * time.Millisecond
	testPoHTickDelay   = 5 * time.Millisecond
	testVotingDuration = 1 * time.Minute
	testDPoSSetSize    = 21
	testDPoSEpoch      = 100

	testTagLachesis  = "LACHESIS"
	testTagDPoS      = "DPoS"
	testTagPoH       = "POH"
	testTagGov       = "GOVERNANCE"
	testTagConsensus = "CONSENSUS"
)

// Thread-safe test logger
type testLogger struct {
	t  *testing.T
	mu sync.Mutex
}

func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{t: t}
}
func (l *testLogger) logf(module string, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	l.t.Logf("[%s] %s: %s", time.Now().Format("15:04:05.000"), module, msg)
}

// testSetup encloses the HybridConsensus + test references
type testSetup struct {
	t      *testing.T
	hc     *HybridConsensus
	logger *testLogger
	ctx    context.Context
	cancel context.CancelFunc
}

// Here we now call NewHybridConsensusWithConfig(TestHybridConfig()) so we slow PoH & allow drift
func setupTest(t *testing.T) *testSetup {
	ctx, cancel := context.WithTimeout(context.Background(), consensusTimeout)
	logger := newTestLogger(t)

	logger.logf(testTagConsensus, "Creating new consensus instance (Test config)")

	// We fetch a test config with drift tolerance, etc.
	testCfg := TestHybridConfig()
	// You can override certain fields if you want, e.g.:
	// testCfg.PoHTickDelay = 5 * time.Millisecond
	// testCfg.PoHDriftTolerance = 3

	hc := NewHybridConsensusWithConfig(testCfg)

	logger.logf(testTagConsensus, "Starting consensus")
	err := hc.Start()
	require.NoError(t, err, "Failed to start consensus")

	ts := &testSetup{
		t:      t,
		hc:     hc,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
	// Register cleanup
	t.Cleanup(func() {
		ts.cleanup()
	})
	return ts
}

// Cleanup function
func (ts *testSetup) cleanup() {
	ts.logger.logf(testTagConsensus, "Running cleanup")

	// Cancel test context
	ts.cancel()

	// Stop consensus once
	done := make(chan struct{})
	go func() {
		if err := ts.hc.Stop(); err != nil {
			ts.logger.logf(testTagConsensus, "Error during cleanup: %v", err)
		} else {
			ts.logger.logf(testTagConsensus, "Consensus stopped successfully")
		}
		close(done)
	}()

	// Wait or timeout
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		ts.logger.logf(testTagConsensus, "Warning: Cleanup timed out")
	}

	// Small pause
	time.Sleep(100 * time.Millisecond)
	ts.logger.logf(testTagConsensus, "Cleanup completed successfully")
}

// Helper for test validator setup
func (ts *testSetup) setupValidator(stake uint64) [32]byte {
	validatorID := [32]byte{byte(rand.Intn(256))}
	ts.logger.logf(testTagConsensus, "Setting up validator %x with stake %d", validatorID, stake)

	ts.hc.AddValidator(validatorID, stake)

	err := waitForValidatorActive(ts.t, ts.hc, validatorID, consensusTimeout)
	if err != nil {
		ts.t.Fatalf("Failed to setup validator: %v", err)
	}
	return validatorID
}

func waitForValidatorActive(
	t *testing.T, hc *HybridConsensus,
	validatorID [32]byte,
	timeout time.Duration,
) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	attempts := 0

	t.Logf("Waiting for validator %x to become active (timeout: %v)", validatorID, timeout)
	for time.Now().Before(deadline) {
		if hc.dpos.IsActiveValidator(validatorID) {
			t.Logf("Validator %x became active after %d attempts", validatorID, attempts)
			return nil
		}
		attempts++
		if attempts%10 == 0 {
			t.Logf("Still waiting for validator %x after %d attempts", validatorID, attempts)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for validator to become active after %d attempts", attempts)
}

// detect leftover goroutines
func ensureNoGoroutineLeak(t *testing.T) func() {
	t.Helper()
	initial := runtime.NumGoroutine()
	t.Logf("Starting goroutine count: %d", initial)

	return func() {
		time.Sleep(100 * time.Millisecond)
		final := runtime.NumGoroutine()
		if final > initial {
			t.Errorf("Goroutine leak detected: started with %d, ended with %d", initial, final)
		} else {
			t.Logf("No goroutine leaks detected: started with %d, ended with %d", initial, final)
		}
	}
}

// repeatedly tries to finalize an event
func waitForEventFinalization(hc *HybridConsensus, ev *types.Event, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	attempt := 0
	var lastErr error

	for time.Now().Before(deadline) {
		attempt++
		f, err := hc.FinalizeEvent(ev)
		if err == nil && f {
			return nil
		}
		if err != nil {
			lastErr = err
			if attempt%3 == 0 {
				log.Printf("Finalization attempt %d failed: %v", attempt, err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("event finalization timed out after %v: last error: %v", timeout, lastErr)
	}
	return fmt.Errorf("event finalization timed out after %v", timeout)
}

// --- Basic tests
func TestBasicFunctionality(t *testing.T) {
	t.Run("ConsensusInitialization", func(t *testing.T) {
		hc := NewHybridConsensus(testGossipDelay, testPoHTickDelay, testDPoSSetSize, testDPoSEpoch, testVotingDuration)
		require.NotNil(t, hc)
		require.NotNil(t, hc.lachesis)
		require.NotNil(t, hc.dpos)
		require.NotNil(t, hc.poh)
		require.NotNil(t, hc.optimizer)
		require.NotNil(t, hc.governance)
	})

	t.Run("StartStop", func(t *testing.T) {
		ts := setupTest(t)

		err := ts.hc.Start()
		assert.Error(t, err, "Double start should fail")

		err = ts.hc.Stop()
		assert.NoError(t, err, "Failed to stop consensus")

		err = ts.hc.Stop()
		assert.Error(t, err, "Double stop should fail")
	})

	t.Run("BlockHeightProgression", func(t *testing.T) {
		ts := setupTest(t)

		dposImpl, ok := ts.hc.dpos.(*diamantepos.DPoS)
		require.True(t, ok, "Failed to assert DPoS impl")

		numValidators := 3
		for i := 0; i < numValidators; i++ {
			id := [32]byte{byte(i + 1)}
			stake := uint64(1000)
			dposImpl.AddValidator(id, stake)
		}
		require.Eventually(t, func() bool {
			return len(dposImpl.GetActiveValidators()) == numValidators
		}, time.Second*10, time.Millisecond*100)

		initHeight := ts.hc.GetCurrentHeight()
		require.Eventually(t, func() bool {
			ts.hc.blockHeightMu.RLock()
			curr := ts.hc.lastBlockHeight
			ts.hc.blockHeightMu.RUnlock()
			return curr > initHeight
		}, time.Second*15, time.Millisecond*100)
	})
}

func TestModuleIntegration(t *testing.T) {
	testTimeout := 10 * time.Second
	if testing.Short() {
		testTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	doneCh := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(doneCh)
		ts := setupTest(t)

		select {
		case <-ctx.Done():
			errCh <- fmt.Errorf("test timeout after %v", testTimeout)
			return
		default:
		}

		ts.logger.logf(testTagLachesis, "Testing Lachesis event creation and finalization")
		validatorID := ts.setupValidator(1000)

		if !ts.hc.dpos.IsActiveValidator(validatorID) {
			errCh <- fmt.Errorf("validator not active")
			return
		}

		var ev *types.Event
		eventCh := make(chan *types.Event)

		go func() {
			select {
			case <-ctx.Done():
				return
			default:
				e := ts.hc.CreateEvent(validatorID, nil, []byte("test data"))
				eventCh <- e
			}
		}()

		select {
		case ev = <-eventCh:
			if ev == nil {
				errCh <- fmt.Errorf("failed to create event")
				return
			}
		case <-ctx.Done():
			errCh <- fmt.Errorf("event creation timed out")
			return
		}

		finalized := false
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				errCh <- fmt.Errorf("finalization timeout")
				return
			default:
				f, ferr := ts.hc.FinalizeEvent(ev)
				if ferr == nil && f {
					finalized = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		if !finalized {
			errCh <- fmt.Errorf("event finalization timed out")
			return
		}
		validateConsensusState(t, ts.hc)
	}()

	select {
	case <-ctx.Done():
		t.Fatal("Test timed out")
	case err := <-errCh:
		t.Fatal(err)
	case <-doneCh:
	}
}

func TestFullConsensusFlow(t *testing.T) {
	defer ensureNoGoroutineLeak(t)()
	ts := setupTest(t)

	ts.logger.logf(testTagDPoS, "Setting up multiple validators")
	validatorCount := 3
	validators := make([][32]byte, validatorCount)

	require.Equal(t, uint64(0), ts.hc.GetLastBlockHeight())

	for i := 0; i < validatorCount; i++ {
		validators[i] = ts.setupValidator(1000 * uint64(i+1))
		require.True(t, ts.hc.dpos.IsActiveValidator(validators[i]))
		time.Sleep(50 * time.Millisecond)
	}

	metricsCh := make(chan struct{})
	tk := time.NewTicker(100 * time.Millisecond)
	go func() {
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				ts.hc.collectMetrics()
			case <-metricsCh:
				return
			}
		}
	}()

	ts.logger.logf(testTagLachesis, "Creating events from different validators")
	for _, vid := range validators {
		ev := ts.hc.CreateEvent(vid, nil, []byte("test data"))
		require.NotNil(t, ev)

		nextBlock := ts.hc.GetLastBlockHeight() + 1
		err := ts.hc.ProcessBlock(nextBlock)
		require.NoError(t, err)

		var success bool
		for attempts := 0; attempts < maxFinalizationRetries; attempts++ {
			blockEvents, _ := ts.hc.GetFinalizedEvents(nextBlock, nextBlock)
			for _, be := range blockEvents {
				if be.ID == ev.ID {
					success = true
					break
				}
			}
			if success {
				break
			}
			time.Sleep(finalizationRetryDelay)
		}
		require.True(t, success, "Event failed to finalize after forcing a block")
	}

	ts.logger.logf(testTagPoH, "Processing blocks and verifying PoH progression")
	var prevHeight = ts.hc.GetLastBlockHeight()
	for i := 1; i <= 3; i++ {
		next := ts.hc.GetLastBlockHeight() + 1
		require.NoError(t, ts.hc.ProcessBlock(next))
		require.Equal(t, uint64(prevHeight+1), ts.hc.GetLastBlockHeight())
		prevHeight = ts.hc.GetLastBlockHeight()

		time.Sleep(50 * time.Millisecond)
		validateConsensusState(t, ts.hc)
	}

	close(metricsCh)

	require.Greater(t, ts.hc.GetCurrentHeight(), uint64(0))
	require.NotEmpty(t, ts.hc.GetActiveValidators())

	ts.hc.cleanupGoroutines()
}

func TestSystemStability(t *testing.T) {
	t.Run("ErrorRecovery", func(t *testing.T) {
		ts := setupTest(t)

		ts.logger.logf(testTagDPoS, "Setting up validator for stability test")
		validatorID := ts.setupValidator(1000)

		time.Sleep(100 * time.Millisecond)

		ts.logger.logf(testTagLachesis, "Creating test events")
		for i := 0; i < 3; i++ {
			ev := ts.hc.CreateEvent(validatorID, nil, []byte(fmt.Sprintf("test data %d", i)))
			require.NotNil(t, ev)

			err := waitForEventFinalization(ts.hc, ev, 5*time.Second)
			require.NoError(t, err)
		}

		ts.logger.logf(testTagPoH, "Processing regular blocks")
		for i := uint64(1); i <= 3; i++ {
			err := ts.hc.ProcessBlock(i)
			require.NoError(t, err)
		}
		require.True(t, ts.hc.dpos.IsActiveValidator(validatorID))

		ts.logger.logf(testTagConsensus, "Creating test checkpoint")
		err := ts.hc.createTestCheckpoint(1000)
		require.NoError(t, err)

		ts.logger.logf(testTagConsensus, "Forcing error state")
		ts.hc.SetLastBlockHeight(1000)

		ts.logger.logf(testTagConsensus, "Attempting recovery")
		recErr := ts.hc.recoverFromError()
		require.NoError(t, recErr)

		require.Equal(t, uint64(1000), ts.hc.GetLastBlockHeight())
		require.True(t, ts.hc.HasCheckpoint(1000))
		require.True(t, ts.hc.dpos.IsActiveValidator(validatorID))

		time.Sleep(100 * time.Millisecond)
		err = ts.hc.ProcessBlock(1001)
		require.NoError(t, err)
		validateConsensusState(t, ts.hc)
	})
}

// Minimal final-state checks
func validateConsensusState(t *testing.T, hc *HybridConsensus) {
	assert.Greater(t, hc.GetCurrentHeight(), uint64(0), "Current height should be positive")
	assert.NotEmpty(t, hc.GetActiveValidators(), "Should have active validators")
	assert.Greater(t, hc.poh.GetCount(), uint64(0), "PoH count should be positive")

	if hc.lastBlockHeight >= CheckpointInterval {
		ck := hc.getLastCheckpoint()
		assert.NotNil(t, ck, "Should have checkpoint")
		assert.Equal(t, ck.BlockNumber%CheckpointInterval, uint64(0),
			"Checkpoint block number should be multiple of interval")
	}
}

package diamantepos_test

import (
	"crypto/rand"
	"encoding/json"
	"math"
	"testing"
	"time"

	"diamante/consensus/diamantepos"
)

type mockLogger struct{}

func (m *mockLogger) Info(msg string, keyvals ...interface{})  {}
func (m *mockLogger) Error(msg string, keyvals ...interface{}) {}

func randomValidatorID(t *testing.T) [32]byte {
	var out [32]byte
	if _, err := rand.Read(out[:]); err != nil {
		t.Fatalf("failed to get random bytes: %v", err)
	}
	return out
}

func TestDPoS_AddValidator(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(5, 100, logger)

	valID := randomValidatorID(t)
	dpos.AddValidator(valID, 1000)

	// Check total stake
	if dpos.GetTotalStake() != 1000 {
		t.Errorf("expected totalStake=1000, got %d", dpos.GetTotalStake())
	}

	// Check active set
	actives := dpos.GetActiveValidators()
	if len(actives) == 0 {
		t.Error("expected at least 1 active validator")
	} else if actives[0].ID != valID {
		t.Error("the active validator doesn't match the one we added")
	}
}

func TestDPoS_UpdateStake(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(5, 100, logger)

	valID := randomValidatorID(t)
	dpos.AddValidator(valID, 500)
	dpos.UpdateStake(valID, 2000)

	if dpos.GetTotalStake() != 2000 {
		t.Errorf("expected totalStake=2000 after update, got %d", dpos.GetTotalStake())
	}
	st := dpos.GetValidatorStake(valID)
	if st != 2000 {
		t.Errorf("expected stake=2000, got %d", st)
	}
}

func TestDPoS_ProcessEpoch(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(3, 2, logger) // epoch every 2 blocks

	valA := randomValidatorID(t)
	valB := randomValidatorID(t)
	dpos.AddValidator(valA, 100)
	dpos.AddValidator(valB, 200)

	// We haven't reached 2 blocks yet => no new epoch
	if err := dpos.ProcessEpoch(1); err != nil {
		t.Errorf("unexpected error at block=1: %v", err)
	}

	// At block=2 => epoch triggered => new epoch => reward distribution
	if err := dpos.ProcessEpoch(2); err != nil {
		t.Errorf("unexpected error at block=2: %v", err)
	}
	// We can infer that an epoch incremented if stake changed
	ts := dpos.GetTotalStake()
	if ts <= 300 {
		t.Errorf("expected totalStake to have grown due to rewards, got %d", ts)
	}
}

func TestDPoS_NextValidator(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(5, 10, logger)

	val1 := randomValidatorID(t)
	val2 := randomValidatorID(t)
	dpos.AddValidator(val1, 500)
	dpos.AddValidator(val2, 300)

	v := dpos.GetNextValidator(1, [32]byte{})
	if v == nil {
		t.Fatal("expected a validator, got nil")
	}
	if v.ID != val1 && v.ID != val2 {
		t.Errorf("unexpected validator ID %x", v.ID)
	}
}

func TestDPoS_Slashing(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(3, 2, logger)

	valA := randomValidatorID(t)
	dpos.AddValidator(valA, 100)

	// Introduce misbehavior by publicly accessible method
	// instead of direct field usage
	dpos.InjectMisbehaviorCount(valA, 2) // We'll define this in diamantepos.go

	// Trigger epoch => should slash
	if err := dpos.ProcessEpoch(2); err != nil {
		t.Errorf("ProcessEpoch error: %v", err)
	}

	st := dpos.GetValidatorStake(valA)
	var expectedPenalty uint64 = 2
	if st != (100 - expectedPenalty) {
		t.Errorf("expected stake=98 after slashing, got %d", st)
	}

	// Check slash log
	slashLog := dpos.GetSlashLog()
	if len(slashLog) == 0 {
		t.Error("expected at least 1 slash event logged")
	} else if slashLog[0].Amount != expectedPenalty {
		t.Errorf("expected slash Amount=%d, got %d", expectedPenalty, slashLog[0].Amount)
	}
}

func TestDPoS_RewardValidator(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(3, 10, logger)
	valA := randomValidatorID(t)
	dpos.AddValidator(valA, 1000)

	dpos.RewardValidator(valA)

	// We'll check performance via some public accessor or the slash log or just confirm it from inside
	// If you do have a public getter for performance, use that. Otherwise, we rely on internal logic:
	// For now, let's assume we define "GetPerformance(validatorID)" method or so. If not, just skip.

	// If we absolutely must check an unexported field, the test must be in the same package (diamantepos).
	// We'll assume we introduced a helper like below to read performance:
	p := dpos.GetValidatorPerformance(valA)
	// Performance should slightly increase, but is capped at 1.0
	if p != 1.0 {
		t.Errorf("expected performance=1.0 (capped), got %.2f", p)
	}
}

func TestDPoS_UpdatePerformance(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(3, 10, logger)
	valA := randomValidatorID(t)
	dpos.AddValidator(valA, 1000)

	// We'll define a method to set LastUpdateTime artificially if we want to test. E.g.:
	dpos.InjectLastUpdateTime(valA, time.Now().Add(-2*time.Hour))

	// Force an epoch
	if err := dpos.ProcessEpoch(10); err != nil {
		t.Errorf("ProcessEpoch error: %v", err)
	}

	// Now check performance. We'll define "GetValidatorPerformance" to read
	perf := dpos.GetValidatorPerformance(valA)

	// Performance decays: 1.0 * 0.99^(2 hours) => ~0.9801
	if math.Abs(perf-0.98) > 0.02 {
		t.Errorf("expected performance near ~0.98, got %.2f", perf)
	}
}

func TestDPoS_Serialization(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(3, 10, logger)

	valA := randomValidatorID(t)
	valB := randomValidatorID(t)
	dpos.AddValidator(valA, 100)
	dpos.AddValidator(valB, 200)

	data, err := dpos.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	// Inspect data
	var raw map[string]interface{}
	if err2 := json.Unmarshal(data, &raw); err2 != nil {
		t.Errorf("json.Unmarshal error: %v", err2)
	}

	// Make a fresh instance and restore
	dpos2 := diamantepos.NewDPoS(5, 5, logger)
	if err := dpos2.RestoreState(data); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	if dpos2.GetTotalStake() != 300 {
		t.Errorf("expected totalStake=300 after restore, got %d", dpos2.GetTotalStake())
	}
}

func TestDPoS_SetSetSize(t *testing.T) {
	logger := &mockLogger{}
	dpos := diamantepos.NewDPoS(5, 10, logger)
	valA := randomValidatorID(t)
	valB := randomValidatorID(t)
	valC := randomValidatorID(t)
	dpos.AddValidator(valA, 100)
	dpos.AddValidator(valB, 200)
	dpos.AddValidator(valC, 300)

	if dpos.GetSetSize() != 5 {
		t.Errorf("expected maxSetSize=5, got %d", dpos.GetSetSize())
	}

	dpos.SetSetSize(2)
	if dpos.GetSetSize() != 2 {
		t.Errorf("expected new maxSetSize=2, got %d", dpos.GetSetSize())
	}

	// top 2 => valC(300), valB(200)
	acts := dpos.GetActiveValidators()
	if len(acts) != 2 {
		t.Errorf("expected 2 active validators, got %d", len(acts))
	}
}

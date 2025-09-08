package governance_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"diamante/consensus/governance"
	"diamante/consensus/types"
)

// ---------- Mocks & Helpers ----------

type mockLogger struct{}

func (m *mockLogger) Info(msg string, keyvals ...interface{})  {}
func (m *mockLogger) Error(msg string, keyvals ...interface{}) {}

type mockConsensusAdapter struct {
	t                *testing.T
	activeValidators map[[32]byte]bool
	totalStake       uint64
	currentHeight    uint64
}

func (m *mockConsensusAdapter) GetDPoS() types.DPoS {
	return &mockDPoS{
		t:          m.t,
		activeVals: m.activeValidators,
		totalStake: m.totalStake,
	}
}
func (m *mockConsensusAdapter) GetLachesis() types.Lachesis {
	return &mockLachesis{}
}
func (m *mockConsensusAdapter) GetCurrentHeight() uint64 {
	return m.currentHeight
}
func (m *mockConsensusAdapter) ScheduleUpgrade(version string, height uint64) error {
	if height <= m.currentHeight {
		return fmt.Errorf("upgrade height (%d) must be > current (%d)", height, m.currentHeight)
	}
	return nil
}

// mockDPoS is a minimal DPoS implementing only the needed interface.
type mockDPoS struct {
	t          *testing.T
	activeVals map[[32]byte]bool
	totalStake uint64
}

// Each active validator is recognized, but we assume no stake if inactive.
func (m *mockDPoS) IsActiveValidator(id [32]byte) bool {
	isActive := m.activeVals[id]
	m.t.Logf("[DPoS DEBUG] IsActiveValidator(%x) => %v", id, isActive)
	return isActive
}

// Each active validator gets 100 stake, matching the test’s final checks.
func (m *mockDPoS) GetValidatorStake(id [32]byte) uint64 {
	if !m.activeVals[id] {
		return 0
	}
	// The tests expect each active to have 100.
	return 100
}

// The total stake is just (#active * 100).
func (m *mockDPoS) GetTotalStake() uint64 {
	activeCount := 0
	for _, isActive := range m.activeVals {
		if isActive {
			activeCount++
		}
	}
	return uint64(activeCount) * 100
}

// Stubs to satisfy the DPoS interface:

func (m *mockDPoS) AddValidator([32]byte, uint64)           {}
func (m *mockDPoS) UpdateStake([32]byte, uint64)            {}
func (m *mockDPoS) GetValidators() []*types.Validator       { return nil }
func (m *mockDPoS) GetActiveValidators() []*types.Validator { return nil }
func (m *mockDPoS) GetSetSize() int                         { return 0 }
func (m *mockDPoS) SetSetSize(int)                          {}
func (m *mockDPoS) GetEpochDuration() uint64                { return 0 }
func (m *mockDPoS) SetEpochDuration(uint64)                 {}
func (m *mockDPoS) GetNextValidator(uint64, [32]byte) *types.Validator {
	return nil
}
func (m *mockDPoS) ProcessEpoch(uint64) error { return nil }
func (m *mockDPoS) RewardValidator([32]byte)  {}
func (m *mockDPoS) GetState() ([]byte, error) { return nil, nil }
func (m *mockDPoS) RestoreState([]byte) error { return nil }

// mockLachesis satisfies Lachesis with minimal stubs for governance usage.
type mockLachesis struct{}

func (l *mockLachesis) SetGossipDelay(_ time.Duration)   {}
func (l *mockLachesis) SetVotingThreshold(_ float64)     {}
func (l *mockLachesis) GetGossipDelay() time.Duration    { return 0 }
func (l *mockLachesis) GetVotingThreshold() float64      { return 0 }
func (l *mockLachesis) AddNode([32]byte, uint64)         {}
func (l *mockLachesis) UpdateNodeStake([32]byte, uint64) {}
func (l *mockLachesis) AdjustNetworkLoad(float64)        {}
func (l *mockLachesis) GetNetworkLoad() float64          { return 0 }
func (l *mockLachesis) ProcessEvent(*types.Event) bool   { return false }
func (l *mockLachesis) Start() error                     { return nil }
func (l *mockLachesis) Stop() error                      { return nil }
func (l *mockLachesis) CreateEvent([32]byte, [][32]byte, []byte) *types.Event {
	return nil
}
func (l *mockLachesis) GetFinalizedEvents(uint64, uint64) ([]*types.Event, error) {
	return nil, nil
}
func (l *mockLachesis) GetState() ([]byte, error) { return nil, nil }
func (l *mockLachesis) RestoreState([]byte) error { return nil }

// ---------- Tests ----------

func randomValidatorID() [32]byte {
	var b [32]byte
	_, _ = rand.Read(b[:]) // ignoring error for brevity
	return b
}

func TestGovernance_CreateProposal(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    50,
	}
	gov := governance.NewGovernance(mc, 2*time.Minute, &mockLogger{})

	creator := randomValidatorID()
	propID, err := gov.CreateProposal(governance.ConsensusChange, "test change", []byte("consensus data"), creator)
	if err != nil {
		t.Fatalf("CreateProposal error: %v", err)
	}

	prop, err2 := gov.GetProposal(propID)
	if err2 != nil {
		t.Fatalf("GetProposal error: %v", err2)
	}
	if prop.Type != governance.ConsensusChange {
		t.Errorf("expected proposal type=ConsensusChange, got %v", prop.Type)
	}
	if prop.Status != governance.Pending {
		t.Errorf("expected status=Pending, got %v", prop.Status)
	}
}

func TestGovernance_CancelProposal(t *testing.T) {
	t.Logf("[DEBUG] Starting TestGovernance_CancelProposal")

	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    0,
	}

	shortDuration := 100 * time.Millisecond
	t.Logf("[DEBUG] Creating Governance with shortDuration=%v", shortDuration)
	gov := governance.NewGovernance(mc, shortDuration, &mockLogger{})

	// Create a new proposal
	creator := randomValidatorID()
	t.Logf("[DEBUG] Creating proposal with creator=%x ...", creator)
	propID, err := gov.CreateProposal(governance.ParameterChange, "test param", nil, creator)
	if err != nil {
		t.Fatalf("[DEBUG] CreateProposal error: %v", err)
	}
	t.Logf("[DEBUG] Created proposal => %x", propID)

	// Wait so that StartTime <= now => after we call ProcessProposals(), it becomes Active
	time.Sleep(shortDuration + 50*time.Millisecond)

	t.Logf("[DEBUG] About to call ProcessProposals")
	gov.ProcessProposals()
	t.Logf("[DEBUG] ProcessProposals returned, let's attempt cancellation")

	// Attempt to cancel by random => should fail
	rando := randomValidatorID()
	t.Logf("[DEBUG] Cancelling with rando => expect error")
	err = gov.CancelProposal(propID, rando)
	if err == nil {
		t.Error("[DEBUG] expected error, got nil")
	} else {
		t.Logf("[DEBUG] got expected error: %v", err)
	}

	// Cancel by creator => expect success
	t.Logf("[DEBUG] Cancelling with creator => expect success")
	err = gov.CancelProposal(propID, creator)
	if err != nil {
		t.Fatalf("[DEBUG] unexpected error: %v", err)
	} else {
		t.Logf("[DEBUG] successfully canceled by creator")
	}

	// Confirm it’s removed
	_, err2 := gov.GetProposal(propID)
	if err2 == nil {
		t.Error("[DEBUG] expected error retrieving removed proposal, got nil")
	} else {
		t.Logf("[DEBUG] confirm removed => %v", err2)
	}

	t.Logf("[DEBUG] TestGovernance_CancelProposal DONE")
}

func TestGovernance_VoteAndProcess(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       3000,
		currentHeight:    100,
	}
	gov := governance.NewGovernance(mc, 2*time.Second, &mockLogger{})

	// Mark 3 active validators
	v1, v2, v3 := randomValidatorID(), randomValidatorID(), randomValidatorID()
	mc.activeValidators[v1] = true
	mc.activeValidators[v2] = true
	mc.activeValidators[v3] = true

	propID, err := gov.CreateProposal(governance.ParameterChange, "test param", []byte("param data"), v1)
	if err != nil {
		t.Fatalf("CreateProposal error: %v", err)
	}

	// Move from Pending->Active
	gov.ProcessProposals()

	// Cast votes
	if err := gov.Vote(propID, v1, true); err != nil {
		t.Errorf("Vote by v1 error: %v", err)
	}
	if err := gov.Vote(propID, v2, false); err != nil {
		t.Errorf("Vote by v2 error: %v", err)
	}
	if err := gov.Vote(propID, v3, true); err != nil {
		t.Errorf("Vote by v3 error: %v", err)
	}

	// End time is in 2 seconds, let's artificially skip ahead
	time.Sleep(2100 * time.Millisecond)
	gov.ProcessProposals()

	prop, _ := gov.GetProposal(propID)
	if prop.Status != governance.Passed {
		t.Errorf("expected proposal to pass, got %v", prop.Status)
	}
}

func TestGovernance_ExecuteProposal(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       2000,
		currentHeight:    30,
	}
	gov := governance.NewGovernance(mc, 1*time.Second, &mockLogger{})

	creator := randomValidatorID()
	propID, _ := gov.CreateProposal(governance.UpgradeProposal, "upgrade net", []byte(`{"new_version":"v2","upgrade_height":40}`), creator)

	// Force pass
	prop, _ := gov.GetProposal(propID)
	prop.Status = governance.Passed
	gov.ProcessProposals()

	if err := gov.ExecuteProposal(propID); err != nil {
		t.Errorf("unexpected error executing proposal: %v", err)
	}

	prop2, _ := gov.GetProposal(propID)
	if prop2.Status != governance.Executed {
		t.Errorf("expected status=Executed, got %v", prop2.Status)
	}
}

func TestGovernance_ExecutePassedProposals(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    10,
	}
	gov := governance.NewGovernance(mc, 500*time.Millisecond, &mockLogger{})

	creator := randomValidatorID()
	pid1, _ := gov.CreateProposal(governance.ConsensusChange, "consensus param", nil, creator)
	pid2, _ := gov.CreateProposal(governance.ParameterChange, "some param", []byte(`{"new_epoch_duration":50}`), creator)

	// Mark them passed
	prop1, _ := gov.GetProposal(pid1)
	prop1.Status = governance.Passed
	prop2, _ := gov.GetProposal(pid2)
	prop2.Status = governance.Passed

	// Execute all
	errs := gov.ExecutePassedProposals()
	if len(errs) > 0 {
		t.Errorf("expected no errors in execution, got %v", errs)
	}

	prop1, _ = gov.GetProposal(pid1)
	prop2, _ = gov.GetProposal(pid2)
	if prop1.Status != governance.Executed || prop2.Status != governance.Executed {
		t.Error("expected both proposals to be Executed now")
	}
}

func TestGovernance_CleanupOldProposals(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    0,
	}
	gov := governance.NewGovernance(mc, 100*time.Millisecond, &mockLogger{})

	creator := randomValidatorID()
	oldPropID, _ := gov.CreateProposal(governance.ParameterChange, "old param", nil, creator)
	time.Sleep(110 * time.Millisecond)
	gov.ProcessProposals() // moves to Active, then finalizes if needed

	// artificially set it to Rejected with an old EndTime
	oldProp, _ := gov.GetProposal(oldPropID)
	oldProp.Status = governance.Rejected
	oldProp.EndTime = time.Now().Add(-time.Minute)

	// Another new proposal
	newPropID, _ := gov.CreateProposal(governance.ParameterChange, "new param", nil, creator)

	removed := gov.CleanupOldProposals(30 * time.Second)
	if removed != 0 {
		t.Errorf("expected to remove 0 proposals because it's only 1 minute old < 30s threshold, got %d", removed)
	}

	removed2 := gov.CleanupOldProposals(5 * time.Minute)
	if removed2 != 1 {
		t.Errorf("expected to remove 1 old proposal, got %d", removed2)
	}

	// Ensure old prop is gone
	if _, err := gov.GetProposal(oldPropID); err == nil {
		t.Error("expected error retrieving removed old proposal, got nil")
	}
	// Ensure new prop still exists
	if _, err := gov.GetProposal(newPropID); err != nil {
		t.Errorf("unexpected error retrieving new proposal: %v", err)
	}
}

func TestGovernance_ChangeVotingDuration(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    0,
	}
	gov := governance.NewGovernance(mc, 2*time.Minute, &mockLogger{})

	if err := gov.ChangeVotingDuration(30 * time.Minute); err != nil {
		t.Errorf("expected success changing voting duration, got %v", err)
	}

	// Attempt invalid durations
	if err := gov.ChangeVotingDuration(30 * time.Minute * 48 * 7); err == nil {
		t.Error("expected error for duration > 1 week, got nil")
	}
	if err := gov.ChangeVotingDuration(30 * time.Minute / 2); err == nil {
		t.Error("expected error for duration < 1 hour, got nil")
	}
}

func TestGovernance_VotingResults(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: map[[32]byte]bool{},
		totalStake:       2000,
		currentHeight:    0,
	}
	gov := governance.NewGovernance(mc, 1*time.Second, &mockLogger{})

	// Mark 2 active validators
	a, b := randomValidatorID(), randomValidatorID()
	mc.activeValidators[a] = true
	mc.activeValidators[b] = true

	propID, _ := gov.CreateProposal(governance.ParameterChange, "stake changes", nil, a)
	gov.ProcessProposals() // => Active

	// a votes yes => stake=100
	// b votes no  => stake=100
	gov.Vote(propID, a, true)
	gov.Vote(propID, b, false)

	res, err := gov.GetVotingResults(propID)
	if err != nil {
		t.Fatalf("GetVotingResults error: %v", err)
	}

	// For 2 active validators, each has 100 => total=200
	// yes=100, no=100
	if res["yes"] != 100 || res["no"] != 100 || res["total"] != 200 {
		t.Errorf("unexpected results: yes=%d, no=%d, total=%d", res["yes"], res["no"], res["total"])
	}
}

func TestGovernance_HasVoted(t *testing.T) {
	mc := &mockConsensusAdapter{
		t:                t,
		activeValidators: make(map[[32]byte]bool),
		totalStake:       1000,
		currentHeight:    0,
	}
	gov := governance.NewGovernance(mc, time.Minute, &mockLogger{})

	vID := randomValidatorID()
	mc.activeValidators[vID] = true

	pID, _ := gov.CreateProposal(governance.ConsensusChange, "consensus", nil, vID)
	gov.ProcessProposals() // => Active

	// No vote yet
	hasV, val, err := gov.HasVoted(pID, vID)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if hasV || val {
		t.Errorf("expected not voted, got hasV=%v val=%v", hasV, val)
	}

	// Vote yes
	if err2 := gov.Vote(pID, vID, true); err2 != nil {
		t.Errorf("Vote error: %v", err2)
	}
	hasV, val, _ = gov.HasVoted(pID, vID)
	if !hasV || !val {
		t.Errorf("expected hasV=true, val=true after voting yes, got (%v, %v)", hasV, val)
	}
}

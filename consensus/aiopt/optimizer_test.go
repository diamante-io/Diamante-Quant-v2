package aiopt_test

import (
	"diamante/consensus/aiopt"
	"diamante/consensus/types"
	"testing"
	"time"
)

// --------------------- Mocks ---------------------

type mockLogger struct{}

func (m *mockLogger) Info(msg string, keyvals ...interface{})  {}
func (m *mockLogger) Error(msg string, keyvals ...interface{}) {}

// mockConsensus implements types.Consensus. We must provide *all* methods in the interface.
type mockConsensus struct {
	networkLoad float64
	lachesis    *mockLachesis
	dpos        *mockDPoS
	poh         *mockPoH
}

// GetActiveValidators implements types.Consensus.
func (m *mockConsensus) GetActiveValidators() []*types.Validator {
	panic("unimplemented")
}

// GetFinalizedEvents implements types.Consensus.
func (m *mockConsensus) GetFinalizedEvents(fromHeight uint64, toHeight uint64) ([]*types.Event, error) {
	panic("unimplemented")
}

// GetPendingEvents implements types.Consensus.
func (m *mockConsensus) GetPendingEvents() []*types.Event {
	panic("unimplemented")
}

// GetValidators implements types.Consensus.
func (m *mockConsensus) GetValidators() []*types.Validator {
	panic("unimplemented")
}

// SynchronizeState implements types.Consensus.
func (m *mockConsensus) SynchronizeState(targetState [32]byte, targetCount uint64) error {
	panic("unimplemented")
}

// ---- Satisfy types.Consensus interface ----
func (m *mockConsensus) GetNetworkLoad() float64 {
	return m.networkLoad
}
func (m *mockConsensus) GetLachesis() types.Lachesis {
	return m.lachesis
}
func (m *mockConsensus) GetDPoS() types.DPoS {
	return m.dpos
}
func (m *mockConsensus) GetPoH() types.PoH {
	return m.poh
}

// Additional methods from types.Consensus that might be needed:
func (m *mockConsensus) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	// Not used in these tests, so return nil
	return nil
}
func (m *mockConsensus) ProcessBlock(blockNumber uint64) error {
	return nil
}
func (m *mockConsensus) FinalizeEvent(event *types.Event) (bool, error) {
	// Stub for the sake of the interface
	return false, nil
}
func (m *mockConsensus) Start() error {
	return nil
}
func (m *mockConsensus) Stop() error {
	return nil
}

// mockLachesis implements types.Lachesis. We add all missing stubs: AdjustNetworkLoad, etc.
type mockLachesis struct {
	gossipDelay     time.Duration
	votingThreshold float64
}

// CreateEvent implements types.Lachesis.
func (m *mockLachesis) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	panic("unimplemented")
}

// GetFinalizedEvents implements types.Lachesis.
func (m *mockLachesis) GetFinalizedEvents(fromHeight uint64, toHeight uint64) ([]*types.Event, error) {
	panic("unimplemented")
}

// GetState implements types.Lachesis.
func (m *mockLachesis) GetState() ([]byte, error) {
	panic("unimplemented")
}

// RestoreState implements types.Lachesis.
func (m *mockLachesis) RestoreState(state []byte) error {
	panic("unimplemented")
}

func (m *mockLachesis) GetGossipDelay() time.Duration                 { return m.gossipDelay }
func (m *mockLachesis) SetGossipDelay(d time.Duration)                { m.gossipDelay = d }
func (m *mockLachesis) GetVotingThreshold() float64                   { return m.votingThreshold }
func (m *mockLachesis) SetVotingThreshold(th float64)                 { m.votingThreshold = th }
func (m *mockLachesis) AddNode(nodeID [32]byte, stake uint64)         {}
func (m *mockLachesis) UpdateNodeStake(nodeID [32]byte, stake uint64) {}
func (m *mockLachesis) ProcessEvent(ev *types.Event) bool             { return false }

// Missing methods for some Lachesis interfaces:
func (m *mockLachesis) AdjustNetworkLoad(adjustment float64) {
	// no-op
}
func (m *mockLachesis) GetNetworkLoad() float64 {
	// If Lachesis also has GetNetworkLoad,
	// either return a fixed value or store it in the struct.
	return 0
}
func (m *mockLachesis) Start() error {
	return nil
}
func (m *mockLachesis) Stop() error {
	return nil
}

// mockDPoS implements types.DPoS, including GetActiveValidators now.
type mockDPoS struct {
	setSize       int
	epochDuration uint64
}

// GetState implements types.DPoS.
func (m *mockDPoS) GetState() ([]byte, error) {
	panic("unimplemented")
}

// GetValidators implements types.DPoS.
func (m *mockDPoS) GetValidators() []*types.Validator {
	panic("unimplemented")
}

// RestoreState implements types.DPoS.
func (m *mockDPoS) RestoreState(stateData []byte) error {
	panic("unimplemented")
}

func (m *mockDPoS) GetSetSize() int                          { return m.setSize }
func (m *mockDPoS) SetSetSize(size int)                      { m.setSize = size }
func (m *mockDPoS) GetEpochDuration() uint64                 { return m.epochDuration }
func (m *mockDPoS) SetEpochDuration(duration uint64)         { m.epochDuration = duration }
func (m *mockDPoS) AddValidator(id [32]byte, stake uint64)   {}
func (m *mockDPoS) UpdateStake(id [32]byte, newStake uint64) {}
func (m *mockDPoS) RewardValidator(id [32]byte)              {}
func (m *mockDPoS) GetNextValidator(blockNumber uint64, h [32]byte) *types.Validator {
	return nil
}
func (m *mockDPoS) ProcessEpoch(blockNumber uint64) error {
	return nil
}
func (m *mockDPoS) IsActiveValidator(id [32]byte) bool {
	return false
}
func (m *mockDPoS) GetValidatorStake(id [32]byte) uint64 { return 0 }
func (m *mockDPoS) GetTotalStake() uint64                { return 0 }

// Required for types.DPoS: GetActiveValidators
func (m *mockDPoS) GetActiveValidators() []*types.Validator {
	return nil
}
func (m *mockDPoS) Start() error {
	return nil
}
func (m *mockDPoS) Stop() error {
	return nil
}

// mockPoH implements types.PoH, including EstimateTimeToCount now.
type mockPoH struct {
	tickDelay time.Duration
}

// GenerateProof implements types.PoH.
func (m *mockPoH) GenerateProof(data []byte, iterations uint64) ([32]byte, [32]byte, uint64, error) {
	panic("unimplemented")
}

// VerifyHashRange implements types.PoH.
func (m *mockPoH) VerifyHashRange(startState [32]byte, startCount uint64, hashes [][32]byte) bool {
	panic("unimplemented")
}

// VerifyProof implements types.PoH.
func (m *mockPoH) VerifyProof(startState [32]byte, data []byte, proof [32]byte, startCount uint64, iterations uint64) (bool, error) {
	panic("unimplemented")
}

func (m *mockPoH) GetTickDelay() time.Duration { return m.tickDelay }
func (m *mockPoH) SetTickDelay(d time.Duration) error {
	m.tickDelay = d
	return nil
}
func (m *mockPoH) Record(data []byte) [32]byte {
	return [32]byte{}
}
func (m *mockPoH) Verify(prevState [32]byte, data []byte, proof [32]byte, count uint64) bool {
	return false
}
func (m *mockPoH) Synchronize(targetState [32]byte, targetCount uint64) error {
	return nil
}
func (m *mockPoH) Tick()                          {}
func (m *mockPoH) AdvanceState(iterations uint64) {}
func (m *mockPoH) GetState() [32]byte             { return [32]byte{} }
func (m *mockPoH) GetCount() uint64               { return 0 }

// Required for types.PoH: EstimateTimeToCount
func (m *mockPoH) EstimateTimeToCount(targetCount uint64) time.Duration {
	return 0
}
func (m *mockPoH) Start() error {
	return nil
}
func (m *mockPoH) Stop() error {
	return nil
}

// -------------------- Tests -----------------------

func TestOptimizer_CollectSample(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.3,
		lachesis: &mockLachesis{
			gossipDelay:     100 * time.Millisecond,
			votingThreshold: 0.66,
		},
		dpos: &mockDPoS{
			setSize:       21,
			epochDuration: 100,
		},
		poh: &mockPoH{
			tickDelay: 50 * time.Millisecond,
		},
	}
	logger := &mockLogger{}
	opt := aiopt.NewOptimizer(cons, logger)

	opt.CollectSample()

	stats := opt.GetOptimizationStats()
	if stats["sample_count"] != 1 {
		t.Errorf("expected sample_count=1, got %v", stats["sample_count"])
	}
	if stats["current_load"] != 0.3 {
		t.Errorf("expected current_load=0.3, got %v", stats["current_load"])
	}
}

func TestOptimizer_PredictLoad(t *testing.T) {
	cons := &mockConsensus{networkLoad: 0.5}
	opt := aiopt.NewOptimizer(cons, &mockLogger{})

	// Add multiple samples
	for i := 0; i < 20; i++ {
		cons.networkLoad = float64(i) / 20.0
		opt.CollectSample()
	}

	pred := opt.PredictLoad()
	if pred < 0 || pred > 1 {
		t.Errorf("predictLoad should be within [0,1], got %.2f", pred)
	}
}

func TestOptimizer_OptimizeConsensus(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.8,
		lachesis: &mockLachesis{
			gossipDelay:     200 * time.Millisecond,
			votingThreshold: 0.66,
		},
		dpos: &mockDPoS{
			setSize:       21,
			epochDuration: 100,
		},
		poh: &mockPoH{
			tickDelay: 50 * time.Millisecond,
		},
	}
	logger := &mockLogger{}
	opt := aiopt.NewOptimizer(cons, logger)

	for i := 0; i < 15; i++ {
		opt.CollectSample()
	}
	opt.OptimizeConsensus()

	lach := cons.lachesis
	if lach.GetGossipDelay() == 200*time.Millisecond {
		t.Errorf("expected gossipDelay to adjust under high load, but it didn't")
	}
	if lach.GetVotingThreshold() <= 0.66 {
		t.Errorf("expected votingThreshold to exceed 0.66 under high load")
	}
	dp := cons.dpos
	if dp.GetSetSize() <= 21 {
		t.Errorf("expected setSize to be above 21 under high load")
	}
	ph := cons.poh
	if ph.GetTickDelay() <= 50*time.Millisecond {
		t.Errorf("expected tickDelay to exceed 50ms under high load")
	}
}

func TestOptimizer_Run(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.1,
		lachesis: &mockLachesis{
			gossipDelay:     100 * time.Millisecond,
			votingThreshold: 0.66,
		},
		dpos: &mockDPoS{
			setSize:       21,
			epochDuration: 100,
		},
		poh: &mockPoH{
			tickDelay: 50 * time.Millisecond,
		},
	}
	opt := aiopt.NewOptimizer(cons, &mockLogger{})

	stopChan := make(chan struct{})
	go opt.Run(stopChan)

	time.Sleep(300 * time.Millisecond)
	close(stopChan)

	stats := opt.GetOptimizationStats()
	count, ok := stats["sample_count"].(int)
	if !ok || count < 1 {
		t.Errorf("expected sample_count>=1, got %v", count)
	}
}

func TestOptimizer_ResetOptimization(t *testing.T) {
	cons := &mockConsensus{
		networkLoad: 0.5,
		lachesis: &mockLachesis{
			gossipDelay:     200 * time.Millisecond,
			votingThreshold: 0.7,
		},
		dpos: &mockDPoS{
			setSize:       30,
			epochDuration: 500,
		},
		poh: &mockPoH{
			tickDelay: 80 * time.Millisecond,
		},
	}
	opt := aiopt.NewOptimizer(cons, &mockLogger{})

	for i := 0; i < 5; i++ {
		opt.CollectSample()
	}

	opt.ResetOptimization()

	stats := opt.GetOptimizationStats()
	if stats["sample_count"] != 0 {
		t.Errorf("expected sample_count=0 after reset, got %v", stats["sample_count"])
	}
	if cons.lachesis.GetGossipDelay() != 100*time.Millisecond {
		t.Errorf("lachesis gossipDelay not reset to default 100ms")
	}
	if cons.lachesis.GetVotingThreshold() != 0.66 {
		t.Errorf("lachesis voting threshold not reset to 0.66")
	}
	if cons.dpos.GetSetSize() != 21 {
		t.Errorf("DPoS setSize not reset to 21")
	}
	if cons.dpos.GetEpochDuration() != 100 {
		t.Errorf("DPoS epochDuration not reset to 100")
	}
	if cons.poh.GetTickDelay() != 50*time.Millisecond {
		t.Errorf("PoH tickDelay not reset to 50ms")
	}
}

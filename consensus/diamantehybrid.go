// consensus/diamantehybrid.go

package consensus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"diamante/consensus/aiopt"
	finality "diamante/consensus/diamantefinality"
	"diamante/consensus/diamantepoh"
	"diamante/consensus/diamantepos"
	"diamante/consensus/governance"
	"diamante/consensus/types"
)

type HybridMode int

const (
	ProductionMode HybridMode = iota
	TestMode
)

// By default, we do not allow any drift in production:
var DefaultPoHDriftTolerance uint64 = 0

// HybridConsensusConfig wraps your parameters (like gossipDelay, etc.)
// plus optional PoH drift tolerance.
type HybridConsensusConfig struct {
	Mode           HybridMode
	GossipDelay    time.Duration
	PoHTickDelay   time.Duration
	DPoSSetSize    int
	DPoSEpoch      uint64
	VotingDuration time.Duration

	// PoHDriftTolerance => how many PoH ticks we allow a finalizing event
	// to be "ahead" of us before failing verification. 0 means none allowed.
	PoHDriftTolerance uint64
}

// Helper: Production defaults
func DefaultHybridConfig() HybridConsensusConfig {
	return HybridConsensusConfig{
		Mode:              ProductionMode,
		GossipDelay:       1 * time.Second,
		PoHTickDelay:      1 * time.Second,
		DPoSSetSize:       21,
		DPoSEpoch:         1000,
		VotingDuration:    2 * time.Minute,
		PoHDriftTolerance: 0,
	}
}

// Helper: Test defaults
func TestHybridConfig() HybridConsensusConfig {
	return HybridConsensusConfig{
		Mode:              TestMode,
		GossipDelay:       50 * time.Millisecond,
		PoHTickDelay:      200 * time.Millisecond,
		DPoSSetSize:       21,
		DPoSEpoch:         100,
		VotingDuration:    1 * time.Minute,
		PoHDriftTolerance: 5, // let events be ~5 ticks ahead
	}
}

const (
	CheckpointInterval  = 1000
	MaxPendingEvents    = 10000
	defaultTestTimeout  = 5 * time.Second
	shortTestTimeout    = 1 * time.Second
	defaultWaitTime     = 100 * time.Millisecond
	finalizationTimeout = 5 * time.Second
)

type lachesisTestState struct {
	DAGState        []byte              `json:"DAGState"`
	GossipState     []byte              `json:"GossipState"`
	VotingState     []byte              `json:"VotingState"`
	FinalizerState  []byte              `json:"FinalizerState"`
	FinalizedEvents map[uint64][]string `json:"FinalizedEvents"`
}

type Block struct {
	Number    uint64         `json:"number"`
	Timestamp time.Time      `json:"timestamp"`
	Producer  string         `json:"producer"` // Hex-encoded
	Events    []*types.Event `json:"events"`
	PoHHash   string         `json:"poh_hash"` // Hex-encoded
}

type Checkpoint struct {
	BlockNumber   uint64
	LachesisState []byte
	DPoSState     []byte
	PoHState      [32]byte
	PoHCount      uint64
}

type hybridConsensusLogger struct {
	logger *log.Logger
}

var _ types.Consensus = (*HybridConsensus)(nil)

// loggerAdapter implements Lachesis/DPoS’s logging adapter interface.
type loggerAdapter struct {
	logger *log.Logger
}

func (l *loggerAdapter) Info(msg string, keyvals ...interface{}) {
	l.logger.Printf("INFO: "+msg, keyvals...)
}
func (l *loggerAdapter) Error(msg string, keyvals ...interface{}) {
	l.logger.Printf("ERROR: "+msg, keyvals...)
}

// The core HybridConsensus struct
type HybridConsensus struct {
	lachesis   types.Lachesis
	dpos       types.DPoS
	poh        types.PoH
	optimizer  *aiopt.Optimizer
	governance *governance.Governance
	logger     *hybridConsensusLogger

	// NEW: store entire config + drift tolerance
	cfg            HybridConsensusConfig
	driftTolerance uint64

	// Mutexes for different state variables
	stateMu           sync.RWMutex
	blockHeightMu     sync.RWMutex
	lastBlockHashMu   sync.RWMutex
	finalizedEventsMu sync.RWMutex
	pendingEventsMu   sync.RWMutex
	checkpointsMu     sync.RWMutex

	lastBlockHeight     uint64
	lastBlockHash       [32]byte
	pendingEvents       []*types.Event
	finalizedEvents     map[uint64][]*types.Event
	lastFinalizedHeight uint64
	checkpoints         map[uint64]*Checkpoint
	lastCheckpoint      uint64

	stopChan  chan struct{}
	wg        sync.WaitGroup
	running   bool
	startOnce sync.Once
	stopOnce  sync.Once
	ctx       context.Context
	cancel    context.CancelFunc

	cleanupWg sync.WaitGroup

	eventProcessingMu   sync.Mutex
	stateVerificationMu sync.Mutex
}

// -----------------------------------------------------
// NEW - clamp logic for PoH and optional block limit
// -----------------------------------------------------

// These constants keep PoH or Gossip from going infinite or 0:
const (
	minPoHDelay    = 1 * time.Millisecond
	maxPoHDelay    = 5 * time.Second
	minGossipDelay = 5 * time.Millisecond
	maxGossipDelay = 1 * time.Second
)

// clampDuration: ensures d is between [lower, upper].
func clampDuration(d, lower, upper time.Duration) time.Duration {
	if d < lower {
		return lower
	}
	if d > upper {
		return upper
	}
	return d
}

// Optionally, if in TestMode, we can limit the total # of blocks so tests don’t run forever:
const testMaxBlocks = 200 // or pick your limit

// -----------------------------------------------------
// NEW - Alternative constructor w/ HybridConsensusConfig
// -----------------------------------------------------
func NewHybridConsensusWithConfig(cfg HybridConsensusConfig) *HybridConsensus {
	ctx, cancel := context.WithCancel(context.Background())
	initialState := sha256.Sum256([]byte("Diamante Initial State"))
	logger := newHybridConsensusLogger()
	logAdapter := &loggerAdapter{logger: logger.logger}

	lachesis := finality.NewLachesis(cfg.GossipDelay)
	dpos := diamantepos.NewDPoS(cfg.DPoSSetSize, cfg.DPoSEpoch, logAdapter)
	poh := diamantepoh.NewPoH(initialState, cfg.PoHTickDelay, logAdapter)

	hc := &HybridConsensus{
		lachesis:       lachesis,
		dpos:           dpos,
		poh:            poh,
		cfg:            cfg,
		driftTolerance: cfg.PoHDriftTolerance,

		checkpoints:     make(map[uint64]*Checkpoint),
		lastCheckpoint:  0,
		pendingEvents:   make([]*types.Event, 0, MaxPendingEvents),
		finalizedEvents: make(map[uint64][]*types.Event),

		logger:              logger,
		lastFinalizedHeight: 0,
		lastBlockHeight:     0,
		lastBlockHash:       initialState,
		stopChan:            make(chan struct{}),
		ctx:                 ctx,
		cancel:              cancel,
	}

	// Create AI optimizer
	hc.optimizer = aiopt.NewOptimizer(hc, logAdapter)
	// Create Governance
	hc.governance = governance.NewGovernance(hc, cfg.VotingDuration, logAdapter)

	return hc
}

// -----------------------------------------
// Keep old constructor for compatibility
// -----------------------------------------
func NewHybridConsensus(
	gossipDelay, pohTickDelay time.Duration,
	dposSetSize int,
	dposEpochDuration uint64,
	votingDuration time.Duration,
) *HybridConsensus {
	// For production if not specified otherwise
	cfg := HybridConsensusConfig{
		Mode:              ProductionMode,
		GossipDelay:       gossipDelay,
		PoHTickDelay:      pohTickDelay,
		DPoSSetSize:       dposSetSize,
		DPoSEpoch:         dposEpochDuration,
		VotingDuration:    votingDuration,
		PoHDriftTolerance: 0,
	}
	return NewHybridConsensusWithConfig(cfg)
}

// -------------------------
// Existing code below
// -------------------------

func newHybridConsensusLogger() *hybridConsensusLogger {
	return &hybridConsensusLogger{
		logger: log.New(os.Stdout, "HybridConsensus: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *hybridConsensusLogger) Info(msg string, keyvals ...interface{}) {
	if len(keyvals)%2 != 0 {
		l.logger.Printf("INFO: %s - Keyvalues must be in pairs", msg)
		return
	}
	var sb strings.Builder
	sb.WriteString("INFO: ")
	sb.WriteString(msg)
	for i := 0; i < len(keyvals); i += 2 {
		sb.WriteString(fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1]))
	}
	l.logger.Println(sb.String())
}
func (l *hybridConsensusLogger) Error(msg string, keyvals ...interface{}) {
	if len(keyvals)%2 != 0 {
		l.logger.Printf("ERROR: %s - Keyvalues must be in pairs", msg)
		return
	}
	var sb strings.Builder
	sb.WriteString("ERROR: ")
	sb.WriteString(msg)
	for i := 0; i < len(keyvals); i += 2 {
		sb.WriteString(fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1]))
	}
	l.logger.Println(sb.String())
}
func (l *hybridConsensusLogger) Printf(format string, v ...interface{}) {
	l.logger.Printf(format, v...)
}

// Implement getters
func (hc *HybridConsensus) GetLachesis() types.Lachesis { return hc.lachesis }
func (hc *HybridConsensus) GetDPoS() types.DPoS         { return hc.dpos }
func (hc *HybridConsensus) GetPoH() types.PoH           { return hc.poh }

// Safely handle block height
func (hc *HybridConsensus) GetLastBlockHeight() uint64 {
	hc.blockHeightMu.RLock()
	defer hc.blockHeightMu.RUnlock()
	return hc.lastBlockHeight
}
func (hc *HybridConsensus) SetLastBlockHeight(h uint64) {
	hc.blockHeightMu.Lock()
	defer hc.blockHeightMu.Unlock()
	hc.lastBlockHeight = h
}

// Safely handle last block hash
func (hc *HybridConsensus) GetLastBlockHash() [32]byte {
	hc.lastBlockHashMu.RLock()
	defer hc.lastBlockHashMu.RUnlock()
	return hc.lastBlockHash
}
func (hc *HybridConsensus) SetLastBlockHash(hash [32]byte) {
	hc.lastBlockHashMu.Lock()
	defer hc.lastBlockHashMu.Unlock()
	hc.lastBlockHash = hash
}

// running state
func (hc *HybridConsensus) IsRunning() bool {
	hc.stateMu.RLock()
	defer hc.stateMu.RUnlock()
	return hc.running
}
func (hc *HybridConsensus) SetRunning(r bool) {
	hc.stateMu.Lock()
	defer hc.stateMu.Unlock()
	hc.running = r
}

// Add / remove validators
func (hc *HybridConsensus) AddValidator(id [32]byte, stake uint64) {
	hc.lachesis.AddNode(id, stake)
	hc.dpos.AddValidator(id, stake)
}

// Create a dummy checkpoint used in tests
func (hc *HybridConsensus) createTestCheckpoint(blockNumber uint64) error {
	lachesisState := lachesisTestState{
		DAGState:        []byte(`{"events":{},"nodes":{},"max_height":0}`),
		GossipState:     []byte(`{"Peers":{},"BaseDelay":100000000,"CurrentDelay":100000000,"NetworkLoad":0}`),
		VotingState:     []byte(`{"Threshold":0.66}`),
		FinalizerState:  []byte(`{"Finalized":{},"Checkpoints":{}}`),
		FinalizedEvents: make(map[uint64][]string),
	}
	lachesisData, err := json.Marshal(lachesisState)
	if err != nil {
		return fmt.Errorf("failed to marshal Lachesis state: %w", err)
	}

	// Collect DPoS validator info
	activeVals := hc.dpos.GetValidators()
	validatorMap := make(map[string]interface{})
	activeIDs := make([]string, 0)
	for _, v := range activeVals {
		idStr := hex.EncodeToString(v.ID[:])
		validatorMap[idStr] = struct {
			ID    string `json:"id"`
			Stake uint64 `json:"stake"`
		}{
			ID:    idStr,
			Stake: v.Stake,
		}
		activeIDs = append(activeIDs, idStr)
	}
	var epochDur uint64
	var maxSetSize int
	if dposImpl, ok := hc.dpos.(*diamantepos.DPoS); ok {
		epochDur = dposImpl.GetEpochDuration()
		maxSetSize = dposImpl.GetSetSize()
	} else {
		epochDur = 100
		maxSetSize = 21
	}
	dposState := struct {
		Validators       map[string]interface{} `json:"validators"`
		ActiveValidators []string               `json:"active_validators"`
		LastBlockHeight  uint64                 `json:"last_block_height"`
		EpochDuration    uint64                 `json:"epoch_duration"`
		CurrentEpoch     uint64                 `json:"current_epoch"`
		MaxSetSize       int                    `json:"max_set_size"`
	}{
		Validators:       validatorMap,
		ActiveValidators: activeIDs,
		LastBlockHeight:  blockNumber,
		EpochDuration:    epochDur,
		CurrentEpoch:     blockNumber / epochDur,
		MaxSetSize:       maxSetSize,
	}
	dposData, err := json.Marshal(dposState)
	if err != nil {
		return fmt.Errorf("failed to marshal DPoS state: %w", err)
	}

	ck := &Checkpoint{
		BlockNumber:   blockNumber,
		LachesisState: lachesisData,
		DPoSState:     dposData,
		PoHState:      hc.poh.GetState(),
		PoHCount:      hc.poh.GetCount(),
	}

	hc.checkpointsMu.Lock()
	hc.checkpoints[blockNumber] = ck
	hc.lastCheckpoint = blockNumber
	hc.checkpointsMu.Unlock()

	return nil
}
func (hc *HybridConsensus) HasCheckpoint(blockNum uint64) bool {
	hc.checkpointsMu.RLock()
	defer hc.checkpointsMu.RUnlock()
	_, ok := hc.checkpoints[blockNum]
	return ok
}

// The crucial CreateEvent method
func (hc *HybridConsensus) CreateEvent(
	creator [32]byte,
	parentIDs [][32]byte,
	data []byte,
) *types.Event {
	hc.logger.Info("CreateEvent: Starting", "creator", hex.EncodeToString(creator[:]))
	hc.stateVerificationMu.Lock()
	defer hc.stateVerificationMu.Unlock()

	if !hc.dpos.IsActiveValidator(creator) {
		return nil
	}
	pohState := hc.poh.GetState()
	pohCount := hc.poh.GetCount()
	pohHash := hc.poh.Record(data)

	e := hc.lachesis.CreateEvent(creator, parentIDs, data)
	if e == nil {
		return nil
	}
	// Attach PoH info
	e.PoHState = pohState
	e.PoHCount = pohCount
	e.PoHProof = pohHash

	// Immediately add this event to pending so the *next* ProcessBlock includes it
	hc.pendingEventsMu.Lock()
	hc.pendingEvents = append(hc.pendingEvents, e)
	hc.pendingEventsMu.Unlock()

	// Reward
	hc.dpos.RewardValidator(creator)
	return e
}

// FinalizeEvent verifies PoH (with optional drift) & calls lachesis.ProcessEvent
func (hc *HybridConsensus) FinalizeEvent(ev *types.Event) (bool, error) {
	if ev == nil {
		return false, errors.New("nil event")
	}
	hc.stateVerificationMu.Lock()
	defer hc.stateVerificationMu.Unlock()

	if !hc.dpos.IsActiveValidator(ev.Creator) {
		return false, errors.New("event creator is not an active validator")
	}
	// REPLACE old direct PoH verify with drift-based helper:
	if !hc.verifyPoHWithDrift(ev.PoHState, ev.Data, ev.PoHProof, ev.PoHCount) {
		return false, errors.New("PoH verification failed (with drift check)")
	}
	if hc.lachesis.ProcessEvent(ev) {
		blockHeight := hc.GetLastBlockHeight()
		hc.finalizedEventsMu.Lock()
		hc.finalizedEvents[blockHeight] = append(hc.finalizedEvents[blockHeight], ev)
		hc.finalizedEventsMu.Unlock()

		atomic.StoreUint64(&hc.lastFinalizedHeight, ev.Height)
		hc.dpos.RewardValidator(ev.Creator)
		return true, nil
	}
	return false, nil
}

// NEW - verifyPoHWithDrift tries exact PoH first, else checks drift.
func (hc *HybridConsensus) verifyPoHWithDrift(
	state [32]byte,
	data []byte,
	proof [32]byte,
	count uint64,
) bool {
	// If exact verify passes => good
	if hc.poh.Verify(state, data, proof, count) {
		return true
	}
	// If no drift tolerance => fail
	if hc.driftTolerance == 0 {
		return false
	}
	// Otherwise, see if count is within [poh.GetCount()+1 ... poh.GetCount()+driftTolerance]
	currentCount := hc.poh.GetCount()
	if count > currentCount && count <= currentCount+hc.driftTolerance {
		hc.logger.Info("PoH drift tolerance triggered",
			"currentCount", currentCount,
			"eventCount", count,
			"tolerance", hc.driftTolerance,
		)
		return true
	}
	return false
}

// Utility to gather pending events
func (hc *HybridConsensus) collectPendingEvents() []*types.Event {
	hc.pendingEventsMu.Lock()
	defer hc.pendingEventsMu.Unlock()
	evs := hc.pendingEvents
	hc.pendingEvents = nil
	return evs
}

// produceBlock lumps pending events into a new block
func (hc *HybridConsensus) produceBlock(blockNumber uint64, producerID [32]byte) (*Block, error) {
	events := hc.collectPendingEvents()
	data, err := serializeEvents(events)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize events: %w", err)
	}
	hash := sha256.Sum256(data)
	pohHash := hc.poh.Record(hash[:])

	return &Block{
		Number:    blockNumber,
		Timestamp: time.Now(),
		Producer:  hex.EncodeToString(producerID[:]),
		Events:    events,
		PoHHash:   hex.EncodeToString(pohHash[:]),
	}, nil
}

// validateBlock
func (hc *HybridConsensus) validateBlock(block *Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	expected := hc.GetLastBlockHeight() + 1
	if block.Number != expected {
		return fmt.Errorf("invalid block number: expected %d, got %d", expected, block.Number)
	}
	if block.Timestamp.After(time.Now().Add(time.Second)) {
		return errors.New("block timestamp is in the future")
	}
	if block.Producer == "" {
		return errors.New("block producer is empty")
	}
	if block.PoHHash == "" {
		return errors.New("block PoH hash is empty")
	}
	for _, ev := range block.Events {
		if ev == nil {
			return errors.New("block contains nil event")
		}
	}
	return nil
}

// applyBlock finalizes events, re-processes leftover pending, and rewards the producer
func (hc *HybridConsensus) applyBlock(block *Block) error {
	for _, ev := range block.Events {
		finalized, err := hc.FinalizeEvent(ev)
		if err != nil {
			return fmt.Errorf("failed to finalize event: %w", err)
		}
		if !finalized {
			hc.pendingEventsMu.Lock()
			hc.pendingEvents = append(hc.pendingEvents, ev)
			hc.pendingEventsMu.Unlock()
		}
	}
	// Re-process leftover
	hc.processPendingEvents()

	hc.finalizedEventsMu.Lock()
	hc.finalizedEvents[block.Number] = block.Events
	hc.finalizedEventsMu.Unlock()

	producerBytes, err := hex.DecodeString(block.Producer)
	if err != nil {
		return fmt.Errorf("failed to decode producer ID: %w", err)
	}
	var pid [32]byte
	copy(pid[:], producerBytes)
	hc.dpos.RewardValidator(pid)
	return nil
}

// ProcessBlock is the main block-creation path each tick
func (hc *HybridConsensus) ProcessBlock(blockNumber uint64) error {
	hc.logger.Info("ProcessBlock: Starting", "blockNumber", blockNumber)
	if !hc.IsRunning() {
		return errors.New("consensus is not running")
	}
	// checkpoint continuity
	if blockNumber > hc.GetLastBlockHeight()+1 {
		if !hc.HasCheckpoint(blockNumber - 1) {
			return fmt.Errorf("missing checkpoint for block %d", blockNumber-1)
		}
		if err := hc.recoverFromError(); err != nil {
			return fmt.Errorf("failed to recover: %w", err)
		}
	}
	// DPoS epoch checks
	if err := hc.dpos.ProcessEpoch(blockNumber); err != nil {
		if shouldAttemptRecovery(err) {
			hc.logger.Info("Attempting recovery from DPoS error", "error", err)
			if recErr := hc.recoverFromError(); recErr != nil {
				return fmt.Errorf(
					"failed to recover from DPoS error: %w (original error: %v)",
					recErr, err,
				)
			}
		}
		return fmt.Errorf("failed to process DPoS epoch: %w", err)
	}
	// next block producer
	validator := hc.dpos.GetNextValidator(blockNumber, hc.GetLastBlockHash())
	if validator == nil {
		return errors.New("no validator available for block creation")
	}

	// produce
	block, err := hc.produceBlock(blockNumber, validator.ID)
	if err != nil {
		if shouldAttemptRecovery(err) {
			hc.logger.Info("Attempting recovery from block production error", "error", err)
			if recErr := hc.recoverFromError(); recErr != nil {
				return fmt.Errorf("failed to recover from block production error: %w", recErr)
			}
		}
		return fmt.Errorf("failed to produce block: %w", err)
	}
	// validate
	if err := hc.validateBlock(block); err != nil {
		return fmt.Errorf("block validation failed: %w", err)
	}
	// apply
	if err := hc.applyBlock(block); err != nil {
		return fmt.Errorf("failed to apply block: %w", err)
	}

	// update lastBlockHash
	blockData, err := serializeBlock(block)
	if err != nil {
		return fmt.Errorf("failed to serialize block: %w", err)
	}
	hc.SetLastBlockHash(sha256.Sum256(blockData))
	hc.SetLastBlockHeight(blockNumber)

	// Possibly create checkpoint
	if blockNumber%CheckpointInterval == 0 {
		if ckErr := hc.createCheckpoint(blockNumber); ckErr != nil {
			hc.logger.Error("Failed to create checkpoint", "error", ckErr)
		}
	}
	// update lastFinalizedHeight
	if len(block.Events) > 0 {
		lastEvent := block.Events[len(block.Events)-1]
		atomic.StoreUint64(&hc.lastFinalizedHeight, lastEvent.Height)
	}
	return nil
}

func shouldAttemptRecovery(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "state corruption") ||
		strings.Contains(s, "synchronization failed") ||
		strings.Contains(s, "checkpoint not found") ||
		strings.Contains(s, "consensus state mismatch") ||
		strings.Contains(s, "database corruption") ||
		strings.Contains(s, "validator set inconsistency")
}

func serializeBlock(block *Block) ([]byte, error) {
	return json.Marshal(block)
}

func (hc *HybridConsensus) UpdateStake(id [32]byte, newStake uint64) {
	hc.dpos.UpdateStake(id, newStake)
	hc.lachesis.UpdateNodeStake(id, newStake)
}

// get network load from lachesis
func (hc *HybridConsensus) GetNetworkLoad() float64 {
	if lWithLoad, ok := hc.lachesis.(interface{ GetNetworkLoad() float64 }); ok {
		return lWithLoad.GetNetworkLoad()
	}
	return 0
}

// HandleNetworkPartition finalizes or re-pends
func (hc *HybridConsensus) HandleNetworkPartition(events []*types.Event) error {
	sort.Slice(events, func(i, j int) bool {
		return events[i].Height < events[j].Height
	})
	for _, ev := range events {
		if ev.Height <= atomic.LoadUint64(&hc.lastFinalizedHeight) {
			continue
		}
		finalized, err := hc.FinalizeEvent(ev)
		if err != nil {
			return fmt.Errorf("failed to finalize partition event: %w", err)
		}
		if !finalized {
			hc.pendingEventsMu.Lock()
			hc.pendingEvents = append(hc.pendingEvents, ev)
			hc.pendingEventsMu.Unlock()
		}
	}
	hc.processPendingEvents()
	return nil
}

func (hc *HybridConsensus) Start() error {
	hc.stateMu.Lock()
	if hc.running {
		hc.stateMu.Unlock()
		return errors.New("consensus is already running")
	}
	hc.running = true
	hc.stopChan = make(chan struct{})
	hc.ctx, hc.cancel = context.WithCancel(context.Background())
	hc.stateMu.Unlock()

	var startErr error
	hc.startOnce.Do(func() {
		if s, ok := hc.lachesis.(interface{ Start() error }); ok {
			if err := s.Start(); err != nil {
				startErr = fmt.Errorf("failed to start Lachesis: %w", err)
				return
			}
		}
		if s, ok := hc.dpos.(interface{ Start() error }); ok {
			if err := s.Start(); err != nil {
				startErr = fmt.Errorf("failed to start DPoS: %w", err)
				return
			}
		}
		hc.wg.Add(4)

		// 1) PoH
		go func() {
			defer hc.wg.Done()
			if err := hc.startPoH(); err != nil {
				hc.logger.Error("PoH error", "error", err)
			}
		}()

		// 2) Optimizer => track with cleanupWg
		hc.cleanupWg.Add(1)
		go func() {
			defer hc.cleanupWg.Done()
			defer hc.wg.Done()
			hc.startOptimizer()
		}()

		// 3) Governance => track with cleanupWg
		hc.cleanupWg.Add(1)
		go func() {
			defer hc.cleanupWg.Done()
			defer hc.wg.Done()
			hc.startGovernance()
		}()

		// 4) Block Production
		go func() {
			defer hc.wg.Done()
			if err := hc.startBlockProduction(); err != nil {
				hc.logger.Error("Block production error", "error", err)
			}
		}()

		hc.logger.Info("Consensus started successfully")
	})
	return startErr
}

// START PoH => each tick
func (hc *HybridConsensus) startPoH() error {
	// clamp on start
	delay := clampDuration(hc.poh.GetTickDelay(), minPoHDelay, maxPoHDelay)
	hc.poh.SetTickDelay(delay)

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// re-clamp each loop (if we had dynamic changes)
			d := hc.poh.GetTickDelay()
			d = clampDuration(d, minPoHDelay, maxPoHDelay)
			hc.poh.SetTickDelay(d)

			hc.poh.Tick()
			cnt := hc.poh.GetCount()
			hc.logger.Info("PoH Tick", "count", cnt, "state", fmt.Sprintf("%x", hc.poh.GetState()))

		case <-hc.stopChan:
			hc.logger.Info("PoH stopped via stopChan")
			return nil
		case <-hc.ctx.Done():
			hc.logger.Info("PoH stopped via ctx.Done()")
			return nil
		}
	}
}

func (hc *HybridConsensus) startOptimizer() {
	select {
	case <-hc.stopChan:
		hc.logger.Info("Optimizer stopping")
		return
	default:
		hc.optimizer.Run(hc.stopChan)
	}
}

func (hc *HybridConsensus) startGovernance() {
	select {
	case <-hc.stopChan:
		hc.logger.Info("Governance stopping")
		return
	default:
		hc.governance.Run(hc.stopChan, 2*time.Second)
	}
}

// START block production => possibly clamp or limit total # of blocks if test
func (hc *HybridConsensus) startBlockProduction() error {
	ticker := time.NewTicker(time.Second)
	metricsTicker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer metricsTicker.Stop()

	// If in test mode, announce + track the block limit
	if hc.cfg.Mode == TestMode {
		hc.logger.Info("Test mode detected; limiting block production to", "max_blocks", testMaxBlocks)
	}

	for {
		select {
		case <-ticker.C:
			// If consensus is no longer running, exit
			if !hc.IsRunning() {
				return nil
			}

			// If we are in test mode, stop after testMaxBlocks
			if hc.cfg.Mode == TestMode && hc.GetLastBlockHeight() >= testMaxBlocks {
				hc.logger.Info("Reached test block limit => stopping block production")
				// Explicitly call Stop so that PoH and other goroutines exit
				go hc.Stop()
				return nil
			}

			// Attempt next block
			bnum := hc.GetLastBlockHeight() + 1
			if err := hc.ProcessBlock(bnum); err != nil {
				hc.logger.Error("Block processing error", "block", bnum, "error", err)
				if rErr := hc.recoverFromError(); rErr != nil {
					hc.logger.Error("Recovery failed", "block", bnum, "error", rErr)
				}
			}

		case <-metricsTicker.C:
			hc.collectMetrics()

		case <-hc.stopChan:
			// We got a stop signal from somewhere
			return nil
		}
	}
}

func (hc *HybridConsensus) Stop() error {
	hc.stateMu.Lock()
	if !hc.running {
		hc.stateMu.Unlock()
		return errors.New("consensus is not running")
	}
	hc.stateMu.Unlock()

	var stopErr error
	hc.stopOnce.Do(func() {
		hc.stateMu.Lock()
		hc.running = false
		hc.cancel()
		close(hc.stopChan)
		hc.stateMu.Unlock()

		if stopper, ok := hc.lachesis.(interface{ Stop() error }); ok {
			if err := stopper.Stop(); err != nil {
				hc.logger.Error("Failed to stop Lachesis", "error", err)
			}
		}
		if stopper, ok := hc.dpos.(interface{ Stop() error }); ok {
			if err := stopper.Stop(); err != nil {
				hc.logger.Error("Failed to stop DPoS", "error", err)
			}
		}

		done := make(chan struct{})
		go func() {
			hc.wg.Wait()
			hc.cleanupWg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(defaultTestTimeout):
			stopErr = errors.New("timeout waiting for goroutines to stop")
		}
	})
	return stopErr
}

func (hc *HybridConsensus) GetCurrentHeight() uint64 {
	return atomic.LoadUint64(&hc.lastFinalizedHeight)
}

// createCheckpoint at the given blockNumber
func (hc *HybridConsensus) createCheckpoint(blockNumber uint64) error {
	hc.logger.Info("Creating checkpoint", "blockNumber", blockNumber)

	lachState, err := hc.lachesis.(interface{ GetState() ([]byte, error) }).GetState()
	if err != nil {
		return fmt.Errorf("failed to get Lachesis state: %w", err)
	}
	dposState, err := hc.dpos.(interface{ GetState() ([]byte, error) }).GetState()
	if err != nil {
		return fmt.Errorf("failed to get DPoS state: %w", err)
	}
	pohSt := hc.poh.GetState()
	pohCnt := hc.poh.GetCount()

	ck := &Checkpoint{
		BlockNumber:   blockNumber,
		LachesisState: lachState,
		DPoSState:     dposState,
		PoHState:      pohSt,
		PoHCount:      pohCnt,
	}
	hc.checkpointsMu.Lock()
	hc.checkpoints[blockNumber] = ck
	hc.lastCheckpoint = blockNumber
	hc.checkpointsMu.Unlock()

	hc.cleanOldCheckpoints(blockNumber)
	hc.logger.Info("Checkpoint created successfully",
		"blockNumber", blockNumber,
		"pohCount", pohCnt)
	return nil
}

func (hc *HybridConsensus) cleanOldCheckpoints(currentBlockNumber uint64) {
	if currentBlockNumber <= CheckpointInterval*2 {
		return
	}
	hc.checkpointsMu.Lock()
	defer hc.checkpointsMu.Unlock()
	for bn := range hc.checkpoints {
		if bn < currentBlockNumber-CheckpointInterval*2 {
			delete(hc.checkpoints, bn)
		}
	}
}

func (hc *HybridConsensus) GetValidators() []*types.Validator {
	return hc.dpos.GetValidators()
}
func (hc *HybridConsensus) GetActiveValidators() []*types.Validator {
	return hc.dpos.GetActiveValidators()
}

// Attempt to re-finalize any pending events
func (hc *HybridConsensus) processPendingEvents() {
	hc.pendingEventsMu.Lock()
	defer hc.pendingEventsMu.Unlock()

	var remaining []*types.Event
	for _, e := range hc.pendingEvents {
		finalized, err := hc.FinalizeEvent(e)
		if err != nil {
			hc.logger.Printf("Error finalizing event: %v\n", err)
			remaining = append(remaining, e)
		} else if !finalized {
			remaining = append(remaining, e)
		}
	}
	hc.pendingEvents = remaining
}

func (hc *HybridConsensus) GetPendingEvents() []*types.Event {
	hc.pendingEventsMu.RLock()
	defer hc.pendingEventsMu.RUnlock()
	return append([]*types.Event(nil), hc.pendingEvents...)
}

func (hc *HybridConsensus) GetFinalizedEvents(from, to uint64) ([]*types.Event, error) {
	if from > to {
		return nil, errors.New("invalid height range")
	}
	hc.finalizedEventsMu.RLock()
	defer hc.finalizedEventsMu.RUnlock()

	var evs []*types.Event
	for h := from; h <= to; h++ {
		if blockEvs, ok := hc.finalizedEvents[h]; ok {
			evs = append(evs, blockEvs...)
		}
	}
	return evs, nil
}

func (hc *HybridConsensus) SynchronizeState(targetState [32]byte, targetCount uint64) error {
	if err := hc.poh.Synchronize(targetState, targetCount); err != nil {
		return fmt.Errorf("failed to synchronize PoH: %w", err)
	}
	hc.checkpointsMu.RLock()
	defer hc.checkpointsMu.RUnlock()

	var latestCk *Checkpoint
	for bn, c := range hc.checkpoints {
		if c.PoHCount <= targetCount && (latestCk == nil || bn > latestCk.BlockNumber) {
			latestCk = c
		}
	}
	if latestCk == nil {
		return errors.New("no valid checkpoint found for synchronization")
	}
	if err := hc.lachesis.(interface{ RestoreState([]byte) error }).RestoreState(latestCk.LachesisState); err != nil {
		return fmt.Errorf("failed to restore Lachesis state: %w", err)
	}
	if err := hc.dpos.(interface{ RestoreState([]byte) error }).RestoreState(latestCk.DPoSState); err != nil {
		return fmt.Errorf("failed to restore DPoS state: %w", err)
	}
	evs, err := hc.GetFinalizedEvents(latestCk.BlockNumber, hc.GetLastBlockHeight())
	if err != nil {
		return fmt.Errorf("failed to get finalized events for replay: %w", err)
	}
	for _, e := range evs {
		if _, err := hc.FinalizeEvent(e); err != nil {
			return fmt.Errorf("failed to replay event: %w", err)
		}
	}
	return nil
}

// Governance proposals
func (hc *HybridConsensus) ProposeGovernanceChange(
	propType governance.ProposalType,
	desc string,
	data []byte,
	creatorID [32]byte,
) ([32]byte, error) {
	return hc.governance.CreateProposal(propType, desc, data, creatorID)
}
func (hc *HybridConsensus) VoteOnProposal(propID [32]byte, valID [32]byte, vote bool) error {
	return hc.governance.Vote(propID, valID, vote)
}
func (hc *HybridConsensus) ExecuteProposal(propID [32]byte) error {
	return hc.governance.ExecuteProposal(propID)
}
func (hc *HybridConsensus) ScheduleUpgrade(version string, height uint64) error {
	hc.logger.Printf("Scheduled upgrade to version %s at height %d", version, height)
	return nil
}

func (hc *HybridConsensus) getLastCheckpoint() *Checkpoint {
	hc.checkpointsMu.RLock()
	defer hc.checkpointsMu.RUnlock()
	return hc.checkpoints[hc.lastCheckpoint]
}

// recoverFromError reloads from the last checkpoint
func (hc *HybridConsensus) recoverFromError() error {
	ck := hc.getLastCheckpoint()
	if ck == nil {
		hc.logger.Printf("No checkpoint available for recovery")
		return errors.New("no checkpoint available for recovery")
	}
	hc.logger.Printf("Attempting recovery from checkpoint at block %d", ck.BlockNumber)

	if err := hc.lachesis.RestoreState(ck.LachesisState); err != nil {
		return fmt.Errorf("failed to restore Lachesis state: %w", err)
	}
	if err := hc.dpos.RestoreState(ck.DPoSState); err != nil {
		return fmt.Errorf("failed to restore DPoS state: %w", err)
	}
	if err := hc.poh.Synchronize(ck.PoHState, ck.PoHCount); err != nil {
		return fmt.Errorf("failed to synchronize PoH: %w", err)
	}
	hc.SetLastBlockHeight(ck.BlockNumber)
	hc.collectMetrics()
	return nil
}

func (hc *HybridConsensus) collectMetrics() {
	lbh := hc.GetLastBlockHeight()
	hc.pendingEventsMu.RLock()
	pendCount := len(hc.pendingEvents)
	hc.pendingEventsMu.RUnlock()

	activeVals := hc.dpos.GetActiveValidators()
	totalStake := hc.dpos.GetTotalStake()
	netLoad := 0.0
	if withLoad, ok := hc.lachesis.(interface{ GetNetworkLoad() float64 }); ok {
		netLoad = withLoad.GetNetworkLoad()
	}
	pohCount := uint64(0)
	if hc.poh != nil {
		pohCount = hc.poh.GetCount()
	}
	m := map[string]interface{}{
		"last_block_height": lbh,
		"pending_events":    pendCount,
		"active_validators": len(activeVals),
		"total_stake":       totalStake,
		"network_load":      netLoad,
		"poh_count":         pohCount,
	}
	hc.logger.Printf("Hybrid Consensus Metrics: %+v", m)
}

func serializeEvents(events []*types.Event) ([]byte, error) {
	return json.Marshal(events)
}

// cleanupGoroutines
func (hc *HybridConsensus) cleanupGoroutines() {
	hc.logger.Info("cleanupGoroutines: waiting for goroutines to finish")
	hc.wg.Wait()
	hc.cleanupWg.Wait()

	hc.eventProcessingMu.Lock()
	defer hc.eventProcessingMu.Unlock()
	hc.pendingEventsMu.Lock()
	hc.pendingEvents = nil
	hc.pendingEventsMu.Unlock()
}

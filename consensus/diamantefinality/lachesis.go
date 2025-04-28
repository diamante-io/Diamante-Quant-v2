// consensus/diamantefinality/finality/lachesis.go

package finality

import (
	"context"
	"diamante/consensus/types"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

// Lachesis orchestrates a DAG-based aBFT consensus using a DAG, GossipProtocol,
// VirtualVoting, and a Finalizer. It also tracks finalized events, pending events,
// and manages start/stop for the consensus loop.
type Lachesis struct {
	DAG            *DAG            // The DAG holding nodes and events
	GossipProtocol *GossipProtocol // Manages event propagation among peers
	VirtualVoting  *VirtualVoting  // Provides the core voting logic
	Finalizer      *Finalizer      // Finalizes events using the voting results

	// Configuration
	gossipDelay     time.Duration
	votingThreshold float64 // e.g., 0.66 => 66% stake needed for finality
	networkLoad     float64 // ∈ [0,1] for dynamic gossip scaling
	configMu        sync.RWMutex

	// State
	stateMu         sync.RWMutex
	pendingEvents   []*types.Event            // Temporary buffer of newly created events
	finalizedEvents map[uint64][]*types.Event // Maps event height -> slice of finalized events at that height
	running         bool                      // True if Lachesis is actively running

	// Additional concurrency structures
	activeProposals   sync.Map   // Tracks proposals & states if using governance extensions
	eventProcessingMu sync.Mutex // Serializes event processing in ProcessEvent

	// NEW: unified context and logging for cancellation and structured logging.
	ctx    context.Context
	cancel context.CancelFunc
	logger *log.Logger
}

// NewLachesis constructs a Lachesis instance with default votingThreshold=0.66
// and sets up the DAG, GossipProtocol, VirtualVoting, and Finalizer submodules.
func NewLachesis(gossipDelay time.Duration) *Lachesis {
	dag := NewDAG()
	l := &Lachesis{
		DAG:             dag,
		gossipDelay:     gossipDelay,
		votingThreshold: 0.66, // can be adjusted later
		networkLoad:     0,
		finalizedEvents: make(map[uint64][]*types.Event),
		// Initialize logger with default settings.
		logger: log.New(os.Stdout, "Lachesis: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
	l.GossipProtocol = NewGossipProtocol(dag, gossipDelay)
	l.VirtualVoting = NewVirtualVoting(dag)
	l.Finalizer = NewFinalizer(dag, l.VirtualVoting)
	return l
}

// ---------------------- Getters/Setters for Config ----------------------

// GetGossipDelay returns the current gossip delay used by Lachesis.
func (l *Lachesis) GetGossipDelay() time.Duration {
	l.configMu.RLock()
	defer l.configMu.RUnlock()
	return l.gossipDelay
}

// SetGossipDelay updates gossipDelay and also applies it to the underlying GossipProtocol.
func (l *Lachesis) SetGossipDelay(delay time.Duration) {
	l.configMu.Lock()
	defer l.configMu.Unlock()
	l.gossipDelay = delay
	l.GossipProtocol.SetBaseDelay(delay)
}

// AdjustNetworkLoad modifies the networkLoad by the given amount (positive or negative),
// clamps it to [0,1], and updates the GossipProtocol. This can be used by external modules
// to indicate changes in network traffic or system load.
func (l *Lachesis) AdjustNetworkLoad(adjustment float64) {
	l.configMu.Lock()
	defer l.configMu.Unlock()
	newLoad := l.networkLoad + adjustment
	l.networkLoad = math.Max(0, math.Min(1, newLoad))
	l.GossipProtocol.UpdateNetworkLoad(l.networkLoad)
}

// GetNetworkLoad returns the current load factor used for scaling gossip delay.
func (l *Lachesis) GetNetworkLoad() float64 {
	l.configMu.RLock()
	defer l.configMu.RUnlock()
	return l.networkLoad
}

// GetVotingThreshold returns the fraction of stake required to finalize an event.
func (l *Lachesis) GetVotingThreshold() float64 {
	l.configMu.RLock()
	defer l.configMu.RUnlock()
	return l.votingThreshold
}

// SetVotingThreshold updates the fraction of stake needed for finality, and applies it to VirtualVoting.
func (l *Lachesis) SetVotingThreshold(threshold float64) {
	l.configMu.Lock()
	defer l.configMu.Unlock()
	l.votingThreshold = threshold
	l.VirtualVoting.SetThreshold(threshold)
}

// ---------------------- DAG / Node Management ----------------------

// AddNode registers a new node in the DAG and also adds it as a gossip peer.
func (l *Lachesis) AddNode(nodeID [32]byte, stake uint64) {
	l.DAG.AddNode(nodeID, stake)
	l.GossipProtocol.AddPeer(nodeID)
}

// UpdateNodeStake changes the stake of an existing node and recalculates VirtualVoting weights.
func (l *Lachesis) UpdateNodeStake(nodeID [32]byte, newStake uint64) {
	l.DAG.UpdateNodeStake(nodeID, newStake)
	l.VirtualVoting.RecalculateWeights()
}

// GetDAGState returns a snapshot of the DAG's known events.
func (l *Lachesis) GetDAGState() map[[32]byte]*types.Event {
	return l.DAG.GetEvents()
}

// GetActiveNodes returns a list of currently active node IDs from the DAG.
func (l *Lachesis) GetActiveNodes() [][32]byte {
	return l.DAG.GetActiveNodes()
}

// ---------------------- Event Creation & Processing ----------------------

// CreateEvent inserts a new event into the DAG (if the creator is active)
// and spawns a goroutine to attempt to finalize it. The newly created event is also
// appended to pendingEvents for reference.
func (l *Lachesis) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *types.Event {
	event, err := l.DAG.NewEvent(creator, parentIDs, data)
	if err != nil {
		return nil // e.g., creator node not active or other error
	}

	l.stateMu.Lock()
	l.pendingEvents = append(l.pendingEvents, event)
	l.stateMu.Unlock()

	// Attempt finalization asynchronously
	go l.ProcessEvent(event)
	return event
}

// ProcessEvent first checks if the creator is active, then uses VirtualVoting
// to see if the event can be finalized. If yes, it calls Finalizer and records
// the event in finalizedEvents. Returns true on success.
func (l *Lachesis) ProcessEvent(event *types.Event) bool {
	if !l.DAG.IsActiveValidator(event.Creator) {
		return false
	}
	// Only one event can be processed at a time
	l.eventProcessingMu.Lock()
	defer l.eventProcessingMu.Unlock()

	if !l.VirtualVoting.Vote(event) {
		return false
	}
	finalized, err := l.Finalizer.Finalize(event)
	if err != nil || !finalized {
		return false
	}
	// Mark event as finalized
	l.stateMu.Lock()
	l.addFinalizedEvent(event)
	l.stateMu.Unlock()

	return true
}

// ---------------------- Proposals (Optional Governance) ----------------------

// UpdateProposalState changes the state of a proposal in activeProposals.
// This is just an example of storing proposal states in a sync.Map.
func (l *Lachesis) UpdateProposalState(proposalID [32]byte, currentState, newState string) error {
	if !isValidStateTransition(currentState, newState) {
		return fmt.Errorf("invalid state transition from %q to %q", currentState, newState)
	}
	l.activeProposals.Store(proposalID, newState)
	return nil
}

// isValidStateTransition is a helper for checking proposal-state transitions.
func isValidStateTransition(current, new string) bool {
	transitions := map[string][]string{
		"pending":  {"active"},
		"active":   {"passed", "rejected"},
		"passed":   {"executed"},
		"rejected": {},
		"executed": {},
	}
	allowed, exists := transitions[current]
	if !exists {
		return false
	}
	for _, st := range allowed {
		if st == new {
			return true
		}
	}
	return false
}

// ---------------------- Finalization Data ----------------------

// addFinalizedEvent appends the event to the map of finalizedEvents by its height.
func (l *Lachesis) addFinalizedEvent(event *types.Event) {
	h := event.Height
	l.finalizedEvents[h] = append(l.finalizedEvents[h], event)
}

// GetFinalizedEvents returns the finalized events in [fromHeight, toHeight].
func (l *Lachesis) GetFinalizedEvents(fromHeight, toHeight uint64) ([]*types.Event, error) {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()

	var result []*types.Event
	for h := fromHeight; h <= toHeight; h++ {
		if evs, ok := l.finalizedEvents[h]; ok {
			result = append(result, evs...)
		}
	}
	return result, nil
}

// detectFinality checks if enough stake has "seen" this event (via VirtualVoting).
// If totalWeight >= requiredWeight => event is considered finalizable.
func (l *Lachesis) detectFinality(event *types.Event) bool {
	activeNodes := l.DAG.GetActiveNodes()
	totalStake := l.DAG.GetTotalStake()

	// Read threshold under config lock
	l.configMu.RLock()
	requiredWeight := float64(totalStake) * l.votingThreshold
	l.configMu.RUnlock()

	var totalWeight float64
	var remainingStake float64 = float64(totalStake)

	// This needs nodesMu to safely read node stakes
	l.DAG.nodesMu.RLock()
	defer l.DAG.nodesMu.RUnlock()

	for _, nid := range activeNodes {
		node, ok := l.DAG.Nodes[nid]
		if !ok || !node.Active {
			continue
		}
		stk := float64(node.Stake)
		remainingStake -= stk

		if l.VirtualVoting.HasSeen(nid, event) {
			totalWeight += stk
			if totalWeight >= requiredWeight {
				return true
			}
		}
		// If even with remaining stake, we can't reach requiredWeight, short-circuit
		if totalWeight+remainingStake < requiredWeight {
			return false
		}
	}
	return false
}

// ---------------------- Pending Events ----------------------

// AddPendingEvent manually appends an event to pendingEvents (if needed).
func (l *Lachesis) AddPendingEvent(event *types.Event) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	l.pendingEvents = append(l.pendingEvents, event)
}

// GetPendingEvents returns a copy of the current pendingEvents slice.
func (l *Lachesis) GetPendingEvents() []*types.Event {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	return append([]*types.Event(nil), l.pendingEvents...)
}

// ClearPendingEvents empties the pendingEvents slice.
func (l *Lachesis) ClearPendingEvents() {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	l.pendingEvents = nil
}

// ---------------------- Lifecycle ----------------------

// Start triggers Lachesis to run if not already running. It starts the GossipProtocol
// and the internal consensus loop (runConsensus).
func (l *Lachesis) Start() error {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()

	if l.running {
		return errors.New("Lachesis is already running")
	}
	l.running = true
	l.activeProposals = sync.Map{} // Reset proposals if needed
	// Create a cancellable context for the consensus loop.
	l.ctx, l.cancel = context.WithCancel(context.Background())

	// Start gossip loops
	go l.GossipProtocol.Start()

	// Start consensus worker using runConsensus which now checks context cancellation.
	go l.runConsensus()
	l.logger.Println("Lachesis started successfully")
	return nil
}

// Stop halts the consensus loop and stops gossip. No finality checks occur after this call.
func (l *Lachesis) Stop() error {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()

	if !l.running {
		return errors.New("Lachesis is not running")
	}
	l.running = false
	if l.cancel != nil {
		l.cancel()
	}
	l.GossipProtocol.Stop()
	l.logger.Println("Lachesis stopped")
	return nil
}

// runConsensus periodically processes unfinalized events to see if they meet the finality conditions.
func (l *Lachesis) runConsensus() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.processEvents()
		case <-l.ctx.Done():
			l.logger.Println("runConsensus: context cancelled, exiting")
			return
		}
	}
}

// processEvents checks for unfinalized events in the DAG and calls detectFinality on them.
// If an event meets finality, we add it to finalizedEvents.
func (l *Lachesis) processEvents() {
	events := l.DAG.GetUnfinalizedEvents()
	for _, ev := range events {
		if l.detectFinality(ev) {
			l.stateMu.Lock()
			l.addFinalizedEvent(ev)
			ev.Finalized = true
			l.stateMu.Unlock()
		}
	}
}

// ForceSync asks the GossipProtocol to broadcast all known events to all peers.
func (l *Lachesis) ForceSync() {
	l.GossipProtocol.ForceSyncAll()
}

// ---------------------- State Serialization ----------------------

// GetState collects DAG, GossipProtocol, VirtualVoting, Finalizer states, plus the list of finalizedEvents.
func (l *Lachesis) GetState() ([]byte, error) {
	l.stateMu.RLock()
	finalizedCopy := make(map[uint64][]string)
	for h, evs := range l.finalizedEvents {
		ids := make([]string, len(evs))
		for i, e := range evs {
			ids[i] = byteArrayToString(e.ID)
		}
		finalizedCopy[h] = ids
	}
	l.stateMu.RUnlock()

	state := struct {
		DAGState        []byte
		GossipState     []byte
		VotingState     []byte
		FinalizerState  []byte
		FinalizedEvents map[uint64][]string
	}{
		FinalizedEvents: finalizedCopy,
	}

	var err error
	if state.DAGState, err = l.DAG.GetState(); err != nil {
		return nil, err
	}
	if state.GossipState, err = l.GossipProtocol.GetState(); err != nil {
		return nil, err
	}
	if state.VotingState, err = l.VirtualVoting.GetState(); err != nil {
		return nil, err
	}
	if state.FinalizerState, err = l.Finalizer.GetState(); err != nil {
		return nil, err
	}

	return json.Marshal(state)
}

// RestoreState loads the Lachesis state from JSON. It first restores DAG, gossip, voting,
// and finalizer states, then rebuilds finalizedEvents references from the DAG.
func (l *Lachesis) RestoreState(stateData []byte) error {
	var s struct {
		DAGState        []byte
		GossipState     []byte
		VotingState     []byte
		FinalizerState  []byte
		FinalizedEvents map[uint64][]string
	}
	if err := json.Unmarshal(stateData, &s); err != nil {
		return err
	}

	if err := l.DAG.RestoreState(s.DAGState); err != nil {
		return err
	}
	if err := l.GossipProtocol.RestoreState(s.GossipState); err != nil {
		return err
	}
	if err := l.VirtualVoting.RestoreState(s.VotingState); err != nil {
		return err
	}
	if err := l.Finalizer.RestoreState(s.FinalizerState); err != nil {
		return err
	}

	l.stateMu.Lock()
	defer l.stateMu.Unlock()

	l.finalizedEvents = make(map[uint64][]*types.Event)
	for h, eventIDs := range s.FinalizedEvents {
		for _, idStr := range eventIDs {
			evID, err := stringToByteArray(idStr)
			if err != nil {
				return fmt.Errorf("invalid event ID %q: %v", idStr, err)
			}
			ev, eErr := l.DAG.GetEvent(evID)
			if eErr != nil {
				return fmt.Errorf("DAG missing event ID %q: %v", idStr, eErr)
			}
			l.finalizedEvents[h] = append(l.finalizedEvents[h], ev)
		}
	}
	return nil
}

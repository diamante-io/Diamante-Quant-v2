// consensus/diamantefinality/finality/gossip.go

package finality

import (
	"diamante/consensus/types"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"
)

// GossipProtocol handles event propagation among network peers via a gossip mechanism.
// Each peer has a dedicated channel receiving events. This protocol automatically
// broadcasts newly seen events to all other peers, ensuring DAG synchronization.
type GossipProtocol struct {
	dag *DAG

	// peers maps each peer's ID to a buffered channel of *types.Event.
	// When we broadcast an event, we attempt to enqueue it into each peer's channel.
	peers map[[32]byte]chan *types.Event

	mu           sync.RWMutex               // Protects peers, stopChannels, baseDelay, currentDelay
	baseDelay    time.Duration              // Base interval between gossip ticks
	currentDelay time.Duration              // Dynamically adjusted delay based on networkLoad
	stopChannels map[[32]byte]chan struct{} // Allows us to stop each peer's gossip loop

	// networkLoad is used to scale currentDelay. If networkLoad=1, currentDelay = 2*baseDelay.
	// If 0, currentDelay=baseDelay.
	networkLoad float64
	loadMu      sync.RWMutex
}

// NewGossipProtocol returns a new GossipProtocol with the specified base delay
// for sending/receiving gossip events.
func NewGossipProtocol(dag *DAG, baseDelay time.Duration) *GossipProtocol {
	return &GossipProtocol{
		dag:          dag,
		peers:        make(map[[32]byte]chan *types.Event),
		baseDelay:    baseDelay,
		currentDelay: baseDelay,
		stopChannels: make(map[[32]byte]chan struct{}),
	}
}

// AddPeer registers a new peer by creating a buffered channel and a stop channel.
// If the peer already exists, this call does nothing.
func (g *GossipProtocol) AddPeer(nodeID [32]byte) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.peers[nodeID]; !exists {
		g.peers[nodeID] = make(chan *types.Event, 1000)
		g.stopChannels[nodeID] = make(chan struct{})
	}
}

// RemovePeer closes the peer's event channel and signals its gossip loop to terminate.
// After removal, subsequent broadcasts won't target this peer.
func (g *GossipProtocol) RemovePeer(nodeID [32]byte) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Close event channel
	if ch, exists := g.peers[nodeID]; exists {
		close(ch)
		delete(g.peers, nodeID)
	}
	// Close stop channel
	if stopCh, exists := g.stopChannels[nodeID]; exists {
		close(stopCh)
		delete(g.stopChannels, nodeID)
	}
}

// BroadcastEvent sends an event to all peers except excludeNodeID. If a peer's channel
// is full, this call drops the event for that peer rather than blocking.
func (g *GossipProtocol) BroadcastEvent(event *types.Event, excludeNodeID [32]byte) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for peerID, peerChan := range g.peers {
		if peerID == excludeNodeID {
			continue // Skip sender
		}
		select {
		case peerChan <- event:
			// Enqueued successfully
		default:
			// Channel is full; skip to avoid blocking the entire broadcast
		}
	}
}

// GetNetworkLoad returns the current network load ∈ [0,1].
func (g *GossipProtocol) GetNetworkLoad() float64 {
	g.loadMu.RLock()
	defer g.loadMu.RUnlock()
	return g.networkLoad
}

// UpdateNetworkLoad sets the new network load (clamped to [0,1]) and updates gossip rate accordingly.
func (g *GossipProtocol) UpdateNetworkLoad(load float64) {
	g.loadMu.Lock()
	defer g.loadMu.Unlock()

	g.networkLoad = math.Max(0, math.Min(1, load))
	g.adjustGossipRate()
}

// SetBaseDelay changes the base delay used in gossip loops and recalculates currentDelay.
func (g *GossipProtocol) SetBaseDelay(delay time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.baseDelay = delay
	// Recompute currentDelay using the existing networkLoad
	g.adjustGossipRateLocked()
}

// getCurrentDelay safely returns the current gossip delay.
func (g *GossipProtocol) getCurrentDelay() time.Duration {
	g.loadMu.RLock()
	defer g.loadMu.RUnlock()
	return g.currentDelay
}

// adjustGossipRate recalculates currentDelay using the formula:
//
//	currentDelay = baseDelay * (1 + networkLoad)
//
// Must be called under g.loadMu.Lock() for thread safety, but
// we also read baseDelay which is protected by g.mu.
func (g *GossipProtocol) adjustGossipRate() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.adjustGossipRateLocked()
}

// adjustGossipRateLocked expects both g.mu and g.loadMu to be locked by the caller (or exclusively locked).
func (g *GossipProtocol) adjustGossipRateLocked() {
	loadFactor := 1 + g.networkLoad
	g.currentDelay = time.Duration(float64(g.baseDelay) * loadFactor)
}

// StartGossiping spawns a goroutine that continuously processes events from the peer's channel
// and re-broadcasts them to other peers. The loop also ticks periodically.
func (g *GossipProtocol) StartGossiping(nodeID [32]byte) {
	go func() {
		ticker := time.NewTicker(g.getCurrentDelay())
		defer ticker.Stop()

		for {
			// We read from peers and stopChannels under a read lock
			g.mu.RLock()
			peerChan, ok1 := g.peers[nodeID]
			stopChan, ok2 := g.stopChannels[nodeID]
			g.mu.RUnlock()

			if !ok1 || !ok2 {
				// The peer or stop channel was removed
				return
			}

			select {
			case <-stopChan:
				// Gossip routine ends
				return
			case event, ok := <-peerChan:
				if !ok {
					// The peerChan was closed (peer removed)
					return
				}
				g.processReceivedEvent(nodeID, event)
			case <-ticker.C:
				// Periodic tick for maintenance, if any
			}
		}
	}()
}

func (g *GossipProtocol) processReceivedEvent(nodeID [32]byte, event *types.Event) {
	if event == nil {
		return
	}
	// If we already have this event, skip
	if existingEvent, err := g.dag.GetEvent(event.ID); err == nil && existingEvent != nil {
		return
	}

	// If node is missing, auto-add with small stake so DAG.NewEvent won't fail
	if !g.dag.IsActiveValidator(nodeID) {
		g.dag.AddNode(nodeID, 1)
	}

	// Attempt to insert
	if _, err := g.dag.NewEvent(nodeID, event.ParentIDs, event.Data); err != nil {
		fmt.Printf("[Gossip] DAG.NewEvent error: %v\n", err)
		return
	}

	// Re-broadcast
	g.BroadcastEvent(event, nodeID)
	// Slightly increase load
	curLoad := g.GetNetworkLoad()
	g.UpdateNetworkLoad(curLoad + 0.01)
}

// ForceSyncAll broadcasts all known events from the DAG to every peer (excluding no one).
func (g *GossipProtocol) ForceSyncAll() {
	events := g.dag.GetEvents()
	for _, ev := range events {
		g.BroadcastEvent(ev, [32]byte{}) // excludeNodeID is empty => broadcast to all
	}
}

// GetPeers returns a slice of peer IDs currently known to the GossipProtocol.
func (g *GossipProtocol) GetPeers() [][32]byte {
	g.mu.RLock()
	defer g.mu.RUnlock()

	results := make([][32]byte, 0, len(g.peers))
	for peerID := range g.peers {
		results = append(results, peerID)
	}
	return results
}

// GetState encodes the gossip protocol's essential fields: peers, baseDelay/currentDelay, and networkLoad.
// We store only the presence of each peer (true boolean) rather than the entire event channel.
func (g *GossipProtocol) GetState() ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	state := struct {
		Peers        map[string]bool `json:"peers"`
		BaseDelay    time.Duration   `json:"base_delay"`
		CurrentDelay time.Duration   `json:"current_delay"`
		NetworkLoad  float64         `json:"network_load"`
	}{
		Peers:        make(map[string]bool, len(g.peers)),
		BaseDelay:    g.baseDelay,
		CurrentDelay: g.currentDelay,
		NetworkLoad:  g.networkLoad,
	}

	// Convert each peer ID from [32]byte to hex
	for peerID := range g.peers {
		state.Peers[byteArrayToString(peerID)] = true
	}
	return json.Marshal(state)
}

// RestoreState rebuilds the peers map (with new channels) and sets the delays/load from the provided JSON data.
func (g *GossipProtocol) RestoreState(stateData []byte) error {
	var state struct {
		Peers        map[string]bool `json:"peers"`
		BaseDelay    time.Duration   `json:"base_delay"`
		CurrentDelay time.Duration   `json:"current_delay"`
		NetworkLoad  float64         `json:"network_load"`
	}

	if err := json.Unmarshal(stateData, &state); err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Rebuild peers
	newPeers := make(map[[32]byte]chan *types.Event, len(state.Peers))
	newStopChannels := make(map[[32]byte]chan struct{}, len(state.Peers))
	for peerIDStr := range state.Peers {
		peerID, err := stringToByteArray(peerIDStr)
		if err != nil {
			return fmt.Errorf("failed to parse peer ID %q: %v", peerIDStr, err)
		}
		newPeers[peerID] = make(chan *types.Event, 1000)
		newStopChannels[peerID] = make(chan struct{})
	}

	g.peers = newPeers
	g.stopChannels = newStopChannels
	g.baseDelay = state.BaseDelay
	g.currentDelay = state.CurrentDelay
	g.networkLoad = state.NetworkLoad

	return nil
}

// Start triggers gossip loops for all currently known peers.
func (g *GossipProtocol) Start() {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for nodeID := range g.peers {
		g.StartGossiping(nodeID)
	}
}

// Stop closes each peer's stopChan, halting their gossip loops.
func (g *GossipProtocol) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for nodeID, stopCh := range g.stopChannels {
		close(stopCh) // stop the goroutine
		delete(g.stopChannels, nodeID)
	}
}

func (g *GossipProtocol) GetCurrentDelay() time.Duration {
	g.loadMu.RLock()
	defer g.loadMu.RUnlock()
	return g.currentDelay
}

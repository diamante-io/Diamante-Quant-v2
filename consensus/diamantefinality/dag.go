// consensus/diamantefinality/dag.go

package diamantefinality

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"diamante/common"
	"diamante/consensus/types"
)

// Node represents a participant in the DAG with a unique ID, stake, and a list of events it created.
type Node struct {
	ID     [32]byte       // Unique node identifier
	Events []*types.Event // Historical events created by this node
	Stake  uint64         // Current stake for this node
	Active bool           // True if the node is still participating
}

// DAG manages the Directed Acyclic Graph (DAG) of events for Lachesis-like consensus.
// It stores nodes (validators) and events, each event referencing parent events in the DAG.
type DAG struct {
	Nodes     map[[32]byte]*Node        // All known nodes, active or inactive
	Events    map[[32]byte]*types.Event // All known events keyed by their ID
	MaxHeight uint64                    // Tracks the highest event height currently in the DAG

	// Separate mutexes protect different areas of state to minimize lock contention
	nodesMu     sync.RWMutex
	eventsMu    sync.RWMutex
	maxHeightMu sync.RWMutex

	// NEW: Logger for structured logging.
	logger *log.Logger
}

// NewDAG creates a new DAG with empty node and event maps and initializes a default logger.
func NewDAG() *DAG {
	return &DAG{
		Nodes:  make(map[[32]byte]*Node),
		Events: make(map[[32]byte]*types.Event),
		logger: log.New(os.Stdout, "DAG: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// AddNode registers or updates a node in the DAG with a given stake.
// If the node already exists, its stake is updated and it's marked Active=true.
func (d *DAG) AddNode(nodeID [32]byte, stake uint64) {
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()

	if node, exists := d.Nodes[nodeID]; exists {
		node.Stake = stake
		node.Active = true
		d.logger.Printf("Updated node %x: stake set to %d and marked active", nodeID, stake)
	} else {
		d.Nodes[nodeID] = &Node{
			ID:     nodeID,
			Stake:  stake,
			Active: true,
		}
		d.logger.Printf("Added new node %x with stake %d", nodeID, stake)
	}
}

// UpdateNodeStake modifies the stake of an existing node.
func (d *DAG) UpdateNodeStake(nodeID [32]byte, newStake uint64) error {
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()

	node, exists := d.Nodes[nodeID]
	if !exists {
		return errors.New("UpdateNodeStake: node does not exist")
	}
	node.Stake = newStake
	d.logger.Printf("Node %x stake updated to %d", nodeID, newStake)
	return nil
}

// RemoveNode deactivates a node by marking it as inactive, retaining its historical data.
func (d *DAG) RemoveNode(nodeID [32]byte) error {
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()

	node, exists := d.Nodes[nodeID]
	if !exists {
		return errors.New("RemoveNode: node does not exist")
	}
	node.Active = false
	d.logger.Printf("Node %x marked as inactive", nodeID)
	return nil
}

// NewEvent creates a new event in the DAG, linking it to its parent events.
// The event's height is computed as 1 higher than the maximum parent height.
// If the creator node is unknown or inactive, an error is returned.
func (d *DAG) NewEvent(
	creator [32]byte,
	parentIDs [][32]byte,
	data []byte,
) (*types.Event, error) {

	// Verify creator node.
	d.nodesMu.RLock()
	creatorNode, exists := d.Nodes[creator]
	d.nodesMu.RUnlock()
	if !exists {
		return nil, errors.New("NewEvent: creator node does not exist")
	}
	if !creatorNode.Active {
		return nil, errors.New("NewEvent: creator node is inactive")
	}

	// Compute the event height (height = max parent height + 1).
	var height uint64 = 1
	if len(parentIDs) > 0 {
		d.eventsMu.RLock()
		for _, pid := range parentIDs {
			if parentEvent, ok := d.Events[pid]; ok && parentEvent.Height >= height {
				height = parentEvent.Height
			}
		}
		d.eventsMu.RUnlock()
		height++
	}

	// Construct the event.
	event := &types.Event{
		ParentIDs: parentIDs,
		Timestamp: common.ConsensusNow(),
		Data:      data,
		Creator:   creator,
		Height:    height,
		Finalized: false,
	}

	// Generate a deterministic event ID.
	d.eventsMu.RLock()
	currentCount := len(d.Events) + 1
	d.eventsMu.RUnlock()

	nonce := make([]byte, 8)
	binary.BigEndian.PutUint64(nonce, uint64(currentCount))

	idData := make([]byte, 8+32+8+len(data))
	binary.BigEndian.PutUint64(idData[:8], uint64(event.Timestamp.UnixNano()))
	copy(idData[8:40], creator[:])
	copy(idData[40:48], nonce)
	copy(idData[48:], data)

	event.ID = sha256.Sum256(idData)

	// Commit the event to the DAG.
	d.eventsMu.Lock()
	d.Events[event.ID] = event
	d.eventsMu.Unlock()

	// Append the event to the creator's node.
	d.nodesMu.Lock()
	creatorNode.Events = append(creatorNode.Events, event)
	d.nodesMu.Unlock()

	// Update MaxHeight if this event's height is the new maximum.
	d.maxHeightMu.Lock()
	if height > d.MaxHeight {
		d.MaxHeight = height
		d.logger.Printf("MaxHeight updated to %d", height)
	}
	d.maxHeightMu.Unlock()

	d.logger.Printf("New event created: creator %x, height %d, eventID %x", creator, height, event.ID)
	return event, nil
}

// GetEvent retrieves an event by its ID (returns an error if it doesn't exist).
func (d *DAG) GetEvent(id [32]byte) (*types.Event, error) {
	d.eventsMu.RLock()
	defer d.eventsMu.RUnlock()

	event, exists := d.Events[id]
	if !exists {
		return nil, errors.New("GetEvent: event not found")
	}
	return event, nil
}

// GetEvents returns a shallow copy of all events in the DAG.
func (d *DAG) GetEvents() map[[32]byte]*types.Event {
	d.eventsMu.RLock()
	defer d.eventsMu.RUnlock()

	eventsCopy := make(map[[32]byte]*types.Event, len(d.Events))
	for id, ev := range d.Events {
		eventsCopy[id] = ev
	}
	return eventsCopy
}

// GetActiveNodes returns the IDs of all currently active nodes.
func (d *DAG) GetActiveNodes() [][32]byte {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	var active []([32]byte)
	for id, node := range d.Nodes {
		if node.Active {
			active = append(active, id)
		}
	}
	return active
}

// GetNodeStake returns the stake of a node, or an error if the node doesn't exist.
func (d *DAG) GetNodeStake(nodeID [32]byte) (uint64, error) {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	node, exists := d.Nodes[nodeID]
	if !exists {
		return 0, errors.New("GetNodeStake: node not found")
	}
	return node.Stake, nil
}

// GetTotalStake calculates the sum of stakes for all active nodes.
func (d *DAG) GetTotalStake() uint64 {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	var total uint64
	for _, node := range d.Nodes {
		if node.Active {
			total += node.Stake
		}
	}
	return total
}

// IsActiveValidator checks if a node is both known and marked active.
func (d *DAG) IsActiveValidator(nodeID [32]byte) bool {
	d.nodesMu.RLock()
	defer d.nodesMu.RUnlock()

	node, exists := d.Nodes[nodeID]
	return exists && node.Active
}

// GetState serializes all nodes, events, and the current MaxHeight into JSON.
func (d *DAG) GetState() ([]byte, error) {
	// Copy data under locks before serialization.
	d.nodesMu.RLock()
	nodesCopy := make(map[string]*Node, len(d.Nodes))
	for nodeID, node := range d.Nodes {
		nodeCopy := *node
		nodesCopy[byteArrayToString(nodeID)] = &nodeCopy
	}
	d.nodesMu.RUnlock()

	d.eventsMu.RLock()
	eventsCopy := make(map[string]*types.Event, len(d.Events))
	for evID, ev := range d.Events {
		evCopy := *ev
		eventsCopy[byteArrayToString(evID)] = &evCopy
	}
	d.eventsMu.RUnlock()

	d.maxHeightMu.RLock()
	currentMaxHeight := d.MaxHeight
	d.maxHeightMu.RUnlock()

	state := struct {
		Nodes     map[string]*Node        `json:"nodes"`
		Events    map[string]*types.Event `json:"events"`
		MaxHeight uint64                  `json:"max_height"`
	}{
		Nodes:     nodesCopy,
		Events:    eventsCopy,
		MaxHeight: currentMaxHeight,
	}
	d.logger.Printf("DAG state serialized with MaxHeight=%d", currentMaxHeight)
	return json.Marshal(state)
}

// RestoreState overwrites the DAG's current state with the provided JSON data.
func (d *DAG) RestoreState(stateData []byte) error {
	var state struct {
		Nodes     map[string]*Node        `json:"nodes"`
		Events    map[string]*types.Event `json:"events"`
		MaxHeight uint64                  `json:"max_height"`
	}

	if err := json.Unmarshal(stateData, &state); err != nil {
		return fmt.Errorf("RestoreState: failed to unmarshal state data: %w", err)
	}

	// Acquire all locks needed.
	d.nodesMu.Lock()
	defer d.nodesMu.Unlock()

	d.eventsMu.Lock()
	defer d.eventsMu.Unlock()

	d.maxHeightMu.Lock()
	defer d.maxHeightMu.Unlock()

	// Convert string IDs back to [32]byte and rebuild DAG.
	d.Nodes = make(map[[32]byte]*Node, len(state.Nodes))
	for nodeIDStr, nodeVal := range state.Nodes {
		nodeID, err := stringToByteArray(nodeIDStr)
		if err != nil {
			return fmt.Errorf("RestoreState: failed to parse node ID '%s': %w", nodeIDStr, err)
		}
		d.Nodes[nodeID] = nodeVal
	}
	d.Events = make(map[[32]byte]*types.Event, len(state.Events))
	for evIDStr, evVal := range state.Events {
		evID, err := stringToByteArray(evIDStr)
		if err != nil {
			return fmt.Errorf("RestoreState: failed to parse event ID '%s': %w", evIDStr, err)
		}
		d.Events[evID] = evVal
	}

	d.MaxHeight = state.MaxHeight
	d.logger.Printf("DAG state restored with MaxHeight=%d", d.MaxHeight)
	return nil
}

// GetUnfinalizedEvents collects all events that are not marked Finalized.
func (d *DAG) GetUnfinalizedEvents() []*types.Event {
	d.eventsMu.RLock()
	defer d.eventsMu.RUnlock()

	var results []*types.Event
	for _, ev := range d.Events {
		if !ev.Finalized {
			results = append(results, ev)
		}
	}
	return results
}

// Helper function: [32]byte -> hex string.
func byteArrayToString(b [32]byte) string {
	return fmt.Sprintf("%x", b)
}

// Helper function: hex string -> [32]byte.
func stringToByteArray(s string) ([32]byte, error) {
	var out [32]byte
	data, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	if len(data) != 32 {
		return out, fmt.Errorf("expected 32 bytes, got %d", len(data))
	}
	copy(out[:], data)
	return out, nil
}

// VerifyConsistency checks the internal consistency of the DAG
func (d *DAG) VerifyConsistency() error {
	d.nodesMu.RLock()
	d.eventsMu.RLock()
	defer d.nodesMu.RUnlock()
	defer d.eventsMu.RUnlock()

	// 1. Check that all events have valid creators
	for id, event := range d.Events {
		_, exists := d.Nodes[event.Creator]
		if !exists {
			return fmt.Errorf("event %x has creator %x that doesn't exist in nodes",
				id, event.Creator)
		}
	}

	// 2. Check that all events have valid parents (if any)
	for id, event := range d.Events {
		for _, parentID := range event.ParentIDs {
			_, exists := d.Events[parentID]
			if !exists {
				return fmt.Errorf("event %x has parent %x that doesn't exist in events",
					id, parentID)
			}
		}
	}

	// 3. Check for height consistency
	for id, event := range d.Events {
		if event.Height == 0 {
			return fmt.Errorf("event %x has invalid height 0", id)
		}

		// If event has parents, its height should be at least 1 + max parent height
		if len(event.ParentIDs) > 0 {
			maxParentHeight := uint64(0)
			for _, parentID := range event.ParentIDs {
				parent := d.Events[parentID]
				if parent.Height > maxParentHeight {
					maxParentHeight = parent.Height
				}
			}

			if event.Height <= maxParentHeight {
				return fmt.Errorf("event %x has height %d <= max parent height %d",
					id, event.Height, maxParentHeight)
			}
		}
	}

	// 4. Check that MaxHeight is consistent with event heights
	actualMaxHeight := uint64(0)
	for _, event := range d.Events {
		if event.Height > actualMaxHeight {
			actualMaxHeight = event.Height
		}
	}

	if d.MaxHeight != actualMaxHeight {
		return fmt.Errorf("DAG MaxHeight %d doesn't match actual max event height %d",
			d.MaxHeight, actualMaxHeight)
	}

	return nil
}

// RepairConsistency attempts to fix common consistency issues in the DAG
func (d *DAG) RepairConsistency() (bool, error) {
	d.nodesMu.Lock()
	d.eventsMu.Lock()
	defer d.nodesMu.Unlock()
	defer d.eventsMu.Unlock()

	madeChanges := false

	// 1. Remove events with non-existent creators
	var eventsToRemove [][32]byte
	for id, event := range d.Events {
		_, exists := d.Nodes[event.Creator]
		if !exists {
			eventsToRemove = append(eventsToRemove, id)
			madeChanges = true
		}
	}

	// Actually remove the events after identification
	for _, id := range eventsToRemove {
		delete(d.Events, id)
		d.logger.Printf("Removed event %x with non-existent creator", id)
	}

	// 2. Remove events with non-existent parents
	eventsToRemove = eventsToRemove[:0] // Clear the slice
	for id, event := range d.Events {
		hasInvalidParent := false
		for _, parentID := range event.ParentIDs {
			_, exists := d.Events[parentID]
			if !exists {
				hasInvalidParent = true
				break
			}
		}

		if hasInvalidParent {
			eventsToRemove = append(eventsToRemove, id)
			madeChanges = true
		}
	}

	// Actually remove the events
	for _, id := range eventsToRemove {
		delete(d.Events, id)
		d.logger.Printf("Removed event %x with invalid parent references", id)
	}

	// 3. Fix MaxHeight
	actualMaxHeight := uint64(0)
	for _, event := range d.Events {
		if event.Height > actualMaxHeight {
			actualMaxHeight = event.Height
		}
	}

	if d.MaxHeight != actualMaxHeight {
		d.MaxHeight = actualMaxHeight
		madeChanges = true
		d.logger.Printf("Corrected MaxHeight to %d", actualMaxHeight)
	}

	return madeChanges, nil
}

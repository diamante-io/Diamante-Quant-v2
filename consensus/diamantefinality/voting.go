// consensus/diamantefinality/finality/voting.go

package finality

import (
	"diamante/consensus/types"
	"encoding/json"
	"sync"
)

// VirtualVoting implements a simplified voting mechanism for finality checks.
// Each validator "sees" an event if it can trace a path in the DAG from the event
// up to an event created by that validator. If enough stake "has seen" the event,
// it is considered voted for finality.
type VirtualVoting struct {
	dag       *DAG    // Reference to the DAG
	threshold float64 // Fraction of total stake required for a successful vote
	mu        sync.RWMutex
}

// NewVirtualVoting creates a new VirtualVoting instance for a given DAG,
// with a default threshold of 66% stake required for finality.
func NewVirtualVoting(dag *DAG) *VirtualVoting {
	return &VirtualVoting{
		dag:       dag,
		threshold: 0.66,
	}
}

// SetThreshold updates the fraction of total stake needed for an event to pass the vote.
func (v *VirtualVoting) SetThreshold(threshold float64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.threshold = threshold
}

// Vote determines whether the event has enough stake support to be considered finalized.
// In this aBFT approach, "support" means that enough validators' events (by stake) can be
// found as ancestors in the DAG.
func (v *VirtualVoting) Vote(event *types.Event) bool {
	v.mu.RLock()
	threshold := v.threshold
	v.mu.RUnlock()

	// Get total stake and compute required threshold
	totalStake := v.dag.GetTotalStake()
	requiredStake := float64(totalStake) * threshold
	accumulatedStake := float64(0)

	// For each active node, check if that node has "seen" the event
	activeNodes := v.dag.GetActiveNodes()
	for _, nodeID := range activeNodes {
		if v.HasSeen(nodeID, event) {
			stake, err := v.dag.GetNodeStake(nodeID)
			if err != nil {
				// Node might have been removed or an error occurred;
				// skip this node's stake.
				continue
			}
			accumulatedStake += float64(stake)
			if accumulatedStake >= requiredStake {
				return true
			}
		}
	}
	return false
}

// HasSeen checks if validatorID has created an ancestor event of 'event' in the DAG.
// We do this by traversing upward (parents) from 'event' and seeing if we find an event
// with Creator == validatorID.
func (v *VirtualVoting) HasSeen(validatorID [32]byte, event *types.Event) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	seen := make(map[[32]byte]bool)
	queue := []*types.Event{event}

	for len(queue) > 0 {
		e := queue[0]
		queue = queue[1:]

		// If we’ve already visited this event, skip
		if seen[e.ID] {
			continue
		}
		seen[e.ID] = true

		// If the validator created this event, we conclude "HasSeen" is true
		if e.Creator == validatorID {
			return true
		}

		// Move to the parents
		for _, parentID := range e.ParentIDs {
			parent, err := v.dag.GetEvent(parentID)
			if err != nil {
				// Missing parent, skip
				continue
			}
			queue = append(queue, parent)
		}
	}

	return false
}

// RecalculateWeights is a no-op here; we do a direct stake check in Vote().
func (v *VirtualVoting) RecalculateWeights() {}

// GetConsensusEvents finds all events at a given height for which Vote(event) is true.
func (v *VirtualVoting) GetConsensusEvents(height uint64) []*types.Event {
	var consensusEvents []*types.Event
	allEvents := v.dag.GetEvents()
	for _, ev := range allEvents {
		if ev.Height == height && v.Vote(ev) {
			consensusEvents = append(consensusEvents, ev)
		}
	}
	return consensusEvents
}

// GetState serializes the current threshold (float64) into JSON.
func (v *VirtualVoting) GetState() ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	state := struct {
		Threshold float64 `json:"threshold"`
	}{
		Threshold: v.threshold,
	}
	return json.Marshal(state)
}

// RestoreState loads a threshold float64 from JSON into this VirtualVoting instance.
func (v *VirtualVoting) RestoreState(stateData []byte) error {
	var state struct {
		Threshold float64 `json:"threshold"`
	}
	if err := json.Unmarshal(stateData, &state); err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.threshold = state.Threshold
	return nil
}

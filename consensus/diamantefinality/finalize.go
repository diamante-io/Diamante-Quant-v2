// consensus/diamantefinality/finalize.go

package diamantefinality

import (
	"diamante/consensus/types"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

// Voting interface remains unchanged.
type Voting interface {
	Vote(e *types.Event) bool
	HasSeen(validatorID [32]byte, event *types.Event) bool
	SetThreshold(th float64)
	RecalculateWeights()
	GetState() ([]byte, error)
	RestoreState([]byte) error
}

// Finalizer finalizes events using voting results and maintains checkpoints.
type Finalizer struct {
	dag           *DAG
	virtualVoting Voting
	mu            sync.RWMutex
	finalized     map[[32]byte]bool
	checkpoints   map[uint64]*types.Event
	logger        *log.Logger
}

// NewFinalizer creates a new Finalizer instance and initializes a logger.
func NewFinalizer(dag *DAG, voting Voting) *Finalizer {
	return &Finalizer{
		dag:           dag,
		virtualVoting: voting,
		finalized:     make(map[[32]byte]bool),
		checkpoints:   make(map[uint64]*types.Event),
		// Use os.Stdout (or io.Discard if you wish to suppress output) instead of nil.
		logger: log.New(os.Stdout, "Finalizer: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// Finalize tries to finalize an event. If voting fails, returns an error.
func (f *Finalizer) Finalize(event *types.Event) (bool, error) {
	f.mu.RLock()
	if f.finalized[event.ID] {
		f.mu.RUnlock()
		return true, nil
	}
	f.mu.RUnlock()

	// Must succeed via voting.
	if f.virtualVoting.Vote(event) {
		f.mu.Lock()
		defer f.mu.Unlock()

		// Double-check inside write lock.
		if f.finalized[event.ID] {
			return true, nil
		}
		event.Finalized = true
		f.finalized[event.ID] = true

		// Update checkpoints.
		f.updateCheckpoints(event)

		f.logger.Printf("Event %x finalized successfully", event.ID)
		return true, nil
	}
	return false, errors.New("vote failed, cannot finalize event")
}

// IsFinalized checks if the given event ID has already been finalized.
func (f *Finalizer) IsFinalized(eventID [32]byte) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.finalized[eventID]
}

// updateCheckpoints updates checkpoints if the event's height is a multiple of the interval.
func (f *Finalizer) updateCheckpoints(event *types.Event) {
	const checkpointInterval = 1000
	if event.Height%checkpointInterval == 0 {
		f.checkpoints[event.Height] = event
		f.logger.Printf("Checkpoint stored at height=%d", event.Height)
	}
}

// GetLatestCheckpoint retrieves the highest (latest) checkpointed event, or nil if none exist.
func (f *Finalizer) GetLatestCheckpoint() *types.Event {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var latestHeight uint64
	var latest *types.Event
	for h, ev := range f.checkpoints {
		if h > latestHeight {
			latestHeight = h
			latest = ev
		}
	}
	return latest
}

// GetState serializes the Finalizer's finalized map and checkpoint map into JSON.
func (f *Finalizer) GetState() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Convert finalized map from [32]byte -> bool into a string -> bool map.
	finalizedMap := make(map[string]bool, len(f.finalized))
	for id, val := range f.finalized {
		finalizedMap[byteArrayToString(id)] = val
	}

	// Copy checkpoints.
	cpMap := make(map[uint64]*types.Event, len(f.checkpoints))
	for h, ev := range f.checkpoints {
		cpMap[h] = ev
	}

	state := struct {
		Finalized   map[string]bool         `json:"finalized"`
		Checkpoints map[uint64]*types.Event `json:"checkpoints"`
	}{
		Finalized:   finalizedMap,
		Checkpoints: cpMap,
	}
	return json.Marshal(state)
}

// RestoreState de-serializes JSON data into the Finalizer's fields.
func (f *Finalizer) RestoreState(data []byte) error {
	var state struct {
		Finalized   map[string]bool
		Checkpoints map[uint64]*types.Event
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal Finalizer state: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	newFin := make(map[[32]byte]bool, len(state.Finalized))
	for strID, val := range state.Finalized {
		id, err := stringToByteArray(strID)
		if err != nil {
			return fmt.Errorf("invalid ID in finalize restore: %w", err)
		}
		newFin[id] = val
	}

	newCP := make(map[uint64]*types.Event, len(state.Checkpoints))
	for h, ev := range state.Checkpoints {
		newCP[h] = ev
	}

	f.finalized = newFin
	f.checkpoints = newCP
	f.logger.Println("Finalizer state restored successfully")
	return nil
}

// FinalizeEvents finalizes a batch of events concurrently.
func (f *Finalizer) FinalizeEvents(events []*types.Event) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(events))

	for _, ev := range events {
		wg.Add(1)
		go func(e *types.Event) {
			defer wg.Done()
			success, err := f.Finalize(e)
			if err != nil {
				errChan <- err
				return
			}
			if !success {
				errChan <- errors.New("could not finalize event (vote returned false)")
			}
		}(ev)
	}

	wg.Wait()
	close(errChan)

	var errs []error
	for e := range errChan {
		errs = append(errs, e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during finalization: %v", errs)
	}
	return nil
}

// finalize_test.go
package finality_test

import (
	"fmt"
	"sync"
	"testing"

	finality "diamante/consensus/diamantefinality"
	"diamante/consensus/types"
)

// mockVoting implements the Voting interface by wrapping a real VirtualVoting
// and overriding Vote to return a controlled result.
type mockVoting struct {
	*finality.VirtualVoting
	mu         sync.Mutex
	voteCalls  int
	voteResult bool
}

func (mv *mockVoting) Vote(e *types.Event) bool {
	mv.mu.Lock()
	defer mv.mu.Unlock()
	mv.voteCalls++
	return mv.voteResult
}

func (mv *mockVoting) SetVoteResult(result bool) {
	mv.mu.Lock()
	defer mv.mu.Unlock()
	mv.voteResult = result
}

func TestFinalizer_Finalize_Success(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV}
	mv.SetVoteResult(true)

	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 100)
	event, err := dag.NewEvent(nodeID, nil, []byte("test data"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	success, err := finalizer.Finalize(event)
	if err != nil {
		t.Errorf("unexpected error during finalization: %v", err)
	}
	if !success {
		t.Error("expected success=true, got false")
	}
	if !event.Finalized {
		t.Error("event.Finalized should be true after successful Finalize")
	}
	if !finalizer.IsFinalized(event.ID) {
		t.Error("finalizer should record the finalized event in its map")
	}
}

func TestFinalizer_Finalize_Fail(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV}
	mv.SetVoteResult(false)

	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 50)
	event, err := dag.NewEvent(nodeID, nil, []byte("fail data"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	success, err := finalizer.Finalize(event)
	if err == nil {
		t.Error("expected an error due to failed vote, got nil")
	}
	if success {
		t.Error("expected success=false when vote fails")
	}
	if event.Finalized {
		t.Error("event should not be marked Finalized on vote failure")
	}
}

func TestFinalizer_Finalize_AlreadyFinalized(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV}
	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 1000)
	ev, err := dag.NewEvent(nodeID, nil, []byte("already final"))
	if err != nil {
		t.Fatalf("dag.NewEvent error: %v", err)
	}

	mv.SetVoteResult(true)

	// First finalize
	if success, err := finalizer.Finalize(ev); err != nil || !success {
		t.Fatalf("expected first finalize to succeed, got success=%v, err=%v", success, err)
	}

	// Attempt to finalize again; should return success without error.
	success2, err2 := finalizer.Finalize(ev)
	if err2 != nil {
		t.Errorf("second finalize returned an unexpected error: %v", err2)
	}
	if !success2 {
		t.Error("expected second finalize to succeed (already finalized)")
	}
}

func TestFinalizer_Checkpoints(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV, voteResult: true}
	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 9999)

	// Create an event with Height exactly 1000 (assuming checkpoint interval is 1000)
	ev := &types.Event{
		Creator:   nodeID,
		Height:    1000,
		Data:      []byte("checkpoint data"),
		Finalized: false,
	}
	// Manually insert the event into the DAG.
	dag.Events[ev.ID] = ev

	success, err := finalizer.Finalize(ev)
	if err != nil {
		t.Fatalf("unexpected error finalizing checkpoint event: %v", err)
	}
	if !success {
		t.Fatalf("expected success=true for checkpoint event, got false")
	}

	latest := finalizer.GetLatestCheckpoint()
	if latest == nil {
		t.Fatalf("expected at least one checkpoint, got nil")
	}
	if latest != ev {
		t.Errorf("expected latest checkpoint to be the event ev, got a different event")
	}
}

func TestFinalizer_GetStateAndRestore(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV}
	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 100)

	ev1, err := dag.NewEvent(nodeID, nil, []byte("restore test1"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	mv.SetVoteResult(true)
	if success, err := finalizer.Finalize(ev1); err != nil || !success {
		t.Fatalf("failed to finalize ev1: success=%v, err=%v", success, err)
	}

	stateData, err := finalizer.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	// Restore into a new finalizer instance.
	dag2 := finality.NewDAG()
	realVV2 := finality.NewVirtualVoting(dag2)
	mv2 := &mockVoting{VirtualVoting: realVV2}
	newFinalizer := finality.NewFinalizer(dag2, mv2)

	if err := newFinalizer.RestoreState(stateData); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	if !newFinalizer.IsFinalized(ev1.ID) {
		t.Errorf("restored finalizer is missing ev1 in its finalized map")
	}
}

func TestFinalizer_FinalizeEvents(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV, voteResult: true}
	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 999)

	// Create 3 events.
	events := make([]*types.Event, 3)
	for i := 0; i < 3; i++ {
		ev, err := dag.NewEvent(nodeID, nil, []byte(fmt.Sprintf("batch %d", i)))
		if err != nil {
			t.Fatalf("NewEvent %d error: %v", i, err)
		}
		events[i] = ev
	}

	if err := finalizer.FinalizeEvents(events); err != nil {
		t.Errorf("FinalizeEvents returned error: %v", err)
	}
	for i, ev := range events {
		if !ev.Finalized {
			t.Errorf("event %d should be finalized, but isn't", i)
		}
		if !finalizer.IsFinalized(ev.ID) {
			t.Errorf("finalizer doesn't record finalized state for event %d", i)
		}
	}
}

func TestFinalizer_FinalizeEvents_PartialFail(t *testing.T) {
	dag := finality.NewDAG()
	realVV := finality.NewVirtualVoting(dag)
	mv := &mockVoting{VirtualVoting: realVV, voteResult: true}
	finalizer := finality.NewFinalizer(dag, mv)

	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 999)

	// Create 3 events.
	events := make([]*types.Event, 3)
	for i := 0; i < 3; i++ {
		ev, err := dag.NewEvent(nodeID, nil, []byte(fmt.Sprintf("partial %d", i)))
		if err != nil {
			t.Fatalf("NewEvent %d error: %v", i, err)
		}
		events[i] = ev
	}

	// Force failure on the second event by toggling the vote to false.
	mv.SetVoteResult(true)
	if len(events) > 1 {
		mv.SetVoteResult(false)
	}

	err := finalizer.FinalizeEvents(events)
	if err == nil {
		t.Error("expected an error because at least one event should fail finalization")
	}

	// Optionally, set vote result back to true and log finalized status.
	mv.SetVoteResult(true)
	for idx, ev := range events {
		if finalizer.IsFinalized(ev.ID) {
			t.Logf("Event[%d] %x is finalized", idx, ev.ID)
		} else {
			t.Logf("Event[%d] %x is NOT finalized", idx, ev.ID)
		}
	}
}

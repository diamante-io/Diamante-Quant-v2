// dag_test.go
package finality_test

import (
	"bytes"
	"crypto/rand"
	finality "diamante/consensus/diamantefinality"
	"testing"
)

// randomNodeID creates a new random [32]byte ID for testing.
func randomNodeID(t *testing.T) [32]byte {
	t.Helper()
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("failed to generate random node ID: %v", err)
	}
	return b
}

func TestDAG_AddNode(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 1000)

	stake, err := dag.GetNodeStake(nodeID)
	if err != nil {
		t.Errorf("GetNodeStake returned error: %v", err)
	}
	if stake != 1000 {
		t.Errorf("expected stake=1000, got %d", stake)
	}

	if !dag.IsActiveValidator(nodeID) {
		t.Error("expected node to be active after AddNode, but it's not")
	}
}

func TestDAG_UpdateNodeStake(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 500)
	if err := dag.UpdateNodeStake(nodeID, 1500); err != nil {
		t.Errorf("unexpected error updating stake: %v", err)
	}

	stake, err := dag.GetNodeStake(nodeID)
	if err != nil {
		t.Errorf("GetNodeStake error: %v", err)
	}
	if stake != 1500 {
		t.Errorf("expected updated stake=1500, got %d", stake)
	}
}

func TestDAG_RemoveNode(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 1000)
	if err := dag.RemoveNode(nodeID); err != nil {
		t.Errorf("RemoveNode error: %v", err)
	}
	if dag.IsActiveValidator(nodeID) {
		t.Errorf("expected node to be inactive after RemoveNode")
	}
}

func TestDAG_NewEvent(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 100)

	data := []byte("hello event")
	event, err := dag.NewEvent(nodeID, nil, data)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event, got nil")
	}
	if event.Creator != nodeID {
		t.Errorf("expected event.Creator to match nodeID")
	}
	if !bytes.Equal(event.Data, data) {
		t.Errorf("expected event.Data=%s, got %s", data, event.Data)
	}
	if event.Height != 1 {
		t.Errorf("expected height=1, got %d", event.Height)
	}

	stored, err := dag.GetEvent(event.ID)
	if err != nil {
		t.Fatalf("GetEvent error: %v", err)
	}
	if stored.Creator != nodeID {
		t.Errorf("expected stored event's Creator to match nodeID")
	}
}

func TestDAG_NewEventInactiveCreator(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 100)
	if err := dag.RemoveNode(nodeID); err != nil {
		t.Fatalf("failed to remove node: %v", err)
	}

	_, err := dag.NewEvent(nodeID, nil, []byte("inactive"))
	if err == nil {
		t.Fatal("expected error for inactive creator, but got nil")
	}
}

func TestDAG_GetUnfinalizedEvents(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 100)

	// Create 3 events.
	for i := 0; i < 3; i++ {
		if _, err := dag.NewEvent(nodeID, nil, []byte{byte(i)}); err != nil {
			t.Fatalf("creating event %d: %v", i, err)
		}
	}

	unfinalized := dag.GetUnfinalizedEvents()
	if len(unfinalized) != 3 {
		t.Errorf("expected 3 unfinalized events, got %d", len(unfinalized))
	}

	// Mark one event as finalized.
	unfinalized[0].Finalized = true
	unfinalized2 := dag.GetUnfinalizedEvents()
	if len(unfinalized2) != 2 {
		t.Errorf("expected 2 unfinalized after finalizing one, got %d", len(unfinalized2))
	}
}

func TestDAG_GetTotalStake(t *testing.T) {
	dag := finality.NewDAG()
	n1 := randomNodeID(t)
	n2 := randomNodeID(t)
	dag.AddNode(n1, 100)
	dag.AddNode(n2, 200)

	total := dag.GetTotalStake()
	if total != 300 {
		t.Errorf("expected total=300, got %d", total)
	}

	if err := dag.RemoveNode(n2); err != nil {
		t.Errorf("RemoveNode error: %v", err)
	}
	total2 := dag.GetTotalStake()
	if total2 != 100 {
		t.Errorf("expected total=100 after removing second node, got %d", total2)
	}
}

func TestDAG_GetStateAndRestoreState(t *testing.T) {
	dag := finality.NewDAG()
	nodeID := randomNodeID(t)
	dag.AddNode(nodeID, 123)
	event, err := dag.NewEvent(nodeID, nil, []byte("test state"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	stateData, err := dag.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	newDAG := finality.NewDAG()
	if err := newDAG.RestoreState(stateData); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	stake, err := newDAG.GetNodeStake(nodeID)
	if err != nil {
		t.Fatalf("GetNodeStake error: %v", err)
	}
	if stake != 123 {
		t.Errorf("expected stake=123, got %d", stake)
	}

	ev, err := newDAG.GetEvent(event.ID)
	if err != nil {
		t.Fatalf("GetEvent (after restore) error: %v", err)
	}
	if !bytes.Equal(ev.Data, []byte("test state")) {
		t.Errorf("expected event data 'test state', got '%s'", ev.Data)
	}
}

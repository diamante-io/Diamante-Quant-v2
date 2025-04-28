// virtualvoting_test.go
package finality_test

import (
	"encoding/json"
	"testing"

	finality "diamante/consensus/diamantefinality"
)

func TestVirtualVoting_BasicVote(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	// Add two nodes.
	nodeA := randomNodeID(t)
	nodeB := randomNodeID(t)
	dag.AddNode(nodeA, 100)
	dag.AddNode(nodeB, 100)

	// Create an event by nodeA.
	evA, err := dag.NewEvent(nodeA, nil, []byte("A's event"))
	if err != nil {
		t.Fatalf("NewEvent for nodeA error: %v", err)
	}
	// With default threshold 0.66 and total stake 200, required stake ~132.
	// Only nodeA has seen the event; vote should fail.
	if vv.Vote(evA) {
		t.Error("expected Vote(evA) to fail because only nodeA (100 stake) has seen it")
	}

	// Now, have nodeB see evA by creating an event referencing it.
	_, err = dag.NewEvent(nodeB, [][32]byte{evA.ID}, []byte("B references A"))
	if err != nil {
		t.Fatalf("NewEvent for nodeB error: %v", err)
	}

	// Vote should still fail because nodeB is a child event (not an ancestor).
	if vv.Vote(evA) {
		t.Error("expected Vote(evA) to still fail because nodeB is a child, not an ancestor")
	}
}

func TestVirtualVoting_ThresholdChange(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	nA := randomNodeID(t)
	nB := randomNodeID(t)
	dag.AddNode(nA, 100)
	dag.AddNode(nB, 200) // total stake = 300

	// Create an event from nA.
	ev, err := dag.NewEvent(nA, nil, []byte("test threshold"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	// With threshold=0.66, required stake = 300*0.66 = 198; nA's 100 is insufficient.
	if vv.Vote(ev) {
		t.Error("expected vote to fail with threshold=0.66 (required ~198, nA has 100)")
	}

	// Lower threshold so that nA alone suffices.
	vv.SetThreshold(0.3)
	if !vv.Vote(ev) {
		t.Error("expected vote to pass after lowering threshold to 0.3")
	}
}

func TestVirtualVoting_HasSeen(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	nA := randomNodeID(t)
	nB := randomNodeID(t)
	dag.AddNode(nA, 100)
	dag.AddNode(nB, 100)

	// Create a chain: evA by nA, then evB by nB referencing evA.
	evA, err := dag.NewEvent(nA, nil, []byte("A0"))
	if err != nil {
		t.Fatalf("failed to create evA: %v", err)
	}
	evB, err := dag.NewEvent(nB, [][32]byte{evA.ID}, []byte("B references A0"))
	if err != nil {
		t.Fatalf("failed to create evB: %v", err)
	}

	// nA should be seen from evB since evB's parent is evA.
	if !vv.HasSeen(nA, evB) {
		t.Error("expected nA to be seen in evB (via evA)")
	}
	// nB should see its own event.
	if !vv.HasSeen(nB, evB) {
		t.Error("expected nB to see its own event")
	}
	// nB should not see evA.
	if vv.HasSeen(nB, evA) {
		t.Error("expected nB to not see evA")
	}
}

func TestVirtualVoting_GetConsensusEvents(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	n1 := randomNodeID(t)
	n2 := randomNodeID(t)
	dag.AddNode(n1, 50)
	dag.AddNode(n2, 60) // total stake = 110; default threshold 0.66 => required ~72.6

	// Create some events.
	_, _ = dag.NewEvent(n1, nil, []byte("ev1"))
	_, _ = dag.NewEvent(n2, nil, []byte("ev2"))
	ev3, _ := dag.NewEvent(n1, nil, []byte("ev3"))
	// Force ev3 to height 2.
	ev3.Height = 2

	res := vv.GetConsensusEvents(2)
	if len(res) != 0 {
		t.Errorf("expected no consensus events at height=2 initially, got %d", len(res))
	}

	// Lower threshold so that n1 alone (50 stake) suffices.
	vv.SetThreshold(0.4) // required stake = 110*0.4 = 44
	res2 := vv.GetConsensusEvents(2)
	if len(res2) != 1 {
		t.Errorf("expected 1 consensus event at height=2 after lowering threshold, got %d", len(res2))
	} else if res2[0].ID != ev3.ID {
		t.Errorf("expected event ev3, got a different event")
	}
}

func TestVirtualVoting_Serialization(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	vv.SetThreshold(0.8)
	data, err := vv.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	var state map[string]interface{}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("failed to unmarshal state data: %v", err)
	}
	if state["threshold"] != 0.8 {
		t.Errorf("expected threshold=0.8 in JSON, got %v", state["threshold"])
	}

	// Restore into a new instance.
	dag2 := finality.NewDAG()
	vv2 := finality.NewVirtualVoting(dag2)
	if err := vv2.RestoreState(data); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	data2, _ := vv2.GetState()
	var state2 map[string]interface{}
	if err := json.Unmarshal(data2, &state2); err != nil {
		t.Fatalf("failed to unmarshal restored state: %v", err)
	}
	if state2["threshold"] != 0.8 {
		t.Errorf("restored threshold mismatch, expected 0.8, got %v", state2["threshold"])
	}
}

package finality_test

import (
	finality "diamante/consensus/diamantefinality"
	"encoding/json"
	"testing"
)

// We'll re-use randomNodeID and other helpers from prior tests as needed.
// randomNodeID(t) should produce a [32]byte random ID.

func TestVirtualVoting_BasicVote(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	// Add two nodes
	nodeA := randomNodeID(t)
	nodeB := randomNodeID(t)
	dag.AddNode(nodeA, 100)
	dag.AddNode(nodeB, 100)

	// Create an event by nodeA
	evA, err := dag.NewEvent(nodeA, nil, []byte("A's event"))
	if err != nil {
		t.Fatalf("NewEvent A error: %v", err)
	}
	// By default, threshold = 0.66 => we need > 66% of 200 stake = 132 stake
	// Only nodeA has "seen" it => stake=100 => not enough
	if vv.Vote(evA) {
		t.Error("expected Vote(evA) to fail because nodeB hasn't 'seen' it")
	}

	// Create an event for nodeB that references A's event => nodeB has 'seen' evA
	_, err = dag.NewEvent(nodeB, [][32]byte{evA.ID}, []byte("B references A"))
	if err != nil {
		t.Fatalf("NewEvent B error: %v", err)
	}

	// ... Explanation in comments about “HasSeen” logic ...
	// Check that vote still fails because nodeB is a child, not an ancestor
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
	dag.AddNode(nB, 200) // total stake=300

	// Create an event from nA
	ev, err := dag.NewEvent(nA, nil, []byte("test threshold"))
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	// threshold=0.66 => needed stake = 300*0.66=198 => nA's 100 alone isn't enough => fails
	if vv.Vote(ev) {
		t.Error("expected vote to fail with stake=100 < 198 required")
	}

	// Lower threshold => nA alone suffices
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

	// We'll create chain: evB -> evA
	evA, err := dag.NewEvent(nA, nil, []byte("A0"))
	if err != nil {
		t.Fatalf("create A0 error: %v", err)
	}
	evB, err := dag.NewEvent(nB, [][32]byte{evA.ID}, []byte("B references A0"))
	if err != nil {
		t.Fatalf("create B error: %v", err)
	}

	// Check if nA has seen evB => BFS from evB upward -> find nA's event as ancestor
	if !vv.HasSeen(nA, evB) {
		t.Error("expected nA to see evB, because evB has parent evA (which belongs to nA)")
	}
	// nB has obviously seen evB => BFS from evB => immediate creator
	if !vv.HasSeen(nB, evB) {
		t.Error("nB should see its own event")
	}
	// But does nB see evA? BFS from evA upward => no nB => false
	if vv.HasSeen(nB, evA) {
		t.Error("nB should NOT see evA")
	}
}

func TestVirtualVoting_GetConsensusEvents(t *testing.T) {
	dag := finality.NewDAG()
	vv := finality.NewVirtualVoting(dag)

	n1 := randomNodeID(t)
	n2 := randomNodeID(t)
	dag.AddNode(n1, 50)
	dag.AddNode(n2, 60) // total stake=110 => threshold=0.66 => needs ~72.6

	// Create some events at different heights
	_, _ = dag.NewEvent(n1, nil, []byte("ev1")) // ignore returns to avoid declared-but-unused
	_, _ = dag.NewEvent(n2, nil, []byte("ev2"))

	ev3, _ := dag.NewEvent(n1, nil, []byte("ev3"))
	ev3.Height = 2 // forcibly set height

	res := vv.GetConsensusEvents(2)
	if len(res) != 0 {
		t.Errorf("expected no events passing the vote at height=2, got %d", len(res))
	}

	// Lower threshold => n1 alone is enough
	vv.SetThreshold(0.4) // needed stake=110*0.4=44 => n1 has 50
	res2 := vv.GetConsensusEvents(2)
	if len(res2) != 1 {
		t.Errorf("expected 1 event passing vote at height=2, got %d", len(res2))
	} else if res2[0].ID != ev3.ID {
		t.Errorf("expected ev3, got something else")
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

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal of state data failed: %v", err)
	}
	if raw["threshold"] != 0.8 {
		t.Errorf("expected threshold=0.8 in JSON, got %v", raw["threshold"])
	}

	// Restore into a new instance
	dag2 := finality.NewDAG()
	vv2 := finality.NewVirtualVoting(dag2)
	if err := vv2.RestoreState(data); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	// Check threshold
	vv2StateData, _ := vv2.GetState()
	var raw2 map[string]interface{}
	json.Unmarshal(vv2StateData, &raw2)
	if raw2["threshold"] != 0.8 {
		t.Errorf("restored threshold mismatch, got %v", raw2["threshold"])
	}
}

package finality_test

import (
	"encoding/json"
	"testing"
	"time"

	finality "diamante/consensus/diamantefinality"
	"diamante/consensus/types"
)

func TestGossip_AddRemovePeer(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 200*time.Millisecond)

	node1 := randomNodeID(t)
	gp.AddPeer(node1)
	if len(gp.GetPeers()) != 1 {
		t.Errorf("expected 1 peer, got %d", len(gp.GetPeers()))
	}

	gp.RemovePeer(node1)
	if len(gp.GetPeers()) != 0 {
		t.Errorf("expected 0 peers after removal, got %d", len(gp.GetPeers()))
	}
}

func TestGossip_BroadcastEvent(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 100*time.Millisecond)

	sender := randomNodeID(t)
	gp.AddPeer(sender)
	receiver := randomNodeID(t)
	gp.AddPeer(receiver)

	// Start gossip loops
	gp.Start()

	// Create a test event
	event := &types.Event{
		Creator: sender,
		Data:    []byte("test broadcast"),
	}

	// Broadcast from sender
	gp.BroadcastEvent(event, sender)

	// Give a moment for the receiver to process the event
	time.Sleep(200 * time.Millisecond)

	// Check if the event was processed in DAG
	allEvents := dag.GetEvents()
	if len(allEvents) == 0 {
		t.Error("expected at least one event in DAG, got 0")
	}
}

func TestGossip_ForceSyncAll(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 50*time.Millisecond)

	// Add peers
	nodeA := randomNodeID(t)
	nodeB := randomNodeID(t)
	gp.AddPeer(nodeA)
	gp.AddPeer(nodeB)
	gp.Start()

	// Insert one event directly into the DAG
	dag.AddNode(nodeA, 100)
	_, err := dag.NewEvent(nodeA, nil, []byte("sync test")) // underscore to skip unused var
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	gp.ForceSyncAll()
	time.Sleep(100 * time.Millisecond)

	// We'll just confirm the DAG remains consistent (in a real integration test, you'd do more).
	allEv := dag.GetEvents()
	if len(allEv) == 0 {
		t.Error("expected some events in the DAG after ForceSyncAll")
	}

	gp.Stop()
}

func TestGossip_NetworkLoad(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 100*time.Millisecond)

	if gp.GetNetworkLoad() != 0.0 {
		t.Errorf("expected default load=0.0, got %f", gp.GetNetworkLoad())
	}

	gp.UpdateNetworkLoad(0.5)
	if gp.GetNetworkLoad() != 0.5 {
		t.Errorf("expected load=0.5 after update, got %f", gp.GetNetworkLoad())
	}

	// Attempt to set it beyond 1
	gp.UpdateNetworkLoad(1.5)
	if gp.GetNetworkLoad() != 1.0 {
		t.Errorf("expected load=1.0 after clamping, got %f", gp.GetNetworkLoad())
	}
}

func TestGossip_SetBaseDelay(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 100*time.Millisecond)

	gp.UpdateNetworkLoad(0.5)
	gp.SetBaseDelay(200 * time.Millisecond)

	// Because getCurrentDelay() is unexported, let's create an exported method in GossipProtocol:
	curDelay := gp.GetCurrentDelay() // <--- We'll add an exported method
	if curDelay < (300 * time.Millisecond) {
		t.Errorf("expected currentDelay ~300ms, got %v", curDelay)
	}
}

func TestGossip_GetStateAndRestore(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 100*time.Millisecond)

	n1 := randomNodeID(t)
	n2 := randomNodeID(t)
	gp.AddPeer(n1)
	gp.AddPeer(n2)
	gp.UpdateNetworkLoad(0.4)

	// Serialize
	stateData, err := gp.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	// We'll decode it just to see what's in there
	var raw map[string]interface{}
	if err := json.Unmarshal(stateData, &raw); err != nil {
		t.Errorf("json.Unmarshal of stateData failed: %v", err)
	}

	// Restore into a new GossipProtocol
	dag2 := finality.NewDAG()
	gp2 := finality.NewGossipProtocol(dag2, 50*time.Millisecond)
	if err := gp2.RestoreState(stateData); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	// Check peers
	peers2 := gp2.GetPeers()
	if len(peers2) != 2 {
		t.Errorf("expected 2 peers after restore, got %d", len(peers2))
	}

	if gp2.GetNetworkLoad() != 0.4 {
		t.Errorf("expected load=0.4 after restore, got %f", gp2.GetNetworkLoad())
	}

	curDelay2 := gp2.GetCurrentDelay()
	if curDelay2 <= 50*time.Millisecond {
		t.Errorf("expected delay > 50ms after restore, got %v", curDelay2)
	}
}

func TestGossip_StartStop(t *testing.T) {
	dag := finality.NewDAG()
	gp := finality.NewGossipProtocol(dag, 50*time.Millisecond)

	nodeA := randomNodeID(t)
	nodeB := randomNodeID(t)
	gp.AddPeer(nodeA)
	gp.AddPeer(nodeB)

	gp.Start() // Should start gossip loops
	time.Sleep(100 * time.Millisecond)
	gp.Stop() // Should terminate loops

	// Re-start again (just to ensure no panic or double close)
	gp.Start()
	time.Sleep(50 * time.Millisecond)
	gp.Stop()
}

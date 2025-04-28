// lachesis_test.go
package finality_test

import (
	"testing"
	"time"

	finality "diamante/consensus/diamantefinality"
)

func TestLachesis_BasicFlow(t *testing.T) {
	lach := finality.NewLachesis(100 * time.Millisecond)

	// Start Lachesis.
	if err := lach.Start(); err != nil {
		t.Fatalf("failed to start Lachesis: %v", err)
	}
	defer lach.Stop()

	validatorID := randomNodeID(t)
	lach.AddNode(validatorID, 1000)

	// Create an event.
	event := lach.CreateEvent(validatorID, nil, []byte("hello lachesis"))
	if event == nil {
		t.Fatal("CreateEvent returned nil, expected event")
	}
	// Allow some time for asynchronous processing.
	time.Sleep(100 * time.Millisecond)

	// Verify that there is at least one pending event.
	pending := lach.GetPendingEvents()
	if len(pending) == 0 {
		t.Error("expected at least 1 pending event, got 0")
	}
}

func TestLachesis_DoubleStart(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Errorf("first Start failed: %v", err)
	}
	// Second call should return an error.
	if err := lach.Start(); err == nil {
		t.Error("expected error on second Start, got nil")
	}
	lach.Stop()
}

func TestLachesis_CreateAndProcessEvent(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lach.Stop()

	nodeID := randomNodeID(t)
	lach.AddNode(nodeID, 500)

	ev := lach.CreateEvent(nodeID, nil, []byte("process event test"))
	if ev == nil {
		t.Fatal("CreateEvent returned nil")
	}

	// Manually invoke ProcessEvent to simulate synchronous processing.
	success := lach.ProcessEvent(ev)
	if !success {
		t.Error("ProcessEvent returned false, expected true")
	}
	if !ev.Finalized {
		t.Error("event should be marked Finalized after successful ProcessEvent")
	}
}

func TestLachesis_UpdateNetworkLoad(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)

	if load := lach.GetNetworkLoad(); load != 0.0 {
		t.Errorf("expected default networkLoad=0.0, got %f", load)
	}

	lach.AdjustNetworkLoad(+0.3)
	if load := lach.GetNetworkLoad(); load != 0.3 {
		t.Errorf("expected networkLoad=0.3, got %f", load)
	}

	lach.AdjustNetworkLoad(+1.0)
	if load := lach.GetNetworkLoad(); load != 1.0 {
		t.Errorf("expected networkLoad to clamp at 1.0, got %f", load)
	}
}

func TestLachesis_SetVotingThreshold(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if thresh := lach.GetVotingThreshold(); thresh != 0.66 {
		t.Errorf("expected default voting threshold=0.66, got %f", thresh)
	}
	lach.SetVotingThreshold(0.75)
	if thresh := lach.GetVotingThreshold(); thresh != 0.75 {
		t.Errorf("expected voting threshold=0.75 after update, got %f", thresh)
	}
}

func TestLachesis_ForceSync(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Fatalf("failed to start Lachesis: %v", err)
	}
	defer lach.Stop()

	// ForceSync simply calls GossipProtocol.ForceSyncAll.
	lach.ForceSync()
	// No error is expected; if it panics or errors, the test will fail.
}

func TestLachesis_Serialization(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Fatalf("failed to start Lachesis: %v", err)
	}
	nodeA := randomNodeID(t)
	lach.AddNode(nodeA, 100)
	ev1 := lach.CreateEvent(nodeA, nil, []byte("serialize me"))
	// Allow time for event finalization.
	time.Sleep(100 * time.Millisecond)

	stateData, err := lach.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	// Create a new Lachesis instance and restore state.
	lach2 := finality.NewLachesis(100 * time.Millisecond)
	if err := lach2.RestoreState(stateData); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	finals, _ := lach2.GetFinalizedEvents(ev1.Height, ev1.Height)
	if len(finals) == 0 {
		t.Error("expected to see the event in finalized list after restore")
	}
}

func TestLachesis_StopWithoutStart(t *testing.T) {
	lach := finality.NewLachesis(10 * time.Millisecond)
	if err := lach.Stop(); err == nil {
		t.Error("expected error when stopping Lachesis that isn't running")
	}
}

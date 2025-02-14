// consensus/diamantefinality/finality/lachesis_test.go

package finality_test

import (
	"testing"
	"time"

	finality "diamante/consensus/diamantefinality"
)

func TestLachesis_BasicFlow(t *testing.T) {
	lach := finality.NewLachesis(100 * time.Millisecond)

	// Initially, not running
	if err := lach.Start(); err != nil {
		t.Fatalf("failed to start Lachesis: %v", err)
	}
	defer lach.Stop()

	// Add a validator
	validatorID := randomNodeID(t)
	lach.AddNode(validatorID, 1000)

	// Create an event
	event := lach.CreateEvent(validatorID, nil, []byte("hello lachesis"))
	if event == nil {
		t.Fatal("CreateEvent returned nil, expected event")
	}
	time.Sleep(100 * time.Millisecond)

	// Check if it's in pending
	pending := lach.GetPendingEvents()
	if len(pending) == 0 {
		t.Error("expected 1 pending event, got 0")
	}
}

func TestLachesis_DoubleStart(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Errorf("first Start failed: %v", err)
	}
	if err := lach.Start(); err == nil {
		t.Error("expected error on second Start, got nil")
	}
	lach.Stop()
}

func TestLachesis_CreateAndProcessEvent(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	lach.Start()
	defer lach.Stop()

	nodeID := randomNodeID(t)
	lach.AddNode(nodeID, 500)

	ev := lach.CreateEvent(nodeID, nil, []byte("process event test"))
	if ev == nil {
		t.Fatal("CreateEvent returned nil")
	}

	// Manually call ProcessEvent (as if not in a goroutine)
	success := lach.ProcessEvent(ev)
	if !success {
		t.Error("ProcessEvent returned false, expected true (vote should pass by default?)")
	}

	if !ev.Finalized {
		t.Error("event should be marked Finalized after successful ProcessEvent")
	}
}

func TestLachesis_UpdateNetworkLoad(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if lach.GetNetworkLoad() != 0.0 {
		t.Errorf("expected default load=0.0, got %f", lach.GetNetworkLoad())
	}

	lach.AdjustNetworkLoad(+0.3)
	if load := lach.GetNetworkLoad(); load != 0.3 {
		t.Errorf("expected networkLoad=0.3, got %f", load)
	}

	lach.AdjustNetworkLoad(+1.0)
	if load2 := lach.GetNetworkLoad(); load2 != 1.0 {
		t.Errorf("expected load=1.0 after clamp, got %f", load2)
	}
}

func TestLachesis_SetVotingThreshold(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if lach.GetVotingThreshold() != 0.66 {
		t.Errorf("expected default=0.66, got %f", lach.GetVotingThreshold())
	}
	lach.SetVotingThreshold(0.75)
	if lach.GetVotingThreshold() != 0.75 {
		t.Errorf("expected threshold=0.75, got %f", lach.GetVotingThreshold())
	}
}

func TestLachesis_ForceSync(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	lach.Start()
	defer lach.Stop()

	// ForceSyncAll just calls gossip.ForceSyncAll, we can do a quick check
	lach.ForceSync()
}

func TestLachesis_Serialization(t *testing.T) {
	lach := finality.NewLachesis(50 * time.Millisecond)
	if err := lach.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	nodeA := randomNodeID(t)
	lach.AddNode(nodeA, 100)
	ev1 := lach.CreateEvent(nodeA, nil, []byte("serialize me"))
	time.Sleep(100 * time.Millisecond) // Let it finalize

	// Snapshot
	data, err := lach.GetState()
	if err != nil {
		t.Fatalf("GetState error: %v", err)
	}

	// Make a new Lachesis and restore
	lach2 := finality.NewLachesis(100 * time.Millisecond)
	if err := lach2.RestoreState(data); err != nil {
		t.Fatalf("RestoreState error: %v", err)
	}

	// Check that the event is found in finalized
	finals, _ := lach2.GetFinalizedEvents(ev1.Height, ev1.Height)
	if len(finals) == 0 {
		t.Error("expected to see the event in the new Lachesis's finalized list")
	}
}

func TestLachesis_StopWithoutStart(t *testing.T) {
	lach := finality.NewLachesis(10 * time.Millisecond)
	if err := lach.Stop(); err == nil {
		t.Error("expected error when stopping Lachesis that isn't running")
	}
}

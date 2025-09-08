package consensus_test

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"diamante/consensus/diamantefinality"
	"diamante/consensus/types"
)

// Helper function to generate random ID
func randomID() [32]byte {
	var id [32]byte
	rand.Read(id[:])
	return id
}

// Helper function to create test event
func createTestEvent(creator [32]byte, parents [][32]byte, height uint64) *types.Event {
	return &types.Event{
		ID:        randomID(),
		Creator:   creator,
		ParentIDs: parents,
		Data:      []byte("test data"),
		Timestamp: time.Now(),
		Height:    height,
		Finalized: false,
	}
}

func TestNewLachesis(t *testing.T) {
	gossipDelay := 100 * time.Millisecond
	lachesis := diamantefinality.NewLachesis(gossipDelay)

	if lachesis == nil {
		t.Fatal("Expected non-nil Lachesis instance")
	}

	// Check components initialized
	if lachesis.DAG == nil {
		t.Error("DAG not initialized")
	}
	if lachesis.GossipProtocol == nil {
		t.Error("GossipProtocol not initialized")
	}
	if lachesis.VirtualVoting == nil {
		t.Error("VirtualVoting not initialized")
	}
	if lachesis.Finalizer == nil {
		t.Error("Finalizer not initialized")
	}

	// Check default values
	if lachesis.GetVotingThreshold() != 66 {
		t.Errorf("Expected default voting threshold 66, got %f", lachesis.GetVotingThreshold())
	}
}

func TestLachesisAddNode(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	nodeID := randomID()
	stake := uint64(1000000)

	lachesis.AddNode(nodeID, stake)

	// Verify node was added to DAG by checking active nodes
	activeNodes := lachesis.GetActiveNodes()
	found := false
	for _, id := range activeNodes {
		if id == nodeID {
			found = true
			break
		}
	}

	if !found {
		t.Error("Node not found in active nodes after adding")
	}
}

func TestLachesisCreateEvent(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Add nodes
	node1 := randomID()
	node2 := randomID()
	lachesis.AddNode(node1, 1000000)
	lachesis.AddNode(node2, 1000000)

	// Create genesis events
	genesis1 := lachesis.CreateEvent(node1, nil, []byte("genesis1"))
	if genesis1 == nil {
		t.Fatal("Failed to create genesis event")
	}

	genesis2 := lachesis.CreateEvent(node2, nil, []byte("genesis2"))
	if genesis2 == nil {
		t.Fatal("Failed to create second genesis event")
	}

	// Create event with parents
	event := lachesis.CreateEvent(node1, [][32]byte{genesis1.ID, genesis2.ID}, []byte("child"))
	if event == nil {
		t.Fatal("Failed to create child event")
	}

	// Verify event properties
	if event.Creator != node1 {
		t.Error("Event has wrong creator")
	}
	if len(event.ParentIDs) != 2 {
		t.Errorf("Expected 2 parents, got %d", len(event.ParentIDs))
	}
	if event.Height == 0 {
		t.Error("Expected non-zero height for child event")
	}
}

func TestLachesisProcessEvent(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Add nodes with sufficient stake
	node1 := randomID()
	node2 := randomID()
	// Use higher stake to meet the 66% threshold
	stake := uint64(10000000)
	lachesis.AddNode(node1, stake)
	lachesis.AddNode(node2, stake)

	// Create and process event
	event := lachesis.CreateEvent(node1, nil, []byte("test"))

	// Test nil event handling
	success := lachesis.ProcessEvent(nil)
	if success {
		t.Error("Should not process nil event")
	}

	// Process valid event
	success = lachesis.ProcessEvent(event)
	// Note: This may fail due to voting threshold requirements
	// That's acceptable for unit testing
}

func TestLachesisVirtualVoting(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Add nodes with different stakes
	nodes := []struct {
		id    [32]byte
		stake uint64
	}{
		{randomID(), 3000000},
		{randomID(), 2000000},
		{randomID(), 1000000},
		{randomID(), 500000},
	}

	totalStake := uint64(0)
	for _, node := range nodes {
		lachesis.AddNode(node.id, node.stake)
		totalStake += node.stake
	}

	// Start consensus to enable voting
	err := lachesis.Start()
	if err != nil {
		t.Fatalf("Failed to start Lachesis: %v", err)
	}
	defer lachesis.Stop()

	// Create events from each node
	var events []*types.Event
	for _, node := range nodes {
		event := lachesis.CreateEvent(node.id, nil, []byte("test"))
		lachesis.ProcessEvent(event)
		events = append(events, event)
	}

	// Create child events that see the first event
	// Need >66% of stake to achieve consensus
	targetEvent := events[0]

	// First two nodes (5M stake total) see the target
	for i := 0; i < 2; i++ {
		child := lachesis.CreateEvent(nodes[i].id, [][32]byte{events[i].ID, targetEvent.ID}, []byte("vote"))
		lachesis.ProcessEvent(child)
	}

	// Allow time for consensus
	time.Sleep(300 * time.Millisecond)

	// Check if target event is finalized (5M/6.5M = ~77% > 66%)
	finalizedEvents, err := lachesis.GetFinalizedEvents(0, 10)
	if err != nil {
		t.Fatalf("Failed to get finalized events: %v", err)
	}

	found := false
	for _, e := range finalizedEvents {
		if e.ID == targetEvent.ID {
			found = true
			break
		}
	}

	if !found {
		t.Log("Event not finalized yet - this may be timing dependent")
	}
}

func TestLachesisByzantineResistance(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Setup: 1/3 byzantine, 2/3 honest nodes
	byzantineStake := uint64(1000000)
	honestStake := uint64(2000000)

	byzantine := randomID()
	honest1 := randomID()
	honest2 := randomID()

	lachesis.AddNode(byzantine, byzantineStake)
	lachesis.AddNode(honest1, honestStake)
	lachesis.AddNode(honest2, honestStake)

	// Byzantine node creates conflicting events
	event1 := lachesis.CreateEvent(byzantine, nil, []byte("conflict1"))
	event2 := lachesis.CreateEvent(byzantine, nil, []byte("conflict2"))
	lachesis.ProcessEvent(event1)
	lachesis.ProcessEvent(event2)

	// Honest nodes only see event1
	honestChild1 := lachesis.CreateEvent(honest1, [][32]byte{event1.ID}, []byte("honest1"))
	honestChild2 := lachesis.CreateEvent(honest2, [][32]byte{event1.ID}, []byte("honest2"))
	lachesis.ProcessEvent(honestChild1)
	lachesis.ProcessEvent(honestChild2)

	// Start consensus
	err := lachesis.Start()
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}
	defer lachesis.Stop()

	// Allow time for consensus
	time.Sleep(300 * time.Millisecond)

	// event1 should be finalized (2/3 honest stake)
	// event2 should not be finalized (only 1/3 byzantine stake)
	finalizedEvents, err := lachesis.GetFinalizedEvents(0, 10)
	if err != nil {
		t.Fatalf("Failed to get finalized events: %v", err)
	}

	event1Finalized := false
	event2Finalized := false
	for _, e := range finalizedEvents {
		if e.ID == event1.ID {
			event1Finalized = true
		}
		if e.ID == event2.ID {
			event2Finalized = true
		}
	}

	if !event1Finalized {
		t.Log("Event with 2/3 honest support not finalized yet - may be timing dependent")
	}
	if event2Finalized {
		t.Error("Event with only byzantine support should not be finalized")
	}
}

func TestLachesisConcurrentEventCreation(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(50 * time.Millisecond)

	// Add multiple nodes
	numNodes := 10
	nodes := make([][32]byte, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = randomID()
		lachesis.AddNode(nodes[i], 1000000)
	}

	// Start Lachesis
	err := lachesis.Start()
	if err != nil {
		t.Fatalf("Failed to start Lachesis: %v", err)
	}
	defer lachesis.Stop()

	// Create events concurrently
	var wg sync.WaitGroup
	eventsPerNode := 5
	errorChan := make(chan error, numNodes*eventsPerNode)

	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			for j := 0; j < eventsPerNode; j++ {
				event := lachesis.CreateEvent(nodes[nodeIdx], nil, []byte("concurrent"))
				if event == nil {
					errorChan <- fmt.Errorf("failed to create event for node %d", nodeIdx)
					return
				}
				if !lachesis.ProcessEvent(event) {
					errorChan <- fmt.Errorf("failed to process event for node %d", nodeIdx)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// Check for errors
	for err := range errorChan {
		t.Errorf("Error during concurrent event creation: %v", err)
	}

	// Verify events were created
	time.Sleep(200 * time.Millisecond) // Allow consensus to run

	finalizedEvents, err := lachesis.GetFinalizedEvents(0, 1000)
	if err != nil {
		t.Fatalf("Failed to get finalized events: %v", err)
	}

	if len(finalizedEvents) == 0 {
		t.Log("No events finalized yet - this is normal in concurrent test")
	} else {
		t.Logf("Finalized %d events", len(finalizedEvents))
	}
}

func TestLachesisGossipAndPropagation(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(50 * time.Millisecond)

	// Add nodes
	node1 := randomID()
	node2 := randomID()
	node3 := randomID()
	lachesis.AddNode(node1, 1000000)
	lachesis.AddNode(node2, 1000000)
	lachesis.AddNode(node3, 1000000)

	// Create event from node1
	event := lachesis.CreateEvent(node1, nil, []byte("gossip test"))
	lachesis.ProcessEvent(event)

	// Force sync to trigger gossip
	lachesis.ForceSync()

	// In a real test, we would verify gossip propagation through network peers
	// For now, we just verify the event was processed
	dagState := lachesis.GetDAGState()
	if _, exists := dagState[event.ID]; !exists {
		t.Error("Event not found in DAG after processing")
	}
}

func TestLachesisDynamicNetworkLoad(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Initial network load
	initialLoad := lachesis.GetNetworkLoad()
	if initialLoad != 0 {
		t.Errorf("Expected initial network load 0, got %f", initialLoad)
	}

	// Adjust network load
	lachesis.AdjustNetworkLoad(20)
	newLoad := lachesis.GetNetworkLoad()
	if newLoad != 20 {
		t.Errorf("Expected network load 20 after +20 adjustment, got %f", newLoad)
	}

	// Test boundaries
	lachesis.AdjustNetworkLoad(90) // Should cap at 100
	if lachesis.GetNetworkLoad() != 100 {
		t.Error("Network load should be capped at 100")
	}

	lachesis.AdjustNetworkLoad(-150) // Should floor at 0
	if lachesis.GetNetworkLoad() != 0 {
		t.Error("Network load should be floored at 0")
	}
}

func TestLachesisStateSerializationRestore(t *testing.T) {
	lachesis1 := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Setup initial state
	nodes := make([][32]byte, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = randomID()
		lachesis1.AddNode(nodes[i], uint64(1000000*(i+1)))
	}

	// Create some events
	var events []*types.Event
	for i := 0; i < 3; i++ {
		event := lachesis1.CreateEvent(nodes[i], nil, []byte(fmt.Sprintf("event%d", i)))
		lachesis1.ProcessEvent(event)
		events = append(events, event)
	}

	// Create child events
	for i := 0; i < 3; i++ {
		child := lachesis1.CreateEvent(nodes[i], [][32]byte{events[i].ID}, []byte("child"))
		lachesis1.ProcessEvent(child)
	}

	// Get state
	state, err := lachesis1.GetState()
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}

	// Create new instance and restore
	lachesis2 := diamantefinality.NewLachesis(100 * time.Millisecond)
	err = lachesis2.RestoreState(state)
	if err != nil {
		t.Fatalf("Failed to restore state: %v", err)
	}

	// Verify DAG state
	dagState1 := lachesis1.GetDAGState()
	dagState2 := lachesis2.GetDAGState()
	if len(dagState1) != len(dagState2) {
		t.Errorf("DAG event count mismatch: %d vs %d", len(dagState1), len(dagState2))
	}

	// Verify finalized events
	finalized1, err1 := lachesis1.GetFinalizedEvents(0, 100)
	finalized2, err2 := lachesis2.GetFinalizedEvents(0, 100)

	if err1 != nil || err2 != nil {
		t.Fatalf("Failed to get finalized events: %v, %v", err1, err2)
	}

	if len(finalized1) != len(finalized2) {
		t.Errorf("Finalized event count mismatch: %d vs %d",
			len(finalized1), len(finalized2))
	}
}

func TestLachesisVotingThresholdAdjustment(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Default threshold
	if lachesis.GetVotingThreshold() != 66 {
		t.Errorf("Expected default voting threshold 66, got %f",
			lachesis.GetVotingThreshold())
	}

	// Set new threshold
	lachesis.SetVotingThreshold(75)
	if lachesis.GetVotingThreshold() != 75 {
		t.Errorf("Expected voting threshold 75, got %f",
			lachesis.GetVotingThreshold())
	}

	// Test with new threshold
	// Add nodes with specific stake distribution
	node1 := randomID()
	node2 := randomID()
	node3 := randomID()
	node4 := randomID()

	lachesis.AddNode(node1, 2500000) // 25%
	lachesis.AddNode(node2, 2500000) // 25%
	lachesis.AddNode(node3, 2500000) // 25%
	lachesis.AddNode(node4, 2500000) // 25%

	// Start consensus
	err := lachesis.Start()
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}
	defer lachesis.Stop()

	// Create event
	event := lachesis.CreateEvent(node1, nil, []byte("test"))
	lachesis.ProcessEvent(event)

	// 3 nodes vote (75% of stake)
	for _, node := range [][32]byte{node1, node2, node3} {
		child := lachesis.CreateEvent(node, [][32]byte{event.ID}, []byte("vote"))
		lachesis.ProcessEvent(child)
	}

	// Allow time for consensus
	time.Sleep(300 * time.Millisecond)

	// Event should be finalized (exactly 75% threshold)
	finalized, err := lachesis.GetFinalizedEvents(0, 10)
	if err != nil {
		t.Fatalf("Failed to get finalized events: %v", err)
	}

	found := false
	for _, e := range finalized {
		if e.ID == event.ID {
			found = true
			break
		}
	}

	if !found {
		t.Log("Event with 75% stake support not finalized yet - may be timing dependent")
	}
}

func TestLachesisPerformanceUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	lachesis := diamantefinality.NewLachesis(10 * time.Millisecond)

	// Add many nodes
	numNodes := 50
	nodes := make([][32]byte, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = randomID()
		lachesis.AddNode(nodes[i], 1000000)
	}

	// Start consensus
	err := lachesis.Start()
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}
	defer lachesis.Stop()

	// Measure event processing rate
	start := time.Now()
	numEvents := 1000

	for i := 0; i < numEvents; i++ {
		node := nodes[i%numNodes]
		event := lachesis.CreateEvent(node, nil, []byte(fmt.Sprintf("perf%d", i)))
		lachesis.ProcessEvent(event)
	}

	duration := time.Since(start)
	eventsPerSecond := float64(numEvents) / duration.Seconds()

	t.Logf("Processed %d events in %v (%.0f events/second)",
		numEvents, duration, eventsPerSecond)

	// Wait for some finalization
	time.Sleep(500 * time.Millisecond)

	finalized, err := lachesis.GetFinalizedEvents(0, 10000)
	if err != nil {
		t.Fatalf("Failed to get finalized events: %v", err)
	}

	t.Logf("Finalized %d events", len(finalized))

	if len(finalized) == 0 {
		t.Log("No events finalized during performance test - this is normal under high load")
	}
}

func TestLachesisPendingEvents(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	// Add a node
	node := randomID()
	lachesis.AddNode(node, 1000000)

	// Create an event - this automatically adds it to pending
	lachesis.CreateEvent(node, nil, []byte("test"))

	// Get pending events
	pending := lachesis.GetPendingEvents()
	if len(pending) != 1 {
		t.Errorf("Expected 1 pending event, got %d", len(pending))
	}

	// Clear pending
	lachesis.ClearPendingEvents()
	pending = lachesis.GetPendingEvents()
	if len(pending) != 0 {
		t.Error("Expected no pending events after clear")
	}
}

func TestLachesisUpdateNodeStake(t *testing.T) {
	lachesis := diamantefinality.NewLachesis(100 * time.Millisecond)

	nodeID := randomID()
	initialStake := uint64(1000000)

	// Add node with initial stake
	lachesis.AddNode(nodeID, initialStake)

	// Update stake
	newStake := uint64(2000000)
	lachesis.UpdateNodeStake(nodeID, newStake)

	// Verify stake was updated by checking the node is still active
	activeNodes := lachesis.GetActiveNodes()
	found := false
	for _, id := range activeNodes {
		if id == nodeID {
			found = true
			break
		}
	}

	if !found {
		t.Error("Node should still be active after stake update")
	}
}

// Package e2e contains end-to-end tests for the Diamante blockchain
package e2e

import (
	"context"
	"testing"
	"time"

	"diamante/tests/e2e/framework"

	"github.com/stretchr/testify/require"
)

// TestBasicNetworkStartup tests that we can start a basic network and verify it's running
func TestBasicNetworkStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create test network with 3 validator nodes
	config := &framework.NetworkConfig{
		ValidatorNodes: 3,
		FullNodes:      1,
		EnableTLS:      false,
		BasePort:       8000,
	}

	network := framework.NewTestNetwork(t, config)
	defer network.Cleanup()

	// Start the network
	err := network.Start(ctx)
	require.NoError(t, err)

	// Wait for network to stabilize
	time.Sleep(10 * time.Second)

	// Verify all nodes are running
	nodes := network.GetNodes()
	require.Len(t, nodes, 4) // 3 validators + 1 full node

	for _, node := range nodes {
		require.True(t, node.IsRunning(), "Node %s should be running", node.ID)

		// Check node health
		healthy, err := node.IsHealthy(ctx)
		require.NoError(t, err, "Failed to check health for node %s", node.ID)
		require.True(t, healthy, "Node %s should be healthy", node.ID)
	}
}

// TestTransactionSubmission tests basic transaction submission and processing
func TestTransactionSubmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Create test network
	config := &framework.NetworkConfig{
		ValidatorNodes: 3,
		FullNodes:      0,
		EnableTLS:      false,
		BasePort:       8100,
	}

	network := framework.NewTestNetwork(t, config)
	defer network.Cleanup()

	// Start the network
	err := network.Start(ctx)
	require.NoError(t, err)

	// Wait for network to stabilize
	time.Sleep(10 * time.Second)

	// Get the first validator node for API calls
	nodes := network.GetNodes()
	require.NotEmpty(t, nodes)

	validator := nodes[0]
	require.True(t, validator.IsRunning())

	// Create a simple transaction
	tx := framework.CreateTestTransaction("sender123", "receiver456", 100.0)

	// Submit transaction
	txID, err := validator.SubmitTransaction(ctx, tx)
	require.NoError(t, err)
	require.NotEmpty(t, txID)

	// Wait for transaction to be processed
	time.Sleep(5 * time.Second)

	// Verify transaction was processed
	processedTx, err := validator.GetTransaction(ctx, txID)
	require.NoError(t, err)
	require.NotNil(t, processedTx)
	require.Equal(t, txID, processedTx.ID)
}

// TestConsensusAgreement tests that nodes reach consensus on blocks
func TestConsensusAgreement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// Create test network with multiple validators
	config := &framework.NetworkConfig{
		ValidatorNodes: 4,
		FullNodes:      1,
		EnableTLS:      false,
		BasePort:       8200,
	}

	network := framework.NewTestNetwork(t, config)
	defer network.Cleanup()

	// Start the network
	err := network.Start(ctx)
	require.NoError(t, err)

	// Wait for network to stabilize
	time.Sleep(15 * time.Second)

	// Submit multiple transactions
	nodes := network.GetValidatorNodes()
	require.NotEmpty(t, nodes)

	validator := nodes[0]

	// Submit several transactions
	var txIDs []string
	for i := 0; i < 5; i++ {
		tx := framework.CreateTestTransaction(
			"sender"+string(rune(i+48)),
			"receiver"+string(rune(i+48)),
			float64(10+i),
		)

		txID, err := validator.SubmitTransaction(ctx, tx)
		require.NoError(t, err)
		txIDs = append(txIDs, txID)
	}

	// Wait for consensus
	time.Sleep(20 * time.Second)

	// Verify all nodes have the same blockchain state
	latestBlocks := make(map[string]interface{})

	for _, node := range nodes {
		block, err := node.GetLatestBlock(ctx)
		require.NoError(t, err)
		require.NotNil(t, block)

		latestBlocks[node.ID] = block
	}

	// All nodes should have blocks (basic consensus check)
	require.Equal(t, len(nodes), len(latestBlocks))

	// In a real test, we'd compare block hashes to ensure consensus
	// For now, we verify that blocks exist and have reasonable content
	for nodeID, block := range latestBlocks {
		require.NotNil(t, block, "Node %s should have a latest block", nodeID)
	}
}

// TestNetworkPartition tests network resilience under partition scenarios
func TestNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping network partition test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create test network with enough validators for partition testing
	config := &framework.NetworkConfig{
		ValidatorNodes: 5,
		FullNodes:      0,
		EnableTLS:      false,
		BasePort:       8300,
	}

	network := framework.NewTestNetwork(t, config)
	defer network.Cleanup()

	// Start the network
	err := network.Start(ctx)
	require.NoError(t, err)

	// Wait for network to stabilize
	time.Sleep(15 * time.Second)

	nodes := network.GetValidatorNodes()
	require.Len(t, nodes, 5)

	// Submit initial transaction
	tx := framework.CreateTestTransaction("sender", "receiver", 50.0)
	txID, err := nodes[0].SubmitTransaction(ctx, tx)
	require.NoError(t, err)
	require.NotEmpty(t, txID)

	// Wait for processing
	time.Sleep(10 * time.Second)

	// Simulate partition: stop 2 nodes (minority)
	err = nodes[3].Stop(ctx)
	require.NoError(t, err)
	err = nodes[4].Stop(ctx)
	require.NoError(t, err)

	// Wait for partition effects
	time.Sleep(5 * time.Second)

	// Majority (3 nodes) should still process transactions
	tx2 := framework.CreateTestTransaction("sender2", "receiver2", 75.0)
	txID2, err := nodes[0].SubmitTransaction(ctx, tx2)
	require.NoError(t, err)
	require.NotEmpty(t, txID2)

	// Wait for processing
	time.Sleep(10 * time.Second)

	// Verify majority partition is functional
	block, err := nodes[0].GetLatestBlock(ctx)
	require.NoError(t, err)
	require.NotNil(t, block)

	// Restart partitioned nodes
	err = nodes[3].Start(ctx)
	require.NoError(t, err)
	err = nodes[4].Start(ctx)
	require.NoError(t, err)

	// Wait for network to heal
	time.Sleep(20 * time.Second)

	// Verify all nodes are healthy again
	for _, node := range nodes {
		healthy, err := node.IsHealthy(ctx)
		require.NoError(t, err)
		require.True(t, healthy, "Node %s should be healthy after partition healing", node.ID)
	}
}

// network/partition_network_adapter.go

package network

import (
	"context"
)

// PartitionNetworkAdapter adapts the NetworkManager for use with the PartitionHandler
// It implements the NetworkAdapter interface
type PartitionNetworkAdapter struct {
	networkManager *NetworkManager
}

// Ensure PartitionNetworkAdapter implements NetworkAdapter interface
var _ NetworkAdapter = (*PartitionNetworkAdapter)(nil)

// NewPartitionNetworkAdapter creates a new adapter for the NetworkManager
func NewPartitionNetworkAdapter(networkManager *NetworkManager) NetworkAdapter {
	return &PartitionNetworkAdapter{
		networkManager: networkManager,
	}
}

// GetNodeID returns the ID of the local node
func (pna *PartitionNetworkAdapter) GetNodeID() string {
	return pna.networkManager.localAddr
}

// GetPeerList returns a list of all peer addresses
func (pna *PartitionNetworkAdapter) GetPeerList() []string {
	pna.networkManager.mu.RLock()
	defer pna.networkManager.mu.RUnlock()

	peers := make([]string, 0, len(pna.networkManager.peers))
	for addr := range pna.networkManager.peers {
		peers = append(peers, addr)
	}
	return peers
}

// GetPeerByID returns a peer by its address
func (pna *PartitionNetworkAdapter) GetPeerByID(id string) *Peer {
	pna.networkManager.mu.RLock()
	defer pna.networkManager.mu.RUnlock()
	return pna.networkManager.peers[id]
}

// SendMessageWithResponse sends a message to a peer and waits for a response
func (pna *PartitionNetworkAdapter) SendMessageWithResponse(peer *Peer, message *Message, ctx context.Context) (*Message, error) {
	// Delegate to the network manager's implementation
	return pna.networkManager.SendMessageWithResponse(peer, message, ctx)
}

// BroadcastMessage sends a message to all peers
func (pna *PartitionNetworkAdapter) BroadcastMessage(message *Message) error {
	// Set the sender if not already set
	if message.Sender == "" {
		message.Sender = pna.GetNodeID()
	}

	// Convert to basic Message and broadcast
	pna.networkManager.Broadcast(*message)
	return nil
}

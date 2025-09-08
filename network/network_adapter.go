package network

import (
	"context"
)

// NetworkAdapter defines the interface for network adapters used by the partition handler
type NetworkAdapter interface {
	// GetNodeID returns the ID of the local node
	GetNodeID() string

	// GetPeerList returns a list of all peer addresses
	GetPeerList() []string

	// GetPeerByID returns a peer by its address
	GetPeerByID(id string) *Peer

	// SendMessageWithResponse sends a message to a peer and waits for a response
	SendMessageWithResponse(peer *Peer, message *Message, ctx context.Context) (*Message, error)

	// BroadcastMessage sends a message to all peers
	BroadcastMessage(message *Message) error
}

package network

import (
	"log"
	"sync"
	"time"
)

// P2PNode demonstrates a higher-level node that uses NetworkManager for advanced logic.
type P2PNode struct {
	netMgr  *NetworkManager
	storage sync.Map // Example local in-memory store (if needed)
}

// NewP2PNode creates a new node with a manager and returns it.
func NewP2PNode(manager *NetworkManager) *P2PNode {
	return &P2PNode{
		netMgr: manager,
	}
}

// Start starts the node’s manager.
func (n *P2PNode) Start() error {
	return n.netMgr.Start()
}

// Stop stops the node’s manager.
func (n *P2PNode) Stop() error {
	return n.netMgr.Stop()
}

// ConnectToPeer tries to dial an external peer.
func (n *P2PNode) ConnectToPeer(peerAddr string) {
	if err := n.netMgr.DialPeer(peerAddr); err != nil {
		log.Printf("Error dialing peer %s: %v\n", peerAddr, err)
	}
}

// BroadcastMsg sends a message to all peers.
func (n *P2PNode) BroadcastMsg(msgType string, payload interface{}) {
	msg := Message{
		Type:    msgType,
		Payload: payload,
	}
	n.netMgr.Broadcast(msg)
}

// ExampleBackgroundTask a demonstration of a node-based background routine.
func (n *P2PNode) ExampleBackgroundTask() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			load := n.netMgr.GetNetworkHealth()
			log.Printf("[Node Background] Current network load: %d%%\n", load)
		}
	}()
}

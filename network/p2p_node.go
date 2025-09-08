package network

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"diamante/common"
	"github.com/sirupsen/logrus"
)

// P2PNode demonstrates a higher-level node that uses NetworkManager for advanced logic.
type P2PNode struct {
	netMgr *NetworkManager
	logger *logrus.Entry
}

// NewP2PNode creates a new node with a manager and returns it.
func NewP2PNode(manager *NetworkManager) *P2PNode {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return &P2PNode{
		netMgr: manager,
		logger: logger.WithField("component", "P2PNode"),
	}
}

// GetNetworkManager returns the underlying network manager
func (n *P2PNode) GetNetworkManager() *NetworkManager {
	return n.netMgr
}

// GetPeers returns all connected peers
func (n *P2PNode) GetPeers() []*Peer {
	if n.netMgr != nil {
		return n.netMgr.GetPeers()
	}
	return nil
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
		n.logger.WithFields(logrus.Fields{
			"peer":  peerAddr,
			"error": err,
		}).Error("Error dialing peer")
	} else {
		n.logger.WithField("peer", peerAddr).Info("Successfully connected to peer")
	}
}

// BroadcastMsg sends a message to all peers.
func (n *P2PNode) BroadcastMsg(msgType string, payload MessagePayload) {
	// Marshal the payload to json.RawMessage
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		n.logger.WithField("error", err).Error("Failed to marshal payload")
		return
	}

	msg := Message{
		Type:    msgType,
		Payload: payloadBytes,
	}

	if err := n.netMgr.Broadcast(msg); err != nil {
		n.logger.WithField("error", err).Error("Failed to broadcast message")
	}
}

// BroadcastHeartbeat sends a heartbeat message to all peers
func (n *P2PNode) BroadcastHeartbeat() {
	heartbeat := &HeartbeatPayload{
		NodeID:        n.netMgr.GetNodeID(),
		Timestamp:     common.ConsensusUnix(),
		NetworkStatus: "healthy", // Use NetworkStatus field
		Version:       "1.0.0",
		Signature:     "heartbeat_signature", // In real implementation, this would be properly signed
	}
	n.BroadcastMsg("Heartbeat", heartbeat)
}

// ExampleBackgroundTask a demonstration of a node-based background routine.
func (n *P2PNode) ExampleBackgroundTask() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			load, err := n.netMgr.GetNetworkHealth()
			if err != nil {
				n.logger.WithField("error", err).Error("Failed to get network health")
				load = 0 // Use 0 as fallback value
			}
			n.logger.WithField("load", load).Debug("Current network load")
		}
	}()
}

// GetPeerCount returns the number of connected peers
func (n *P2PNode) GetPeerCount() int {
	if n.netMgr == nil {
		return 0
	}
	// Get the peer list from NetworkManager
	peers, err := n.netMgr.GetPeerList()
	if err != nil {
		return 0
	}
	return len(peers)
}

// SetMaxPeers sets the maximum number of allowed peer connections
func (n *P2PNode) SetMaxPeers(max int) {
	if n.netMgr != nil {
		n.netMgr.SetMaxPeers(max)
	}
}

// EnableTLS enables TLS for the P2P connections
func (n *P2PNode) EnableTLS(cert, key string) error {
	if n.netMgr == nil {
		return fmt.Errorf("network manager is nil")
	}

	// Load certificate and key
	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	// Create TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
	}

	return n.netMgr.EnableTLS(tlsConfig)
}

// AddBootstrapNode adds a bootstrap node address to the network
func (n *P2PNode) AddBootstrapNode(addr string) error {
	// This would add the node to NetworkManager's bootstrap list
	// For now, we can use ConnectToPeer as a workaround
	n.ConnectToPeer(addr)
	return nil
}

// SetTransactionHandler sets the handler for incoming transaction broadcasts
func (n *P2PNode) SetTransactionHandler(handler func(*TransactionPayload) error) {
	// Delegate to the network manager
	if n.netMgr != nil {
		n.netMgr.SetTransactionHandler(handler)
	}
}

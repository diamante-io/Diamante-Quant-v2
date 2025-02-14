package network

import (
	"diamante/common"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// NetworkManager is responsible for overall peer connections, network health checks, etc.
type NetworkManager struct {
	mu        sync.RWMutex
	localAddr string
	peers     map[string]*Peer
	discovery Discovery // Basic peer discovery interface
	isRunning bool
	stopChan  chan struct{}
	health    int // Simplistic "load" or "health" metric: 0..100
}

// NewNetworkManager initializes a new manager with a local listening address and a Discovery mechanism.
func NewNetworkManager(localAddr string, d Discovery) *NetworkManager {
	return &NetworkManager{
		localAddr: localAddr,
		peers:     make(map[string]*Peer),
		discovery: d,
		stopChan:  make(chan struct{}),
		health:    0,
	}
}

// Start begins listening for inbound connections and also attempts outgoing connections if needed.
func (nm *NetworkManager) Start() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.isRunning {
		return fmt.Errorf("network manager is already running")
	}
	nm.isRunning = true

	go nm.listenInbound()
	go nm.periodicHealthCheck()
	go nm.discovery.Start()

	log.Printf("NetworkManager started, listening on %s\n", nm.localAddr)
	return nil
}

// Stop signals the network manager to close all peers and stop listening.
func (nm *NetworkManager) Stop() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if !nm.isRunning {
		return fmt.Errorf("network manager is not running")
	}
	nm.isRunning = false

	close(nm.stopChan)
	nm.discovery.Stop()

	for _, peer := range nm.peers {
		_ = peer.Close()
	}
	nm.peers = make(map[string]*Peer)

	log.Println("NetworkManager stopped")
	return nil
}

// listenInbound listens on nm.localAddr for new inbound TCP connections, spawns a Peer for each.
func (nm *NetworkManager) listenInbound() {
	ln, err := net.Listen("tcp", nm.localAddr)
	if err != nil {
		log.Printf("Error listening on %s: %v\n", nm.localAddr, err)
		return
	}
	defer ln.Close()

	for {
		select {
		case <-nm.stopChan:
			log.Println("listenInbound shutting down")
			return
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v\n", err)
			continue
		}
		log.Printf("Accepted inbound from %s\n", conn.RemoteAddr().String())
		go nm.handleInbound(conn)
	}
}

func (nm *NetworkManager) handleInbound(conn net.Conn) {
	peer := NewPeer(conn.RemoteAddr().String(), conn, nm)
	nm.addPeer(peer)
	go peer.Run()
}

// addPeer registers a peer in the manager’s peer map.
func (nm *NetworkManager) addPeer(peer *Peer) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.peers[peer.Addr] = peer
}

// RemovePeer removes a peer from the manager’s peer map.
func (nm *NetworkManager) RemovePeer(addr string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	delete(nm.peers, addr)
	log.Printf("Peer %s removed\n", addr)
}

// DialPeer attempts an outbound connection to a peer.
func (nm *NetworkManager) DialPeer(addr string) error {
	nm.mu.RLock()
	if !nm.isRunning {
		nm.mu.RUnlock()
		return fmt.Errorf("network manager is not running, cannot dial peer")
	}
	nm.mu.RUnlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to dial peer %s: %w", addr, err)
	}
	log.Printf("Outbound connection established to %s\n", addr)
	peer := NewPeer(addr, conn, nm)
	nm.addPeer(peer)
	go peer.Run()
	return nil
}

// Broadcast sends a Message to all connected peers.
func (nm *NetworkManager) Broadcast(msg Message) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	for _, peer := range nm.peers {
		peer.Send(msg)
	}
}

// GetNetworkHealth returns a simplistic network load metric (0..100).
func (nm *NetworkManager) GetNetworkHealth() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.health
}

// periodicHealthCheck is a simplistic ticker that increments 'health' or adjusts it.
func (nm *NetworkManager) periodicHealthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-nm.stopChan:
			return
		case <-ticker.C:
			nm.mu.Lock()
			// For demonstration, just vary the metric a bit:
			nm.health = (nm.health + 5) % 100
			nm.mu.Unlock()
		}
	}
}

func (nm *NetworkManager) BroadcastTransaction(tx common.Transaction) {
	// Re-use the existing Broadcast(msg) method:
	nm.Broadcast(Message{
		Type:    common.TransactionBroadcast, // from your common package
		Payload: tx,
	})
}

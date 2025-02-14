package network

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Peer represents a connected node in the network.
type Peer struct {
	Addr     string
	conn     net.Conn
	netMgr   *NetworkManager
	incoming chan Message
	quit     chan struct{}
	mu       sync.Mutex
}

// NewPeer creates a new Peer.
func NewPeer(addr string, conn net.Conn, nm *NetworkManager) *Peer {
	return &Peer{
		Addr:     addr,
		conn:     conn,
		netMgr:   nm,
		incoming: make(chan Message, 32),
		quit:     make(chan struct{}),
	}
}

// Run starts read & write loops for the Peer.
func (p *Peer) Run() {
	go p.readLoop()
	go p.writeLoop()
}

// Close closes the peer’s connection.
func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.quit:
		// Already closed
		return nil
	default:
		close(p.quit)
		err := p.conn.Close()
		p.netMgr.RemovePeer(p.Addr)
		log.Printf("Peer %s closed: %v\n", p.Addr, err)
		return err
	}
}

// Send queues a message to be written to this peer.
func (p *Peer) Send(msg Message) {
	select {
	case p.incoming <- msg:
		// message enqueued
	default:
		log.Printf("Peer %s incoming channel is full, dropping message\n", p.Addr)
	}
}

// readLoop continuously reads messages from the peer’s socket.
func (p *Peer) readLoop() {
	defer p.Close()

	decoder := json.NewDecoder(p.conn)
	for {
		select {
		case <-p.quit:
			return
		default:
		}

		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				log.Printf("Peer %s disconnected\n", p.Addr)
			} else {
				log.Printf("Peer %s read error: %v\n", p.Addr, err)
			}
			return
		}
		log.Printf("Peer %s received message: %v\n", p.Addr, msg)
		// Here you could handle messages or pass them to netMgr or consensus, etc.
	}
}

// writeLoop sends any queued messages to the peer.
func (p *Peer) writeLoop() {
	encoder := json.NewEncoder(p.conn)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.quit:
			return
		case msg := <-p.incoming:
			if err := encoder.Encode(msg); err != nil {
				log.Printf("Peer %s write error: %v\n", p.Addr, err)
				return
			}
		case <-ticker.C:
			// Periodic keepalive or ping, if desired
			keepalive := Message{
				Type:    "keepalive",
				Payload: fmt.Sprintf("Time: %s", time.Now().Format(time.RFC3339)),
			}
			if err := encoder.Encode(keepalive); err != nil {
				log.Printf("Peer %s keepalive write error: %v\n", p.Addr, err)
				return
			}
		}
	}
}

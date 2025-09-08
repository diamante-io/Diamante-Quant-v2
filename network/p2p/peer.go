package p2p

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"

	"diamante/apperrors"
	"diamante/consensus"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

// PeerState represents the current state of a peer connection
type PeerState int

const (
	PeerStateDisconnected PeerState = iota
	PeerStateConnecting
	PeerStateHandshaking
	PeerStateConnected
	PeerStateDisconnecting
)

// String returns the string representation of PeerState
func (ps PeerState) String() string {
	switch ps {
	case PeerStateDisconnected:
		return "disconnected"
	case PeerStateConnecting:
		return "connecting"
	case PeerStateHandshaking:
		return "handshaking"
	case PeerStateConnected:
		return "connected"
	case PeerStateDisconnecting:
		return "disconnecting"
	default:
		return "unknown"
	}
}

// PeerServices represents the services offered by a peer
type PeerServices uint64

const (
	ServiceNodeNetwork PeerServices = 1 << iota // Full node with complete blockchain
	ServiceNodeBloom                            // Supports bloom filter queries
	ServiceNodeWitness                          // Witness node for light clients
	ServiceNodeCompact                          // Supports compact block relay
	ServiceNodeSegwit                           // Supports segregated witness
)

// Peer represents a connected peer in the P2P network
type Peer struct {
	// Basic peer information
	ID       string
	Address  string
	Port     uint16
	Services PeerServices

	// Connection details
	conn    net.Conn
	state   PeerState
	stateMu sync.RWMutex
	inbound bool

	// Protocol information
	version     uint8
	userAgent   string
	blockHeight uint64

	// Timing information
	connectedAt time.Time
	lastSeen    time.Time
	lastPing    time.Time
	lastPong    time.Time
	pingLatency time.Duration

	// Message handling
	sendQueue chan *Message
	recvQueue chan *Message
	protocol  *Protocol

	// Cryptographic keys
	publicKey  *crypto.DilithiumKeyPair
	privateKey *crypto.DilithiumKeyPair

	// Statistics
	messagesSent     uint64
	messagesReceived uint64
	bytesSent        uint64
	bytesReceived    uint64

	// Control channels
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// Logger
	logger *logrus.Entry

	// Mutex for thread safety
	mu sync.RWMutex
}

// NewPeer creates a new peer instance
func NewPeer(id, address string, port uint16, conn net.Conn, inbound bool, protocol *Protocol, logger *logrus.Logger) *Peer {
	if logger == nil {
		logger = logrus.New()
	}

	ctx, cancel := context.WithCancel(context.Background())

	peer := &Peer{
		ID:      id,
		Address: address,
		Port:    port,
		conn:    conn,
		state:   PeerStateConnecting,
		inbound: inbound,
		version: ProtocolVersion,

		connectedAt: consensus.ConsensusNow(),
		lastSeen:    consensus.ConsensusNow(),

		sendQueue: make(chan *Message, 100), // Buffered channel for outgoing messages
		recvQueue: make(chan *Message, 100), // Buffered channel for incoming messages
		protocol:  protocol,

		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),

		logger: logger.WithFields(logrus.Fields{
			"peer_id": id,
			"address": address,
			"inbound": inbound,
		}),
	}

	return peer
}

// Start begins the peer's message handling loops
func (p *Peer) Start() error {
	p.stateMu.Lock()
	if p.state != PeerStateConnecting {
		p.stateMu.Unlock()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"peer is not in connecting state")
	}
	p.state = PeerStateHandshaking
	p.stateMu.Unlock()

	p.logger.Info("Starting peer connection")

	// Start message handling goroutines
	go p.sendLoop()
	go p.recvLoop()
	go p.pingLoop()

	return nil
}

// Stop gracefully stops the peer connection
func (p *Peer) Stop() error {
	p.stateMu.Lock()
	if p.state == PeerStateDisconnected || p.state == PeerStateDisconnecting {
		p.stateMu.Unlock()
		return nil
	}
	p.state = PeerStateDisconnecting
	p.stateMu.Unlock()

	p.logger.Info("Stopping peer connection")

	// Cancel context to stop all goroutines
	p.cancel()

	// Close connection
	if p.conn != nil {
		p.conn.Close()
	}

	// Close channels
	close(p.sendQueue)
	close(p.recvQueue)

	// Wait for goroutines to finish
	<-p.done

	p.stateMu.Lock()
	p.state = PeerStateDisconnected
	p.stateMu.Unlock()

	p.logger.Info("Peer connection stopped")
	return nil
}

// SendMessage queues a message to be sent to the peer
func (p *Peer) SendMessage(msg *Message) error {
	if msg == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message is nil")
	}

	p.stateMu.RLock()
	state := p.state
	p.stateMu.RUnlock()

	if state != PeerStateConnected && state != PeerStateHandshaking {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"peer is not connected")
	}

	select {
	case p.sendQueue <- msg:
		return nil
	case <-p.ctx.Done():
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"peer is shutting down")
	default:
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"send queue is full")
	}
}

// GetState returns the current state of the peer
func (p *Peer) GetState() PeerState {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.state
}

// SetState sets the peer state
func (p *Peer) SetState(state PeerState) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	if p.state != state {
		p.logger.WithFields(logrus.Fields{
			"old_state": p.state.String(),
			"new_state": state.String(),
		}).Debug("Peer state changed")
		p.state = state
	}
}

// GetInfo returns basic peer information
func (p *Peer) GetInfo() *PeerInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return &PeerInfo{
		ID:          p.ID,
		Address:     p.Address,
		Port:        p.Port,
		Services:    uint64(p.Services),
		UserAgent:   p.userAgent,
		BlockHeight: p.blockHeight,
	}
}

// UpdateInfo updates peer information from handshake
func (p *Peer) UpdateInfo(info *PeerInfo) {
	if info == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.Services = PeerServices(info.Services)
	p.userAgent = info.UserAgent
	p.blockHeight = info.BlockHeight
	p.lastSeen = consensus.ConsensusNow()

	p.logger.WithFields(logrus.Fields{
		"services":     p.Services,
		"user_agent":   p.userAgent,
		"block_height": p.blockHeight,
	}).Debug("Updated peer information")
}

// GetLatency returns the current ping latency
func (p *Peer) GetLatency() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pingLatency
}

// GetStatistics returns peer statistics
func (p *Peer) GetStatistics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"messages_sent":     p.messagesSent,
		"messages_received": p.messagesReceived,
		"bytes_sent":        p.bytesSent,
		"bytes_received":    p.bytesReceived,
		"connected_at":      p.connectedAt,
		"last_seen":         p.lastSeen,
		"ping_latency":      p.pingLatency,
		"state":             p.GetState().String(),
	}
}

// SetKeys sets the cryptographic keys for the peer
func (p *Peer) SetKeys(publicKey, privateKey *crypto.DilithiumKeyPair) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.publicKey = publicKey
	p.privateKey = privateKey
}

// GetPublicKey returns the peer's public key
func (p *Peer) GetPublicKey() *crypto.DilithiumKeyPair {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.publicKey
}

// sendLoop handles outgoing messages
func (p *Peer) sendLoop() {
	defer func() {
		p.logger.Debug("Send loop terminated")
		select {
		case p.done <- struct{}{}:
		default:
		}
	}()

	for {
		select {
		case msg := <-p.sendQueue:
			if msg == nil {
				continue
			}

			if err := p.sendMessage(msg); err != nil {
				p.logger.WithError(err).Error("Failed to send message")
				return
			}

		case <-p.ctx.Done():
			p.logger.Debug("Send loop context cancelled")
			return
		}
	}
}

// recvLoop handles incoming messages
func (p *Peer) recvLoop() {
	defer func() {
		p.logger.Debug("Receive loop terminated")
		select {
		case p.done <- struct{}{}:
		default:
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Receive loop context cancelled")
			return
		default:
			// Set read timeout
			p.conn.SetReadDeadline(consensus.ConsensusNow().Add(30 * time.Second))

			msg, err := p.receiveMessage()
			if err != nil {
				p.logger.WithError(err).Error("Failed to receive message")
				return
			}

			if msg != nil {
				p.handleIncomingMessage(msg)
			}
		}
	}
}

// pingLoop sends periodic ping messages
func (p *Peer) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if p.GetState() == PeerStateConnected {
				if err := p.sendPing(); err != nil {
					p.logger.WithError(err).Error("Failed to send ping")
				}
			}

		case <-p.ctx.Done():
			p.logger.Debug("Ping loop context cancelled")
			return
		}
	}
}

// sendMessage sends a message over the connection
func (p *Peer) sendMessage(msg *Message) error {
	// Encode message
	data, err := p.protocol.EncodeMessage(msg)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to encode message")
	}

	// Set write timeout
	p.conn.SetWriteDeadline(consensus.ConsensusNow().Add(10 * time.Second))

	// Send message
	_, err = p.conn.Write(data)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to write message")
	}

	// Update statistics
	p.mu.Lock()
	p.messagesSent++
	p.bytesSent += uint64(len(data))
	p.mu.Unlock()

	p.logger.WithFields(logrus.Fields{
		"type": msg.Type,
		"size": len(data),
	}).Debug("Message sent")

	return nil
}

// receiveMessage receives a message from the connection
func (p *Peer) receiveMessage() (*Message, error) {
	// Read header first
	headerBuf := make([]byte, HeaderSize)
	_, err := p.conn.Read(headerBuf)
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to read message header")
	}

	// Decode header to get payload length
	if len(headerBuf) < 12 {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"invalid header size")
	}

	payloadLength := uint32(headerBuf[8])<<24 | uint32(headerBuf[9])<<16 | uint32(headerBuf[10])<<8 | uint32(headerBuf[11])

	// Read payload if present
	var fullMessage []byte
	if payloadLength > 0 {
		payloadBuf := make([]byte, payloadLength)
		_, err = p.conn.Read(payloadBuf)
		if err != nil {
			return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
				"failed to read message payload")
		}

		fullMessage = append(headerBuf, payloadBuf...)
	} else {
		fullMessage = headerBuf
	}

	// Decode complete message
	msg, err := p.protocol.DecodeMessage(fullMessage)
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to decode message")
	}

	// Update statistics
	p.mu.Lock()
	p.messagesReceived++
	p.bytesReceived += uint64(len(fullMessage))
	p.lastSeen = consensus.ConsensusNow()
	p.mu.Unlock()

	p.logger.WithFields(logrus.Fields{
		"type": msg.Type,
		"size": len(fullMessage),
	}).Debug("Message received")

	return msg, nil
}

// handleIncomingMessage processes an incoming message
func (p *Peer) handleIncomingMessage(msg *Message) {
	// Handle protocol-level messages
	switch msg.Type {
	case MessageTypePing:
		p.handlePing(msg)
		return
	case MessageTypePong:
		p.handlePong(msg)
		return
	case MessageTypeHandshake:
		p.handleHandshake(msg)
		return
	}

	// For other messages, use the protocol handler
	if p.protocol != nil {
		if err := p.protocol.HandleMessage(p, msg); err != nil {
			p.logger.WithError(err).Error("Protocol handler failed")
		}
	}
}

// handlePing processes a ping message
func (p *Peer) handlePing(msg *Message) {
	// Create pong response
	pong, err := p.protocol.CreatePongMessage(msg.ID, msg.Timestamp)
	if err != nil {
		p.logger.WithError(err).Error("Failed to create pong message")
		return
	}

	// Send pong
	if err := p.SendMessage(pong); err != nil {
		p.logger.WithError(err).Error("Failed to send pong message")
	}
}

// handlePong processes a pong message
func (p *Peer) handlePong(msg *Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastPong = consensus.ConsensusNow()

	// Calculate latency if this is a response to our ping
	if !p.lastPing.IsZero() {
		p.pingLatency = p.lastPong.Sub(p.lastPing)
		p.logger.WithField("latency", p.pingLatency).Debug("Updated ping latency")
	}
}

// handleHandshake processes a handshake message
func (p *Peer) handleHandshake(msg *Message) {
	// Decode handshake payload
	if len(msg.Payload) == 0 {
		p.logger.Warn("Received empty handshake payload")
		return
	}

	// Parse handshake data (simple JSON format)
	var handshakeData map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &handshakeData); err != nil {
		p.logger.WithError(err).Warn("Failed to parse handshake payload")
		return
	}

	// Update peer info from handshake
	if version, ok := handshakeData["version"].(float64); ok {
		p.version = uint8(version)
	}
	if userAgent, ok := handshakeData["userAgent"].(string); ok {
		p.userAgent = userAgent
	}
	if blockHeight, ok := handshakeData["blockHeight"].(float64); ok {
		p.blockHeight = uint64(blockHeight)
	}

	// Store additional peer information in a temporary map for logging
	peerInfo := make(map[string]interface{})
	if nodeID, ok := handshakeData["nodeID"].(string); ok {
		peerInfo["nodeID"] = nodeID
	}
	if capabilities, ok := handshakeData["capabilities"].([]interface{}); ok {
		peerInfo["capabilities"] = capabilities
	}

	// Mark as connected
	p.SetState(PeerStateConnected)

	// Log handshake completion with available information
	logFields := logrus.Fields{
		"version":     p.version,
		"userAgent":   p.userAgent,
		"blockHeight": p.blockHeight,
	}

	// Add additional peer info to log fields
	for key, value := range peerInfo {
		logFields[key] = value
	}

	p.logger.WithFields(logFields).Info("Handshake completed, peer connected")
}

// sendPing sends a ping message
func (p *Peer) sendPing() error {
	ping, err := p.protocol.CreatePingMessage()
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create ping message")
	}

	p.mu.Lock()
	p.lastPing = consensus.ConsensusNow()
	p.mu.Unlock()

	return p.SendMessage(ping)
}

// IsConnected returns true if the peer is connected
func (p *Peer) IsConnected() bool {
	return p.GetState() == PeerStateConnected
}

// GetRemoteAddr returns the remote address of the connection
func (p *Peer) GetRemoteAddr() net.Addr {
	if p.conn != nil {
		return p.conn.RemoteAddr()
	}
	return nil
}

// GetLocalAddr returns the local address of the connection
func (p *Peer) GetLocalAddr() net.Addr {
	if p.conn != nil {
		return p.conn.LocalAddr()
	}
	return nil
}

// UpdateActivity updates the peer's last activity time
func (p *Peer) UpdateActivity() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSeen = consensus.ConsensusNow()
}

package p2p

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"diamante/apperrors"
	"diamante/consensus"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

// Node represents a P2P network node that integrates all networking components
type Node struct {
	// Core components
	transport     *Transport
	protocol      *Protocol
	peerManager   *PeerManager
	peerScorer    *PeerScorer
	messageCache  *MessageCache
	cryptoManager *crypto.CryptoManager

	// Configuration
	config *NodeConfig

	// Node information
	nodeInfo *PeerInfo

	// Message handlers
	handlers map[MessageType]MessageHandler
	mu       sync.RWMutex

	// Lifecycle management
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	started   bool
	startedMu sync.RWMutex

	// Metrics and monitoring
	metrics *NodeMetrics
	logger  *logrus.Entry
}

// NodeConfig holds configuration for the P2P node
type NodeConfig struct {
	// Network configuration
	ListenAddress string
	ListenPort    uint16
	ExternalIP    string
	MaxPeers      int

	// Protocol settings
	ProtocolVersion uint8
	UserAgent       string
	Services        uint64

	// Transport settings
	EnableTLS     bool
	TLSCertFile   string
	TLSKeyFile    string
	CAFile        string
	EnableUPnP    bool
	NATPMPEnabled bool

	// Peer management
	PeerManager  *PeerManagerConfig
	MessageCache *MessageCacheConfig

	// Discovery settings
	BootstrapPeers    []string
	EnableDiscovery   bool
	DiscoveryInterval time.Duration

	// Timeouts and intervals
	HandshakeTimeout  time.Duration
	PingInterval      time.Duration
	ReconnectInterval time.Duration

	// Security
	RequireAuth bool
	BanDuration time.Duration
}

// DefaultNodeConfig returns default configuration
func DefaultNodeConfig() *NodeConfig {
	// Get listen address from environment variable with fallback
	listenAddr := os.Getenv("DIAMANTE_P2P_LISTEN_ADDRESS")
	if listenAddr == "" {
		listenAddr = "0.0.0.0"
	}

	// Get listen port from environment variable with fallback
	listenPort := uint16(8545)
	if portStr := os.Getenv("DIAMANTE_P2P_LISTEN_PORT"); portStr != "" {
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err == nil && port > 0 && port <= 65535 {
			listenPort = uint16(port)
		}
	}

	return &NodeConfig{
		ListenAddress:     listenAddr,
		ListenPort:        listenPort,
		MaxPeers:          100,
		ProtocolVersion:   1,
		UserAgent:         "Diamante/1.0.0",
		Services:          1, // Full node
		EnableTLS:         true,
		EnableUPnP:        true,
		NATPMPEnabled:     true,
		PeerManager:       DefaultPeerManagerConfig(),
		MessageCache:      DefaultMessageCacheConfig(),
		BootstrapPeers:    []string{},
		EnableDiscovery:   true,
		DiscoveryInterval: 30 * time.Second,
		HandshakeTimeout:  10 * time.Second,
		PingInterval:      30 * time.Second,
		ReconnectInterval: 5 * time.Minute,
		RequireAuth:       false,
		BanDuration:       24 * time.Hour,
	}
}

// NodeMetrics tracks P2P node metrics
type NodeMetrics struct {
	StartTime             time.Time
	MessagesReceived      uint64
	MessagesSent          uint64
	BytesReceived         uint64
	BytesSent             uint64
	ConnectionAttempts    uint64
	SuccessfulConnections uint64
	FailedConnections     uint64
	ActivePeers           uint64
	TotalPeers            uint64
	mu                    sync.RWMutex
}

// NewNode creates a new P2P network node
func NewNode(config *NodeConfig, cryptoManager *crypto.CryptoManager, logger *logrus.Logger) (*Node, error) {
	if config == nil {
		config = DefaultNodeConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	// Create context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	// Create transport
	transportConfig := &TransportConfig{
		ListenAddress: config.ListenAddress,
		Port:          config.ListenPort,
		EnableTLS:     config.EnableTLS,
		TLSCertFile:   config.TLSCertFile,
		TLSKeyFile:    config.TLSKeyFile,
	}

	// Create TLS config if enabled
	var tlsConfig *tls.Config
	if config.EnableTLS {
		// Create secure TLS configuration
		tlsConfig = &tls.Config{
			MinVersion:       tls.VersionTLS13,
			CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP384},
			CipherSuites: []uint16{
				tls.TLS_AES_256_GCM_SHA384,
				tls.TLS_CHACHA20_POLY1305_SHA256,
				tls.TLS_AES_128_GCM_SHA256,
			},
			ClientAuth: tls.RequireAndVerifyClientCert,
		}

		// Load certificates if provided
		if config.TLSCertFile != "" && config.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
			if err != nil {
				cancel()
				return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
					"failed to load TLS certificates")
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Load CA if provided for client verification
		if config.CAFile != "" {
			caCert, err := os.ReadFile(config.CAFile)
			if err != nil {
				cancel()
				return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
					"failed to read CA certificate")
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.ClientCAs = caCertPool
			tlsConfig.RootCAs = caCertPool
		}
	}

	transport := NewTransport(transportConfig, tlsConfig, logger)

	// Create protocol
	protocol := NewProtocol(logger, cryptoManager)

	// Create peer scorer with default config
	peerScorer := NewPeerScorer(DefaultScoringConfig(), logger)

	// Create message cache
	messageCache, err := NewMessageCache(config.MessageCache, logger)
	if err != nil {
		cancel()
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create message cache")
	}

	// Create peer manager
	peerManager := NewPeerManager(transport, protocol, peerScorer, config.PeerManager, logger)

	// Determine external IP
	externalIP := config.ExternalIP
	if externalIP == "" {
		if detectedIP, err := detectExternalIP(); err == nil {
			externalIP = detectedIP
		} else {
			// Use environment variable fallback before using localhost
			externalIP = os.Getenv("DIAMANTE_P2P_EXTERNAL_IP")
			if externalIP == "" {
				externalIP = "127.0.0.1" // Final fallback
			}
		}
	}

	// Create node info
	nodeInfo := &PeerInfo{
		ID:          generateNodeID(),
		Address:     externalIP,
		Port:        config.ListenPort,
		Services:    config.Services,
		UserAgent:   config.UserAgent,
		BlockHeight: 0, // Will be updated by blockchain component
	}

	node := &Node{
		transport:     transport,
		protocol:      protocol,
		peerManager:   peerManager,
		peerScorer:    peerScorer,
		messageCache:  messageCache,
		cryptoManager: cryptoManager,
		config:        config,
		nodeInfo:      nodeInfo,
		handlers:      make(map[MessageType]MessageHandler),
		ctx:           ctx,
		cancel:        cancel,
		metrics:       &NodeMetrics{StartTime: consensus.ConsensusNow()},
		logger:        logger.WithField("component", "p2p_node"),
	}

	// Register default handlers
	node.registerDefaultHandlers()

	// Set peer manager callbacks
	peerManager.SetCallbacks(
		node.onPeerConnected,
		node.onPeerDisconnected,
		node.onPeerBanned,
	)

	return node, nil
}

// Start initializes and starts the P2P node
func (n *Node) Start() error {
	n.startedMu.Lock()
	defer n.startedMu.Unlock()

	if n.started {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"node already started")
	}

	n.logger.Info("Starting P2P node", "address", n.config.ListenAddress, "port", n.config.ListenPort)

	// Start message cache
	n.messageCache.Start()

	// Start peer manager
	if err := n.peerManager.Start(); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to start peer manager")
	}

	// Start background routines
	n.wg.Add(3)
	go n.discoveryLoop()
	go n.metricsLoop()
	go n.maintenanceLoop()

	// Connect to bootstrap peers
	if len(n.config.BootstrapPeers) > 0 {
		n.wg.Add(1)
		go n.connectToBootstrapPeers()
	}

	n.started = true
	n.logger.Info("P2P node started successfully")

	return nil
}

// Stop gracefully shuts down the P2P node
func (n *Node) Stop() error {
	n.startedMu.Lock()
	defer n.startedMu.Unlock()

	if !n.started {
		return nil
	}

	n.logger.Info("Stopping P2P node")

	// Cancel context to stop all operations
	n.cancel()

	// Stop peer manager
	if err := n.peerManager.Stop(); err != nil {
		n.logger.WithError(err).Error("Failed to stop peer manager")
	}

	// Stop message cache
	n.messageCache.Stop()

	// Wait for all goroutines to finish
	n.wg.Wait()

	n.started = false
	n.logger.Info("P2P node stopped")

	return nil
}

// RegisterHandler registers a message handler for a specific message type
func (n *Node) RegisterHandler(msgType MessageType, handler MessageHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.handlers[msgType] = handler
	n.protocol.RegisterHandler(msgType, handler)

	n.logger.Debug("Registered message handler", "type", msgType)
}

// UnregisterHandler removes a message handler
func (n *Node) UnregisterHandler(msgType MessageType) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.handlers, msgType)
	n.protocol.UnregisterHandler(msgType)

	n.logger.Debug("Unregistered message handler", "type", msgType)
}

// BroadcastMessage sends a message to all connected peers
func (n *Node) BroadcastMessage(msgType MessageType, payload []byte, excludePeers ...string) error {
	// Create message
	msg, err := n.protocol.CreateMessage(msgType, payload)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create message")
	}

	// Check if already seen
	if n.messageCache.Has(n.generateMessageID(msg)) {
		return nil // Don't rebroadcast
	}

	// Add to cache
	n.messageCache.Add(msg, n.nodeInfo.ID)

	// Broadcast to peers
	err = n.peerManager.BroadcastMessage(msg, excludePeers...)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to broadcast message")
	}

	// Update metrics
	n.updateSentMetrics(msg)

	return nil
}

// SendMessage sends a message to a specific peer
func (n *Node) SendMessage(peerID string, msgType MessageType, payload []byte) error {
	// Create message
	msg, err := n.protocol.CreateMessage(msgType, payload)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create message")
	}

	// Send to peer
	err = n.peerManager.SendMessage(peerID, msg)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to send message")
	}

	// Update metrics
	n.updateSentMetrics(msg)

	return nil
}

// ConnectToPeer establishes a connection to a specific peer
func (n *Node) ConnectToPeer(address string) error {
	ctx, cancel := context.WithTimeout(n.ctx, n.config.HandshakeTimeout)
	defer cancel()

	return n.peerManager.Connect(ctx, address)
}

// DisconnectPeer disconnects from a specific peer
func (n *Node) DisconnectPeer(peerID string, reason string) error {
	return n.peerManager.Disconnect(peerID, reason)
}

// GetPeers returns information about all connected peers
func (n *Node) GetPeers() []*PeerConnection {
	return n.peerManager.GetPeers()
}

// GetPeerCount returns the number of connected peers
func (n *Node) GetPeerCount() (total, inbound, outbound int) {
	return n.peerManager.GetPeerCount()
}

// BanPeer bans a peer for the configured duration
func (n *Node) BanPeer(peerID string, reason string) {
	n.peerManager.BanPeer(peerID, n.config.BanDuration, reason)
}

// GetNodeInfo returns information about this node
func (n *Node) GetNodeInfo() *PeerInfo {
	return n.nodeInfo
}

// UpdateBlockHeight updates the node's current block height
func (n *Node) UpdateBlockHeight(height uint64) {
	n.nodeInfo.BlockHeight = height
}

// GetMetrics returns current node metrics
func (n *Node) GetMetrics() *NodeMetrics {
	n.metrics.mu.RLock()
	defer n.metrics.mu.RUnlock()

	// Return a copy
	return &NodeMetrics{
		StartTime:             n.metrics.StartTime,
		MessagesReceived:      n.metrics.MessagesReceived,
		MessagesSent:          n.metrics.MessagesSent,
		BytesReceived:         n.metrics.BytesReceived,
		BytesSent:             n.metrics.BytesSent,
		ConnectionAttempts:    n.metrics.ConnectionAttempts,
		SuccessfulConnections: n.metrics.SuccessfulConnections,
		FailedConnections:     n.metrics.FailedConnections,
		ActivePeers:           n.metrics.ActivePeers,
		TotalPeers:            n.metrics.TotalPeers,
	}
}

// IsStarted returns whether the node is currently started
func (n *Node) IsStarted() bool {
	n.startedMu.RLock()
	defer n.startedMu.RUnlock()
	return n.started
}

// Private methods

func (n *Node) registerDefaultHandlers() {
	// Register ping/pong handlers
	n.protocol.RegisterHandler(MessageTypePing, MessageHandlerFunc(n.handlePing))
	n.protocol.RegisterHandler(MessageTypePong, MessageHandlerFunc(n.handlePong))
	n.protocol.RegisterHandler(MessageTypeHandshake, MessageHandlerFunc(n.handleHandshake))
	n.protocol.RegisterHandler(MessageTypeDisconnect, MessageHandlerFunc(n.handleDisconnect))
}

func (n *Node) handlePing(peer *Peer, msg *Message) error {
	// Create pong response
	pong, err := n.protocol.CreatePongMessage(msg.ID, msg.Timestamp)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create pong message")
	}

	// Send pong back to peer
	return peer.SendMessage(pong)
}

func (n *Node) handlePong(peer *Peer, msg *Message) error {
	// Calculate latency and update peer score
	// This is simplified - in a real implementation, we'd track ping timestamps
	n.logger.Debug("Received pong from peer", "peer_id", peer.ID)
	return nil
}

func (n *Node) handleHandshake(peer *Peer, msg *Message) error {
	// Handle handshake message
	n.logger.Debug("Received handshake from peer", "peer_id", peer.ID)

	// In a real implementation, we'd parse the handshake payload
	// and update peer information

	return nil
}

func (n *Node) handleDisconnect(peer *Peer, msg *Message) error {
	// Handle disconnect message
	n.logger.Debug("Received disconnect from peer", "peer_id", peer.ID)

	// Gracefully disconnect the peer
	n.peerManager.Disconnect(peer.ID, "peer requested disconnect")

	return nil
}

func (n *Node) onPeerConnected(peerConn *PeerConnection) {
	n.logger.Info("Peer connected", "peer_id", peerConn.Peer.ID, "address", peerConn.Peer.Address)

	// Update metrics
	n.metrics.mu.Lock()
	n.metrics.SuccessfulConnections++
	n.metrics.mu.Unlock()
}

func (n *Node) onPeerDisconnected(peerID string, err error) {
	n.logger.Info("Peer disconnected", "peer_id", peerID, "error", err)
}

func (n *Node) onPeerBanned(peerID string, duration time.Duration, reason string) {
	n.logger.Warn("Peer banned", "peer_id", peerID, "duration", duration, "reason", reason)
}

func (n *Node) discoveryLoop() {
	defer n.wg.Done()

	if !n.config.EnableDiscovery {
		return
	}

	ticker := time.NewTicker(n.config.DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.performPeerDiscovery()
		}
	}
}

func (n *Node) performPeerDiscovery() {
	// Get current peer count
	total, _, _ := n.peerManager.GetPeerCount()

	// Only discover new peers if we're below the maximum
	if total >= n.config.MaxPeers {
		return
	}

	// Request peer information from connected peers
	peerRequestMsg, err := n.protocol.CreateMessage(MessageTypePeerRequest, []byte{})
	if err != nil {
		n.logger.WithError(err).Error("Failed to create peer request message")
		return
	}

	// Send to a few random peers
	peers := n.peerManager.GetBestPeers(3)
	for _, peer := range peers {
		peer.Peer.SendMessage(peerRequestMsg)
	}
}

func (n *Node) metricsLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.updatePeerMetrics()
		}
	}
}

func (n *Node) updatePeerMetrics() {
	total, _, _ := n.peerManager.GetPeerCount()

	n.metrics.mu.Lock()
	n.metrics.ActivePeers = uint64(total)
	n.metrics.mu.Unlock()
}

func (n *Node) maintenanceLoop() {
	defer n.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.performMaintenance()
		}
	}
}

func (n *Node) performMaintenance() {
	// Clean up message cache
	n.messageCache.ForceCleanup()

	// Log current stats
	total, inbound, outbound := n.peerManager.GetPeerCount()
	n.logger.Info("Node maintenance",
		"total_peers", total,
		"inbound", inbound,
		"outbound", outbound,
		"cache_size", n.messageCache.GetSize(),
		"cache_hit_ratio", n.messageCache.GetHitRatio())
}

func (n *Node) connectToBootstrapPeers() {
	defer n.wg.Done()

	for _, address := range n.config.BootstrapPeers {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		n.logger.Info("Connecting to bootstrap peer", "address", address)

		if err := n.ConnectToPeer(address); err != nil {
			n.logger.WithError(err).Warn("Failed to connect to bootstrap peer", "address", address)
		}

		// Small delay between connections - use context-aware timing
		select {
		case <-time.After(1 * time.Second):
			// Connection delay completed
		case <-n.ctx.Done():
			// Context cancelled during delay
			return
		}
	}
}

func (n *Node) updateSentMetrics(msg *Message) {
	n.metrics.mu.Lock()
	defer n.metrics.mu.Unlock()

	n.metrics.MessagesSent++
	n.metrics.BytesSent += uint64(len(msg.Payload))
}

func (n *Node) generateMessageID(msg *Message) [32]byte {
	// This would generate a unique ID for message deduplication
	// For now, use the message's built-in ID
	var id [32]byte
	copy(id[:16], msg.ID[:])
	return id
}

// Helper functions

func generateNodeID() string {
	// Generate a unique node ID
	return fmt.Sprintf("node_%d", consensus.ConsensusUnixNano())
}

func detectExternalIP() (string, error) {
	// Try to detect external IP address
	// This is a simplified implementation
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

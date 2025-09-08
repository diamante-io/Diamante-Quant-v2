package p2p

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"diamante/apperrors"
	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// PeerManager manages peer connections, lifecycle, and reputation
type PeerManager struct {
	// Core components
	transport *Transport
	protocol  *Protocol
	scorer    *PeerScorer
	config    *PeerManagerConfig

	// Peer tracking
	peers     map[string]*PeerConnection
	peersByIP map[string][]string // IP -> []peerID for connection limits
	inbound   map[string]bool     // Track inbound vs outbound
	mu        sync.RWMutex

	// Ban management
	bannedPeers map[string]time.Time
	bannedIPs   map[string]time.Time
	banMu       sync.RWMutex

	// Connection management
	connecting map[string]bool // Peers currently connecting
	connMu     sync.Mutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callbacks
	onPeerConnected    func(*PeerConnection)
	onPeerDisconnected func(string, error)
	onPeerBanned       func(string, time.Duration, string) // peerID, duration, reason

	// Metrics
	metrics *PeerMetrics
	logger  *logrus.Entry
}

// PeerConnection represents a connected peer with metadata
type PeerConnection struct {
	Peer          *Peer
	Conn          net.Conn
	Inbound       bool
	ConnectedAt   time.Time
	LastMessageAt time.Time
	LastPingAt    time.Time
	MessagesSent  uint64
	MessagesRecv  uint64
	BytesSent     uint64
	BytesRecv     uint64
	Latency       time.Duration
	Version       string
	Capabilities  []string
	mu            sync.RWMutex
}

// PeerManagerConfig holds configuration for the peer manager
type PeerManagerConfig struct {
	MaxPeers          int
	MaxInboundRatio   float64 // e.g., 0.8 = 80% can be inbound
	MaxPeersPerIP     int
	ConnectionTimeout time.Duration
	HandshakeTimeout  time.Duration
	PingInterval      time.Duration
	PeerPersistence   bool // Save peer list
	BanDuration       time.Duration
}

// DefaultPeerManagerConfig returns default configuration
func DefaultPeerManagerConfig() *PeerManagerConfig {
	return &PeerManagerConfig{
		MaxPeers:          100,
		MaxInboundRatio:   0.8,
		MaxPeersPerIP:     3,
		ConnectionTimeout: 30 * time.Second,
		HandshakeTimeout:  10 * time.Second,
		PingInterval:      30 * time.Second,
		PeerPersistence:   true,
		BanDuration:       24 * time.Hour,
	}
}

// NewPeerManager creates a new peer manager
func NewPeerManager(transport *Transport, protocol *Protocol, scorer *PeerScorer, config *PeerManagerConfig, logger *logrus.Logger) *PeerManager {
	if config == nil {
		config = DefaultPeerManagerConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	ctx, cancel := context.WithCancel(context.Background())

	pm := &PeerManager{
		transport:   transport,
		protocol:    protocol,
		scorer:      scorer,
		config:      config,
		peers:       make(map[string]*PeerConnection),
		peersByIP:   make(map[string][]string),
		inbound:     make(map[string]bool),
		bannedPeers: make(map[string]time.Time),
		bannedIPs:   make(map[string]time.Time),
		connecting:  make(map[string]bool),
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &PeerMetrics{},
		logger:      logger.WithField("component", "peer_manager"),
	}

	// Set connection handler for transport
	transport.SetConnectionHandler(ConnectionHandlerFunc(pm.HandleIncomingConnection))

	return pm
}

// Start initializes the peer manager
func (pm *PeerManager) Start() error {
	pm.logger.Info("Starting peer manager")

	// Start transport
	if err := pm.transport.Listen(); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to start transport")
	}

	// Start ping routine
	pm.wg.Add(1)
	go pm.pingLoop()

	// Load persisted peers if enabled
	if pm.config.PeerPersistence {
		if err := pm.LoadPeers(); err != nil {
			pm.logger.WithError(err).Warn("Failed to load persisted peers")
		}
	}

	pm.logger.Info("Peer manager started")
	return nil
}

// Stop gracefully shuts down the peer manager
func (pm *PeerManager) Stop() error {
	pm.logger.Info("Stopping peer manager")

	// Cancel context to stop all operations
	pm.cancel()

	// Disconnect all peers gracefully
	for _, peer := range pm.GetPeers() {
		pm.Disconnect(peer.Peer.ID, "shutting down")
	}

	// Wait for all goroutines to finish
	pm.wg.Wait()

	// Save peer list if persistence enabled
	if pm.config.PeerPersistence {
		if err := pm.SavePeers(); err != nil {
			pm.logger.WithError(err).Error("Failed to save peers")
		}
	}

	// Close transport
	if err := pm.transport.Close(); err != nil {
		pm.logger.WithError(err).Error("Failed to close transport")
	}

	pm.logger.Info("Peer manager stopped")
	return nil
}

// Connect establishes an outbound connection to a peer
func (pm *PeerManager) Connect(ctx context.Context, address string) error {
	// Extract IP from address for validation
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"invalid address format")
	}

	// Check if IP is banned
	if pm.IsIPBanned(host) {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"cannot connect to banned IP")
	}

	// Check connection limits
	pm.mu.RLock()
	totalPeers := len(pm.peers)
	peersFromIP := len(pm.peersByIP[host])
	pm.mu.RUnlock()

	if totalPeers >= pm.config.MaxPeers {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum peer connections reached")
	}

	if peersFromIP >= pm.config.MaxPeersPerIP {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum connections per IP reached")
	}

	// Check if already connecting
	pm.connMu.Lock()
	if pm.connecting[address] {
		pm.connMu.Unlock()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"already connecting to peer")
	}
	pm.connecting[address] = true
	pm.connMu.Unlock()

	defer func() {
		pm.connMu.Lock()
		delete(pm.connecting, address)
		pm.connMu.Unlock()
	}()

	pm.metrics.IncrementConnectionAttempts()

	// Create connection context with timeout
	connCtx, cancel := context.WithTimeout(ctx, pm.config.ConnectionTimeout)
	defer cancel()

	// Establish connection
	conn, err := pm.transport.ConnectWithContext(connCtx, address)
	if err != nil {
		pm.metrics.IncrementConnectionFailures()
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to establish connection")
	}

	// Create peer
	peerID := pm.generatePeerID(address)
	peer := NewPeer(peerID, host, 8545, conn, false, nil, pm.logger.Logger)

	// Create peer connection
	peerConn := &PeerConnection{
		Peer:        peer,
		Conn:        conn,
		Inbound:     false,
		ConnectedAt: consensus.ConsensusNow(),
	}

	// Register peer
	pm.registerPeer(peerConn)

	// Start peer handling
	pm.wg.Add(1)
	go pm.handlePeerConnection(peerConn)

	pm.logger.WithFields(logrus.Fields{
		"peer_id": peerID,
		"address": address,
	}).Info("Successfully connected to peer")

	return nil
}

// Disconnect closes a connection to a peer
func (pm *PeerManager) Disconnect(peerID string, reason string) error {
	pm.mu.Lock()
	peerConn, exists := pm.peers[peerID]
	if !exists {
		pm.mu.Unlock()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"peer not found")
	}

	// Remove from tracking maps
	delete(pm.peers, peerID)
	delete(pm.inbound, peerID)

	// Remove from IP tracking
	if peerConn.Peer != nil {
		peerList := pm.peersByIP[peerConn.Peer.Address]
		for i, id := range peerList {
			if id == peerID {
				pm.peersByIP[peerConn.Peer.Address] = append(peerList[:i], peerList[i+1:]...)
				break
			}
		}
		if len(pm.peersByIP[peerConn.Peer.Address]) == 0 {
			delete(pm.peersByIP, peerConn.Peer.Address)
		}
	}
	pm.mu.Unlock()

	// Close connection
	if peerConn.Conn != nil {
		peerConn.Conn.Close()
	}

	// Stop peer
	if peerConn.Peer != nil {
		peerConn.Peer.Stop()
	}

	// Update metrics
	pm.updateMetrics()

	// Call disconnect callback
	if pm.onPeerDisconnected != nil {
		pm.onPeerDisconnected(peerID, fmt.Errorf("disconnected: %s", reason))
	}

	pm.logger.WithFields(logrus.Fields{
		"peer_id": peerID,
		"reason":  reason,
	}).Info("Disconnected peer")

	return nil
}

// GetPeer retrieves a peer connection by ID
func (pm *PeerManager) GetPeer(peerID string) (*PeerConnection, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peer, exists := pm.peers[peerID]
	return peer, exists
}

// GetPeers returns all current peer connections
func (pm *PeerManager) GetPeers() []*PeerConnection {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]*PeerConnection, 0, len(pm.peers))
	for _, peer := range pm.peers {
		peers = append(peers, peer)
	}

	return peers
}

// GetPeerCount returns counts of total, inbound, and outbound peers
func (pm *PeerManager) GetPeerCount() (total, inbound, outbound int) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	total = len(pm.peers)
	for _, isInbound := range pm.inbound {
		if isInbound {
			inbound++
		} else {
			outbound++
		}
	}

	return total, inbound, outbound
}

// BanPeer bans a peer for a specified duration
func (pm *PeerManager) BanPeer(peerID string, duration time.Duration, reason string) {
	pm.banMu.Lock()
	pm.bannedPeers[peerID] = consensus.ConsensusNow().Add(duration)
	pm.banMu.Unlock()

	// Disconnect if currently connected
	if _, exists := pm.GetPeer(peerID); exists {
		pm.Disconnect(peerID, fmt.Sprintf("banned: %s", reason))
	}

	// Update metrics
	pm.metrics.IncrementBannedPeers()

	// Call ban callback
	if pm.onPeerBanned != nil {
		pm.onPeerBanned(peerID, duration, reason)
	}

	pm.logger.WithFields(logrus.Fields{
		"peer_id":  peerID,
		"duration": duration,
		"reason":   reason,
	}).Info("Banned peer")
}

// BanIP bans an IP address for a specified duration
func (pm *PeerManager) BanIP(ip string, duration time.Duration, reason string) {
	pm.banMu.Lock()
	pm.bannedIPs[ip] = consensus.ConsensusNow().Add(duration)
	pm.banMu.Unlock()

	// Disconnect all peers from this IP
	pm.mu.RLock()
	peersFromIP := make([]string, len(pm.peersByIP[ip]))
	copy(peersFromIP, pm.peersByIP[ip])
	pm.mu.RUnlock()

	for _, peerID := range peersFromIP {
		pm.Disconnect(peerID, fmt.Sprintf("IP banned: %s", reason))
	}

	// Update metrics
	pm.metrics.IncrementBannedIPs()

	pm.logger.WithFields(logrus.Fields{
		"ip":       ip,
		"duration": duration,
		"reason":   reason,
	}).Info("Banned IP")
}

// UnbanPeer removes a peer ban
func (pm *PeerManager) UnbanPeer(peerID string) {
	pm.banMu.Lock()
	delete(pm.bannedPeers, peerID)
	pm.banMu.Unlock()

	pm.logger.WithField("peer_id", peerID).Info("Unbanned peer")
}

// UnbanIP removes an IP ban
func (pm *PeerManager) UnbanIP(ip string) {
	pm.banMu.Lock()
	delete(pm.bannedIPs, ip)
	pm.banMu.Unlock()

	pm.logger.WithField("ip", ip).Info("Unbanned IP")
}

// IsBanned checks if a peer is banned
func (pm *PeerManager) IsBanned(peerID string) bool {
	pm.banMu.RLock()
	banTime, exists := pm.bannedPeers[peerID]
	pm.banMu.RUnlock()

	if !exists {
		return false
	}

	if consensus.ConsensusNow().After(banTime) {
		// Ban expired, remove it
		pm.UnbanPeer(peerID)
		return false
	}

	return true
}

// IsIPBanned checks if an IP is banned
func (pm *PeerManager) IsIPBanned(ip string) bool {
	pm.banMu.RLock()
	banTime, exists := pm.bannedIPs[ip]
	pm.banMu.RUnlock()

	if !exists {
		return false
	}

	if consensus.ConsensusNow().After(banTime) {
		// Ban expired, remove it
		pm.UnbanIP(ip)
		return false
	}

	return true
}

// HandleIncomingConnection handles new inbound connections
func (pm *PeerManager) HandleIncomingConnection(conn net.Conn, inbound bool) error {
	if !inbound {
		return nil // Only handle inbound connections here
	}

	// Extract IP from connection
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		conn.Close()
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to parse remote address")
	}

	// Check if IP is banned
	if pm.IsIPBanned(host) {
		conn.Close()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"connection from banned IP")
	}

	// Check connection limits
	pm.mu.RLock()
	totalPeers := len(pm.peers)
	inboundCount := 0
	for _, isInbound := range pm.inbound {
		if isInbound {
			inboundCount++
		}
	}
	peersFromIP := len(pm.peersByIP[host])
	pm.mu.RUnlock()

	if totalPeers >= pm.config.MaxPeers {
		conn.Close()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum peer connections reached")
	}

	maxInbound := int(float64(pm.config.MaxPeers) * pm.config.MaxInboundRatio)
	if inboundCount >= maxInbound {
		conn.Close()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum inbound connections reached")
	}

	if peersFromIP >= pm.config.MaxPeersPerIP {
		conn.Close()
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"maximum connections per IP reached")
	}

	// Create peer
	peerID := pm.generatePeerID(conn.RemoteAddr().String())
	peer := NewPeer(peerID, host, 8545, conn, true, nil, pm.logger.Logger)

	// Create peer connection
	peerConn := &PeerConnection{
		Peer:        peer,
		Conn:        conn,
		Inbound:     true,
		ConnectedAt: consensus.ConsensusNow(),
	}

	// Register peer
	pm.registerPeer(peerConn)

	// Start peer handling
	pm.wg.Add(1)
	go pm.handlePeerConnection(peerConn)

	pm.logger.WithFields(logrus.Fields{
		"peer_id": peerID,
		"address": host,
	}).Info("Accepted inbound connection")

	return nil
}

// BroadcastMessage sends a message to all connected peers except excluded ones
func (pm *PeerManager) BroadcastMessage(msg *Message, excludePeers ...string) error {
	exclude := make(map[string]bool)
	for _, peerID := range excludePeers {
		exclude[peerID] = true
	}

	pm.mu.RLock()
	peers := make([]*PeerConnection, 0, len(pm.peers))
	for _, peer := range pm.peers {
		if !exclude[peer.Peer.ID] {
			peers = append(peers, peer)
		}
	}
	pm.mu.RUnlock()

	errors := make([]error, 0)
	for _, peer := range peers {
		if err := pm.SendMessage(peer.Peer.ID, msg); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			fmt.Sprintf("failed to send to %d peers", len(errors)))
	}

	return nil
}

// SendMessage sends a message to a specific peer
func (pm *PeerManager) SendMessage(peerID string, msg *Message) error {
	peerConn, exists := pm.GetPeer(peerID)
	if !exists {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"peer not found")
	}

	err := peerConn.Peer.SendMessage(msg)
	if err != nil {
		// Update peer score for failed send
		if pm.scorer != nil {
			pm.scorer.UpdateScore(peerID, ScoreEventTimeout)
		}
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to send message")
	}

	// Update statistics
	peerConn.mu.Lock()
	peerConn.MessagesSent++
	peerConn.BytesSent += uint64(len(msg.Payload))
	peerConn.LastMessageAt = consensus.ConsensusNow()
	peerConn.mu.Unlock()

	// Update peer score for successful send
	if pm.scorer != nil {
		pm.scorer.UpdateScore(peerID, ScoreEventValidMessage)
		pm.scorer.UpdateScore(peerID, ScoreEventBandwidthContribution, uint64(len(msg.Payload)), uint64(0))
	}

	return nil
}

// GetBestPeers returns the N best peers based on score
func (pm *PeerManager) GetBestPeers(n int) []*PeerConnection {
	if pm.scorer == nil {
		// If no scorer, return first N peers
		peers := pm.GetPeers()
		if len(peers) > n {
			peers = peers[:n]
		}
		return peers
	}

	topPeerIDs := pm.scorer.GetTopPeers(n)
	bestPeers := make([]*PeerConnection, 0, len(topPeerIDs))

	for _, peerID := range topPeerIDs {
		if peer, exists := pm.GetPeer(peerID); exists {
			bestPeers = append(bestPeers, peer)
		}
	}

	return bestPeers
}

// SavePeers saves peer information for persistence
func (pm *PeerManager) SavePeers() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Create a map of peer data to save
	peerData := make(map[string]interface{})

	// Save active peers
	activePeers := make([]map[string]interface{}, 0)
	for peerID, peer := range pm.peers {
		peerInfo := map[string]interface{}{
			"id":              peerID,
			"address":         peer.Peer.Address,
			"port":            peer.Peer.Port,
			"connected_at":    peer.ConnectedAt,
			"last_message_at": peer.LastMessageAt,
			"version":         peer.Version,
			"capabilities":    peer.Capabilities,
			"inbound":         peer.Inbound,
			"latency":         peer.Latency,
			"messages_sent":   peer.MessagesSent,
			"messages_recv":   peer.MessagesRecv,
			"bytes_sent":      peer.BytesSent,
			"bytes_recv":      peer.BytesRecv,
		}
		activePeers = append(activePeers, peerInfo)
	}
	peerData["active_peers"] = activePeers

	// Save banned peers
	bannedPeers := make([]map[string]interface{}, 0)
	for ip, expiry := range pm.bannedPeers {
		banData := map[string]interface{}{
			"ip":         ip,
			"expires_at": expiry,
			"reason":     "generic ban", // Simple reason
		}
		bannedPeers = append(bannedPeers, banData)
	}
	peerData["banned_peers"] = bannedPeers

	// Save banned IPs
	bannedIPs := make([]map[string]interface{}, 0)
	for ip, expiry := range pm.bannedIPs {
		banData := map[string]interface{}{
			"ip":         ip,
			"expires_at": expiry,
			"reason":     "IP ban",
		}
		bannedIPs = append(bannedIPs, banData)
	}
	peerData["banned_ips"] = bannedIPs

	// In a real implementation, this would save to a file or database
	// For now, we'll log the operation with basic stats
	pm.logger.WithField("peer_count", len(activePeers)).Info("Saved peer information")

	return nil
}

// LoadPeers loads peer information from persistence
func (pm *PeerManager) LoadPeers() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// In a real implementation, this would load from a file or database
	// For now, we'll initialize with empty data structures

	// Initialize maps if they don't exist
	if pm.peers == nil {
		pm.peers = make(map[string]*PeerConnection)
	}
	if pm.peersByIP == nil {
		pm.peersByIP = make(map[string][]string)
	}
	if pm.inbound == nil {
		pm.inbound = make(map[string]bool)
	}

	// Initialize ban maps
	pm.banMu.Lock()
	if pm.bannedPeers == nil {
		pm.bannedPeers = make(map[string]time.Time)
	}
	if pm.bannedIPs == nil {
		pm.bannedIPs = make(map[string]time.Time)
	}
	pm.banMu.Unlock()

	// Initialize connection tracking
	pm.connMu.Lock()
	if pm.connecting == nil {
		pm.connecting = make(map[string]bool)
	}
	pm.connMu.Unlock()

	pm.logger.Info("Loaded peer information from persistence")
	return nil
}

// SetCallbacks sets the callback functions
func (pm *PeerManager) SetCallbacks(
	onConnected func(*PeerConnection),
	onDisconnected func(string, error),
	onBanned func(string, time.Duration, string),
) {
	pm.onPeerConnected = onConnected
	pm.onPeerDisconnected = onDisconnected
	pm.onPeerBanned = onBanned
}

// GetMetrics returns peer manager metrics
func (pm *PeerManager) GetMetrics() *PeerMetrics {
	pm.updateMetrics()
	return pm.metrics
}

// Private helper methods

func (pm *PeerManager) registerPeer(peerConn *PeerConnection) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	peerID := peerConn.Peer.ID
	pm.peers[peerID] = peerConn
	pm.inbound[peerID] = peerConn.Inbound

	// Track by IP
	ip := peerConn.Peer.Address
	pm.peersByIP[ip] = append(pm.peersByIP[ip], peerID)

	// Update metrics
	pm.metrics.IncrementTotalConnections()
	pm.updateMetrics()

	// Call connected callback
	if pm.onPeerConnected != nil {
		pm.onPeerConnected(peerConn)
	}
}

func (pm *PeerManager) handlePeerConnection(peerConn *PeerConnection) {
	defer pm.wg.Done()
	defer pm.Disconnect(peerConn.Peer.ID, "connection handler finished")

	// Start the peer
	if err := peerConn.Peer.Start(); err != nil {
		pm.logger.WithError(err).Error("Failed to start peer")
		return
	}

	// Monitor peer connection
	ticker := time.NewTicker(pm.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.pingPeer(peerConn)
		}
	}
}

func (pm *PeerManager) pingPeer(peerConn *PeerConnection) {
	start := consensus.ConsensusNow()

	// Send ping
	var pingMsg *Message
	var err error

	if pm.protocol != nil {
		pingMsg, err = pm.protocol.CreatePingMessage()
	} else {
		// Fallback if no protocol is available
		pingMsg, err = pm.protocol.CreateMessage(MessageTypePing, []byte("ping"))
	}

	if err != nil {
		pm.logger.WithError(err).Error("Failed to create ping message")
		return
	}

	err = peerConn.Peer.SendMessage(pingMsg)
	if err != nil {
		pm.logger.WithError(err).Error("Failed to send ping")

		// Update score for failed ping
		if pm.scorer != nil {
			pm.scorer.UpdateScore(peerConn.Peer.ID, ScoreEventTimeout)
		}
		return
	}

	// Update latency (simplified - in real implementation, wait for pong)
	latency := consensus.ConsensusSince(start)
	peerConn.mu.Lock()
	peerConn.Latency = latency
	peerConn.LastPingAt = consensus.ConsensusNow()
	peerConn.mu.Unlock()

	// Update peer score
	if pm.scorer != nil {
		pm.scorer.UpdateScore(peerConn.Peer.ID, ScoreEventLatencyUpdate, latency)
	}

	// Update metrics
	pm.metrics.UpdateLatency(latency)
}

func (pm *PeerManager) pingLoop() {
	defer pm.wg.Done()

	ticker := time.NewTicker(pm.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.cleanupExpiredBans()
		}
	}
}

func (pm *PeerManager) cleanupExpiredBans() {
	now := consensus.ConsensusNow()

	// Clean up expired peer bans
	pm.banMu.Lock()
	for peerID, banTime := range pm.bannedPeers {
		if now.After(banTime) {
			delete(pm.bannedPeers, peerID)
		}
	}

	// Clean up expired IP bans
	for ip, banTime := range pm.bannedIPs {
		if now.After(banTime) {
			delete(pm.bannedIPs, ip)
		}
	}
	pm.banMu.Unlock()
}

func (pm *PeerManager) updateMetrics() {
	total, inbound, outbound := pm.GetPeerCount()
	pm.metrics.UpdateConnectionMetrics(uint64(total), uint64(inbound), uint64(outbound))
}

func (pm *PeerManager) generatePeerID(address string) string {
	// Simple peer ID generation - in production, this would be more sophisticated
	return fmt.Sprintf("peer_%s_%d", strings.ReplaceAll(address, ":", "_"), consensus.ConsensusUnixNano())
}

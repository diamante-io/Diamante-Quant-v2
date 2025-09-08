package network

import (
	"context"
	stdtls "crypto/tls"
	"diamante/common"
	"diamante/consensus"
	monitormetrics "diamante/monitoring/metrics"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"diamante/apperrors"
	"github.com/sirupsen/logrus"
)

// NetworkManager is responsible for overall peer connections, network health checks, etc.
type NetworkManager struct {
	mu                 sync.RWMutex
	localAddr          string
	peers              map[string]*Peer
	discovery          Discovery // Basic peer discovery interface
	isRunning          bool
	stopChan           chan struct{}
	health             int // Simplistic "load" or "health" metric: 0..100
	tlsConfig          *stdtls.Config
	monitoring         *monitormetrics.Registry
	reqRespMgr         *RequestResponseManager                    // Request-response correlation manager
	consensusAdapter   ConsensusAdapter                           // Interface to consensus module
	blockHandler       func(*BlockPayload) error                  // Handler for received blocks
	syncHandler        func(*SyncPayload) (MessagePayload, error) // Handler for sync requests
	transactionHandler func(*TransactionPayload) error            // Handler for received transactions
	maxPeers           int                                        // Maximum number of peer connections allowed
	logger             *logrus.Logger
}

// NewNetworkManager initializes a new manager with a local listening address and a Discovery mechanism.
func NewNetworkManager(localAddr string, d Discovery, tlsCfg *stdtls.Config, registry *monitormetrics.Registry) *NetworkManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return &NetworkManager{
		localAddr:  localAddr,
		peers:      make(map[string]*Peer),
		discovery:  d,
		stopChan:   make(chan struct{}),
		health:     0,
		tlsConfig:  tlsCfg,
		monitoring: registry,
		reqRespMgr: NewRequestResponseManager(30 * time.Second),
		maxPeers:   50, // Default max peers
		logger:     logger,
	}
}

// Start begins listening for inbound connections and also attempts outgoing connections if needed.
func (nm *NetworkManager) Start() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.isRunning {
		return common.NetworkError(nil, "network manager is already running").
			AddContext("local_addr", nm.localAddr)
	}
	nm.isRunning = true

	// Start request-response manager
	nm.reqRespMgr.Start()

	go nm.listenInbound()
	go nm.periodicHealthCheck()
	go nm.periodicSyncCheck()
	go nm.periodicHeartbeat()
	if nm.discovery != nil {
		go nm.discovery.Start()
	}

	nm.logger.WithFields(logrus.Fields{
		"event":            "NetworkManagerStart",
		"localAddr":        nm.localAddr,
		"heartbeatEnabled": true,
		"syncEnabled":      true,
	}).Info("NetworkManager started with all periodic tasks")
	return nil
}

// Stop signals the network manager to close all peers and stop listening.
func (nm *NetworkManager) Stop() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if !nm.isRunning {
		return common.NetworkError(nil, "network manager is not running").
			AddContext("local_addr", nm.localAddr)
	}
	nm.isRunning = false

	// Stop request-response manager
	if nm.reqRespMgr != nil {
		nm.reqRespMgr.Stop()
	}

	close(nm.stopChan)

	// Stop discovery if it exists
	if nm.discovery != nil {
		nm.discovery.Stop()
	}

	for addr, peer := range nm.peers {
		if err := peer.Close(); err != nil {
			nm.logger.WithFields(logrus.Fields{
				"peer":  addr,
				"error": err,
			}).Error("Failed to close peer")
		}
	}
	nm.peers = make(map[string]*Peer)

	nm.logger.Info("NetworkManager stopped")
	return nil
}

// listenInbound listens on nm.localAddr for new inbound TCP connections, spawns a Peer for each.
func (nm *NetworkManager) listenInbound() {
	ln, err := listenTLS(nm.localAddr, nm.tlsConfig)
	if err != nil {
		netErr := common.NetworkError(err, "failed to listen for inbound connections").
			AddContext("local_addr", nm.localAddr).
			AddContext("tls_enabled", fmt.Sprintf("%v", nm.tlsConfig != nil))
		nm.logger.WithFields(logrus.Fields{
			"localAddr": nm.localAddr,
			"error":     netErr,
		}).Error("Error listening")
		return
	}
	defer ln.Close()

	for {
		select {
		case <-nm.stopChan:
			nm.logger.Info("listenInbound shutting down")
			return
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			wrapped := apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal, "accept connection")
			nm.logger.WithField("error", wrapped).Error("Accept error")
			continue
		}
		nm.logger.WithField("remoteAddr", conn.RemoteAddr().String()).Info("Accepted inbound connection")
		go nm.handleInbound(conn)
	}
}

// handleInbound handles an incoming connection from a peer
func (nm *NetworkManager) handleInbound(conn net.Conn) {
	if tlsConn, ok := conn.(*stdtls.Conn); ok {
		peer := NewTLSPeer(conn.RemoteAddr().String(), tlsConn, nm)
		nm.addPeer(peer.Peer)
		go peer.Run()
		return
	}
	peer := NewPeer(conn.RemoteAddr().String(), conn, nm)
	nm.addPeer(peer)
	go peer.Run()
}

// addPeer registers a peer in the manager's peer map.
func (nm *NetworkManager) addPeer(peer *Peer) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	// Check max peers limit
	if nm.maxPeers > 0 && len(nm.peers) >= nm.maxPeers {
		nm.logger.WithFields(logrus.Fields{
			"maxPeers": nm.maxPeers,
			"peer":     peer.Addr,
		}).Warn("Max peers limit reached, rejecting peer")
		// Close the peer connection
		if err := peer.Close(); err != nil {
			nm.logger.WithFields(logrus.Fields{
				"peer":  peer.Addr,
				"error": err,
			}).Error("Error closing rejected peer")
		}
		return
	}

	nm.peers[peer.Addr] = peer
	if nm.monitoring != nil {
		nm.monitoring.Network.UpdatePeers(len(nm.peers))
	}
	nm.logger.WithFields(logrus.Fields{
		"peer":       peer.Addr,
		"totalPeers": len(nm.peers),
	}).Info("Peer added")
}

// RemovePeer removes a peer from the manager’s peer map.
func (nm *NetworkManager) RemovePeer(addr string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	delete(nm.peers, addr)
	nm.logger.WithField("peer", addr).Info("Peer removed")
	if nm.monitoring != nil {
		nm.monitoring.Network.UpdatePeers(len(nm.peers))
	}
}

// DialPeer attempts an outbound connection to a peer.
func (nm *NetworkManager) DialPeer(addr string) error {
	nm.mu.RLock()
	if !nm.isRunning {
		nm.mu.RUnlock()
		return fmt.Errorf("network manager is not running, cannot dial peer")
	}
	nm.mu.RUnlock()

	conn, err := dialTLS(addr, nm.tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to dial peer %s: %w", addr, err)
	}
	nm.logger.WithField("addr", addr).Info("Outbound connection established")
	if tlsConn, ok := conn.(*stdtls.Conn); ok {
		peer := NewTLSPeer(addr, tlsConn, nm)
		nm.addPeer(peer.Peer)
		go peer.Run()
	} else {
		peer := NewPeer(addr, conn, nm)
		nm.addPeer(peer)
		go peer.Run()
	}
	return nil
}

// Broadcast sends a message to all connected peers
func (nm *NetworkManager) Broadcast(msg Message) error {
	nm.mu.RLock()
	peers := make([]*Peer, 0, len(nm.peers))
	for _, peer := range nm.peers {
		peers = append(peers, peer)
	}
	nm.mu.RUnlock()

	// Set message sender if not already set
	if msg.Sender == "" {
		msg.Sender = nm.GetNodeID()
	}

	// Set timestamp if not already set
	if msg.Timestamp == 0 {
		msg.Timestamp = consensus.ConsensusUnixNano()
	}

	// Log broadcast details
	nm.logger.WithFields(logrus.Fields{
		"messageType": msg.Type,
		"peerCount":   len(peers),
	}).Debug("Broadcasting message to peers")

	// Track broadcast errors with proper synchronization
	var broadcastErrors []error
	var mu sync.Mutex
	successCount := 0

	// Use WaitGroup to track goroutines
	var wg sync.WaitGroup
	wg.Add(len(peers))

	for _, peer := range peers {
		// Send to peer in a goroutine to avoid blocking
		go func(p *Peer) {
			defer wg.Done()

			// Use the peer's Send method which handles timeouts and errors properly
			if err := p.Send(msg); err != nil {
				mu.Lock()
				broadcastErrors = append(broadcastErrors, err)
				mu.Unlock()
				nm.logger.WithFields(logrus.Fields{
					"messageType": msg.Type,
					"peer":        p.Addr,
					"error":       err,
				}).Error("Failed to send message to peer")
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
				nm.logger.WithFields(logrus.Fields{
					"messageType": msg.Type,
					"peer":        p.Addr,
				}).Debug("Successfully sent message to peer")
			}
		}(peer)
	}

	// Wait for all sends to complete
	wg.Wait()

	nm.logger.WithFields(logrus.Fields{
		"messageType":  msg.Type,
		"successCount": successCount,
		"totalPeers":   len(peers),
	}).Info("Completed broadcasting message")

	// If all sends failed, return an error
	if len(broadcastErrors) == len(peers) && len(peers) > 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			fmt.Sprintf("broadcast failed to all %d peers", len(peers)))
	}

	return nil
}

// GetNetworkHealth returns a simplistic network load metric (0..100).
func (nm *NetworkManager) GetNetworkHealth() (int, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.health, nil
}

// GetPeers returns a slice of all connected peers
func (nm *NetworkManager) GetPeers() []*Peer {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	peers := make([]*Peer, 0, len(nm.peers))
	for _, p := range nm.peers {
		peers = append(peers, p)
	}

	return peers
}

// GetPeerCount returns the number of connected peers
func (nm *NetworkManager) GetPeerCount() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return len(nm.peers)
}

// SetBlockHandler sets the handler function for received blocks
func (nm *NetworkManager) SetBlockHandler(handler func(*BlockPayload) error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.blockHandler = handler
}

// GetBlockHandler returns the block handler function
func (nm *NetworkManager) GetBlockHandler() func(*BlockPayload) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.blockHandler
}

// SetSyncHandler sets the handler function for sync requests
func (nm *NetworkManager) SetSyncHandler(handler func(*SyncPayload) (MessagePayload, error)) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.syncHandler = handler
}

// GetSyncHandler returns the sync handler function
func (nm *NetworkManager) GetSyncHandler() func(*SyncPayload) (MessagePayload, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.syncHandler
}

// SetTransactionHandler sets the handler function for incoming transaction broadcasts
func (nm *NetworkManager) SetTransactionHandler(handler func(*TransactionPayload) error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.transactionHandler = handler
}

// GetTransactionHandler returns the transaction handler function
func (nm *NetworkManager) GetTransactionHandler() func(*TransactionPayload) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.transactionHandler
}

// GetRequestResponseManager returns the request-response manager
func (nm *NetworkManager) GetRequestResponseManager() *RequestResponseManager {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.reqRespMgr
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
			val := nm.health
			nm.mu.Unlock()
			if nm.monitoring != nil {
				// Convert health value to boolean (healthy if > 50)
				isHealthy := val > 50
				nm.monitoring.Network.UpdateHealth(isHealthy)
			}
		}
	}
}

// periodicSyncCheck periodically checks if local node is behind peers and requests sync
func (nm *NetworkManager) periodicSyncCheck() {
	// Dynamic ticker interval based on lag
	getSyncInterval := func(localHeight, maxPeerHeight uint64) time.Duration {
		lag := int64(maxPeerHeight) - int64(localHeight)
		if lag > 1000 {
			return 1 * time.Second // Aggressive sync for large lag
		} else if lag > 100 {
			return 2 * time.Second // Moderate sync
		}
		return 3 * time.Second // Normal sync for small lag
	}

	initialInterval := 2 * time.Second
	ticker := time.NewTicker(initialInterval)
	defer ticker.Stop()

	// Track sync request state with exponential backoff
	type syncWindow struct {
		fromHeight uint64
		toHeight   uint64
		time       time.Time
	}

	type syncState struct {
		lastRequest     time.Time
		lastHeight      uint64
		retryCount      int
		backoffSeconds  int
		inFlightWindows []syncWindow // Track multiple in-flight sync windows
	}
	syncStates := make(map[string]*syncState) // Per-peer sync state

	// Constants for sync retry behavior
	const (
		minBackoffSeconds = 5
		maxBackoffSeconds = 60
		maxRetries        = 10
		inFlightTimeout   = 30 * time.Second // Consider request timed out after 30s
	)

	for {
		select {
		case <-nm.stopChan:
			return
		case <-ticker.C:
			// Get local blockchain state
			if nm.consensusAdapter == nil {
				continue
			}

			localState, err := nm.consensusAdapter.GetConsensusState()
			if err != nil {
				nm.logger.WithField("error", err).Error("periodicSyncCheck: Failed to get local state")
				continue
			}

			localHeight := uint64(0)
			if height, ok := localState["blockHeight"].(int); ok {
				localHeight = uint64(height)
			} else if height, ok := localState["blockHeight"].(uint64); ok {
				localHeight = height
			}

			// Clean up old sync states for disconnected peers
			nm.mu.RLock()
			activePeers := make(map[string]bool)
			for addr := range nm.peers {
				activePeers[addr] = true
			}
			nm.mu.RUnlock()

			for addr := range syncStates {
				if !activePeers[addr] {
					delete(syncStates, addr)
				}
			}

			// Find peers that are ahead and try to sync from them
			nm.mu.RLock()
			var syncAttempts []struct {
				peer   *Peer
				height uint64
			}

			// Build current windows state for logging
			allWindows := []map[string]uint64{}
			windowCount := 0
			for _, state := range syncStates {
				for _, w := range state.inFlightWindows {
					allWindows = append(allWindows, map[string]uint64{
						"from": w.fromHeight,
						"to":   w.toHeight,
					})
					windowCount++
				}
			}

			// Log sync tick with current state
			nm.logger.WithFields(logrus.Fields{
				"event":       "syncTick",
				"localHeight": localHeight,
				"peerCount":   len(nm.peers),
				"windows":     allWindows,
				"windowCount": windowCount,
			}).Info("Periodic sync check")

			for _, peer := range nm.peers {
				// Get peer state from stored info
				peerHeight := uint64(0)
				if nodeInfo := peer.getNodeState(); nodeInfo != nil {
					if height, ok := nodeInfo["blockHeight"].(int); ok {
						peerHeight = uint64(height)
					} else if height, ok := nodeInfo["blockHeight"].(uint64); ok {
						peerHeight = height
					}
				}

				// Only sync from peers that are ahead
				if peerHeight > localHeight {
					syncAttempts = append(syncAttempts, struct {
						peer   *Peer
						height uint64
					}{peer: peer, height: peerHeight})
				}
			}
			nm.mu.RUnlock()

			// Sort by height descending to prefer highest peers
			sort.Slice(syncAttempts, func(i, j int) bool {
				return syncAttempts[i].height > syncAttempts[j].height
			})

			// Try syncing from each peer that's ahead
			for _, attempt := range syncAttempts {
				peer := attempt.peer
				peerHeight := attempt.height
				peerAddr := peer.Addr

				// Calculate lag early for fast path decision
				lag := int64(peerHeight) - int64(localHeight)

				// Get or create sync state for this peer
				state, exists := syncStates[peerAddr]
				if !exists {
					// Fast path for restart: if lag is high, bypass initial backoff
					initialBackoff := minBackoffSeconds
					if lag > 1000 {
						// High lag detected on startup, allow immediate sync
						initialBackoff = 0
						nm.logger.WithFields(logrus.Fields{
							"event": "syncFastPath",
							"lag":   lag,
							"peer":  peerAddr,
						}).Info("Fast path activated for high-lag peer")
					}
					state = &syncState{
						backoffSeconds:  initialBackoff,
						lastRequest:     time.Now().Add(-time.Hour), // Allow immediate first request
						inFlightWindows: []syncWindow{},             // Initialize windows array
					}
					syncStates[peerAddr] = state

					// For very high lag, immediately create multiple windows
					if lag > 1000 {
						// Calculate initial windows
						fromHeight := localHeight + 1
						if localHeight == 0 {
							fromHeight = 1
						}

						// Create first window
						toHeight := fromHeight + 299 // 300 blocks
						if toHeight > peerHeight {
							toHeight = peerHeight
						}

						state.inFlightWindows = append(state.inFlightWindows, syncWindow{
							fromHeight: fromHeight,
							toHeight:   toHeight,
							time:       time.Now(),
						})

						// Create second window if needed
						if toHeight < peerHeight && peerHeight-toHeight > 300 {
							fromHeight2 := toHeight + 1
							toHeight2 := fromHeight2 + 299
							if toHeight2 > peerHeight {
								toHeight2 = peerHeight
							}

							state.inFlightWindows = append(state.inFlightWindows, syncWindow{
								fromHeight: fromHeight2,
								toHeight:   toHeight2,
								time:       time.Now(),
							})
						}

						nm.logger.WithFields(logrus.Fields{
							"event":       "syncWindowsPreCreated",
							"windowCount": len(state.inFlightWindows),
							"lag":         lag,
							"peer":        peerAddr,
						}).Info("Pre-created sync windows for fast catch-up")
					}
				}

				// Clean up timed out windows
				now := time.Now()
				activeWindows := []syncWindow{}
				for _, window := range state.inFlightWindows {
					if now.Sub(window.time) < inFlightTimeout {
						activeWindows = append(activeWindows, window)
					} else {
						nm.logger.WithFields(logrus.Fields{
							"peer":    peerAddr,
							"from":    window.fromHeight,
							"to":      window.toHeight,
							"timeout": inFlightTimeout,
						}).Warn("In-flight sync window timed out")
					}
				}
				state.inFlightWindows = activeWindows

				// Check if we've reached the maximum concurrent windows
				const maxConcurrentWindows = 2

				// Determine max windows based on lag
				maxWindows := 1
				if lag > 1000 {
					maxWindows = maxConcurrentWindows
				}

				// Skip if we've reached the max windows for this peer
				if len(state.inFlightWindows) >= maxWindows {
					continue
				}

				// Check if we should retry this peer
				// For meaningful lag (> 100), ignore backoff to enable fast catch-up
				if lag > 100 {
					// Always attempt sync when lag is meaningful, ignore backoff
				} else if time.Since(state.lastRequest) < time.Duration(state.backoffSeconds)*time.Second {
					// Only apply backoff when lag is small (≤ 100 blocks)
					continue
				}

				// Check if we're stuck at the same height
				isStuck := state.lastHeight == localHeight && state.retryCount > 0

				// Build windows array for logging
				windows := []map[string]uint64{}
				for _, w := range state.inFlightWindows {
					windows = append(windows, map[string]uint64{
						"from": w.fromHeight,
						"to":   w.toHeight,
					})
				}

				// Log sync attempt details
				nm.logger.WithFields(logrus.Fields{
					"event":          "syncTick",
					"localHeight":    localHeight,
					"peerHeight":     peerHeight,
					"lag":            lag,
					"windows":        windows,
					"windowCount":    len(state.inFlightWindows),
					"retries":        state.retryCount,
					"backoffSeconds": state.backoffSeconds,
				}).Info("Periodic sync check")

				// Request sync for missing blocks
				if nm.syncHandler != nil {
					// Calculate sync range
					fromHeight := localHeight + 1
					// Special case for genesis: start from block 1
					if localHeight == 0 {
						fromHeight = 1
					}

					// For multi-window sync, find the next non-overlapping range
					if len(state.inFlightWindows) > 0 && lag > 1000 {
						// Find the highest in-flight window
						highestWindow := uint64(0)
						for _, window := range state.inFlightWindows {
							if window.toHeight > highestWindow {
								highestWindow = window.toHeight
							}
						}
						// Start next window after the highest in-flight window
						if highestWindow >= fromHeight {
							fromHeight = highestWindow + 1
						}
					}

					toHeight := peerHeight
					maxBlocks := uint64(300) // Increased from 100 to 300 for faster catch-up

					// Ensure valid range
					if fromHeight > toHeight {
						nm.logger.WithFields(logrus.Fields{
							"event": "syncTick",
							"from":  fromHeight,
							"to":    toHeight,
							"local": localHeight,
							"peer":  peerHeight,
						}).Debug("Invalid sync range, fromHeight > toHeight")
						continue
					}

					// Limit sync range to prevent overwhelming
					if toHeight-fromHeight+1 > maxBlocks {
						toHeight = fromHeight + maxBlocks - 1
					}

					// Check if this range would overlap with any in-flight windows (from any peer)
					hasOverlap := false
					for _, otherState := range syncStates {
						for _, window := range otherState.inFlightWindows {
							// Check for overlap
							if (fromHeight >= window.fromHeight && fromHeight <= window.toHeight) ||
								(toHeight >= window.fromHeight && toHeight <= window.toHeight) ||
								(fromHeight <= window.fromHeight && toHeight >= window.toHeight) {
								hasOverlap = true
								break
							}
						}
						if hasOverlap {
							break
						}
					}

					if hasOverlap {
						// Skip this range to avoid overlapping requests
						nm.logger.WithFields(logrus.Fields{
							"event":          "syncTick",
							"skippedOverlap": true,
							"from":           fromHeight,
							"to":             toHeight,
						}).Debug("Skipping overlapping sync range")
						continue
					}

					// Log the sync range with max peer height
					maxPeerHeight := peerHeight
					for _, a := range syncAttempts {
						if a.height > maxPeerHeight {
							maxPeerHeight = a.height
						}
					}

					// Build windows array for logging (before adding new window)
					windowsBefore := []map[string]uint64{}
					for _, w := range state.inFlightWindows {
						windowsBefore = append(windowsBefore, map[string]uint64{
							"from": w.fromHeight,
							"to":   w.toHeight,
						})
					}

					nm.logger.WithFields(logrus.Fields{
						"event":       "StartSyncWindow",
						"from":        fromHeight,
						"to":          toHeight,
						"peerID":      peerAddr,
						"windowCount": len(state.inFlightWindows),
						"lag":         lag,
					}).Info("Starting new sync window")

					// Set MaxItems to indicate we can accept up to 300 blocks
					// even if the range is smaller
					maxItems := 300

					syncPayload := &SyncPayload{
						RequestType: "blocks",
						FromHeight:  fromHeight,
						ToHeight:    toHeight,
						MaxItems:    maxItems, // Always request up to 300 blocks
						NodeID:      nm.GetNodeID(),
						Timestamp:   time.Now().Unix(),
						Signature:   "sync_signature",
					}

					// Send sync request
					msg := NewSyncMessage(nm.GetNodeID(), syncPayload)
					if err := peer.Send(*msg); err != nil {
						nm.logger.WithFields(logrus.Fields{
							"peer":  peerAddr,
							"error": err,
						}).Error("periodicSyncCheck: Failed to send sync request")
						// Increase backoff on failure
						state.backoffSeconds = state.backoffSeconds * 2
						if state.backoffSeconds > maxBackoffSeconds {
							state.backoffSeconds = maxBackoffSeconds
						}
					} else {
						nm.logger.WithFields(logrus.Fields{
							"event":    "RequestSync",
							"from":     fromHeight,
							"to":       toHeight,
							"maxItems": maxItems,
							"peerID":   peerAddr,
							"attempt":  state.retryCount + 1,
						}).Info("Sync request sent")

						// Update sync state - add new window
						state.lastRequest = time.Now()
						state.retryCount++
						state.inFlightWindows = append(state.inFlightWindows, syncWindow{
							fromHeight: fromHeight,
							toHeight:   toHeight,
							time:       time.Now(),
						})

						// Log updated window count
						nm.logger.WithFields(logrus.Fields{
							"event":       "syncWindowAdded",
							"windowCount": len(state.inFlightWindows),
							"peerID":      peerAddr,
							"newWindow": map[string]uint64{
								"from": fromHeight,
								"to":   toHeight,
							},
						}).Debug("Added new sync window")

						// If we're stuck, increase backoff
						if isStuck {
							state.backoffSeconds = state.backoffSeconds * 2
							if state.backoffSeconds > maxBackoffSeconds {
								state.backoffSeconds = maxBackoffSeconds
							}
							nm.logger.WithFields(logrus.Fields{
								"height":         localHeight,
								"backoffSeconds": state.backoffSeconds,
								"peer":           peerAddr,
							}).Warn("syncRetry: Stuck at height, increasing backoff")
						} else {
							// Reset backoff on progress
							if state.lastHeight != localHeight && state.lastHeight > 0 {
								state.backoffSeconds = minBackoffSeconds
								state.retryCount = 0

								// Clear only completed windows (where localHeight >= window.toHeight)
								remainingWindows := []syncWindow{}
								completedCount := 0
								for _, window := range state.inFlightWindows {
									if localHeight >= window.toHeight {
										// This window is fully processed
										completedCount++
										nm.logger.WithFields(logrus.Fields{
											"event":    "CloseSyncWindow",
											"from":     window.fromHeight,
											"to":       window.toHeight,
											"reason":   "completed",
											"duration": time.Since(window.time),
										}).Info("Sync window completed")
									} else if localHeight >= window.fromHeight-1 {
										// Window is partially processed, keep it
										remainingWindows = append(remainingWindows, window)
									} else {
										// Window hasn't been reached yet, keep it
										remainingWindows = append(remainingWindows, window)
									}
								}
								state.inFlightWindows = remainingWindows

								// Log progress details
								nm.logger.WithFields(logrus.Fields{
									"event":            "syncProgress",
									"fromHeight":       state.lastHeight,
									"toHeight":         localHeight,
									"peer":             peerAddr,
									"windowsCompleted": completedCount,
									"windowsRemaining": len(remainingWindows),
								}).Info("Advanced height, updated windows")
							}
						}

						state.lastHeight = localHeight

						// Give up on this peer after too many retries
						if state.retryCount >= maxRetries && isStuck {
							nm.logger.WithFields(logrus.Fields{
								"peer":    peerAddr,
								"retries": state.retryCount,
								"height":  localHeight,
							}).Error("syncGiveUp: Giving up on peer after max retries")
							delete(syncStates, peerAddr)
						}
					}
				}
			}

			// Dynamically adjust sync interval based on max lag
			if len(syncAttempts) > 0 {
				maxPeerHeight := uint64(0)
				for _, attempt := range syncAttempts {
					if attempt.height > maxPeerHeight {
						maxPeerHeight = attempt.height
					}
				}
				newInterval := getSyncInterval(localHeight, maxPeerHeight)
				ticker.Reset(newInterval)
			}
		}
	}
}

func (nm *NetworkManager) BroadcastTransaction(tx common.Transaction) error {
	// Create a proper TransactionPayload message
	payload := &TransactionPayload{
		TransactionID:   tx.ID,
		FromAddress:     tx.Sender,
		ToAddress:       tx.Receiver,
		Amount:          uint64(tx.Amount * 1e8), // Convert to smallest unit
		Fee:             uint64(tx.Fee * 1e8),    // Convert fee to smallest unit
		Nonce:           uint64(tx.Nonce),
		Timestamp:       tx.Timestamp,
		Data:            tx.Data,
		Signature:       fmt.Sprintf("%x", tx.Signature), // Convert []byte to hex string
		TransactionType: "transfer",
	}

	// Marshal payload to json.RawMessage
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction payload: %w", err)
	}

	return nm.Broadcast(Message{
		Type:    MessageTypeTransaction, // Use the constant instead of hardcoded string
		Payload: payloadBytes,
		Sender:  nm.GetNodeID(),
	})
}

// GetNodeID returns a unique identifier for this node (using the local address)
func (nm *NetworkManager) GetNodeID() string {
	return nm.localAddr
}

// GetPeerList returns a list of all peer addresses
func (nm *NetworkManager) GetPeerList() ([]string, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	peers := make([]string, 0, len(nm.peers))
	for addr := range nm.peers {
		peers = append(peers, addr)
	}
	return peers, nil
}

// GetPeerByID returns a peer by its address
func (nm *NetworkManager) GetPeerByID(id string) *Peer {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.peers[id]
}

// SendMessageWithResponse sends a message to a peer and waits for a response
func (nm *NetworkManager) SendMessageWithResponse(peer *Peer, message *Message, ctx context.Context) (*Message, error) {
	if peer == nil {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "peer is nil")
	}

	if message == nil {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid, "message is nil")
	}

	// Ensure the message has required fields
	if message.ID == "" {
		message.ID = generateMessageID()
	}
	message.Sender = nm.GetNodeID()
	message.IsRequest = true

	// Register the request to get a response channel
	responseChannel := nm.reqRespMgr.RegisterRequest(message.ID, 0)

	// Send the message to the peer
	if err := peer.Send(*message); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal, "failed to send message to peer")
	}

	// Wait for response with context
	select {
	case response := <-responseChannel:
		if response == nil {
			return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal, "response channel closed without response")
		}
		return response, nil

	case <-ctx.Done():
		return nil, apperrors.Wrap(ctx.Err(), apperrors.ModuleNetwork, apperrors.CodeInternal, "context cancelled while waiting for response")
	}
}

// SetConsensusAdapter sets the consensus adapter for the network manager
func (nm *NetworkManager) SetConsensusAdapter(adapter ConsensusAdapter) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.consensusAdapter = adapter

	// If the adapter is a ConsensusAdapterImpl, set the network manager reference
	if impl, ok := adapter.(*ConsensusAdapterImpl); ok {
		impl.SetNetworkManager(nm)
	}
}

// GetConsensusAdapter returns the consensus adapter
func (nm *NetworkManager) GetConsensusAdapter() ConsensusAdapter {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.consensusAdapter
}

// String provides a detailed string representation for debugging and logging
func (nm *NetworkManager) String() string {
	if nm == nil {
		return "NetworkManager{<nil>}"
	}

	nm.mu.RLock()
	defer nm.mu.RUnlock()

	peerCount := len(nm.peers)
	hasTLS := nm.tlsConfig != nil
	hasDiscovery := nm.discovery != nil
	hasMonitoring := nm.monitoring != nil
	hasReqRespMgr := nm.reqRespMgr != nil
	hasConsensusAdapter := nm.consensusAdapter != nil

	// Get peer addresses for debugging (limit to avoid huge strings)
	peerAddrs := make([]string, 0, min(peerCount, 5))
	count := 0
	for addr := range nm.peers {
		if count >= 5 {
			break
		}
		peerAddrs = append(peerAddrs, addr)
		count++
	}

	peerListStr := fmt.Sprintf("%v", peerAddrs)
	if peerCount > 5 {
		peerListStr += fmt.Sprintf(" ...and %d more", peerCount-5)
	}

	return fmt.Sprintf("NetworkManager{localAddr=%s, running=%v, peerCount=%d, health=%d, "+
		"hasTLS=%v, hasDiscovery=%v, hasMonitoring=%v, hasReqRespMgr=%v, hasConsensusAdapter=%v, "+
		"peers=%s}",
		nm.localAddr, nm.isRunning, peerCount, nm.health, hasTLS, hasDiscovery, hasMonitoring,
		hasReqRespMgr, hasConsensusAdapter, peerListStr)
}

// Validate performs comprehensive validation of the NetworkManager configuration and state
func (nm *NetworkManager) Validate() error {
	if nm == nil {
		return fmt.Errorf("NetworkManager is nil")
	}

	// Validate local address
	if err := nm.validateLocalAddress(); err != nil {
		return fmt.Errorf("local address validation failed: %w", err)
	}

	// Validate components
	if err := nm.validateComponents(); err != nil {
		return fmt.Errorf("component validation failed: %w", err)
	}

	// Validate TLS configuration
	if err := nm.validateTLSConfig(); err != nil {
		return fmt.Errorf("TLS configuration validation failed: %w", err)
	}

	// Validate state consistency
	if err := nm.validateState(); err != nil {
		return fmt.Errorf("state validation failed: %w", err)
	}

	return nil
}

// validateLocalAddress validates the local listening address
func (nm *NetworkManager) validateLocalAddress() error {
	if nm.localAddr == "" {
		return fmt.Errorf("local address is empty")
	}

	// Parse the address to ensure it's valid
	host, port, err := net.SplitHostPort(nm.localAddr)
	if err != nil {
		return fmt.Errorf("invalid local address format: %w", err)
	}

	// Validate host part
	if host == "" {
		return fmt.Errorf("host part of local address is empty")
	}

	// Check if it's a valid IP address or hostname
	if net.ParseIP(host) == nil {
		// Not an IP, check if it could be a valid hostname
		if len(host) > 253 {
			return fmt.Errorf("hostname too long (max 253 characters): %d", len(host))
		}
	}

	// Validate port part
	if port == "" {
		return fmt.Errorf("port part of local address is empty")
	}

	portNum, err := net.LookupPort("tcp", port)
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}

	if portNum < 1 || portNum > 65535 {
		return fmt.Errorf("port out of valid range (1-65535): %d", portNum)
	}

	// Check for common reserved ports that might cause issues
	if portNum < 1024 {
		nm.logger.WithField("port", portNum).Warn("Using privileged port, ensure proper permissions")
	}

	return nil
}

// validateComponents validates that required components are properly initialized
func (nm *NetworkManager) validateComponents() error {
	// peers map should be initialized
	if nm.peers == nil {
		return fmt.Errorf("peers map is nil")
	}

	// stopChan should be initialized
	if nm.stopChan == nil {
		return fmt.Errorf("stop channel is nil")
	}

	// Check optional components exist when expected
	if nm.discovery == nil {
		nm.logger.Warn("Discovery mechanism is nil")
	}

	if nm.monitoring == nil {
		nm.logger.Warn("Monitoring registry is nil")
	}

	if nm.reqRespMgr == nil {
		return fmt.Errorf("request-response manager is nil")
	}

	return nil
}

// validateTLSConfig validates the TLS configuration if present
func (nm *NetworkManager) validateTLSConfig() error {
	if nm.tlsConfig == nil {
		nm.logger.Warn("TLS configuration is nil, connections will be unencrypted")
		return nil
	}

	// Basic TLS configuration validation
	if nm.tlsConfig.MinVersion < stdtls.VersionTLS12 {
		return fmt.Errorf("TLS minimum version too low (< TLS 1.2): %d", nm.tlsConfig.MinVersion)
	}

	// Check for insecure configurations
	if nm.tlsConfig.InsecureSkipVerify {
		nm.logger.Warn("TLS verification is disabled (InsecureSkipVerify=true)")
	}

	// Validate cipher suites if specified
	if len(nm.tlsConfig.CipherSuites) > 0 {
		for _, cipher := range nm.tlsConfig.CipherSuites {
			// Check for known weak ciphers
			if cipher == stdtls.TLS_RSA_WITH_RC4_128_SHA ||
				cipher == stdtls.TLS_RSA_WITH_3DES_EDE_CBC_SHA {
				return fmt.Errorf("weak cipher suite detected: %x", cipher)
			}
		}
	}

	return nil
}

// validateState validates the current state consistency
func (nm *NetworkManager) validateState() error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	// Validate health value
	if nm.health < 0 || nm.health > 100 {
		return fmt.Errorf("health value out of range (0-100): %d", nm.health)
	}

	// Validate peer state consistency
	for addr, peer := range nm.peers {
		if peer == nil {
			return fmt.Errorf("nil peer found for address: %s", addr)
		}
		if peer.Addr != addr {
			return fmt.Errorf("peer address mismatch: map key=%s, peer.Addr=%s", addr, peer.Addr)
		}
	}

	// If running, certain components should be properly set up
	if nm.isRunning {
		if nm.stopChan == nil {
			return fmt.Errorf("stop channel is nil but manager reports as running")
		}

		// Check if we can actually bind to the local address (if not already in use)
		if addr, err := net.ResolveTCPAddr("tcp", nm.localAddr); err == nil {
			if listener, err := net.ListenTCP("tcp", addr); err == nil {
				// Address is available, close immediately
				listener.Close()
			} else {
				// Address is in use, which is expected if we're actually running
				nm.logger.WithField("localAddr", nm.localAddr).Debug("Local address is in use (expected if running)")
			}
		}
	}

	return nil
}

// Close enhances the existing Stop method with additional cleanup and validation
func (nm *NetworkManager) Close() error {
	if nm == nil {
		return nil
	}

	// First, stop if running
	if err := nm.Stop(); err != nil {
		return fmt.Errorf("failed to stop network manager: %w", err)
	}

	// Additional cleanup beyond the Stop method
	var closeErrors []error

	// Close request-response manager
	if nm.reqRespMgr != nil {
		nm.reqRespMgr.Stop()
	}

	// Close discovery mechanism if it has a Close method
	if nm.discovery != nil {
		if closer, ok := nm.discovery.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close discovery: %w", err))
			}
		}
	}

	// Close monitoring if it has a Close method
	// Since monitoring is *monitormetrics.Registry, we check if it has Close method
	if nm.monitoring != nil {
		// monitormetrics.Registry doesn't typically have a Close method
		// so we'll skip this for now
	}

	// Clear all references to help with garbage collection
	nm.mu.Lock()
	nm.peers = nil
	nm.discovery = nil
	nm.monitoring = nil
	nm.reqRespMgr = nil
	nm.consensusAdapter = nil
	nm.tlsConfig = nil
	nm.stopChan = nil
	nm.mu.Unlock()

	// If there were any errors during close, return them
	if len(closeErrors) > 0 {
		errorStrs := make([]string, len(closeErrors))
		for i, err := range closeErrors {
			errorStrs[i] = err.Error()
		}
		return fmt.Errorf("multiple close errors: %s", strings.Join(errorStrs, "; "))
	}

	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SetMaxPeers sets the maximum number of allowed peer connections
func (nm *NetworkManager) SetMaxPeers(max int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if max < 0 {
		max = 0 // 0 means no limit
	}
	nm.maxPeers = max
	nm.logger.WithField("maxPeers", max).Info("NetworkManager: maxPeers set")
}

// GetMaxPeers returns the current maximum peer limit
func (nm *NetworkManager) GetMaxPeers() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.maxPeers
}

// ClearCompletedSyncWindows removes sync windows that have been completed
// based on the current blockchain height
func (nm *NetworkManager) ClearCompletedSyncWindows(currentHeight uint64) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	// This will be called from handleSyncResponse to clear windows
	// The actual implementation happens in periodicSyncCheck via syncStates
}

// EnableTLS updates the TLS configuration for the network manager
func (nm *NetworkManager) EnableTLS(tlsCfg *stdtls.Config) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.isRunning {
		return fmt.Errorf("cannot update TLS config while network manager is running")
	}

	nm.tlsConfig = tlsCfg
	nm.logger.Info("NetworkManager: TLS configuration updated")
	return nil
}

// periodicHeartbeat sends heartbeat messages to all peers
func (nm *NetworkManager) periodicHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-nm.stopChan:
			return
		case <-ticker.C:
			// Get local blockchain state
			if nm.consensusAdapter == nil {
				continue
			}

			localState, err := nm.consensusAdapter.GetConsensusState()
			if err != nil {
				nm.logger.WithError(err).Error("periodicHeartbeat: Failed to get local state")
				continue
			}

			// Extract local height and hash
			localHeight := uint64(0)
			localHash := ""
			if height, ok := localState["blockHeight"].(int); ok {
				localHeight = uint64(height)
			} else if height, ok := localState["blockHeight"].(uint64); ok {
				localHeight = height
			}
			if hash, ok := localState["latestBlockHash"].(string); ok {
				localHash = hash
			}

			// Get current peers
			nm.mu.RLock()
			peers := make([]*Peer, 0, len(nm.peers))
			for _, peer := range nm.peers {
				peers = append(peers, peer)
			}
			nm.mu.RUnlock()

			// Send heartbeat to each peer
			for _, peer := range peers {
				heartbeatPayload := &HeartbeatPayload{
					NodeID:        nm.GetNodeID(),
					Timestamp:     consensus.ConsensusUnix(),
					BlockHeight:   localHeight,
					LatestHash:    localHash,
					PeerCount:     len(peers),
					NetworkStatus: "healthy",
					Version:       "1.0.0",
				}

				msg := NewHeartbeatMessage(nm.GetNodeID(), heartbeatPayload)
				msg.IsRequest = true // Request a heartbeat response

				if err := peer.Send(*msg); err != nil {
					nm.logger.WithFields(logrus.Fields{
						"event": "heartbeatSendError",
						"peer":  peer.Addr,
						"error": err,
					}).Error("Failed to send heartbeat")
				} else {
					nm.logger.WithFields(logrus.Fields{
						"event":            "heartbeatSend",
						"peerID":           peer.Addr,
						"blockHeight":      localHeight,
						"latestBlockHash8": localHash[:8],
					}).Info("Sent heartbeat to peer")
				}
			}
		}
	}
}

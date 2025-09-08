package p2p

import (
	"sync"
	"time"

	"diamante/common"
)

// PeerMetricsSnapshot represents a point-in-time snapshot of peer metrics
type PeerMetricsSnapshot struct {
	TotalConnections    uint64 `json:"total_connections"`
	ActiveConnections   uint64 `json:"active_connections"`
	InboundConnections  uint64 `json:"inbound_connections"`
	OutboundConnections uint64 `json:"outbound_connections"`
	BannedPeers         uint64 `json:"banned_peers"`
	BannedIPs           uint64 `json:"banned_ips"`
	ConnectionAttempts  uint64 `json:"connection_attempts"`
	ConnectionFailures  uint64 `json:"connection_failures"`
	BytesSent           uint64 `json:"bytes_sent"`
	BytesReceived       uint64 `json:"bytes_received"`
	ReconnectAttempts   uint64 `json:"reconnect_attempts"`
	HandshakeFailures   uint64 `json:"handshake_failures"`
	MessageQueueSize    uint64 `json:"message_queue_size"`
	AverageLatencyMs    int64  `json:"average_latency_ms"`
	MaxLatencyMs        int64  `json:"max_latency_ms"`
	MinLatencyMs        int64  `json:"min_latency_ms"`
}

// GossipMetricsSnapshot represents a point-in-time snapshot of gossip metrics
type GossipMetricsSnapshot struct {
	MessagesSent      uint64             `json:"messages_sent"`
	MessagesReceived  uint64             `json:"messages_received"`
	MessagesDropped   uint64             `json:"messages_dropped"`
	MessagesDuplicate uint64             `json:"messages_duplicate"`
	BroadcastCount    uint64             `json:"broadcast_count"`
	AverageHops       float64            `json:"average_hops"`
	PropagationTimeMs int64              `json:"propagation_time_ms"`
	QueueUtilization  map[string]float64 `json:"queue_utilization"`
	HandlerErrors     uint64             `json:"handler_errors"`
	InvalidMessages   uint64             `json:"invalid_messages"`
	RateLimitDrops    uint64             `json:"rate_limit_drops"`
}

// CacheStatsSnapshot represents a point-in-time snapshot of cache statistics
type CacheStatsSnapshot struct {
	Hits           uint64  `json:"hits"`
	Misses         uint64  `json:"misses"`
	Evictions      uint64  `json:"evictions"`
	Size           int     `json:"size"`
	FalsePositives uint64  `json:"false_positives"`
	Inserts        uint64  `json:"inserts"`
	Deletes        uint64  `json:"deletes"`
	Cleanups       uint64  `json:"cleanups"`
	HitRatio       float64 `json:"hit_ratio"`
}

// DiscoveryMetricsSnapshot represents a point-in-time snapshot of discovery metrics
type DiscoveryMetricsSnapshot struct {
	BootstrapAttempts    uint64  `json:"bootstrap_attempts"`
	BootstrapSuccesses   uint64  `json:"bootstrap_successes"`
	BootstrapSuccessRate float64 `json:"bootstrap_success_rate"`
	DiscoveryQueries     uint64  `json:"discovery_queries"`
	PeersDiscovered      uint64  `json:"peers_discovered"`
	DiscoveryErrors      uint64  `json:"discovery_errors"`
	DHTLookups           uint64  `json:"dht_lookups"`
	DHTStores            uint64  `json:"dht_stores"`
	PeerExchanges        uint64  `json:"peer_exchanges"`
	RoutingTableUpdates  uint64  `json:"routing_table_updates"`
	ActiveDiscoveries    uint64  `json:"active_discoveries"`
	LastDiscoveryTime    int64   `json:"last_discovery_time"`
	AvgDiscoveryTimeMs   int64   `json:"avg_discovery_time_ms"`
}

// PeerMetrics tracks metrics for peer connections and operations
type PeerMetrics struct {
	mu                  sync.RWMutex
	TotalConnections    uint64
	ActiveConnections   uint64
	InboundConnections  uint64
	OutboundConnections uint64
	BannedPeers         uint64
	BannedIPs           uint64
	ConnectionAttempts  uint64
	ConnectionFailures  uint64
	BytesSent           uint64
	BytesReceived       uint64
	ReconnectAttempts   uint64
	HandshakeFailures   uint64
	MessageQueueSize    uint64
	AverageLatency      time.Duration
	MaxLatency          time.Duration
	MinLatency          time.Duration
}

// UpdateConnectionMetrics updates connection-related metrics
func (pm *PeerMetrics) UpdateConnectionMetrics(active, inbound, outbound uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.ActiveConnections = active
	pm.InboundConnections = inbound
	pm.OutboundConnections = outbound
}

// IncrementConnectionAttempts increments connection attempt counter
func (pm *PeerMetrics) IncrementConnectionAttempts() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ConnectionAttempts++
}

// IncrementConnectionFailures increments connection failure counter
func (pm *PeerMetrics) IncrementConnectionFailures() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ConnectionFailures++
}

// IncrementTotalConnections increments total connection counter
func (pm *PeerMetrics) IncrementTotalConnections() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.TotalConnections++
}

// IncrementBannedPeers increments banned peer counter
func (pm *PeerMetrics) IncrementBannedPeers() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.BannedPeers++
}

// IncrementBannedIPs increments banned IP counter
func (pm *PeerMetrics) IncrementBannedIPs() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.BannedIPs++
}

// UpdateBandwidth updates bandwidth metrics
func (pm *PeerMetrics) UpdateBandwidth(sent, received uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.BytesSent += sent
	pm.BytesReceived += received
}

// UpdateLatency updates latency metrics
func (pm *PeerMetrics) UpdateLatency(latency time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Update average latency using exponential moving average
	if pm.AverageLatency == 0 {
		pm.AverageLatency = latency
	} else {
		pm.AverageLatency = time.Duration(0.9*float64(pm.AverageLatency) + 0.1*float64(latency))
	}

	// Update min/max latency
	if pm.MinLatency == 0 || latency < pm.MinLatency {
		pm.MinLatency = latency
	}
	if latency > pm.MaxLatency {
		pm.MaxLatency = latency
	}
}

// GetSnapshot returns a snapshot of current metrics
func (pm *PeerMetrics) GetSnapshot() *PeerMetricsSnapshot {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return &PeerMetricsSnapshot{
		TotalConnections:    pm.TotalConnections,
		ActiveConnections:   pm.ActiveConnections,
		InboundConnections:  pm.InboundConnections,
		OutboundConnections: pm.OutboundConnections,
		BannedPeers:         pm.BannedPeers,
		BannedIPs:           pm.BannedIPs,
		ConnectionAttempts:  pm.ConnectionAttempts,
		ConnectionFailures:  pm.ConnectionFailures,
		BytesSent:           pm.BytesSent,
		BytesReceived:       pm.BytesReceived,
		ReconnectAttempts:   pm.ReconnectAttempts,
		HandshakeFailures:   pm.HandshakeFailures,
		MessageQueueSize:    pm.MessageQueueSize,
		AverageLatencyMs:    pm.AverageLatency.Milliseconds(),
		MaxLatencyMs:        pm.MaxLatency.Milliseconds(),
		MinLatencyMs:        pm.MinLatency.Milliseconds(),
	}
}

// GossipMetrics tracks metrics for gossip protocol operations
type GossipMetrics struct {
	mu                sync.RWMutex
	MessagesSent      uint64
	MessagesReceived  uint64
	MessagesDropped   uint64
	MessagesDuplicate uint64
	BroadcastCount    uint64
	AverageHops       float64
	PropagationTime   time.Duration
	QueueUtilization  map[Priority]float64
	HandlerErrors     uint64
	InvalidMessages   uint64
	RateLimitDrops    uint64
}

// IncrementMessagesSent increments sent message counter
func (gm *GossipMetrics) IncrementMessagesSent() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.MessagesSent++
}

// IncrementMessagesReceived increments received message counter
func (gm *GossipMetrics) IncrementMessagesReceived() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.MessagesReceived++
}

// IncrementMessagesDropped increments dropped message counter
func (gm *GossipMetrics) IncrementMessagesDropped() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.MessagesDropped++
}

// IncrementMessagesDuplicate increments duplicate message counter
func (gm *GossipMetrics) IncrementMessagesDuplicate() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.MessagesDuplicate++
}

// IncrementBroadcastCount increments broadcast counter
func (gm *GossipMetrics) IncrementBroadcastCount() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.BroadcastCount++
}

// UpdatePropagationMetrics updates propagation-related metrics
func (gm *GossipMetrics) UpdatePropagationMetrics(hops int, propagationTime time.Duration) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Update average hops using exponential moving average
	if gm.AverageHops == 0 {
		gm.AverageHops = float64(hops)
	} else {
		gm.AverageHops = 0.9*gm.AverageHops + 0.1*float64(hops)
	}

	// Update propagation time using exponential moving average
	if gm.PropagationTime == 0 {
		gm.PropagationTime = propagationTime
	} else {
		gm.PropagationTime = time.Duration(0.9*float64(gm.PropagationTime) + 0.1*float64(propagationTime))
	}
}

// UpdateQueueUtilization updates queue utilization metrics
func (gm *GossipMetrics) UpdateQueueUtilization(priority Priority, utilization float64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.QueueUtilization == nil {
		gm.QueueUtilization = make(map[Priority]float64)
	}
	gm.QueueUtilization[priority] = utilization
}

// IncrementHandlerErrors increments handler error counter
func (gm *GossipMetrics) IncrementHandlerErrors() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.HandlerErrors++
}

// IncrementInvalidMessages increments invalid message counter
func (gm *GossipMetrics) IncrementInvalidMessages() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.InvalidMessages++
}

// IncrementRateLimitDrops increments rate limit drop counter
func (gm *GossipMetrics) IncrementRateLimitDrops() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.RateLimitDrops++
}

// GetSnapshot returns a snapshot of current gossip metrics
func (gm *GossipMetrics) GetSnapshot() *GossipMetricsSnapshot {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	queueUtil := make(map[string]float64)
	for priority, util := range gm.QueueUtilization {
		switch priority {
		case PriorityHigh:
			queueUtil["high"] = util
		case PriorityNormal:
			queueUtil["normal"] = util
		case PriorityLow:
			queueUtil["low"] = util
		}
	}

	return &GossipMetricsSnapshot{
		MessagesSent:      gm.MessagesSent,
		MessagesReceived:  gm.MessagesReceived,
		MessagesDropped:   gm.MessagesDropped,
		MessagesDuplicate: gm.MessagesDuplicate,
		BroadcastCount:    gm.BroadcastCount,
		AverageHops:       gm.AverageHops,
		PropagationTimeMs: gm.PropagationTime.Milliseconds(),
		QueueUtilization:  queueUtil,
		HandlerErrors:     gm.HandlerErrors,
		InvalidMessages:   gm.InvalidMessages,
		RateLimitDrops:    gm.RateLimitDrops,
	}
}

// CacheStats tracks message cache statistics
type CacheStats struct {
	mu             sync.RWMutex
	Hits           uint64
	Misses         uint64
	Evictions      uint64
	Size           int
	FalsePositives uint64
	Inserts        uint64
	Deletes        uint64
	Cleanups       uint64
}

// IncrementHits increments cache hit counter
func (cs *CacheStats) IncrementHits() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Hits++
}

// IncrementMisses increments cache miss counter
func (cs *CacheStats) IncrementMisses() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Misses++
}

// IncrementEvictions increments eviction counter
func (cs *CacheStats) IncrementEvictions() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Evictions++
}

// IncrementFalsePositives increments false positive counter
func (cs *CacheStats) IncrementFalsePositives() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.FalsePositives++
}

// IncrementInserts increments insert counter
func (cs *CacheStats) IncrementInserts() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Inserts++
}

// IncrementDeletes increments delete counter
func (cs *CacheStats) IncrementDeletes() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Deletes++
}

// IncrementCleanups increments cleanup counter
func (cs *CacheStats) IncrementCleanups() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Cleanups++
}

// UpdateSize updates the current cache size
func (cs *CacheStats) UpdateSize(size int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Size = size
}

// GetSnapshot returns a snapshot of current cache statistics
func (cs *CacheStats) GetSnapshot() *CacheStatsSnapshot {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var hitRatio float64
	if cs.Hits+cs.Misses > 0 {
		hitRatio = float64(cs.Hits) / float64(cs.Hits+cs.Misses)
	}

	return &CacheStatsSnapshot{
		Hits:           cs.Hits,
		Misses:         cs.Misses,
		Evictions:      cs.Evictions,
		Size:           cs.Size,
		FalsePositives: cs.FalsePositives,
		Inserts:        cs.Inserts,
		Deletes:        cs.Deletes,
		Cleanups:       cs.Cleanups,
		HitRatio:       hitRatio,
	}
}

// DiscoveryMetrics tracks peer discovery metrics
type DiscoveryMetrics struct {
	mu                   sync.RWMutex
	BootstrapAttempts    uint64
	BootstrapSuccesses   uint64
	DiscoveryQueries     uint64
	PeersDiscovered      uint64
	DiscoveryErrors      uint64
	DHTLookups           uint64
	DHTStores            uint64
	PeerExchanges        uint64
	RoutingTableUpdates  uint64
	ActiveDiscoveries    uint64
	LastDiscoveryTime    time.Time
	AverageDiscoveryTime time.Duration
}

// IncrementBootstrapAttempts increments bootstrap attempt counter
func (dm *DiscoveryMetrics) IncrementBootstrapAttempts() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.BootstrapAttempts++
}

// IncrementBootstrapSuccesses increments bootstrap success counter
func (dm *DiscoveryMetrics) IncrementBootstrapSuccesses() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.BootstrapSuccesses++
}

// IncrementDiscoveryQueries increments discovery query counter
func (dm *DiscoveryMetrics) IncrementDiscoveryQueries() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.DiscoveryQueries++
}

// IncrementPeersDiscovered increments discovered peer counter
func (dm *DiscoveryMetrics) IncrementPeersDiscovered() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.PeersDiscovered++
}

// IncrementDiscoveryErrors increments discovery error counter
func (dm *DiscoveryMetrics) IncrementDiscoveryErrors() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.DiscoveryErrors++
}

// UpdateDiscoveryTime updates discovery timing metrics
func (dm *DiscoveryMetrics) UpdateDiscoveryTime(duration time.Duration) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.LastDiscoveryTime = common.ConsensusNow()

	// Update average discovery time using exponential moving average
	if dm.AverageDiscoveryTime == 0 {
		dm.AverageDiscoveryTime = duration
	} else {
		dm.AverageDiscoveryTime = time.Duration(0.9*float64(dm.AverageDiscoveryTime) + 0.1*float64(duration))
	}
}

// GetSnapshot returns a snapshot of current discovery metrics
func (dm *DiscoveryMetrics) GetSnapshot() *DiscoveryMetricsSnapshot {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	var successRate float64
	if dm.BootstrapAttempts > 0 {
		successRate = float64(dm.BootstrapSuccesses) / float64(dm.BootstrapAttempts)
	}

	return &DiscoveryMetricsSnapshot{
		BootstrapAttempts:    dm.BootstrapAttempts,
		BootstrapSuccesses:   dm.BootstrapSuccesses,
		BootstrapSuccessRate: successRate,
		DiscoveryQueries:     dm.DiscoveryQueries,
		PeersDiscovered:      dm.PeersDiscovered,
		DiscoveryErrors:      dm.DiscoveryErrors,
		DHTLookups:           dm.DHTLookups,
		DHTStores:            dm.DHTStores,
		PeerExchanges:        dm.PeerExchanges,
		RoutingTableUpdates:  dm.RoutingTableUpdates,
		ActiveDiscoveries:    dm.ActiveDiscoveries,
		LastDiscoveryTime:    dm.LastDiscoveryTime.Unix(),
		AvgDiscoveryTimeMs:   dm.AverageDiscoveryTime.Milliseconds(),
	}
}

// Priority defines message priority levels
type Priority int

const (
	PriorityHigh Priority = iota
	PriorityNormal
	PriorityLow
)

// String returns the string representation of Priority
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

// ScoreEvent represents different types of scoring events
type ScoreEvent int

const (
	ScoreEventValidMessage ScoreEvent = iota
	ScoreEventInvalidMessage
	ScoreEventTimeout
	ScoreEventDisconnect
	ScoreEventReconnect
	ScoreEventBandwidthContribution
	ScoreEventLatencyUpdate
	ScoreEventHandshakeSuccess
	ScoreEventHandshakeFailure
)

// String returns the string representation of ScoreEvent
func (se ScoreEvent) String() string {
	switch se {
	case ScoreEventValidMessage:
		return "valid_message"
	case ScoreEventInvalidMessage:
		return "invalid_message"
	case ScoreEventTimeout:
		return "timeout"
	case ScoreEventDisconnect:
		return "disconnect"
	case ScoreEventReconnect:
		return "reconnect"
	case ScoreEventBandwidthContribution:
		return "bandwidth_contribution"
	case ScoreEventLatencyUpdate:
		return "latency_update"
	case ScoreEventHandshakeSuccess:
		return "handshake_success"
	case ScoreEventHandshakeFailure:
		return "handshake_failure"
	default:
		return "unknown"
	}
}

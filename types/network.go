// Package types provides network-specific type definitions
package types

import (
	"time"
)

// NetworkMessageType represents the type of network message
type NetworkMessageType uint8

const (
	NetworkMessageTypeUnknown NetworkMessageType = iota
	NetworkMessageTypeTransaction
	NetworkMessageTypeBlock
	NetworkMessageTypeConsensus
	NetworkMessageTypeSync
	NetworkMessageTypePeer
	NetworkMessageTypeHeartbeat
	NetworkMessageTypeStatus
	NetworkMessageTypeGeneric
)

// GenericPayloadData represents typed data for generic network payloads
type GenericPayloadData struct {
	Fields map[string]*Value `json:"fields"`
}

// NetworkStats represents network statistics
type NetworkStats struct {
	ActivePeers      int           `json:"active_peers"`
	TotalConnections uint64        `json:"total_connections"`
	MessagesReceived uint64        `json:"messages_received"`
	MessagesSent     uint64        `json:"messages_sent"`
	BytesReceived    uint64        `json:"bytes_received"`
	BytesSent        uint64        `json:"bytes_sent"`
	AvgLatency       time.Duration `json:"avg_latency"`
	MaxLatency       time.Duration `json:"max_latency"`
	MinLatency       time.Duration `json:"min_latency"`
	DroppedMessages  uint64        `json:"dropped_messages"`
	ErrorCount       uint64        `json:"error_count"`
	LastError        string        `json:"last_error,omitempty"`
	ProtocolVersion  string        `json:"protocol_version"`
	NetworkID        string        `json:"network_id"`
	Uptime           time.Duration `json:"uptime"`
}

// PeerInfo represents information about a network peer
type PeerInfo struct {
	ID           string            `json:"id"`
	Address      string            `json:"address"`
	Version      string            `json:"version"`
	LastSeen     time.Time         `json:"last_seen"`
	Connected    bool              `json:"connected"`
	Latency      time.Duration     `json:"latency"`
	MessagesSent uint64            `json:"messages_sent"`
	MessagesRecv uint64            `json:"messages_recv"`
	BytesSent    uint64            `json:"bytes_sent"`
	BytesRecv    uint64            `json:"bytes_recv"`
	Score        float64           `json:"score"`
	Features     []string          `json:"features"`
	Metadata     map[string]*Value `json:"metadata,omitempty"`
}

// NetworkConfig represents network configuration
type NetworkConfig struct {
	ListenAddress     string            `json:"listen_address"`
	AdvertiseAddress  string            `json:"advertise_address"`
	MaxPeers          int               `json:"max_peers"`
	MinPeers          int               `json:"min_peers"`
	DialTimeout       time.Duration     `json:"dial_timeout"`
	HandshakeTimeout  time.Duration     `json:"handshake_timeout"`
	EnableTLS         bool              `json:"enable_tls"`
	TLSCertPath       string            `json:"tls_cert_path,omitempty"`
	TLSKeyPath        string            `json:"tls_key_path,omitempty"`
	EnableCompression bool              `json:"enable_compression"`
	CompressionLevel  int               `json:"compression_level"`
	MessageQueueSize  int               `json:"message_queue_size"`
	RateLimitMessages int               `json:"rate_limit_messages"`
	RateLimitInterval time.Duration     `json:"rate_limit_interval"`
	BootstrapPeers    []string          `json:"bootstrap_peers"`
	CustomSettings    map[string]*Value `json:"custom_settings,omitempty"`
}

// PartitionInfo represents information about a network partition
type PartitionInfo struct {
	ID               string        `json:"id"`
	StartTime        time.Time     `json:"start_time"`
	EndTime          time.Time     `json:"end_time,omitempty"`
	Active           bool          `json:"active"`
	AffectedPeers    []string      `json:"affected_peers"`
	PartitionType    string        `json:"partition_type"`
	Duration         time.Duration `json:"duration,omitempty"`
	MessagesDropped  uint64        `json:"messages_dropped"`
	MessagesDelayed  uint64        `json:"messages_delayed"`
	RecoveryStrategy string        `json:"recovery_strategy"`
}

// TransportMetrics represents transport layer metrics
type TransportMetrics struct {
	Protocol          string        `json:"protocol"`
	ConnectionsActive int           `json:"connections_active"`
	ConnectionsTotal  uint64        `json:"connections_total"`
	StreamsActive     int           `json:"streams_active"`
	StreamsTotal      uint64        `json:"streams_total"`
	PacketsReceived   uint64        `json:"packets_received"`
	PacketsSent       uint64        `json:"packets_sent"`
	PacketsLost       uint64        `json:"packets_lost"`
	RetransmitCount   uint64        `json:"retransmit_count"`
	RoundTripTime     time.Duration `json:"round_trip_time"`
	Throughput        float64       `json:"throughput"`
}

// MessageCacheEntry represents a cached message
type MessageCacheEntry struct {
	MessageID   string    `json:"message_id"`
	MessageType string    `json:"message_type"`
	Data        []byte    `json:"data"`
	Size        int       `json:"size"`
	Timestamp   time.Time `json:"timestamp"`
	Sender      string    `json:"sender"`
	TTL         int64     `json:"ttl"`
}

// PeerScore represents peer scoring information
type PeerScore struct {
	PeerID           string    `json:"peer_id"`
	BaseScore        float64   `json:"base_score"`
	BehaviorScore    float64   `json:"behavior_score"`
	LatencyScore     float64   `json:"latency_score"`
	ReliabilityScore float64   `json:"reliability_score"`
	TotalScore       float64   `json:"total_score"`
	LastUpdated      time.Time `json:"last_updated"`
	ViolationCount   int       `json:"violation_count"`
	MessageCount     uint64    `json:"message_count"`
}

// TLSConfig represents TLS configuration
type TLSConfig struct {
	Enabled               bool     `json:"enabled"`
	CertFile              string   `json:"cert_file"`
	KeyFile               string   `json:"key_file"`
	CAFile                string   `json:"ca_file,omitempty"`
	ClientAuth            bool     `json:"client_auth"`
	MinVersion            string   `json:"min_version"`
	CipherSuites          []string `json:"cipher_suites"`
	PreferServerCiphers   bool     `json:"prefer_server_ciphers"`
	SessionTicketsEnabled bool     `json:"session_tickets_enabled"`
	SessionCacheSize      int      `json:"session_cache_size"`
	ALPN                  []string `json:"alpn"`
}

// SyncState represents synchronization state
type SyncState struct {
	NodeID          string            `json:"node_id"`
	CurrentHeight   uint64            `json:"current_height"`
	TargetHeight    uint64            `json:"target_height"`
	StartTime       time.Time         `json:"start_time"`
	Progress        float64           `json:"progress"`
	State           string            `json:"state"`
	PeersConnected  int               `json:"peers_connected"`
	BlocksReceived  uint64            `json:"blocks_received"`
	BlocksValidated uint64            `json:"blocks_validated"`
	BytesReceived   uint64            `json:"bytes_received"`
	AverageSpeed    float64           `json:"average_speed"`
	EstimatedTime   time.Duration     `json:"estimated_time"`
	LastError       string            `json:"last_error,omitempty"`
	Metadata        map[string]*Value `json:"metadata,omitempty"`
}

// SyncStateInfo represents synchronized state information
type SyncStateInfo struct {
	BlockHeight     uint64            `json:"block_height"`
	LatestBlockHash string            `json:"latest_block_hash"`
	StateRoot       string            `json:"state_root"`
	Timestamp       time.Time         `json:"timestamp"`
	ValidatorSet    string            `json:"validator_set"`
	NodeID          string            `json:"node_id"`
	Version         string            `json:"version"`
	NetworkStatus   string            `json:"network_status"`
	PeerCount       int               `json:"peer_count"`
	Attributes      map[string]*Value `json:"attributes,omitempty"`
}

// StateResolution represents state conflict resolution information
type StateResolution struct {
	Strategy     string    `json:"strategy"`
	SelectedPeer string    `json:"selected_peer,omitempty"`
	SelectedHash string    `json:"selected_hash,omitempty"`
	Confidence   float64   `json:"confidence"`
	Timestamp    time.Time `json:"timestamp"`
}

// StateSyncRequest represents a state synchronization request
type StateSyncRequest struct {
	Peers      map[string]*SyncStateInfo `json:"peers"`
	Resolution *StateResolution          `json:"resolution,omitempty"`
	LocalState *SyncStateInfo            `json:"local_state,omitempty"`
}

// ErrorResponse represents a generic error response
type ErrorResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Code   int    `json:"code,omitempty"`
}

// SyncBlocksResponse represents response for block sync requests
type SyncBlocksResponse struct {
	Status     string         `json:"status"`
	Blocks     []BlockSummary `json:"blocks"`
	FromHeight uint64         `json:"from_height"`
	ToHeight   uint64         `json:"to_height"`
	TotalCount int            `json:"total_count"`
}

// BlockSummary represents a summary of a block
type BlockSummary struct {
	Height           uint64 `json:"height"`
	Hash             string `json:"hash"`
	ParentHash       string `json:"parent_hash"`
	Timestamp        int64  `json:"timestamp"`
	TransactionCount int    `json:"transaction_count"`
	Proposer         string `json:"proposer"`
}

// SyncInfoResponse represents general sync information response
type SyncInfoResponse struct {
	Status          string    `json:"status"`
	BlockHeight     uint64    `json:"block_height"`
	LatestBlockHash string    `json:"latest_block_hash"`
	SyncedPeers     int       `json:"synced_peers"`
	LastSyncTime    time.Time `json:"last_sync_time"`
}

// KeepaliveData represents keepalive message data
type KeepaliveData struct {
	Time string `json:"time"`
}

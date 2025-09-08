// storage/integration_interfaces.go

package storage

import (
	"context"
	"time"

	"diamante/common"
)

// EventData represents typed data for storage events
type EventData struct {
	// Block events
	Block *common.Block `json:"block,omitempty"`

	// Transaction events
	Transaction *common.Transaction `json:"transaction,omitempty"`
	TxID        string              `json:"tx_id,omitempty"`

	// State events
	StateKey   []byte `json:"state_key,omitempty"`
	StateValue []byte `json:"state_value,omitempty"`
	OldValue   []byte `json:"old_value,omitempty"`

	// Contract events
	Contract   *common.SmartContract `json:"contract,omitempty"`
	ContractID string                `json:"contract_id,omitempty"`

	// Account events
	Account   *common.Account `json:"account,omitempty"`
	AccountID string          `json:"account_id,omitempty"`

	// General fields
	EventType   string    `json:"event_type"`
	Description string    `json:"description,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// IntegrationStats represents comprehensive storage integration statistics
type IntegrationStats struct {
	// Connection stats
	ConnectedModules   int  `json:"connected_modules"`
	ConsensusConnected bool `json:"consensus_connected"`
	NetworkConnected   bool `json:"network_connected"`
	VMConnected        bool `json:"vm_connected"`

	// Event stats
	EventsReceived  int64            `json:"events_received"`
	EventsProcessed int64            `json:"events_processed"`
	EventErrors     int64            `json:"event_errors"`
	EventsByType    map[string]int64 `json:"events_by_type"`

	// Sync stats
	SyncActive       bool      `json:"sync_active"`
	SyncHeight       uint64    `json:"sync_height"`
	SyncTargetHeight uint64    `json:"sync_target_height"`
	SyncStartTime    time.Time `json:"sync_start_time"`
	SyncProgress     float64   `json:"sync_progress"`

	// Cache stats
	CacheEnabled bool    `json:"cache_enabled"`
	CacheSize    int64   `json:"cache_size"`
	CacheHits    int64   `json:"cache_hits"`
	CacheMisses  int64   `json:"cache_misses"`
	CacheHitRate float64 `json:"cache_hit_rate"`

	// Health stats
	HealthStatus     string    `json:"health_status"`
	LastHealthCheck  time.Time `json:"last_health_check"`
	HealthAlertCount int       `json:"health_alert_count"`

	// Performance stats
	AverageBlockTime      int64   `json:"average_block_time_ms"`
	AverageTxTime         int64   `json:"average_tx_time_ms"`
	BlocksPerSecond       float64 `json:"blocks_per_second"`
	TransactionsPerSecond float64 `json:"transactions_per_second"`
}

// ConsensusEvent represents typed consensus events
type ConsensusEvent struct {
	EventType string
	BlockData *common.Block
	Height    uint64
	Hash      string
	Validator string
	Timestamp time.Time
}

// NetworkEvent represents typed network events
type NetworkEvent struct {
	EventType   string
	PeerID      string
	MessageType string
	Data        []byte
	Timestamp   time.Time
}

// ConsensusEngine interface for consensus integration
type ConsensusEngine interface {
	Subscribe(eventType string, handler func(*ConsensusEvent))
}

// NetworkManager interface for network integration
type NetworkManager interface {
	Subscribe(eventType string, handler func(*NetworkEvent))
}

// VMManager interface for VM integration
type VMManager interface {
	SetStateReadCallback(func([]byte) ([]byte, error))
	SetStateWriteCallback(func([]byte, []byte) error)
}

// MetricsReporter interface for metrics integration
type MetricsReporter interface {
	RecordMetric(name string, value float64, tags map[string]string)
}

// StorageIntegrationInterface defines the interface for storage integration
type StorageIntegrationInterface interface {
	// Module integration
	SetConsensus(consensus ConsensusEngine)
	SetNetwork(network NetworkManager)
	SetVMManager(vmManager VMManager)
	SetMetricsReporter(reporter MetricsReporter)

	// Event handling
	Subscribe(eventType EventType, handler EventHandler) string
	Unsubscribe(id string)

	// Enhanced operations
	GetBlock(height uint64) (*common.Block, error)
	GetTransaction(txID string) (*common.Transaction, error)
	GetState(key []byte) ([]byte, error)
	SaveBlock(block *common.Block) error

	// Health and stats
	GetHealthAlerts() <-chan HealthAlert
	GetStats() *IntegrationStats

	// Lifecycle
	Close() error
}

// EventType represents storage event types
type EventType string

const (
	EventBlockAdded       EventType = "block_added"
	EventTransactionAdded EventType = "transaction_added"
	EventStateChanged     EventType = "state_changed"
	EventContractDeployed EventType = "contract_deployed"
	EventContractUpdated  EventType = "contract_updated"
	EventAccountUpdated   EventType = "account_updated"
)

// StorageEvent represents a storage event
type StorageEvent struct {
	Type      EventType
	Data      *EventData
	Timestamp time.Time
}

// EventHandler handles storage events
type EventHandler func(context.Context, StorageEvent) error

// HealthAlert represents a health alert
type HealthAlert struct {
	Level     string
	Component string
	Message   string
	Timestamp time.Time
}

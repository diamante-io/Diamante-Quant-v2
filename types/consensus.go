// Package types provides consensus-specific type definitions
package types

import (
	"time"
)

// ConsensusMessageType defines the type of consensus messages
type ConsensusMessageType uint8

const (
	ConsensusMessageTypeUnknown ConsensusMessageType = iota
	ConsensusMessageTypeProposal
	ConsensusMessageTypeVote
	ConsensusMessageTypeCommit
	ConsensusMessageTypeSync
	ConsensusMessageTypeHeartbeat
	ConsensusMessageTypeValidatorUpdate
	ConsensusMessageTypeBlockRequest
	ConsensusMessageTypeBlockResponse
)

// ConsensusMessage represents a typed consensus message
type ConsensusMessage struct {
	Type      ConsensusMessageType `json:"type"`
	Height    uint64               `json:"height"`
	Round     uint32               `json:"round"`
	NodeID    string               `json:"node_id"`
	Timestamp int64                `json:"timestamp"`
	Data      *ConsensusPayload    `json:"data"`
	Signature []byte               `json:"signature"`
}

// ConsensusPayload contains the actual consensus data
type ConsensusPayload struct {
	ProposalData    *ProposalData    `json:"proposal_data,omitempty"`
	VoteData        *VoteData        `json:"vote_data,omitempty"`
	CommitData      *CommitData      `json:"commit_data,omitempty"`
	SyncData        *SyncData        `json:"sync_data,omitempty"`
	HeartbeatData   *HeartbeatData   `json:"heartbeat_data,omitempty"`
	ValidatorUpdate *ValidatorUpdate `json:"validator_update,omitempty"`
}

// ProposalData contains block proposal information
type ProposalData struct {
	BlockHash     []byte   `json:"block_hash"`
	BlockData     []byte   `json:"block_data"`
	PreviousHash  []byte   `json:"previous_hash"`
	StateRoot     []byte   `json:"state_root"`
	Timestamp     int64    `json:"timestamp"`
	Transactions  [][]byte `json:"transactions"`
	ValidatorSet  []byte   `json:"validator_set"`
	ProposerIndex uint32   `json:"proposer_index"`
}

// VoteData contains voting information
type VoteData struct {
	BlockHash    []byte   `json:"block_hash"`
	VoteType     VoteType `json:"vote_type"`
	ValidatorIdx uint32   `json:"validator_idx"`
	Timestamp    int64    `json:"timestamp"`
}

// VoteType represents the type of vote
type VoteType uint8

const (
	VoteTypePrevote VoteType = iota
	VoteTypePrecommit
	VoteTypeCommit
)

// CommitData contains block commit information
type CommitData struct {
	BlockHash    []byte   `json:"block_hash"`
	Height       uint64   `json:"height"`
	Round        uint32   `json:"round"`
	Signatures   [][]byte `json:"signatures"`
	ValidatorSet []byte   `json:"validator_set"`
	Timestamp    int64    `json:"timestamp"`
}

// SyncData contains synchronization information
type SyncData struct {
	StartHeight uint64    `json:"start_height"`
	EndHeight   uint64    `json:"end_height"`
	BlockHashes [][]byte  `json:"block_hashes"`
	NodeInfo    *NodeInfo `json:"node_info"`
}

// HeartbeatData contains node heartbeat information
type HeartbeatData struct {
	NodeID      string       `json:"node_id"`
	Height      uint64       `json:"height"`
	Round       uint32       `json:"round"`
	Timestamp   int64        `json:"timestamp"`
	NodeMetrics *NodeMetrics `json:"node_metrics"`
}

// ValidatorUpdate contains validator set update information
type ValidatorUpdate struct {
	Action         ValidatorAction `json:"action"`
	ValidatorID    string          `json:"validator_id"`
	PublicKey      []byte          `json:"public_key"`
	VotingPower    uint64          `json:"voting_power"`
	StakeAmount    uint64          `json:"stake_amount"`
	CommissionRate uint32          `json:"commission_rate"`
	UpdateHeight   uint64          `json:"update_height"`
}

// ValidatorAction represents the type of validator update
type ValidatorAction uint8

const (
	ValidatorActionAdd ValidatorAction = iota
	ValidatorActionRemove
	ValidatorActionUpdate
)

// NodeInfo contains information about a consensus node
type NodeInfo struct {
	NodeID       string   `json:"node_id"`
	PublicKey    []byte   `json:"public_key"`
	Address      string   `json:"address"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
}

// NodeMetrics contains node performance metrics
type NodeMetrics struct {
	CPUUsage       float64 `json:"cpu_usage"`
	MemoryUsage    float64 `json:"memory_usage"`
	DiskUsage      float64 `json:"disk_usage"`
	NetworkLatency int64   `json:"network_latency"`
	BlocksProposed uint64  `json:"blocks_proposed"`
	BlocksMissed   uint64  `json:"blocks_missed"`
}

// ConsensusState represents the current consensus state
type ConsensusState struct {
	Height          uint64        `json:"height"`
	Round           uint32        `json:"round"`
	Step            ConsensusStep `json:"step"`
	StartTime       time.Time     `json:"start_time"`
	CurrentProposer string        `json:"current_proposer"`
	LockedBlock     []byte        `json:"locked_block"`
	LockedRound     int32         `json:"locked_round"`
	ValidRound      int32         `json:"valid_round"`
	Votes           *VoteSet      `json:"votes"`
	LastCommit      *CommitData   `json:"last_commit"`
}

// ConsensusStep represents the current step in consensus
type ConsensusStep uint8

const (
	ConsensusStepNewHeight ConsensusStep = iota
	ConsensusStepNewRound
	ConsensusStepPropose
	ConsensusStepPrevote
	ConsensusStepPrevoteWait
	ConsensusStepPrecommit
	ConsensusStepPrecommitWait
	ConsensusStepCommit
)

// VoteSet tracks votes for a specific height/round
type VoteSet struct {
	Height     uint64               `json:"height"`
	Round      uint32               `json:"round"`
	Type       VoteType             `json:"type"`
	Votes      map[string]*VoteData `json:"votes"`
	VotesCount uint32               `json:"votes_count"`
	Majority   []byte               `json:"majority"`
}

// ConsensusConfig contains consensus configuration parameters
type ConsensusConfig struct {
	// Timing parameters
	ProposeTimeout   time.Duration `json:"propose_timeout"`
	PrevoteTimeout   time.Duration `json:"prevote_timeout"`
	PrecommitTimeout time.Duration `json:"precommit_timeout"`
	CommitTimeout    time.Duration `json:"commit_timeout"`

	// Block parameters
	MaxBlockSize     uint64        `json:"max_block_size"`
	MaxTransactions  uint32        `json:"max_transactions"`
	MinBlockInterval time.Duration `json:"min_block_interval"`
	MaxBlockInterval time.Duration `json:"max_block_interval"`

	// Validator parameters
	MaxValidators   uint32        `json:"max_validators"`
	MinStakeAmount  uint64        `json:"min_stake_amount"`
	UnbondingPeriod time.Duration `json:"unbonding_period"`
	SlashingPenalty uint32        `json:"slashing_penalty"`

	// Network parameters
	MaxPeers          uint32        `json:"max_peers"`
	GossipInterval    time.Duration `json:"gossip_interval"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

// CacheEntry represents a consensus cache entry
type CacheEntry struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	EntryType CacheType `json:"entry_type"`
	Timestamp int64     `json:"timestamp"`
	TTL       int64     `json:"ttl"`
	Height    uint64    `json:"height"`
}

// CacheType represents the type of cache entry
type CacheType uint8

const (
	CacheTypeBlock CacheType = iota
	CacheTypeTransaction
	CacheTypeState
	CacheTypeValidator
	CacheTypeProposal
	CacheTypeVote
)

// ConsensusMetrics contains consensus performance metrics
type ConsensusMetrics struct {
	Height           uint64        `json:"height"`
	Round            uint32        `json:"round"`
	BlockTime        time.Duration `json:"block_time"`
	TransactionCount uint64        `json:"transaction_count"`
	ValidatorCount   uint32        `json:"validator_count"`
	NetworkLatency   time.Duration `json:"network_latency"`
	ConsensusLatency time.Duration `json:"consensus_latency"`
	BlockSize        uint64        `json:"block_size"`
	MissedBlocks     uint64        `json:"missed_blocks"`
	ByzantineFaults  uint32        `json:"byzantine_faults"`
}

// OptimizerConfig contains adaptive optimizer configuration
type OptimizerConfig struct {
	EnableOptimization bool          `json:"enable_optimization"`
	MetricWindow       time.Duration `json:"metric_window"`
	AdjustmentFactor   float64       `json:"adjustment_factor"`
	MinTimeout         time.Duration `json:"min_timeout"`
	MaxTimeout         time.Duration `json:"max_timeout"`
	TargetBlockTime    time.Duration `json:"target_block_time"`
	LatencyThreshold   time.Duration `json:"latency_threshold"`
}

// OptimizerMetrics contains optimizer performance data
type OptimizerMetrics struct {
	AverageBlockTime   time.Duration `json:"average_block_time"`
	TimeoutAdjustments uint64        `json:"timeout_adjustments"`
	NetworkCondition   string        `json:"network_condition"`
	OptimalTimeout     time.Duration `json:"optimal_timeout"`
	SuccessRate        float64       `json:"success_rate"`
}

// OptimizationStats contains AI optimizer statistics
type OptimizationStats struct {
	CurrentLoad             float64       `json:"current_load"`
	PredictedLoad           float64       `json:"predicted_load"`
	SampleCount             int           `json:"sample_count"`
	AverageBlockTime        time.Duration `json:"average_block_time"`
	TPS                     float64       `json:"tps"`
	NetworkLoad             float64       `json:"network_load"`
	LachesisGossipDelay     time.Duration `json:"lachesis_gossip_delay,omitempty"`
	LachesisVotingThreshold float64       `json:"lachesis_voting_threshold,omitempty"`
	DPoSSetSize             int           `json:"dpos_set_size,omitempty"`
	DPoSEpochDuration       time.Duration `json:"dpos_epoch_duration,omitempty"`
	PoHTickDelay            time.Duration `json:"poh_tick_delay,omitempty"`
}

// CircuitBreakerStatus represents the status of a circuit breaker
type CircuitBreakerStatus struct {
	Active        bool      `json:"active"`
	OpenedAt      time.Time `json:"opened_at"`
	TimeRemaining string    `json:"time_remaining"`
}

// CircuitBreakerStatuses maps error codes to their circuit breaker status
type CircuitBreakerStatuses map[string]*CircuitBreakerStatus

// GCStats contains garbage collection statistics
type GCStats struct {
	NumGC         uint32    `json:"num_gc"`
	PauseTotalNs  uint64    `json:"pause_total_ns"`
	LastGC        time.Time `json:"last_gc"`
	GCCountManual int64     `json:"gc_count_manual"`
	LastManualGC  time.Time `json:"last_manual_gc"`
}

// EventMetrics contains event flow metrics
type EventMetrics struct {
	TotalEventsCreated     uint64      `json:"total_events_created"`
	TotalEventsFinalized   uint64      `json:"total_events_finalized"`
	PendingCount           int         `json:"pending_count"`
	FinalizedCount         int         `json:"finalized_count"`
	AvgFinalizationTime    string      `json:"avg_finalization_time"`
	MaxFinalizationTime    string      `json:"max_finalization_time"`
	FinalizationTimeouts   uint64      `json:"finalization_timeouts"`
	EventDuplicateCount    uint64      `json:"event_duplicate_count"`
	EventValidationErrors  uint64      `json:"event_validation_errors"`
	EventPropagationErrors uint64      `json:"event_propagation_errors"`
	EventRetries           uint64      `json:"event_retries"`
	BatchProcessingTime    string      `json:"batch_processing_time"`
	CurrentBatchSize       int         `json:"current_batch_size"`
	SuccessRate            float64     `json:"success_rate"`
	RetryDistribution      map[int]int `json:"retry_distribution"`
}

// AdaptiveParametersMetrics contains adaptive parameters metrics
type AdaptiveParametersMetrics struct {
	NetworkLoad         float64       `json:"network_load"`
	EventProcessingTime time.Duration `json:"event_processing_time"`
	BlockProcessingTime time.Duration `json:"block_processing_time"`
	CPUUtilization      float64       `json:"cpu_utilization"`
	MemoryUtilization   float64       `json:"memory_utilization"`
	GossipDelay         time.Duration `json:"gossip_delay"`
	PoHTickDelay        time.Duration `json:"poh_tick_delay"`
	VotingThreshold     float64       `json:"voting_threshold"`
	BatchSize           int           `json:"batch_size"`
}

// AdaptiveOptimizerStats contains adaptive optimizer statistics
type AdaptiveOptimizerStats struct {
	OptimizationCount    int       `json:"optimization_count"`
	LastOptimization     time.Time `json:"last_optimization"`
	CurrentScore         float64   `json:"current_score"`
	BestScore            float64   `json:"best_score"`
	AverageScore         float64   `json:"average_score"`
	ParameterChanges     int       `json:"parameter_changes"`
	LearningRate         float64   `json:"learning_rate"`
	ExplorationRate      float64   `json:"exploration_rate"`
	ConvergedParams      int       `json:"converged_params"`
	MetricsHistoryLength int       `json:"metrics_history_length"`
}

// ConsensusStateInfo represents the state information for consensus
type ConsensusStateInfo struct {
	// Core state
	CurrentHeight   uint64 `json:"current_height"`
	LatestBlockHash string `json:"latest_block_hash"`
	ConsensusMode   string `json:"consensus_mode"`
	IsActive        bool   `json:"is_active"`
	InPartition     bool   `json:"in_partition"`

	// Timing information
	LastBlockTime    time.Time     `json:"last_block_time"`
	AverageBlockTime time.Duration `json:"average_block_time"`

	// Validator information
	ActiveValidators []ValidatorStateInfo `json:"active_validators"`
	TotalStake       uint64               `json:"total_stake"`

	// Additional metadata
	Metadata map[string]*Value `json:"metadata,omitempty"`
}

// ValidatorStateInfo represents state information for a validator
type ValidatorStateInfo struct {
	ID         string    `json:"id"`
	Stake      uint64    `json:"stake"`
	Active     bool      `json:"active"`
	VoteCount  int       `json:"vote_count"`
	Reputation float64   `json:"reputation"`
	LastSeen   time.Time `json:"last_seen"`
}

// PartitionMetrics represents metrics about network partitions
type PartitionMetrics struct {
	PartitionCount         int           `json:"partition_count"`
	RecoveryCount          int           `json:"recovery_count"`
	CurrentlyPartitioned   bool          `json:"currently_partitioned"`
	LastPartitionTime      time.Time     `json:"last_partition_time"`
	LastRecoveryTime       time.Time     `json:"last_recovery_time"`
	LastRecoveryDuration   time.Duration `json:"last_recovery_duration"`
	TotalPartitionDuration time.Duration `json:"total_partition_duration"`
}

// NewConsensusStateInfo creates a new consensus state info
func NewConsensusStateInfo() *ConsensusStateInfo {
	return &ConsensusStateInfo{
		ActiveValidators: make([]ValidatorStateInfo, 0),
		Metadata:         make(map[string]*Value),
	}
}

// Set sets a metadata value
func (csi *ConsensusStateInfo) Set(key string, vtype ValueType, data []byte) {
	if csi.Metadata == nil {
		csi.Metadata = make(map[string]*Value)
	}
	csi.Metadata[key] = NewValue(vtype, data)
}

// Get retrieves a metadata value
func (csi *ConsensusStateInfo) Get(key string) (*Value, bool) {
	if csi.Metadata == nil {
		return nil, false
	}
	val, ok := csi.Metadata[key]
	return val, ok
}

// Package types provides storage-specific type definitions
package types

import (
	"time"
)

// StorageType represents the type of storage backend
type StorageType uint8

const (
	StorageTypeMongoDB StorageType = iota
	StorageTypeLMDB
	StorageTypeRedis
	StorageTypeMemory
	StorageTypeSQLite
)

// StorageOperation represents a storage operation
type StorageOperation uint8

const (
	StorageOperationGet StorageOperation = iota
	StorageOperationPut
	StorageOperationDelete
	StorageOperationBatch
	StorageOperationQuery
	StorageOperationIterate
)

// StorageKey represents a typed storage key
type StorageKey struct {
	Namespace string `json:"namespace"`
	Category  string `json:"category"`
	ID        string `json:"id"`
	Version   uint64 `json:"version,omitempty"`
}

// StorageValue represents a typed storage value
type StorageValue struct {
	Key       *StorageKey `json:"key"`
	Data      []byte      `json:"data"`
	Metadata  *Metadata   `json:"metadata"`
	Timestamp int64       `json:"timestamp"`
	TTL       int64       `json:"ttl,omitempty"`
}

// Bytes returns the data bytes of the storage value
func (sv *StorageValue) Bytes() []byte {
	if sv == nil {
		return nil
	}
	return sv.Data
}

// StorageQuery represents a storage query
type StorageQuery struct {
	Namespace string                  `json:"namespace"`
	Category  string                  `json:"category,omitempty"`
	Filters   map[string]*QueryFilter `json:"filters,omitempty"`
	OrderBy   string                  `json:"order_by,omitempty"`
	Ascending bool                    `json:"ascending"`
	Limit     uint32                  `json:"limit,omitempty"`
	Offset    uint32                  `json:"offset,omitempty"`
}

// QueryFilter represents a query filter
type QueryFilter struct {
	Field    string         `json:"field"`
	Operator FilterOperator `json:"operator"`
	Value    *Value         `json:"value"`
}

// FilterOperator represents query filter operators
type FilterOperator uint8

const (
	FilterOperatorEqual FilterOperator = iota
	FilterOperatorNotEqual
	FilterOperatorGreaterThan
	FilterOperatorGreaterEqual
	FilterOperatorLessThan
	FilterOperatorLessEqual
	FilterOperatorIn
	FilterOperatorNotIn
	FilterOperatorContains
	FilterOperatorStartsWith
	FilterOperatorEndsWith
)

// StorageBatch represents a batch operation
type StorageBatch struct {
	Operations []*BatchOperation `json:"operations"`
	Atomic     bool              `json:"atomic"`
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Type  StorageOperation `json:"type"`
	Key   *StorageKey      `json:"key"`
	Value *StorageValue    `json:"value,omitempty"`
}

// CacheConfig represents cache configuration
type CacheConfig struct {
	MaxSize         uint64        `json:"max_size"`
	DefaultTTL      time.Duration `json:"default_ttl"`
	EvictionPolicy  string        `json:"eviction_policy"`
	RefreshOnAccess bool          `json:"refresh_on_access"`
	PersistToDisk   bool          `json:"persist_to_disk"`
}

// CacheValue represents a cached value
type CacheValue struct {
	Key         string    `json:"key"`
	Data        []byte    `json:"data"`
	Size        uint64    `json:"size"`
	CreatedAt   time.Time `json:"created_at"`
	AccessedAt  time.Time `json:"accessed_at"`
	AccessCount uint64    `json:"access_count"`
	TTL         int64     `json:"ttl"`
}

// CacheStats represents cache statistics
type CacheStats struct {
	Hits        uint64  `json:"hits"`
	Misses      uint64  `json:"misses"`
	Evictions   uint64  `json:"evictions"`
	Size        uint64  `json:"size"`
	MaxSize     uint64  `json:"max_size"`
	ItemCount   uint64  `json:"item_count"`
	HitRate     float64 `json:"hit_rate"`
	AvgItemSize uint64  `json:"avg_item_size"`
}

// MemoryPoolConfig represents memory pool configuration
type MemoryPoolConfig struct {
	MaxSize         uint64  `json:"max_size"`
	BlockSize       uint64  `json:"block_size"`
	GrowthFactor    float64 `json:"growth_factor"`
	CompactionRatio float64 `json:"compaction_ratio"`
	EnableStats     bool    `json:"enable_stats"`
}

// MemoryPoolItem represents an item in the memory pool
type MemoryPoolItem struct {
	ID          string    `json:"id"`
	Data        []byte    `json:"data"`
	Size        uint64    `json:"size"`
	AllocatedAt time.Time `json:"allocated_at"`
	LastAccess  time.Time `json:"last_access"`
	AccessCount uint64    `json:"access_count"`
	Priority    uint32    `json:"priority"`
}

// MemoryPoolStats represents memory pool statistics
type MemoryPoolStats struct {
	TotalSize          uint64  `json:"total_size"`
	UsedSize           uint64  `json:"used_size"`
	FreeSize           uint64  `json:"free_size"`
	ItemCount          uint64  `json:"item_count"`
	Allocations        uint64  `json:"allocations"`
	Deallocations      uint64  `json:"deallocations"`
	Compactions        uint64  `json:"compactions"`
	FragmentationRatio float64 `json:"fragmentation_ratio"`
}

// StateSnapshot represents a state snapshot
type StateSnapshot struct {
	Height    uint64        `json:"height"`
	StateRoot []byte        `json:"state_root"`
	Timestamp int64         `json:"timestamp"`
	Entries   []*StateEntry `json:"entries"`
	Proof     *StateProof   `json:"proof"`
}

// StateProof represents a merkle proof for state
type StateProof struct {
	Root   []byte   `json:"root"`
	Key    []byte   `json:"key"`
	Value  []byte   `json:"value"`
	Proof  [][]byte `json:"proof"`
	Height uint64   `json:"height"`
}

// StorageMetrics represents storage performance metrics
type StorageMetrics struct {
	ReadOps         uint64        `json:"read_ops"`
	WriteOps        uint64        `json:"write_ops"`
	DeleteOps       uint64        `json:"delete_ops"`
	BatchOps        uint64        `json:"batch_ops"`
	QueryOps        uint64        `json:"query_ops"`
	AvgReadLatency  time.Duration `json:"avg_read_latency"`
	AvgWriteLatency time.Duration `json:"avg_write_latency"`
	TotalSize       uint64        `json:"total_size"`
	KeyCount        uint64        `json:"key_count"`
}

// PruningConfig represents state pruning configuration
type PruningConfig struct {
	Enabled         bool          `json:"enabled"`
	RetentionHeight uint64        `json:"retention_height"`
	PruneInterval   time.Duration `json:"prune_interval"`
	BatchSize       uint32        `json:"batch_size"`
	KeepRecent      uint64        `json:"keep_recent"`
}

// MigrationData represents data migration information
type MigrationData struct {
	Version     uint32    `json:"version"`
	FromVersion uint32    `json:"from_version"`
	Description string    `json:"description"`
	AppliedAt   time.Time `json:"applied_at"`
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
}

// BackupConfig represents backup configuration
type BackupConfig struct {
	Enabled       bool          `json:"enabled"`
	Interval      time.Duration `json:"interval"`
	Destination   string        `json:"destination"`
	Compression   bool          `json:"compression"`
	Encryption    bool          `json:"encryption"`
	RetentionDays uint32        `json:"retention_days"`
}

// BackupMetadata represents backup metadata
type BackupMetadata struct {
	ID         string    `json:"id"`
	Height     uint64    `json:"height"`
	Timestamp  time.Time `json:"timestamp"`
	Size       uint64    `json:"size"`
	Checksum   string    `json:"checksum"`
	Compressed bool      `json:"compressed"`
	Encrypted  bool      `json:"encrypted"`
	Location   string    `json:"location"`
}

// ExtendedBackupMetadata represents extended backup metadata with additional fields
type ExtendedBackupMetadata struct {
	BackupID      string            `json:"backupId"`
	Timestamp     time.Time         `json:"timestamp"`
	BlockHeight   uint64            `json:"blockHeight"`
	DataHash      string            `json:"dataHash"`
	Size          int64             `json:"size"`
	BackupType    string            `json:"backupType"`
	BackupPath    string            `json:"backupPath"`
	RestorePath   string            `json:"restorePath,omitempty"`
	CloudProvider string            `json:"cloudProvider,omitempty"`
	Encrypted     bool              `json:"encrypted"`
	Compressed    bool              `json:"compressed"`
	Extra         map[string]*Value `json:"extra,omitempty"`
}

// ContractUpdate represents updates to a contract
type ContractUpdate struct {
	Version      string            `json:"version,omitempty"`
	CodeHash     string            `json:"codeHash,omitempty"`
	Owner        string            `json:"owner,omitempty"`
	Code         string            `json:"code,omitempty"`
	Language     string            `json:"language,omitempty"`
	Description  string            `json:"description,omitempty"`
	State        map[string]*Value `json:"state,omitempty"`
	Active       *bool             `json:"active,omitempty"`
	Storage      map[string][]byte `json:"storage,omitempty"`
	Metadata     map[string]*Value `json:"metadata,omitempty"`
	LastModified time.Time         `json:"lastModified,omitempty"`
}

// IndexConfig represents database index configuration
type IndexConfig struct {
	Name       string   `json:"name"`
	Fields     []string `json:"fields"`
	Unique     bool     `json:"unique"`
	Sparse     bool     `json:"sparse"`
	Background bool     `json:"background"`
}

// CompactionConfig represents storage compaction configuration
type CompactionConfig struct {
	Enabled          bool          `json:"enabled"`
	Interval         time.Duration `json:"interval"`
	MinFileSize      uint64        `json:"min_file_size"`
	MaxFileSize      uint64        `json:"max_file_size"`
	CompressionLevel int           `json:"compression_level"`
}

// ContractExecutionResult represents the result of executing a smart contract function
type ContractExecutionResult struct {
	Success      bool                  `json:"success"`
	ReturnData   *ContractReturnData   `json:"returnData,omitempty"`
	GasUsed      uint64                `json:"gasUsed"`
	Events       []ContractEvent       `json:"events"`
	StateChanges []ContractStateChange `json:"stateChanges"`
	Error        string                `json:"error,omitempty"`
}

// ContractReturnData represents return data from contract execution
type ContractReturnData struct {
	Type    string `json:"type"`
	Value   *Value `json:"value"`
	Success bool   `json:"success"`
	Data    []byte `json:"data"`
	Message string `json:"message"`
}

// ContractStateChange represents a state change during contract execution
type ContractStateChange struct {
	Key       string    `json:"key"`
	OldValue  *Value    `json:"oldValue,omitempty"`
	NewValue  *Value    `json:"newValue"`
	Timestamp time.Time `json:"timestamp"`
}

// StatePruningStats represents state pruning statistics
type StatePruningStats struct {
	LastPruneHeight  uint64        `json:"lastPruneHeight"`
	LastPruneTime    time.Time     `json:"lastPruneTime"`
	TotalPruned      uint64        `json:"totalPruned"`
	PruningErrors    uint64        `json:"pruningErrors"`
	NextPruneHeight  uint64        `json:"nextPruneHeight"`
	RetainedBlocks   uint64        `json:"retainedBlocks"`
	PruneQueueSize   int           `json:"pruneQueueSize"`
	AvgPruneDuration time.Duration `json:"avgPruneDuration"`
	CheckpointsKept  int           `json:"checkpointsKept"`
}

// Note: Duplicate types have been removed. The first definitions earlier in the file are used.

// ConnectionStats represents database connection statistics
type ConnectionStats struct {
	Active       int           `json:"active"`
	Idle         int           `json:"idle"`
	Total        int           `json:"total"`
	MaxLifetime  time.Duration `json:"max_lifetime"`
	MaxIdleTime  time.Duration `json:"max_idle_time"`
	TotalCreated uint64        `json:"total_created"`
	TotalClosed  uint64        `json:"total_closed"`
	Errors       uint64        `json:"errors"`
}

// MemoryStats represents memory pool statistics
type MemoryStats struct {
	Allocated   int64     `json:"allocated"`
	Used        int64     `json:"used"`
	Free        int64     `json:"free"`
	Utilization float64   `json:"utilization"`
	GCCount     uint64    `json:"gc_count"`
	LastGC      time.Time `json:"last_gc"`
}

// SyncManagerStats represents sync manager statistics
type SyncManagerStats struct {
	Active         bool          `json:"active"`
	LastSync       time.Time     `json:"last_sync"`
	SyncCount      uint64        `json:"sync_count"`
	FailedSyncs    uint64        `json:"failed_syncs"`
	QueueSize      int           `json:"queue_size"`
	ProcessingTime time.Duration `json:"processing_time"`
	Errors         uint64        `json:"errors"`
}

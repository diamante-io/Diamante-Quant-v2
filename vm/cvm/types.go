package cvm

import (
	"encoding/json"
	"time"
)

// VMType identifies the virtual machine type
type VMType uint8

const (
	VMTypeUnknown   VMType = iota
	VMTypeZKEVM            // Type-3 zkEVM
	VMTypeChaincode        // Hyperledger Fabric-style chaincode
	VMTypeNative           // DNA language with WASM runtime
)

func (v VMType) String() string {
	switch v {
	case VMTypeZKEVM:
		return "zkEVM"
	case VMTypeChaincode:
		return "Chaincode"
	case VMTypeNative:
		return "Native"
	default:
		return "Unknown"
	}
}

// MessageID uniquely identifies a cross-VM message
type MessageID [32]byte

// Address represents a universal address across all VMs
type Address struct {
	VM      VMType
	Address []byte // VM-specific address format
}

// AssetID identifies an asset across VMs
type AssetID [32]byte

// CVMMessage represents a cross-VM communication message
type CVMMessage struct {
	ID         MessageID       `json:"id"`
	SourceVM   VMType          `json:"source_vm"`
	SourceAddr Address         `json:"source_addr"`
	TargetVM   VMType          `json:"target_vm"`
	TargetAddr Address         `json:"target_addr"`
	Method     string          `json:"method"`
	Arguments  []byte          `json:"arguments"`
	Assets     []AssetTransfer `json:"assets,omitempty"`
	GasLimit   uint64          `json:"gas_limit"`
	Nonce      uint64          `json:"nonce"`
	Timestamp  time.Time       `json:"timestamp"`
}

// AssetTransfer represents an asset movement between VMs
type AssetTransfer struct {
	AssetID  AssetID `json:"asset_id"`
	Amount   uint64  `json:"amount"`
	Metadata []byte  `json:"metadata,omitempty"`
}

// CVMResponse represents the response from a cross-VM call
type CVMResponse struct {
	MessageID    MessageID     `json:"message_id"`
	Success      bool          `json:"success"`
	Result       []byte        `json:"result,omitempty"`
	Error        string        `json:"error,omitempty"`
	GasUsed      uint64        `json:"gas_used"`
	SubMessages  []CVMMessage  `json:"sub_messages,omitempty"`
	StateChanges []StateChange `json:"state_changes,omitempty"`
}

// StateChange represents a state modification in a VM
type StateChange struct {
	VM       VMType `json:"vm"`
	Key      []byte `json:"key"`
	OldValue []byte `json:"old_value"`
	NewValue []byte `json:"new_value"`
}

// ContractMetadata describes a contract in the registry
type ContractMetadata struct {
	Address      Address            `json:"address"`
	VM           VMType             `json:"vm"`
	Name         string             `json:"name"`
	Version      string             `json:"version"`
	ABI          json.RawMessage    `json:"abi"`
	Permissions  CrossVMPermissions `json:"permissions"`
	Owner        Address            `json:"owner"`
	CreatedAt    time.Time          `json:"created_at"`
	LastModified time.Time          `json:"last_modified"`
}

// CrossVMPermissions defines access control for cross-VM calls
type CrossVMPermissions struct {
	AllowedCallers []Address `json:"allowed_callers"`
	AllowedVMs     []VMType  `json:"allowed_vms"`
	AllowedMethods []string  `json:"allowed_methods"`
	RequireAuth    bool      `json:"require_auth"`
	RateLimit      uint64    `json:"rate_limit"` // calls per minute
}

// AtomicTransaction represents a cross-VM atomic transaction
type AtomicTransaction struct {
	ID          string           `json:"id"`
	Messages    []CVMMessage     `json:"messages"`
	State       TransactionState `json:"state"`
	Checkpoints []Checkpoint     `json:"checkpoints"`
	StartTime   time.Time        `json:"start_time"`
	EndTime     *time.Time       `json:"end_time,omitempty"`
	GasLimit    uint64           `json:"gas_limit"`
	GasUsed     uint64           `json:"gas_used"`
}

// TransactionState represents the state of an atomic transaction
type TransactionState uint8

const (
	TxStatePending TransactionState = iota
	TxStateExecuting
	TxStateCommitting
	TxStateCommitted
	TxStateRollingBack
	TxStateRolledBack
	TxStateFailed
)

// Checkpoint represents a state checkpoint for rollback
type Checkpoint struct {
	VM           VMType    `json:"vm"`
	CheckpointID string    `json:"checkpoint_id"`
	Timestamp    time.Time `json:"timestamp"`
	StateRoot    []byte    `json:"state_root"`
}

// LockInfo represents a locked asset in the bridge
type LockInfo struct {
	AssetID       AssetID   `json:"asset_id"`
	Amount        uint64    `json:"amount"`
	LockedBy      Address   `json:"locked_by"`
	LockedFor     Address   `json:"locked_for"`
	LockTime      time.Time `json:"lock_time"`
	UnlockTime    time.Time `json:"unlock_time"`
	TransactionID string    `json:"transaction_id"`
}

// CVMError represents a cross-VM error with context
type CVMError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	VM      VMType                 `json:"vm"`
	Details map[string]interface{} `json:"details,omitempty"`
}

func (e CVMError) Error() string {
	return e.Message
}

// Common CVM error codes
const (
	ErrCodeVMNotSupported   = "VM_NOT_SUPPORTED"
	ErrCodeContractNotFound = "CONTRACT_NOT_FOUND"
	ErrCodePermissionDenied = "PERMISSION_DENIED"
	ErrCodeInsufficientGas  = "INSUFFICIENT_GAS"
	ErrCodeExecutionFailed  = "EXECUTION_FAILED"
	ErrCodeRollbackFailed   = "ROLLBACK_FAILED"
	ErrCodeAssetLocked      = "ASSET_LOCKED"
	ErrCodeInvalidArguments = "INVALID_ARGUMENTS"
	ErrCodeTimeout          = "TIMEOUT"
	ErrCodeReentrancy       = "REENTRANCY_DETECTED"
)

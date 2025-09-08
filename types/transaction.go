// Package types provides transaction-specific type definitions
package types

import (
	"fmt"
	"time"

	"diamante/common"
)

// TransactionType represents the type of transaction
type TransactionType uint8

const (
	TransactionTypeTransfer TransactionType = iota
	TransactionTypeContractDeploy
	TransactionTypeContractCall
	TransactionTypeStake
	TransactionTypeUnstake
	TransactionTypeValidatorUpdate
	TransactionTypeGovernance
)

// TransactionStatus represents the status of a transaction
type TransactionStatus uint8

const (
	TransactionStatusPending TransactionStatus = iota
	TransactionStatusQueued
	TransactionStatusProcessing
	TransactionStatusExecuted
	TransactionStatusFailed
	TransactionStatusDropped
)

// TransactionPriority represents transaction priority levels
type TransactionPriority uint8

const (
	TransactionPriorityLow TransactionPriority = iota
	TransactionPriorityNormal
	TransactionPriorityHigh
	TransactionPriorityUrgent
)

// TypedTransaction represents a transaction with typed data
type TypedTransaction struct {
	Type      TransactionType       `json:"type"`
	ID        string                `json:"id"`
	From      string                `json:"from"`
	To        string                `json:"to,omitempty"`
	Value     uint64                `json:"value"`
	GasLimit  uint64                `json:"gas_limit"`
	GasPrice  uint64                `json:"gas_price"`
	Nonce     uint64                `json:"nonce"`
	Data      *TypedTransactionData `json:"data,omitempty"`
	Signature []byte                `json:"signature,omitempty"`
	Hash      []byte                `json:"hash,omitempty"`
	Timestamp int64                 `json:"timestamp"`
	Status    TransactionStatus     `json:"status"`
	Priority  TransactionPriority   `json:"priority"`
}

// TypedTransactionData contains typed transaction payload
type TypedTransactionData struct {
	ContractDeploy *ContractDeployData `json:"contract_deploy,omitempty"`
	ContractCall   *ContractCallData   `json:"contract_call,omitempty"`
	StakeData      *StakeData          `json:"stake_data,omitempty"`
	GovernanceData *GovernanceData     `json:"governance_data,omitempty"`
	RawData        []byte              `json:"raw_data,omitempty"`
	Metadata       map[string]*Value   `json:"metadata,omitempty"`
}

// ContractDeployData contains contract deployment information
type ContractDeployData struct {
	Runtime         string              `json:"runtime"` // EVM, WASM, Native
	ByteCode        []byte              `json:"byte_code"`
	ConstructorArgs []*ContractArgument `json:"constructor_args,omitempty"`
	Metadata        map[string]*Value   `json:"metadata,omitempty"`
}

// ContractCallData contains contract call information
type ContractCallData struct {
	ContractAddress string              `json:"contract_address"`
	Method          string              `json:"method"`
	Arguments       []*ContractArgument `json:"arguments,omitempty"`
	Metadata        map[string]*Value   `json:"metadata,omitempty"`
}

// StakeData contains staking transaction information
type StakeData struct {
	ValidatorAddress string            `json:"validator_address"`
	Amount           uint64            `json:"amount"`
	Duration         int64             `json:"duration,omitempty"`
	AutoCompound     bool              `json:"auto_compound"`
	Metadata         map[string]*Value `json:"metadata,omitempty"`
}

// GovernanceData contains governance transaction information
type GovernanceData struct {
	ProposalID   string            `json:"proposal_id,omitempty"`
	ProposalType string            `json:"proposal_type"`
	Title        string            `json:"title"`
	Description  string            `json:"description"`
	Parameters   map[string]*Value `json:"parameters,omitempty"`
	VoteOption   string            `json:"vote_option,omitempty"`
}

// TransactionResult represents the result of transaction execution
type TransactionResult struct {
	TransactionID string              `json:"transaction_id"`
	Success       bool                `json:"success"`
	GasUsed       uint64              `json:"gas_used"`
	Events        []*TransactionEvent `json:"events,omitempty"`
	Error         string              `json:"error,omitempty"`
	ReturnData    *Value              `json:"return_data,omitempty"`
	StateChanges  []*StateChange      `json:"state_changes,omitempty"`
}

// TransactionEvent represents an event emitted during transaction execution
type TransactionEvent struct {
	Type        string            `json:"type"`
	Source      string            `json:"source"`
	EventID     string            `json:"event_id"`
	Data        map[string]*Value `json:"data"`
	Timestamp   int64             `json:"timestamp"`
	BlockHeight uint64            `json:"block_height"`
}

// StateChange represents a state change during transaction execution
type StateChange struct {
	Type     StateChangeType `json:"type"`
	Address  string          `json:"address"`
	Key      string          `json:"key"`
	OldValue *Value          `json:"old_value,omitempty"`
	NewValue *Value          `json:"new_value,omitempty"`
}

// StateChangeType represents the type of state change
type StateChangeType uint8

const (
	StateChangeTypeCreate StateChangeType = iota
	StateChangeTypeUpdate
	StateChangeTypeDelete
)

// TransactionPoolItem represents a transaction in the pool with metadata
type TransactionPoolItem struct {
	Transaction  *TypedTransaction   `json:"transaction"`
	ReceivedAt   time.Time           `json:"received_at"`
	Priority     TransactionPriority `json:"priority"`
	Retries      int                 `json:"retries"`
	LastError    string              `json:"last_error,omitempty"`
	Dependencies []string            `json:"dependencies,omitempty"`
}

// TransactionBatch represents a batch of transactions
type TransactionBatch struct {
	ID           string              `json:"id"`
	Transactions []*TypedTransaction `json:"transactions"`
	CreatedAt    time.Time           `json:"created_at"`
	ProcessedAt  time.Time           `json:"processed_at,omitempty"`
	Status       BatchStatus         `json:"status"`
}

// BatchStatus represents the status of a transaction batch
type BatchStatus uint8

const (
	BatchStatusPending BatchStatus = iota
	BatchStatusProcessing
	BatchStatusCompleted
	BatchStatusFailed
)

// TransactionFilter represents filters for querying transactions
type TransactionFilter struct {
	Type        *TransactionType   `json:"type,omitempty"`
	Status      *TransactionStatus `json:"status,omitempty"`
	From        string             `json:"from,omitempty"`
	To          string             `json:"to,omitempty"`
	StartTime   int64              `json:"start_time,omitempty"`
	EndTime     int64              `json:"end_time,omitempty"`
	MinValue    uint64             `json:"min_value,omitempty"`
	MaxValue    uint64             `json:"max_value,omitempty"`
	BlockHeight uint64             `json:"block_height,omitempty"`
}

// TransactionMetrics contains transaction processing metrics
type TransactionMetrics struct {
	TotalReceived     uint64        `json:"total_received"`
	TotalProcessed    uint64        `json:"total_processed"`
	TotalFailed       uint64        `json:"total_failed"`
	TotalDropped      uint64        `json:"total_dropped"`
	AvgProcessingTime time.Duration `json:"avg_processing_time"`
	AvgGasUsed        uint64        `json:"avg_gas_used"`
	PoolSize          uint64        `json:"pool_size"`
	QueuedCount       uint64        `json:"queued_count"`
}

// TransactionValidationResult represents validation results
type TransactionValidationResult struct {
	Valid            bool     `json:"valid"`
	Errors           []string `json:"errors,omitempty"`
	Warnings         []string `json:"warnings,omitempty"`
	EstimatedGas     uint64   `json:"estimated_gas"`
	EstimatedCost    uint64   `json:"estimated_cost"`
	SimulationResult *Value   `json:"simulation_result,omitempty"`
}

// Helper functions for creating typed transactions

// NewTransferTransaction creates a new transfer transaction
func NewTransferTransaction(from, to string, value, gasLimit, gasPrice, nonce uint64) *TypedTransaction {
	return &TypedTransaction{
		Type:      TransactionTypeTransfer,
		From:      from,
		To:        to,
		Value:     value,
		GasLimit:  gasLimit,
		GasPrice:  gasPrice,
		Nonce:     nonce,
		Timestamp: common.ConsensusUnix(),
		Status:    TransactionStatusPending,
		Priority:  TransactionPriorityNormal,
	}
}

// NewContractDeployTransaction creates a new contract deployment transaction
func NewContractDeployTransaction(from string, bytecode []byte, args []*ContractArgument, gasLimit, gasPrice, nonce uint64) *TypedTransaction {
	return &TypedTransaction{
		Type:     TransactionTypeContractDeploy,
		From:     from,
		Value:    0,
		GasLimit: gasLimit,
		GasPrice: gasPrice,
		Nonce:    nonce,
		Data: &TypedTransactionData{
			ContractDeploy: &ContractDeployData{
				Runtime:         "EVM",
				ByteCode:        bytecode,
				ConstructorArgs: args,
				Metadata:        make(map[string]*Value),
			},
		},
		Timestamp: common.ConsensusUnix(),
		Status:    TransactionStatusPending,
		Priority:  TransactionPriorityNormal,
	}
}

// NewContractCallTransaction creates a new contract call transaction
func NewContractCallTransaction(from, contract, method string, args []*ContractArgument, value, gasLimit, gasPrice, nonce uint64) *TypedTransaction {
	return &TypedTransaction{
		Type:     TransactionTypeContractCall,
		From:     from,
		To:       contract,
		Value:    value,
		GasLimit: gasLimit,
		GasPrice: gasPrice,
		Nonce:    nonce,
		Data: &TypedTransactionData{
			ContractCall: &ContractCallData{
				ContractAddress: contract,
				Method:          method,
				Arguments:       args,
				Metadata:        make(map[string]*Value),
			},
		},
		Timestamp: common.ConsensusUnix(),
		Status:    TransactionStatusPending,
		Priority:  TransactionPriorityNormal,
	}
}

// ConvertArgument converts various types to ContractArgument
func ConvertArgument(name, argType string, value interface{}) (*ContractArgument, error) {
	var typedValue *Value

	switch argType {
	case "uint256", "uint", "int256", "int":
		switch v := value.(type) {
		case uint64:
			typedValue = Uint64ToValue(v)
		case int64:
			typedValue = Int64ToValue(v)
		case string:
			// Parse string to number
			typedValue = StringToValue(v)
		default:
			return nil, fmt.Errorf("invalid value type for %s", argType)
		}

	case "address":
		if addr, ok := value.(string); ok {
			typedValue = &Value{
				Type: ValueTypeAddress,
				Data: []byte(addr),
			}
		} else {
			return nil, fmt.Errorf("address must be string")
		}

	case "bool":
		if b, ok := value.(bool); ok {
			typedValue = BoolToValue(b)
		} else {
			return nil, fmt.Errorf("bool value required")
		}

	case "string":
		if s, ok := value.(string); ok {
			typedValue = StringToValue(s)
		} else {
			return nil, fmt.Errorf("string value required")
		}

	case "bytes", "bytes32":
		switch v := value.(type) {
		case []byte:
			typedValue = BytesToValue(v)
		case string:
			// Assume hex string
			typedValue = StringToValue(v)
		default:
			return nil, fmt.Errorf("bytes value required")
		}

	default:
		// For complex types, use JSON
		jsonValue, err := JSONToValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to JSON: %w", err)
		}
		typedValue = jsonValue
	}

	// Determine ValueType from argType string
	var valueType ValueType
	switch argType {
	case "uint256", "uint", "int256", "int":
		valueType = ValueTypeUint64
	case "address":
		valueType = ValueTypeAddress
	case "bool":
		valueType = ValueTypeBool
	case "string":
		valueType = ValueTypeString
	case "bytes", "bytes32":
		valueType = ValueTypeBytes
	default:
		valueType = ValueTypeJSON
	}

	return &ContractArgument{
		Name:  name,
		Type:  valueType,
		Value: typedValue,
	}, nil
}

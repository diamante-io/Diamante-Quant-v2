// Package common provides typed error context for consensus errors
package common

import (
	"encoding/json"
	"fmt"
	"time"
)

// ErrorContext provides typed context fields for consensus errors
type ErrorContext struct {
	// Core fields
	BlockHeight   uint64 `json:"block_height,omitempty"`
	BlockHash     string `json:"block_hash,omitempty"`
	ValidatorID   string `json:"validator_id,omitempty"`
	TransactionID string `json:"transaction_id,omitempty"`
	Round         uint64 `json:"round,omitempty"`
	Epoch         uint64 `json:"epoch,omitempty"`

	// Network fields
	PeerID         string        `json:"peer_id,omitempty"`
	RemoteAddr     string        `json:"remote_addr,omitempty"`
	NetworkLatency time.Duration `json:"network_latency,omitempty"`

	// State fields
	StateRoot     string `json:"state_root,omitempty"`
	PrevStateRoot string `json:"prev_state_root,omitempty"`
	StateVersion  uint64 `json:"state_version,omitempty"`

	// Performance fields
	Duration time.Duration `json:"duration,omitempty"`
	GasUsed  uint64        `json:"gas_used,omitempty"`
	TxCount  int           `json:"tx_count,omitempty"`

	// Error details
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorDetails string `json:"error_details,omitempty"`
	StackTrace   string `json:"stack_trace,omitempty"`

	// Additional typed fields
	IntValues    map[string]int64     `json:"int_values,omitempty"`
	StringValues map[string]string    `json:"string_values,omitempty"`
	BoolValues   map[string]bool      `json:"bool_values,omitempty"`
	TimeValues   map[string]time.Time `json:"time_values,omitempty"`
}

// NewErrorContext creates a new error context
func NewErrorContext() *ErrorContext {
	return &ErrorContext{
		IntValues:    make(map[string]int64),
		StringValues: make(map[string]string),
		BoolValues:   make(map[string]bool),
		TimeValues:   make(map[string]time.Time),
	}
}

// WithBlockHeight sets the block height
func (ec *ErrorContext) WithBlockHeight(height uint64) *ErrorContext {
	ec.BlockHeight = height
	return ec
}

// WithBlockHash sets the block hash
func (ec *ErrorContext) WithBlockHash(hash string) *ErrorContext {
	ec.BlockHash = hash
	return ec
}

// WithValidatorID sets the validator ID
func (ec *ErrorContext) WithValidatorID(id string) *ErrorContext {
	ec.ValidatorID = id
	return ec
}

// WithTransactionID sets the transaction ID
func (ec *ErrorContext) WithTransactionID(id string) *ErrorContext {
	ec.TransactionID = id
	return ec
}

// WithRound sets the consensus round
func (ec *ErrorContext) WithRound(round uint64) *ErrorContext {
	ec.Round = round
	return ec
}

// WithEpoch sets the epoch
func (ec *ErrorContext) WithEpoch(epoch uint64) *ErrorContext {
	ec.Epoch = epoch
	return ec
}

// WithPeerID sets the peer ID
func (ec *ErrorContext) WithPeerID(id string) *ErrorContext {
	ec.PeerID = id
	return ec
}

// WithDuration sets the operation duration
func (ec *ErrorContext) WithDuration(d time.Duration) *ErrorContext {
	ec.Duration = d
	return ec
}

// WithErrorCode sets the error code
func (ec *ErrorContext) WithErrorCode(code string) *ErrorContext {
	ec.ErrorCode = code
	return ec
}

// WithErrorDetails sets the error details
func (ec *ErrorContext) WithErrorDetails(details string) *ErrorContext {
	ec.ErrorDetails = details
	return ec
}

// AddInt adds an integer value
func (ec *ErrorContext) AddInt(key string, value int64) *ErrorContext {
	if ec.IntValues == nil {
		ec.IntValues = make(map[string]int64)
	}
	ec.IntValues[key] = value
	return ec
}

// AddString adds a string value
func (ec *ErrorContext) AddString(key string, value string) *ErrorContext {
	if ec.StringValues == nil {
		ec.StringValues = make(map[string]string)
	}
	ec.StringValues[key] = value
	return ec
}

// AddBool adds a boolean value
func (ec *ErrorContext) AddBool(key string, value bool) *ErrorContext {
	if ec.BoolValues == nil {
		ec.BoolValues = make(map[string]bool)
	}
	ec.BoolValues[key] = value
	return ec
}

// AddTime adds a time value
func (ec *ErrorContext) AddTime(key string, value time.Time) *ErrorContext {
	if ec.TimeValues == nil {
		ec.TimeValues = make(map[string]time.Time)
	}
	ec.TimeValues[key] = value
	return ec
}

// ToMap converts the context to a map for compatibility
func (ec *ErrorContext) ToMap() map[string]string {
	result := make(map[string]string)

	// Add core fields
	if ec.BlockHeight > 0 {
		result["block_height"] = fmt.Sprintf("%d", ec.BlockHeight)
	}
	if ec.BlockHash != "" {
		result["block_hash"] = ec.BlockHash
	}
	if ec.ValidatorID != "" {
		result["validator_id"] = ec.ValidatorID
	}
	if ec.TransactionID != "" {
		result["transaction_id"] = ec.TransactionID
	}
	if ec.Round > 0 {
		result["round"] = fmt.Sprintf("%d", ec.Round)
	}
	if ec.Epoch > 0 {
		result["epoch"] = fmt.Sprintf("%d", ec.Epoch)
	}

	// Add network fields
	if ec.PeerID != "" {
		result["peer_id"] = ec.PeerID
	}
	if ec.RemoteAddr != "" {
		result["remote_addr"] = ec.RemoteAddr
	}
	if ec.NetworkLatency > 0 {
		result["network_latency"] = ec.NetworkLatency.String()
	}

	// Add state fields
	if ec.StateRoot != "" {
		result["state_root"] = ec.StateRoot
	}
	if ec.PrevStateRoot != "" {
		result["prev_state_root"] = ec.PrevStateRoot
	}
	if ec.StateVersion > 0 {
		result["state_version"] = fmt.Sprintf("%d", ec.StateVersion)
	}

	// Add performance fields
	if ec.Duration > 0 {
		result["duration"] = ec.Duration.String()
	}
	if ec.GasUsed > 0 {
		result["gas_used"] = fmt.Sprintf("%d", ec.GasUsed)
	}
	if ec.TxCount > 0 {
		result["tx_count"] = fmt.Sprintf("%d", ec.TxCount)
	}

	// Add error details
	if ec.ErrorCode != "" {
		result["error_code"] = ec.ErrorCode
	}
	if ec.ErrorDetails != "" {
		result["error_details"] = ec.ErrorDetails
	}

	// Add typed values
	for k, v := range ec.IntValues {
		result[k] = fmt.Sprintf("%d", v)
	}
	for k, v := range ec.StringValues {
		result[k] = v
	}
	for k, v := range ec.BoolValues {
		result[k] = fmt.Sprintf("%t", v)
	}
	for k, v := range ec.TimeValues {
		result[k] = v.Format(time.RFC3339)
	}

	return result
}

// String returns a JSON representation of the context
func (ec *ErrorContext) String() string {
	b, err := json.Marshal(ec)
	if err != nil {
		return fmt.Sprintf("ErrorContext{error: %v}", err)
	}
	return string(b)
}

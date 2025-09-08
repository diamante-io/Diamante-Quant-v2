package network

import (
	"encoding/json"
	"fmt"

	"diamante/common"
	"diamante/consensus"
	dtypes "diamante/types"
)

// MessagePayload represents the base interface for all message payloads
type MessagePayload interface {
	GetType() string
	Validate() error
}

// MessageMetadata represents structured metadata for network messages
type MessageMetadata struct {
	Priority        int    `json:"priority,omitempty"`
	Encryption      string `json:"encryption,omitempty"`
	Compression     string `json:"compression,omitempty"`
	SourceNode      string `json:"source_node,omitempty"`
	DestinationNode string `json:"destination_node,omitempty"`
	NetworkID       string `json:"network_id,omitempty"`
	Version         string `json:"version,omitempty"`
	Signature       string `json:"signature,omitempty"`
}

// TransactionPayload represents transaction-related message payload
type TransactionPayload struct {
	TransactionID   string `json:"transaction_id"`
	FromAddress     string `json:"from_address"`
	ToAddress       string `json:"to_address"`
	Amount          uint64 `json:"amount"`
	Fee             uint64 `json:"fee"`
	Nonce           uint64 `json:"nonce"`
	Timestamp       int64  `json:"timestamp"`
	Data            []byte `json:"data,omitempty"`
	Signature       string `json:"signature"`
	TransactionType string `json:"transaction_type"`
	GasLimit        uint64 `json:"gas_limit,omitempty"`
	GasPrice        uint64 `json:"gas_price,omitempty"`
}

func (tp *TransactionPayload) GetType() string { return "transaction" }
func (tp *TransactionPayload) Validate() error {
	if tp.TransactionID == "" || tp.FromAddress == "" || tp.Signature == "" {
		return fmt.Errorf("invalid transaction payload: missing required fields")
	}
	return nil
}

// BlockPayload represents block-related message payload
type BlockPayload struct {
	BlockHash       string               `json:"block_hash"`
	BlockNumber     uint64               `json:"block_number"`
	ParentHash      string               `json:"parent_hash"`
	StateRoot       string               `json:"state_root"`
	TransactionRoot string               `json:"transaction_root"`
	Timestamp       int64                `json:"timestamp"`
	Proposer        string               `json:"proposer"`
	TransactionIDs  []string             `json:"transaction_ids"`
	Transactions    []common.Transaction `json:"transactions,omitempty"` // Full transaction objects
	Signature       string               `json:"signature"`
	Size            uint64               `json:"size"`
	GasUsed         uint64               `json:"gas_used"`
	GasLimit        uint64               `json:"gas_limit"`
}

func (bp *BlockPayload) GetType() string { return "block" }
func (bp *BlockPayload) Validate() error {
	if bp.BlockHash == "" || bp.ParentHash == "" || bp.Proposer == "" {
		return fmt.Errorf("invalid block payload: missing required fields")
	}
	return nil
}

// VotePayload represents consensus vote message payload
type VotePayload struct {
	VoteID       string `json:"vote_id"`
	VoterID      string `json:"voter_id"`
	BlockHash    string `json:"block_hash"`
	BlockNumber  uint64 `json:"block_number"`
	VoteType     string `json:"vote_type"` // "prevote", "precommit", "commit"
	Round        uint64 `json:"round"`
	Timestamp    int64  `json:"timestamp"`
	Signature    string `json:"signature"`
	ValidatorSet string `json:"validator_set"`
}

func (vp *VotePayload) GetType() string { return "vote" }
func (vp *VotePayload) Validate() error {
	if vp.VoteID == "" || vp.VoterID == "" || vp.BlockHash == "" || vp.Signature == "" {
		return fmt.Errorf("invalid vote payload: missing required fields")
	}
	return nil
}

// StatePayload represents state synchronization message payload
type StatePayload struct {
	StateHash   string            `json:"state_hash"`
	BlockHeight uint64            `json:"block_height"`
	StateData   map[string]string `json:"state_data"`
	Timestamp   int64             `json:"timestamp"`
	NodeID      string            `json:"node_id"`
	Signature   string            `json:"signature"`
	Compressed  bool              `json:"compressed"`
	ChunkIndex  int               `json:"chunk_index,omitempty"`
	TotalChunks int               `json:"total_chunks,omitempty"`
}

func (sp *StatePayload) GetType() string { return "state" }
func (sp *StatePayload) Validate() error {
	if sp.StateHash == "" || sp.NodeID == "" || sp.Signature == "" {
		return fmt.Errorf("invalid state payload: missing required fields")
	}
	return nil
}

// HeartbeatPayload represents heartbeat message payload
type HeartbeatPayload struct {
	NodeID        string `json:"node_id"`
	Timestamp     int64  `json:"timestamp"`
	BlockHeight   uint64 `json:"block_height"`
	LatestHash    string `json:"latest_hash,omitempty"`
	PeerCount     int    `json:"peer_count"`
	NetworkStatus string `json:"network_status"`
	Version       string `json:"version"`
	Signature     string `json:"signature"`
}

func (hp *HeartbeatPayload) GetType() string { return "heartbeat" }
func (hp *HeartbeatPayload) Validate() error {
	if hp.NodeID == "" || hp.Signature == "" {
		return fmt.Errorf("invalid heartbeat payload: missing required fields")
	}
	return nil
}

// SyncPayload represents synchronization request/response payload
type SyncPayload struct {
	RequestType string `json:"request_type"` // "blocks", "state", "peers"
	FromHeight  uint64 `json:"from_height,omitempty"`
	ToHeight    uint64 `json:"to_height,omitempty"`
	MaxItems    int    `json:"max_items,omitempty"`
	NodeID      string `json:"node_id"`
	Timestamp   int64  `json:"timestamp"`
	Signature   string `json:"signature"`
}

func (sp *SyncPayload) GetType() string { return "sync" }
func (sp *SyncPayload) Validate() error {
	if sp.RequestType == "" || sp.NodeID == "" || sp.Signature == "" {
		return fmt.Errorf("invalid sync payload: missing required fields")
	}
	return nil
}

// SyncBlocksPayload represents the response payload for block synchronization
type SyncBlocksPayload struct {
	Status     string         `json:"status"`
	Blocks     []common.Block `json:"blocks"`
	FromHeight uint64         `json:"fromHeight"`
	ToHeight   uint64         `json:"toHeight"`
}

// SyncBlocksEnvelope is the complete sync blocks message envelope
type SyncBlocksEnvelope struct {
	Type    string            `json:"type"` // "sync_blocks"
	Payload SyncBlocksPayload `json:"payload"`
}

// GetType implements MessagePayload interface
func (s *SyncBlocksEnvelope) GetType() string {
	return s.Type
}

// Validate implements MessagePayload interface
func (s *SyncBlocksEnvelope) Validate() error {
	if s.Type != "sync_blocks" {
		return fmt.Errorf("invalid type for sync blocks envelope: %s", s.Type)
	}
	return nil
}

// NewSyncBlocksEnvelope creates a typed sync blocks response envelope
func NewSyncBlocksEnvelope(status string, blocks []common.Block, fromHeight, toHeight uint64) *SyncBlocksEnvelope {
	return &SyncBlocksEnvelope{
		Type: "sync_blocks",
		Payload: SyncBlocksPayload{
			Status:     status,
			Blocks:     blocks,
			FromHeight: fromHeight,
			ToHeight:   toHeight,
		},
	}
}

// GenericPayload represents a generic message payload for arbitrary data
type GenericPayload struct {
	Data     *dtypes.GenericPayloadData `json:"data"`
	DataType string                     `json:"data_type"`
}

func (gp *GenericPayload) GetType() string { return gp.DataType }
func (gp *GenericPayload) Validate() error {
	if gp.DataType == "" {
		return fmt.Errorf("data type must be specified")
	}
	return nil
}

// NewGenericPayload creates a new generic payload
func NewGenericPayload(dataType string, data map[string]interface{}) *GenericPayload {
	// Convert map[string]interface{} to typed fields
	fields := make(map[string]*dtypes.Value)
	for k, v := range data {
		fields[k] = dtypes.InterfaceToValue(v)
	}

	return &GenericPayload{
		Data:     &dtypes.GenericPayloadData{Fields: fields},
		DataType: dataType,
	}
}

// Message is a network message with concrete payload types
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"` // Changed to RawMessage to fix unmarshaling

	// Additional fields for partition handling
	ID        string           `json:"id,omitempty"`
	Sender    string           `json:"sender,omitempty"`
	Receiver  string           `json:"receiver,omitempty"`
	Timestamp int64            `json:"timestamp,omitempty"`
	Metadata  *MessageMetadata `json:"metadata,omitempty"`
	IsRequest bool             `json:"isRequest,omitempty"`
	RequestID string           `json:"requestID,omitempty"`
}

// NewMessage creates a new message with concrete payload
func NewMessage(messageType string, sender string, payload MessagePayload) *Message {
	// Marshal payload to json.RawMessage
	payloadBytes, _ := json.Marshal(payload)

	return &Message{
		Type:      messageType,
		Payload:   payloadBytes,
		ID:        generateMessageID(),
		Sender:    sender,
		Timestamp: consensus.ConsensusUnixNano(),
		Metadata:  &MessageMetadata{},
		IsRequest: true,
	}
}

// NewResponseMessage creates a new response message with concrete payload
func NewResponseMessage(request *Message, payload MessagePayload) *Message {
	// Marshal payload to json.RawMessage
	payloadBytes, _ := json.Marshal(payload)

	return &Message{
		Type:      request.Type + "Response",
		Payload:   payloadBytes,
		ID:        generateMessageID(),
		Sender:    "", // Will be set by the sender
		Receiver:  request.Sender,
		Timestamp: consensus.ConsensusUnixNano(),
		Metadata:  &MessageMetadata{},
		IsRequest: false,
		RequestID: request.ID,
	}
}

// NewTransactionMessage creates a transaction message
func NewTransactionMessage(sender string, payload *TransactionPayload) *Message {
	return NewMessage(MessageTypeTransaction, sender, payload)
}

// NewBlockMessage creates a block message
func NewBlockMessage(sender string, payload *BlockPayload) *Message {
	return NewMessage(MessageTypeBlock, sender, payload)
}

// NewVoteMessage creates a vote message
func NewVoteMessage(sender string, payload *VotePayload) *Message {
	return NewMessage(MessageTypeVote, sender, payload)
}

// NewStateMessage creates a state message
func NewStateMessage(sender string, payload *StatePayload) *Message {
	return NewMessage("state", sender, payload)
}

// NewHeartbeatMessage creates a heartbeat message
func NewHeartbeatMessage(sender string, payload *HeartbeatPayload) *Message {
	return NewMessage(MessageTypeHeartbeat, sender, payload)
}

// NewSyncMessage creates a sync message
func NewSyncMessage(sender string, payload *SyncPayload) *Message {
	return NewMessage(MessageTypeSync, sender, payload)
}

// generateMessageID generates a unique message ID
func generateMessageID() string {
	nanoTime := consensus.ConsensusUnixNano()
	return fmt.Sprintf("%d-%d", nanoTime, nanoTime%1000)
}

// MessageType constants for partition handling
const (
	MessageTypeHeartbeat         = "heartbeat"
	MessageTypeHeartbeatResponse = "heartbeatResponse"
	MessageTypeStateRequest      = "stateRequest"
	MessageTypeStateResponse     = "stateResponse"
	MessageTypeTransaction       = "transaction"
	MessageTypeBlock             = "block"
	MessageTypeVote              = "vote"
	MessageTypeSync              = "sync"
)

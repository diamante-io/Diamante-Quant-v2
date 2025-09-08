package network

import (
	"encoding/json"
	"fmt"
	"time"

	"diamante/common"
)

// SimpleMessage is a temporary message format that works with JSON
type SimpleMessage struct {
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// BlockData represents block data in messages
type BlockData struct {
	Number       int                  `json:"number"`
	Hash         string               `json:"hash"`
	PreviousHash string               `json:"previousHash"`
	Timestamp    int64                `json:"timestamp"`
	Validator    string               `json:"validator"`
	Transactions []common.Transaction `json:"transactions"`
	Signature    []byte               `json:"signature"`
}

// NewSimpleMessage creates a new simple message
func NewSimpleMessage(msgType string, data interface{}) (*SimpleMessage, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	return &SimpleMessage{
		Type:      msgType,
		Timestamp: time.Now().Unix(),
		Data:      dataBytes,
	}, nil
}

// ParseBlockData parses block data from message
func (m *SimpleMessage) ParseBlockData() (*BlockData, error) {
	if m.Type != MessageTypeBlock {
		return nil, fmt.Errorf("not a block message")
	}

	var blockData BlockData
	if err := json.Unmarshal(m.Data, &blockData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block data: %w", err)
	}

	return &blockData, nil
}

// ToCommonBlock converts BlockData to common.Block
func (bd *BlockData) ToCommonBlock() *common.Block {
	return &common.Block{
		Number:       bd.Number,
		Hash:         bd.Hash,
		PreviousHash: bd.PreviousHash,
		Timestamp:    bd.Timestamp,
		Transactions: bd.Transactions,
		Validator:    bd.Validator,
		Signature:    bd.Signature,
	}
}

// BlockFromCommon creates BlockData from common.Block
func BlockFromCommon(block *common.Block) *BlockData {
	return &BlockData{
		Number:       block.Number,
		Hash:         block.Hash,
		PreviousHash: block.PreviousHash,
		Timestamp:    block.Timestamp,
		Validator:    block.Validator,
		Transactions: block.Transactions,
		Signature:    block.Signature,
	}
}

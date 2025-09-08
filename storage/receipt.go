// storage/receipt.go

package storage

import (
	"errors"
	"time"
)

// ReceiptMetadata represents typed metadata for transaction receipts
type ReceiptMetadata struct {
	ContractAddress   string   `json:"contractAddress,omitempty" bson:"contractAddress,omitempty"`
	ContractCreated   bool     `json:"contractCreated,omitempty" bson:"contractCreated,omitempty"`
	ReturnValue       string   `json:"returnValue,omitempty" bson:"returnValue,omitempty"`
	Error             string   `json:"error,omitempty" bson:"error,omitempty"`
	StateRoot         string   `json:"stateRoot,omitempty" bson:"stateRoot,omitempty"`
	CumulativeGasUsed uint64   `json:"cumulativeGasUsed,omitempty" bson:"cumulativeGasUsed,omitempty"`
	EffectiveGasPrice uint64   `json:"effectiveGasPrice,omitempty" bson:"effectiveGasPrice,omitempty"`
	Type              string   `json:"type,omitempty" bson:"type,omitempty"`
	Tags              []string `json:"tags,omitempty" bson:"tags,omitempty"`
}

// Receipt represents a transaction receipt with event logs
type Receipt struct {
	TxID        string          `json:"txId" bson:"txId"`
	BlockHeight uint64          `json:"blockHeight" bson:"blockHeight"`
	BlockHash   string          `json:"blockHash" bson:"blockHash"`
	Status      bool            `json:"status" bson:"status"`
	GasUsed     uint64          `json:"gasUsed" bson:"gasUsed"`
	Logs        []EventLog      `json:"logs" bson:"logs"`
	Metadata    ReceiptMetadata `json:"metadata" bson:"metadata"`
	CreatedAt   time.Time       `json:"createdAt" bson:"createdAt"`
}

// EventLog represents an event log in a receipt
type EventLog struct {
	Address     string   `json:"address" bson:"address"`
	Topics      []string `json:"topics" bson:"topics"`
	Data        []byte   `json:"data" bson:"data"`
	BlockNumber uint64   `json:"blockNumber" bson:"blockNumber"`
	TxHash      string   `json:"txHash" bson:"txHash"`
	Index       uint     `json:"index" bson:"index"`
}

// Validate performs basic validation on the receipt
func (r *Receipt) Validate() error {
	if r.TxID == "" {
		return errors.New("receipt must have a transaction ID")
	}

	if r.BlockHash == "" {
		return errors.New("receipt must have a block hash")
	}

	if r.BlockHeight == 0 {
		return errors.New("receipt must have a valid block height")
	}

	// Validate each log
	for i, log := range r.Logs {
		if err := log.Validate(); err != nil {
			return errors.New("invalid log at index " + string(rune(i)) + ": " + err.Error())
		}
	}

	return nil
}

// Validate performs basic validation on the event log
func (el *EventLog) Validate() error {
	if el.Address == "" {
		return errors.New("event log must have an address")
	}

	if el.TxHash == "" {
		return errors.New("event log must have a transaction hash")
	}

	if el.BlockNumber == 0 {
		return errors.New("event log must have a valid block number")
	}

	return nil
}

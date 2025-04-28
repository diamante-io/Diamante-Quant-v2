package storage

import (
	"time"
)

// Block represents a blockchain block for storage purposes.
type Block struct {
	Number        uint64        `json:"number"`
	Timestamp     time.Time     `json:"timestamp"`
	Transactions  []Transaction `json:"transactions"`
	PrevBlockHash string        `json:"prev_block_hash"`
	BlockHash     string        `json:"block_hash"`
}

// Transaction represents a simple blockchain transaction.
type Transaction struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Receiver  string    `json:"receiver"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// Store defines the interface for block storage.
type Store interface {
	SaveBlock(block *Block) error
	GetBlock(blockNumber uint64) (*Block, error)
}

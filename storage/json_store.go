package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Transaction represents a simple blockchain transaction.
type Transaction struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Receiver  string    `json:"receiver"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// Block represents a blockchain block.
type Block struct {
	Number        uint64        `json:"number"`
	Timestamp     time.Time     `json:"timestamp"`
	Transactions  []Transaction `json:"transactions"`
	PrevBlockHash string        `json:"prev_block_hash"`
	BlockHash     string        `json:"block_hash"`
}

// Store defines the interface for block storage.
type Store interface {
	SaveBlock(block *Block) error
	GetBlock(blockNumber uint64) (*Block, error)
}

// JSONStore is a concrete implementation of Store that persists blocks as JSON files.
type JSONStore struct {
	blocksDir string
	mu        sync.RWMutex
}

// NewJSONStore creates a new JSONStore instance and ensures the blocks directory exists.
func NewJSONStore(blocksDir string) (*JSONStore, error) {
	if err := os.MkdirAll(blocksDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blocks directory: %v", err)
	}
	return &JSONStore{blocksDir: blocksDir}, nil
}

// blockFilePath returns the file path for a given block number.
func (js *JSONStore) blockFilePath(blockNumber uint64) string {
	fileName := strconv.FormatUint(blockNumber, 10) + ".json"
	return filepath.Join(js.blocksDir, fileName)
}

// SaveBlock serializes a Block to JSON and writes it to a file.
func (js *JSONStore) SaveBlock(block *Block) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	data, err := json.MarshalIndent(block, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal block: %v", err)
	}

	filePath := js.blockFilePath(block.Number)
	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write block file: %v", err)
	}
	return nil
}

// GetBlock reads a block file, unmarshals the JSON, and returns a Block.
func (js *JSONStore) GetBlock(blockNumber uint64) (*Block, error) {
	js.mu.RLock()
	defer js.mu.RUnlock()

	filePath := js.blockFilePath(blockNumber)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read block file: %v", err)
	}
	var block Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %v", err)
	}
	return &block, nil
}

package ledger

import (
	"diamante/common"
	"encoding/json"
	"fmt"
	"sync"
)

// BlockStore holds committed blocks and a set of known transaction IDs.
type BlockStore struct {
	mu     sync.RWMutex
	blocks map[uint64]common.Block // keys are uint64
	allTx  map[string]bool
}

// NewBlockStore creates a new, in-memory block store.
func NewBlockStore() *BlockStore {
	return &BlockStore{
		blocks: make(map[uint64]common.Block),
		allTx:  make(map[string]bool),
	}
}

// StoreBlock adds a block by using its Number cast to uint64.
// Transactions are recorded in allTx as known.
func (bs *BlockStore) StoreBlock(block common.Block) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Convert block.Number (an int) to uint64
	bs.blocks[uint64(block.Number)] = block

	for _, tx := range block.Transactions {
		bs.allTx[tx.ID] = true
	}
	return nil
}

// RecordTransaction records a transaction that might not yet be in a block.
func (bs *BlockStore) RecordTransaction(tx common.Transaction) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if _, exists := bs.allTx[tx.ID]; exists {
		return nil // already known
	}
	bs.allTx[tx.ID] = true
	return nil
}

// HasTransaction checks whether a transaction ID is known (in a block or recorded).
func (bs *BlockStore) HasTransaction(txID string) bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	_, ok := bs.allTx[txID]
	return ok
}

// Snapshot serializes the entire block store’s data into JSON.
func (bs *BlockStore) Snapshot() ([]byte, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	snap := map[string]interface{}{
		"blocks": bs.blocks,
		"allTx":  bs.allTx,
	}
	return json.Marshal(snap)
}

// Restore loads the block store from a snapshot (JSON).
func (bs *BlockStore) Restore(data []byte) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var snap map[string]interface{}
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("block store restore invalid JSON: %w", err)
	}

	blocksRaw, ok := snap["blocks"]
	if !ok {
		return fmt.Errorf("snapshot missing 'blocks' key")
	}
	allTxRaw, ok2 := snap["allTx"]
	if !ok2 {
		return fmt.Errorf("snapshot missing 'allTx' key")
	}

	// Rebuild blocks
	blocksMap := make(map[string]json.RawMessage)
	blocksData, _ := json.Marshal(blocksRaw)
	if err := json.Unmarshal(blocksData, &blocksMap); err != nil {
		return fmt.Errorf("failed to unmarshal blocksMap: %w", err)
	}

	newBlocks := make(map[uint64]common.Block)
	for k, v := range blocksMap {
		var b common.Block
		if err := json.Unmarshal(v, &b); err != nil {
			return fmt.Errorf("unmarshal block %s: %w", k, err)
		}
		// cast b.Number -> uint64
		newBlocks[uint64(b.Number)] = b
	}

	// Rebuild allTx
	allTxMap := make(map[string]bool)
	allTxData, _ := json.Marshal(allTxRaw)
	if err := json.Unmarshal(allTxData, &allTxMap); err != nil {
		return fmt.Errorf("failed to unmarshal allTx: %w", err)
	}

	bs.blocks = newBlocks
	bs.allTx = allTxMap
	return nil
}

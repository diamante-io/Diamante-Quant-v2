package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"diamante/common"
	"github.com/sirupsen/logrus"
)

// BlockPersistence provides on-disk storage for blocks.
type BlockPersistence struct {
	mu         sync.RWMutex
	baseDir    string
	indexFile  string
	blockIndex map[uint64]string
	latest     uint64
	logger     *logrus.Logger
}

// NewBlockPersistence creates a new BlockPersistence instance.
func NewBlockPersistence(baseDir string, logger *logrus.Logger) (*BlockPersistence, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create block directory: %w", err)
	}

	bp := &BlockPersistence{
		baseDir:    baseDir,
		indexFile:  filepath.Join(baseDir, "block_index.json"),
		blockIndex: make(map[uint64]string),
		logger:     logger,
	}

	if err := bp.loadIndex(); err != nil {
		return nil, err
	}

	return bp, nil
}

// SaveBlock stores a block on disk and updates the index.
func (bp *BlockPersistence) SaveBlock(block *common.Block) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	filename := fmt.Sprintf("block_%08d.json", block.Number)
	path := filepath.Join(bp.baseDir, filename)

	data, err := json.MarshalIndent(block, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize block: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("failed to write block file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		if removeErr := os.Remove(tmp); removeErr != nil {
			bp.logger.WithError(removeErr).Warn("Failed to cleanup temporary file after rename failure")
		}
		return fmt.Errorf("failed to finalize block file: %w", err)
	}

	bp.blockIndex[uint64(block.Number)] = filename
	bp.latest = uint64(block.Number)

	if err := bp.saveIndex(); err != nil {
		return fmt.Errorf("failed to update block index: %w", err)
	}

	latestLink := filepath.Join(bp.baseDir, "latest_block.json")
	if removeErr := os.Remove(latestLink); removeErr != nil && !os.IsNotExist(removeErr) {
		bp.logger.WithError(removeErr).Warn("Failed to remove existing latest block symlink")
	}
	if err := os.Symlink(filename, latestLink); err != nil {
		bp.logger.Warnf("Failed to create latest block symlink: %v", err)
	}

	return nil
}

// LoadBlock loads a block from disk by height.
func (bp *BlockPersistence) LoadBlock(blockNumber uint64) (*common.Block, error) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	fname, ok := bp.blockIndex[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block %d not found", blockNumber)
	}
	path := filepath.Join(bp.baseDir, fname)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read block file: %w", err)
	}
	var b common.Block
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("failed to deserialize block: %w", err)
	}
	return &b, nil
}

// loadIndex reads the block index from disk if it exists.
func (bp *BlockPersistence) loadIndex() error {
	data, err := os.ReadFile(bp.indexFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read index file: %w", err)
	}
	var stored struct {
		Latest uint64            `json:"latest"`
		Blocks map[string]string `json:"blocks"`
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("failed to decode index file: %w", err)
	}
	for k, v := range stored.Blocks {
		num, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			continue
		}
		bp.blockIndex[num] = v
	}
	bp.latest = stored.Latest
	return nil
}

// saveIndex writes the current block index to disk atomically.
func (bp *BlockPersistence) saveIndex() error {
	idx := struct {
		Latest uint64            `json:"latest"`
		Blocks map[string]string `json:"blocks"`
	}{
		Latest: bp.latest,
		Blocks: make(map[string]string, len(bp.blockIndex)),
	}
	for k, v := range bp.blockIndex {
		idx.Blocks[strconv.FormatUint(k, 10)] = v
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	tmp := bp.indexFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, bp.indexFile)
}

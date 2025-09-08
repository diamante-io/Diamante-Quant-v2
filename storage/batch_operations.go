package storage

import (
	"fmt"
	"sync"
	"time"

	"diamante/common"

	"github.com/sirupsen/logrus"
)

// BatchOperationConfig defines configuration for batch operations
type BatchOperationConfig struct {
	MaxBatchSize    int           // Maximum number of items per batch
	BatchTimeout    time.Duration // Maximum time to wait before processing a batch
	MaxRetries      int           // Maximum number of retries for failed batches
	RetryDelay      time.Duration // Delay between retries
	ParallelWorkers int           // Number of parallel workers for batch processing
}

// DefaultBatchOperationConfig returns default batch operation configuration
func DefaultBatchOperationConfig() *BatchOperationConfig {
	return &BatchOperationConfig{
		MaxBatchSize:    1000,
		BatchTimeout:    100 * time.Millisecond,
		MaxRetries:      3,
		RetryDelay:      500 * time.Millisecond,
		ParallelWorkers: 4,
	}
}

// BatchBlockSaver provides optimized batch saving for blocks and transactions
type BatchBlockSaver struct {
	store         LedgerStore
	config        *BatchOperationConfig
	logger        *logrus.Logger
	blockQueue    chan *blockWithContext
	workerPool    sync.WaitGroup
	stopCh        chan struct{}
	isRunning     bool
	mu            sync.Mutex
	pendingBlocks map[uint64]*blockWithContext // Track pending blocks
}

// blockWithContext wraps a block with additional context
type blockWithContext struct {
	block        *common.Block
	receipts     []*Receipt
	stateUpdates map[string][]byte
	resultCh     chan error
}

// NewBatchBlockSaver creates a new batch block saver
func NewBatchBlockSaver(store LedgerStore, config *BatchOperationConfig, logger *logrus.Logger) *BatchBlockSaver {
	if config == nil {
		config = DefaultBatchOperationConfig()
	}
	if logger == nil {
		logger = logrus.New()
	}

	return &BatchBlockSaver{
		store:         store,
		config:        config,
		logger:        logger,
		blockQueue:    make(chan *blockWithContext, config.MaxBatchSize*2),
		stopCh:        make(chan struct{}),
		pendingBlocks: make(map[uint64]*blockWithContext),
	}
}

// Start begins the batch processing
func (bbs *BatchBlockSaver) Start() error {
	bbs.mu.Lock()
	defer bbs.mu.Unlock()

	if bbs.isRunning {
		return fmt.Errorf("batch block saver already running")
	}

	bbs.isRunning = true

	// Start worker goroutines
	for i := 0; i < bbs.config.ParallelWorkers; i++ {
		bbs.workerPool.Add(1)
		go bbs.batchWorker(i)
	}

	return nil
}

// Stop gracefully shuts down the batch processor
func (bbs *BatchBlockSaver) Stop() error {
	bbs.mu.Lock()
	if !bbs.isRunning {
		bbs.mu.Unlock()
		return nil
	}
	bbs.isRunning = false
	bbs.mu.Unlock()

	// Signal workers to stop
	close(bbs.stopCh)

	// Wait for workers to finish
	bbs.workerPool.Wait()

	// Process any remaining blocks
	bbs.processPendingBlocks()

	return nil
}

// SaveBlockBatch queues a block for batch saving
func (bbs *BatchBlockSaver) SaveBlockBatch(
	block *common.Block,
	receipts []*Receipt,
	stateUpdates map[string][]byte,
) error {
	bbs.mu.Lock()
	if !bbs.isRunning {
		bbs.mu.Unlock()
		return fmt.Errorf("batch block saver not running")
	}
	bbs.mu.Unlock()

	ctx := &blockWithContext{
		block:        block,
		receipts:     receipts,
		stateUpdates: stateUpdates,
		resultCh:     make(chan error, 1),
	}

	select {
	case bbs.blockQueue <- ctx:
		// Block queued successfully
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout queuing block for batch save")
	}

	// Wait for result
	select {
	case err := <-ctx.resultCh:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for block save result")
	}
}

// batchWorker processes batches of blocks
func (bbs *BatchBlockSaver) batchWorker(workerID int) {
	defer bbs.workerPool.Done()

	ticker := time.NewTicker(bbs.config.BatchTimeout)
	defer ticker.Stop()

	batch := make([]*blockWithContext, 0, bbs.config.MaxBatchSize)

	for {
		select {
		case <-bbs.stopCh:
			// Process remaining batch before exiting
			if len(batch) > 0 {
				bbs.processBatch(batch)
			}
			return

		case block := <-bbs.blockQueue:
			batch = append(batch, block)
			if len(batch) >= bbs.config.MaxBatchSize {
				bbs.processBatch(batch)
				batch = make([]*blockWithContext, 0, bbs.config.MaxBatchSize)
			}

		case <-ticker.C:
			// Process partial batch on timeout
			if len(batch) > 0 {
				bbs.processBatch(batch)
				batch = make([]*blockWithContext, 0, bbs.config.MaxBatchSize)
			}
		}
	}
}

// processBatch processes a batch of blocks atomically
func (bbs *BatchBlockSaver) processBatch(batch []*blockWithContext) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	// Create write batch
	writeBatch := NewWriteBatch()

	// Group operations by type
	for _, ctx := range batch {
		// Add block
		writeBatch.AddBlock(ctx.block)

		// Add transactions
		for _, tx := range ctx.block.Transactions {
			txCopy := tx // Create copy to avoid pointer issues
			writeBatch.AddTransaction(&txCopy)
		}

		// Add receipts
		for _, receipt := range ctx.receipts {
			writeBatch.AddReceipt(receipt)
		}

		// Add state updates
		for key, value := range ctx.stateUpdates {
			writeBatch.SetState(key, value)
		}
	}

	// Execute batch write with retries
	var err error
	for retry := 0; retry <= bbs.config.MaxRetries; retry++ {
		if retry > 0 {
			time.Sleep(bbs.config.RetryDelay)
			bbs.logger.WithFields(logrus.Fields{
				"retry":      retry,
				"batch_size": len(batch),
			}).Warn("Retrying batch write")
		}

		err = bbs.executeBatchWrite(writeBatch)
		if err == nil {
			break
		}

		bbs.logger.WithError(err).WithFields(logrus.Fields{
			"retry":      retry,
			"batch_size": len(batch),
		}).Error("Batch write failed")
	}

	// Send results to waiting goroutines
	for _, ctx := range batch {
		ctx.resultCh <- err
		close(ctx.resultCh)
	}

	duration := time.Since(start)
	bbs.logger.WithFields(logrus.Fields{
		"batch_size":   len(batch),
		"duration_ms":  duration.Milliseconds(),
		"success":      err == nil,
		"blocks_range": fmt.Sprintf("%d-%d", batch[0].block.Number, batch[len(batch)-1].block.Number),
	}).Info("Batch processed")
}

// executeBatchWrite executes the batch write operation
func (bbs *BatchBlockSaver) executeBatchWrite(batch *WriteBatch) error {
	// Check if store supports batch operations
	if batchStore, ok := bbs.store.(interface {
		WriteBatch(batch WriteBatch) error
	}); ok {
		return batchStore.WriteBatch(*batch)
	}

	// Fallback to individual operations
	bbs.logger.Warn("Store doesn't support batch operations, falling back to sequential writes")

	// Save blocks
	for _, block := range batch.Blocks {
		if err := bbs.store.SaveBlock(block); err != nil {
			return fmt.Errorf("failed to save block %d: %w", block.Number, err)
		}
	}

	// Save transactions
	for _, tx := range batch.Transactions {
		// Find block height for transaction
		blockHeight := 0
		for _, block := range batch.Blocks {
			for _, blockTx := range block.Transactions {
				if blockTx.ID == tx.ID {
					blockHeight = block.Number
					break
				}
			}
			if blockHeight > 0 {
				break
			}
		}

		if err := bbs.store.SaveTransaction(tx, blockHeight); err != nil {
			return fmt.Errorf("failed to save transaction %s: %w", tx.ID, err)
		}
	}

	// Save accounts
	for _, account := range batch.Accounts {
		if err := bbs.store.SaveAccount(account); err != nil {
			return fmt.Errorf("failed to save account %s: %w", account.ID, err)
		}
	}

	// Save state
	for key, value := range batch.StateWrites {
		if err := bbs.store.SaveState([]byte(key), value); err != nil {
			return fmt.Errorf("failed to save state %s: %w", key, err)
		}
	}

	return nil
}

// processPendingBlocks processes any remaining blocks when shutting down
func (bbs *BatchBlockSaver) processPendingBlocks() {
	bbs.mu.Lock()
	defer bbs.mu.Unlock()

	if len(bbs.pendingBlocks) == 0 {
		return
	}

	batch := make([]*blockWithContext, 0, len(bbs.pendingBlocks))
	for _, block := range bbs.pendingBlocks {
		batch = append(batch, block)
	}

	bbs.logger.WithField("pending_count", len(batch)).Info("Processing pending blocks on shutdown")
	bbs.processBatch(batch)

	// Clear pending blocks
	bbs.pendingBlocks = make(map[uint64]*blockWithContext)
}

// SaveBlockWithTransactions provides a convenience method for saving a block with its transactions
func SaveBlockWithTransactions(
	store LedgerStore,
	block *common.Block,
	receipts []*Receipt,
) error {
	// Create write batch
	batch := NewWriteBatch()

	// Add block
	batch.AddBlock(block)

	// Add transactions
	for _, tx := range block.Transactions {
		txCopy := tx // Create copy to avoid pointer issues
		batch.AddTransaction(&txCopy)
	}

	// Add receipts
	for _, receipt := range receipts {
		batch.AddReceipt(receipt)
	}

	// Execute batch write
	if batchStore, ok := store.(interface {
		WriteBatch(batch WriteBatch) error
	}); ok {
		return batchStore.WriteBatch(*batch)
	}

	// Fallback to sequential saves
	if err := store.SaveBlock(block); err != nil {
		return err
	}

	for _, tx := range block.Transactions {
		txCopy := tx
		if err := store.SaveTransaction(&txCopy, block.Number); err != nil {
			return err
		}
	}

	return nil
}

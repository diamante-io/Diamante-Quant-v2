package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"github.com/sirupsen/logrus"
)

// ArchivalServiceConfig holds configuration for the archival service
type ArchivalServiceConfig struct {
	// Source storage (primary LMDB)
	SourceConnectionString string

	// Target storage (MongoDB archive)
	ArchiveHost     string
	ArchiveDatabase string

	// Archival settings
	ArchivalThreshold  uint64        // Blocks older than this height
	BatchSize          int           // Number of blocks per batch
	WorkerCount        int           // Number of concurrent workers
	CheckInterval      time.Duration // How often to check for blocks to archive
	RetentionPeriod    time.Duration // How long to keep blocks in primary after archival
	VerifyAfterArchive bool          // Verify blocks after archiving
	DeleteAfterArchive bool          // Delete from primary after successful archive

	// Performance settings
	RateLimit          int           // Max blocks per second
	ConnectionPoolSize int           // MongoDB connection pool size
	WriteTimeout       time.Duration // Timeout for write operations
}

// DefaultArchivalServiceConfig returns default configuration
func DefaultArchivalServiceConfig() *ArchivalServiceConfig {
	return &ArchivalServiceConfig{
		ArchiveHost:        "mongodb://localhost:27017",
		ArchiveDatabase:    "diamante_archive",
		ArchivalThreshold:  7 * 24 * 60 * 60 / 2, // 1 week (2s blocks)
		BatchSize:          100,
		WorkerCount:        4,
		CheckInterval:      5 * time.Minute,
		RetentionPeriod:    24 * time.Hour, // Keep for 1 day after archival
		VerifyAfterArchive: true,
		DeleteAfterArchive: false,
		RateLimit:          1000, // 1000 blocks/second
		ConnectionPoolSize: 10,
		WriteTimeout:       30 * time.Second,
	}
}

// ArchivalService manages the archival of old blocks to MongoDB
type ArchivalService struct {
	config  *ArchivalServiceConfig
	source  LedgerStore
	archive *MongoAdapter
	logger  *logrus.Logger

	// State management
	mu        sync.RWMutex
	isRunning atomic.Bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// Progress tracking
	lastArchived  atomic.Uint64
	currentHeight atomic.Uint64

	// Metrics
	metrics struct {
		blocksArchived   atomic.Uint64
		txArchived       atomic.Uint64
		receiptsArchived atomic.Uint64
		errors           atomic.Uint64
		verifyFailures   atomic.Uint64
		deleteFailures   atomic.Uint64
		duration         atomic.Int64 // microseconds
	}

	// Rate limiting
	rateLimiter chan struct{}

	// Work channels
	workQueue   chan *archivalJob
	resultQueue chan *archivalResult
}

// archivalJob represents a block archival job
type archivalJob struct {
	Height     int
	Block      *common.Block
	RetryCount int
}

// archivalResult represents the result of an archival job
type archivalResult struct {
	Job       *archivalJob
	Success   bool
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Verified  bool
}

// NewArchivalService creates a new archival service
func NewArchivalService(config *ArchivalServiceConfig, source LedgerStore, logger *logrus.Logger) (*ArchivalService, error) {
	if config == nil {
		config = DefaultArchivalServiceConfig()
	}

	if source == nil {
		return nil, errors.New("source storage cannot be nil")
	}

	if logger == nil {
		logger = logrus.New()
	}

	// Create archive adapter
	archive, err := NewMongoAdapter(
		config.ArchiveHost,
		config.ArchiveDatabase,
		logger,
		1000, // Small cache for archive
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive adapter: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	svc := &ArchivalService{
		config:      config,
		source:      source,
		archive:     archive,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		rateLimiter: make(chan struct{}, config.RateLimit),
		workQueue:   make(chan *archivalJob, config.BatchSize*2),
		resultQueue: make(chan *archivalResult, config.BatchSize),
	}

	// Initialize rate limiter
	go svc.refillRateLimiter()

	return svc, nil
}

// Start begins the archival service
func (svc *ArchivalService) Start() error {
	if svc.isRunning.Load() {
		return errors.New("archival service already running")
	}

	// Open connections
	if err := svc.source.Open(); err != nil {
		return fmt.Errorf("failed to open source storage: %w", err)
	}

	if err := svc.archive.Open(); err != nil {
		return fmt.Errorf("failed to open archive storage: %w", err)
	}

	// Get current state
	latestBlock, err := svc.source.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}
	svc.currentHeight.Store(uint64(latestBlock.Number))

	// Get last archived height from archive metadata
	lastArchived, err := svc.getLastArchivedHeight()
	if err != nil {
		svc.logger.WithError(err).Warn("Failed to get last archived height, starting from threshold")
		// Prevent underflow when height is less than archival threshold
		if svc.currentHeight.Load() > svc.config.ArchivalThreshold {
			lastArchived = svc.currentHeight.Load() - svc.config.ArchivalThreshold
		} else {
			lastArchived = 0
		}
	}
	svc.lastArchived.Store(lastArchived)

	svc.isRunning.Store(true)

	// Start workers
	for i := 0; i < svc.config.WorkerCount; i++ {
		svc.wg.Add(1)
		go svc.archivalWorker(i)
	}

	// Start result processor
	svc.wg.Add(1)
	go svc.resultProcessor()

	// Start scheduler
	svc.wg.Add(1)
	go svc.scheduler()

	svc.logger.Info("Archival service started",
		"workers", svc.config.WorkerCount,
		"lastArchived", lastArchived,
		"currentHeight", svc.currentHeight.Load())

	return nil
}

// Stop gracefully stops the archival service
func (svc *ArchivalService) Stop() error {
	if !svc.isRunning.Load() {
		return nil
	}

	svc.logger.Info("Stopping archival service...")

	// Cancel context
	svc.cancel()

	// Close channels
	close(svc.workQueue)

	// Wait for workers to finish
	svc.wg.Wait()

	// Close connections
	var errs []error
	if err := svc.archive.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close archive: %w", err))
	}

	if err := svc.source.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close source: %w", err))
	}

	svc.isRunning.Store(false)

	// Log final metrics
	svc.logMetrics()

	if len(errs) > 0 {
		return fmt.Errorf("errors during stop: %v", errs)
	}

	return nil
}

// scheduler periodically checks for blocks to archive
func (svc *ArchivalService) scheduler() {
	defer svc.wg.Done()

	ticker := time.NewTicker(svc.config.CheckInterval)
	defer ticker.Stop()

	// Initial scan
	svc.scanAndQueueBlocks()

	for {
		select {
		case <-svc.ctx.Done():
			return

		case <-ticker.C:
			// Update current height
			if latestBlock, err := svc.source.GetLatestBlock(); err == nil {
				svc.currentHeight.Store(uint64(latestBlock.Number))
			}

			// Scan for blocks to archive
			svc.scanAndQueueBlocks()
		}
	}
}

// scanAndQueueBlocks scans for blocks that need archiving
func (svc *ArchivalService) scanAndQueueBlocks() {
	currentHeight := svc.currentHeight.Load()
	lastArchived := svc.lastArchived.Load()
	targetHeight := currentHeight - svc.config.ArchivalThreshold

	if lastArchived >= targetHeight {
		return // Nothing to archive
	}

	svc.logger.Debug("Scanning for blocks to archive",
		"lastArchived", lastArchived,
		"targetHeight", targetHeight,
		"toArchive", targetHeight-lastArchived)

	// Queue blocks for archival
	queued := 0
	for height := lastArchived + 1; height <= targetHeight && queued < svc.config.BatchSize; height++ {
		select {
		case <-svc.ctx.Done():
			return
		default:
			// Get block
			block, err := svc.source.GetBlock(height)
			if err != nil {
				svc.logger.WithError(err).Error("Failed to get block for archival", "height", height)
				continue
			}

			// Queue for archival
			job := &archivalJob{
				Height: int(height),
				Block:  block,
			}

			select {
			case svc.workQueue <- job:
				queued++
			case <-svc.ctx.Done():
				return
			}
		}
	}

	if queued > 0 {
		svc.logger.Info("Queued blocks for archival", "count", queued)
	}
}

// archivalWorker processes archival jobs
func (svc *ArchivalService) archivalWorker(id int) {
	defer svc.wg.Done()

	svc.logger.Debug("Archival worker started", "id", id)

	for job := range svc.workQueue {
		// Rate limiting
		select {
		case <-svc.rateLimiter:
		case <-svc.ctx.Done():
			return
		}

		// Process job
		result := svc.processArchivalJob(job)

		// Send result
		select {
		case svc.resultQueue <- result:
		case <-svc.ctx.Done():
			return
		}
	}
}

// processArchivalJob archives a single block
func (svc *ArchivalService) processArchivalJob(job *archivalJob) *archivalResult {
	startTime := time.Now()
	result := &archivalResult{
		Job:       job,
		StartTime: startTime,
	}

	// Archive block
	if err := svc.archiveBlock(job.Block); err != nil {
		result.Success = false
		result.Error = err
		result.EndTime = time.Now()
		return result
	}

	// Verify if enabled
	if svc.config.VerifyAfterArchive {
		if err := svc.verifyArchivedBlock(job.Block); err != nil {
			svc.metrics.verifyFailures.Add(1)
			result.Success = false
			result.Error = fmt.Errorf("verification failed: %w", err)
			result.EndTime = time.Now()
			return result
		}
		result.Verified = true
	}

	// Delete from primary if enabled
	if svc.config.DeleteAfterArchive {
		// Only delete if past retention period
		blockAge := time.Since(time.Unix(job.Block.Timestamp, 0))
		if blockAge > svc.config.RetentionPeriod {
			if err := svc.deleteFromPrimary(job.Block); err != nil {
				svc.metrics.deleteFailures.Add(1)
				svc.logger.WithError(err).Warn("Failed to delete block from primary", "height", job.Height)
				// Don't fail the job for delete failures
			}
		}
	}

	result.Success = true
	result.EndTime = time.Now()

	// Update metrics
	svc.metrics.blocksArchived.Add(1)
	svc.metrics.txArchived.Add(uint64(len(job.Block.Transactions)))
	svc.metrics.duration.Add(int64(result.EndTime.Sub(startTime).Microseconds()))

	return result
}

// archiveBlock archives a block and its transactions
func (svc *ArchivalService) archiveBlock(block *common.Block) error {
	// Start transaction
	ctx, cancel := context.WithTimeout(svc.ctx, svc.config.WriteTimeout)
	defer cancel()

	// Archive block
	if err := svc.archive.SaveBlock(block); err != nil {
		return fmt.Errorf("failed to archive block %d: %w", block.Number, err)
	}

	// Archive transactions
	for _, tx := range block.Transactions {
		txPtr := tx // Create a pointer to the transaction
		if err := svc.archive.SaveTransaction(&txPtr, block.Number); err != nil {
			return fmt.Errorf("failed to archive transaction %s: %w", tx.ID, err)
		}

		// Archive receipt if available
		if receipt, err := svc.source.GetReceipt(tx.ID); err == nil {
			if err := svc.archive.SaveReceipt(receipt); err != nil {
				svc.logger.WithError(err).Warn("Failed to archive receipt", "txID", tx.ID)
			} else {
				svc.metrics.receiptsArchived.Add(1)
			}
		}
	}

	// Update metadata
	if err := svc.updateArchivalMetadata(ctx, block.Number); err != nil {
		svc.logger.WithError(err).Warn("Failed to update archival metadata")
	}

	return nil
}

// verifyArchivedBlock verifies that a block was correctly archived
func (svc *ArchivalService) verifyArchivedBlock(block *common.Block) error {
	// Get block from archive
	archivedBlock, err := svc.archive.GetBlock(uint64(block.Number))
	if err != nil {
		return fmt.Errorf("failed to get archived block: %w", err)
	}

	// Compare blocks
	if archivedBlock.Hash != block.Hash {
		return fmt.Errorf("block hash mismatch: expected %s, got %s", block.Hash, archivedBlock.Hash)
	}

	if len(archivedBlock.Transactions) != len(block.Transactions) {
		return fmt.Errorf("transaction count mismatch: expected %d, got %d",
			len(block.Transactions), len(archivedBlock.Transactions))
	}

	// Verify transactions
	for i, tx := range block.Transactions {
		archivedTx, err := svc.archive.GetTransaction(tx.ID)
		if err != nil {
			return fmt.Errorf("failed to get archived transaction %s: %w", tx.ID, err)
		}

		if archivedTx.ID != tx.ID {
			return fmt.Errorf("transaction %d ID mismatch", i)
		}
	}

	return nil
}

// deleteFromPrimary deletes a block from primary storage
func (svc *ArchivalService) deleteFromPrimary(block *common.Block) error {
	// This is a placeholder - actual implementation would depend on
	// whether the primary storage supports deletion
	svc.logger.Debug("Would delete block from primary", "height", block.Number)
	return nil
}

// resultProcessor processes archival results
func (svc *ArchivalService) resultProcessor() {
	defer svc.wg.Done()
	defer close(svc.resultQueue)

	for {
		select {
		case <-svc.ctx.Done():
			return

		case result, ok := <-svc.resultQueue:
			if !ok {
				return
			}

			if result.Success {
				// Update last archived height
				if uint64(result.Job.Height) > svc.lastArchived.Load() {
					svc.lastArchived.Store(uint64(result.Job.Height))
				}

				svc.logger.Debug("Block archived successfully",
					"height", result.Job.Height,
					"duration", result.EndTime.Sub(result.StartTime),
					"verified", result.Verified)
			} else {
				svc.metrics.errors.Add(1)
				svc.logger.WithError(result.Error).Error("Failed to archive block",
					"height", result.Job.Height,
					"retries", result.Job.RetryCount)

				// Retry logic
				if result.Job.RetryCount < 3 {
					result.Job.RetryCount++
					select {
					case svc.workQueue <- result.Job:
					case <-svc.ctx.Done():
						return
					}
				}
			}
		}
	}
}

// refillRateLimiter refills the rate limiter bucket
func (svc *ArchivalService) refillRateLimiter() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-svc.ctx.Done():
			return
		case <-ticker.C:
			// Refill tokens
			for i := 0; i < svc.config.RateLimit; i++ {
				select {
				case svc.rateLimiter <- struct{}{}:
				default:
					// Bucket is full
					break
				}
			}
		}
	}
}

// Helper methods

func (svc *ArchivalService) getLastArchivedHeight() (uint64, error) {
	// This is a simplified implementation
	// In production, you would query the archive metadata collection
	return 0, nil
}

func (svc *ArchivalService) updateArchivalMetadata(ctx context.Context, height int) error {
	// Update metadata in archive
	// This is a simplified implementation
	return nil
}

func (svc *ArchivalService) logMetrics() {
	avgDuration := time.Duration(0)
	if blocks := svc.metrics.blocksArchived.Load(); blocks > 0 {
		avgDuration = time.Duration(svc.metrics.duration.Load()/int64(blocks)) * time.Microsecond
	}

	svc.logger.Info("Archival service metrics",
		"blocksArchived", svc.metrics.blocksArchived.Load(),
		"txArchived", svc.metrics.txArchived.Load(),
		"receiptsArchived", svc.metrics.receiptsArchived.Load(),
		"errors", svc.metrics.errors.Load(),
		"verifyFailures", svc.metrics.verifyFailures.Load(),
		"deleteFailures", svc.metrics.deleteFailures.Load(),
		"avgDuration", avgDuration)
}

// calculatePendingArchival safely calculates pending archival blocks
func calculatePendingArchival(currentHeight, archivalThreshold, lastArchived uint64) uint64 {
	if currentHeight <= archivalThreshold {
		return 0
	}
	targetHeight := currentHeight - archivalThreshold
	if targetHeight <= lastArchived {
		return 0
	}
	return targetHeight - lastArchived
}

// GetMetrics returns current metrics
func (svc *ArchivalService) GetMetrics() map[string]interface{} {
	avgDuration := time.Duration(0)
	if blocks := svc.metrics.blocksArchived.Load(); blocks > 0 {
		avgDuration = time.Duration(svc.metrics.duration.Load()/int64(blocks)) * time.Microsecond
	}

	return map[string]interface{}{
		"blocks_archived":   svc.metrics.blocksArchived.Load(),
		"tx_archived":       svc.metrics.txArchived.Load(),
		"receipts_archived": svc.metrics.receiptsArchived.Load(),
		"errors":            svc.metrics.errors.Load(),
		"verify_failures":   svc.metrics.verifyFailures.Load(),
		"delete_failures":   svc.metrics.deleteFailures.Load(),
		"avg_duration_us":   avgDuration.Microseconds(),
		"last_archived":     svc.lastArchived.Load(),
		"current_height":    svc.currentHeight.Load(),
		"pending_archival":  calculatePendingArchival(svc.currentHeight.Load(), svc.config.ArchivalThreshold, svc.lastArchived.Load()),
		"is_running":        svc.isRunning.Load(),
	}
}

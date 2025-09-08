package transaction

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

// ParallelValidationResult represents the result of validating a transaction in parallel
type ParallelValidationResult struct {
	Transaction *common.Transaction
	Error       error
	Priority    float64
	Size        int64
}

// ParallelValidator handles parallel validation of transactions
type ParallelValidator struct {
	workers        int
	workerPool     chan struct{}
	resultCache    sync.Map // Cache validation results temporarily
	validationFunc func(*common.Transaction) error
	priorityFunc   func(common.Transaction) float64
	logger         *logrus.Logger
	metrics        *ValidationMetrics
}

// ValidationMetrics tracks parallel validation performance
type ValidationMetrics struct {
	TotalValidations      atomic.Uint64
	SuccessfulValidations atomic.Uint64
	FailedValidations     atomic.Uint64
	CacheHits             atomic.Uint64
	CacheMisses           atomic.Uint64
	AvgValidationTimeUs   atomic.Uint64
}

// NewParallelValidator creates a new parallel validator
func NewParallelValidator(workers int, logger *logrus.Logger) *ParallelValidator {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	if logger == nil {
		logger = logrus.New()
	}

	return &ParallelValidator{
		workers:    workers,
		workerPool: make(chan struct{}, workers),
		logger:     logger,
		metrics:    &ValidationMetrics{},
	}
}

// ValidateBatch validates a batch of transactions in parallel
func (pv *ParallelValidator) ValidateBatch(
	ctx context.Context,
	transactions []*common.Transaction,
	validationFunc func(*common.Transaction) error,
	priorityFunc func(common.Transaction) float64,
) []*ParallelValidationResult {
	if len(transactions) == 0 {
		return nil
	}

	pv.validationFunc = validationFunc
	pv.priorityFunc = priorityFunc

	// Initialize worker pool tokens
	for i := 0; i < pv.workers; i++ {
		select {
		case pv.workerPool <- struct{}{}:
		default:
		}
	}

	results := make([]*ParallelValidationResult, len(transactions))
	var wg sync.WaitGroup

	// Process transactions in parallel
	for i, tx := range transactions {
		wg.Add(1)
		go func(idx int, transaction *common.Transaction) {
			defer wg.Done()

			// Get worker token
			select {
			case <-pv.workerPool:
				defer func() { pv.workerPool <- struct{}{} }()
			case <-ctx.Done():
				results[idx] = &ParallelValidationResult{
					Transaction: transaction,
					Error:       ctx.Err(),
				}
				return
			}

			// Validate transaction
			results[idx] = pv.validateSingle(transaction)
		}(i, tx)
	}

	wg.Wait()
	return results
}

// validateSingle validates a single transaction
func (pv *ParallelValidator) validateSingle(tx *common.Transaction) *ParallelValidationResult {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Microseconds()
		pv.updateAvgValidationTime(uint64(duration))
	}()

	pv.metrics.TotalValidations.Add(1)

	// Check cache first
	cacheKey := tx.ID
	if cached, ok := pv.resultCache.Load(cacheKey); ok {
		pv.metrics.CacheHits.Add(1)
		return cached.(*ParallelValidationResult)
	}
	pv.metrics.CacheMisses.Add(1)

	result := &ParallelValidationResult{
		Transaction: tx,
		Size:        int64(estimateTransactionSize(tx)),
	}

	// Validate transaction
	if err := pv.validationFunc(tx); err != nil {
		result.Error = err
		pv.metrics.FailedValidations.Add(1)
	} else {
		result.Priority = pv.priorityFunc(*tx)
		pv.metrics.SuccessfulValidations.Add(1)

		// Cache successful validation (with TTL)
		pv.resultCache.Store(cacheKey, result)
		go func() {
			time.Sleep(5 * time.Second)
			pv.resultCache.Delete(cacheKey)
		}()
	}

	return result
}

// updateAvgValidationTime updates the rolling average validation time
func (pv *ParallelValidator) updateAvgValidationTime(newTime uint64) {
	// Simple exponential moving average
	current := pv.metrics.AvgValidationTimeUs.Load()
	if current == 0 {
		pv.metrics.AvgValidationTimeUs.Store(newTime)
	} else {
		// EMA with alpha=0.1
		newAvg := (current*9 + newTime) / 10
		pv.metrics.AvgValidationTimeUs.Store(newAvg)
	}
}

// GetMetrics returns current validation metrics
func (pv *ParallelValidator) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"total_validations":      pv.metrics.TotalValidations.Load(),
		"successful_validations": pv.metrics.SuccessfulValidations.Load(),
		"failed_validations":     pv.metrics.FailedValidations.Load(),
		"cache_hits":             pv.metrics.CacheHits.Load(),
		"cache_misses":           pv.metrics.CacheMisses.Load(),
		"avg_validation_time_us": pv.metrics.AvgValidationTimeUs.Load(),
		"worker_count":           pv.workers,
	}
}

// ParallelSignatureVerifier provides parallel signature verification
type ParallelSignatureVerifier struct {
	workers    int
	workerPool chan struct{}
	logger     *logrus.Logger
}

// NewParallelSignatureVerifier creates a new parallel signature verifier
func NewParallelSignatureVerifier(workers int, logger *logrus.Logger) *ParallelSignatureVerifier {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	return &ParallelSignatureVerifier{
		workers:    workers,
		workerPool: make(chan struct{}, workers),
		logger:     logger,
	}
}

// VerifyBatch verifies signatures for a batch of transactions in parallel
func (psv *ParallelSignatureVerifier) VerifyBatch(
	ctx context.Context,
	transactions []*common.Transaction,
) map[string]error {
	if len(transactions) == 0 {
		return nil
	}

	// Initialize worker pool
	for i := 0; i < psv.workers; i++ {
		select {
		case psv.workerPool <- struct{}{}:
		default:
		}
	}

	results := make(map[string]error)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, tx := range transactions {
		wg.Add(1)
		go func(transaction *common.Transaction) {
			defer wg.Done()

			// Get worker token
			select {
			case <-psv.workerPool:
				defer func() { psv.workerPool <- struct{}{} }()
			case <-ctx.Done():
				mu.Lock()
				results[transaction.ID] = ctx.Err()
				mu.Unlock()
				return
			}

			// Verify signature
			err := psv.verifySignature(transaction)
			if err != nil {
				mu.Lock()
				results[transaction.ID] = err
				mu.Unlock()
			}
		}(tx)
	}

	wg.Wait()
	return results
}

// verifySignature verifies a single transaction signature
func (psv *ParallelSignatureVerifier) verifySignature(tx *common.Transaction) error {
	// Skip signature verification for development signatures
	if len(tx.Signature) > 8 && string(tx.Signature[:8]) == "DEV_SIG:" {
		if !common.IsDevelopmentMode() {
			return fmt.Errorf("development signatures not allowed in production")
		}
		return nil
	}

	// Get account for public keys
	acc := common.GetAccount(tx.Sender)
	if acc == nil {
		return fmt.Errorf("sender account not found")
	}

	// Verify Dilithium signature (PublicKey field stores Dilithium public key)
	if len(acc.PublicKey) > 0 {
		valid, err := crypto.VerifyDilithium(crypto.DilithiumLevel3, acc.PublicKey, []byte(tx.ID), tx.Signature)
		if err != nil {
			return fmt.Errorf("signature verification error: %w", err)
		}
		if !valid {
			return fmt.Errorf("invalid signature")
		}
		return nil
	}

	return fmt.Errorf("no valid public key found for account")
}

// TransactionBatchProcessor processes transaction batches with parallel validation
type TransactionBatchProcessor struct {
	validator         *ParallelValidator
	signatureVerifier *ParallelSignatureVerifier
	pool              *TransactionPool
	logger            *logrus.Logger
	batchSize         int
	processingQueue   chan []*common.Transaction
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewTransactionBatchProcessor creates a new batch processor
func NewTransactionBatchProcessor(
	pool *TransactionPool,
	batchSize int,
	workers int,
	logger *logrus.Logger,
) *TransactionBatchProcessor {
	if batchSize <= 0 {
		batchSize = 100
	}

	return &TransactionBatchProcessor{
		validator:         NewParallelValidator(workers, logger),
		signatureVerifier: NewParallelSignatureVerifier(workers, logger),
		pool:              pool,
		logger:            logger,
		batchSize:         batchSize,
		processingQueue:   make(chan []*common.Transaction, 10),
		stopCh:            make(chan struct{}),
	}
}

// Start begins the batch processor
func (tbp *TransactionBatchProcessor) Start() {
	tbp.wg.Add(1)
	go tbp.processLoop()
}

// Stop stops the batch processor
func (tbp *TransactionBatchProcessor) Stop() {
	close(tbp.stopCh)
	tbp.wg.Wait()
}

// SubmitBatch submits a batch of transactions for processing
func (tbp *TransactionBatchProcessor) SubmitBatch(transactions []*common.Transaction) error {
	select {
	case tbp.processingQueue <- transactions:
		return nil
	case <-tbp.stopCh:
		return fmt.Errorf("batch processor stopped")
	default:
		return fmt.Errorf("processing queue full")
	}
}

// processLoop continuously processes transaction batches
func (tbp *TransactionBatchProcessor) processLoop() {
	defer tbp.wg.Done()

	for {
		select {
		case batch := <-tbp.processingQueue:
			tbp.processBatch(batch)
		case <-tbp.stopCh:
			return
		}
	}
}

// processBatch processes a single batch of transactions
func (tbp *TransactionBatchProcessor) processBatch(transactions []*common.Transaction) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()

	// Step 1: Parallel signature verification
	sigErrors := tbp.signatureVerifier.VerifyBatch(ctx, transactions)

	// Filter out transactions with invalid signatures
	validTxs := make([]*common.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		if err, hasError := sigErrors[tx.ID]; !hasError || err == nil {
			validTxs = append(validTxs, tx)
		} else {
			tbp.logger.WithFields(logrus.Fields{
				"txID":  tx.ID,
				"error": err,
			}).Debug("Transaction signature verification failed")
		}
	}

	// Step 2: Parallel validation
	validateFunc := func(tx *common.Transaction) error {
		return tbp.pool.validateTransaction(*tx)
	}
	results := tbp.validator.ValidateBatch(ctx, validTxs, validateFunc, tbp.pool.calculatePriority)

	// Step 3: Add valid transactions to pool
	successCount := 0
	for _, result := range results {
		if result.Error == nil {
			// Add to pool (this is still sequential but much faster after parallel validation)
			if err := tbp.pool.AddValidatedTransaction(result.Transaction, result.Priority, result.Size); err != nil {
				tbp.logger.WithFields(logrus.Fields{
					"txID":  result.Transaction.ID,
					"error": err,
				}).Debug("Failed to add validated transaction to pool")
			} else {
				successCount++
			}
		}
	}

	duration := time.Since(start)
	tbp.logger.WithFields(logrus.Fields{
		"batch_size":     len(transactions),
		"valid_txs":      len(validTxs),
		"added_to_pool":  successCount,
		"duration_ms":    duration.Milliseconds(),
		"throughput_tps": float64(len(transactions)) / duration.Seconds(),
	}).Info("Batch processing complete")
}

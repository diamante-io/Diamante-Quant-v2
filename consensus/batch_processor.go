// consensus/batch_processor.go

package consensus

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"

	"diamante/consensus/types"
)

// BatchSize constants define the minimum, default, and maximum batch sizes
const (
	MinBatchSize     = 10
	DefaultBatchSize = 100
	MaxBatchSize     = 1000
)

// BatchProcessorConfig holds configuration for the batch processor
type BatchProcessorConfig struct {
	// BatchSize is the target number of events to process in a batch
	BatchSize int

	// MaxBatchDelay is the maximum time to wait for a batch to fill
	MaxBatchDelay time.Duration

	// MaxBatchBytes is the maximum size of a batch in bytes
	MaxBatchBytes int

	// GroupByCreator determines if events should be grouped by creator
	GroupByCreator bool

	// ParallelProcessing enables parallel processing of batches
	ParallelProcessing bool

	// MaxParallelBatches is the maximum number of batches to process in parallel
	MaxParallelBatches int

	// AdaptiveBatchSize enables dynamic adjustment of batch size
	AdaptiveBatchSize bool
}

// DefaultBatchProcessorConfig returns a default configuration
func DefaultBatchProcessorConfig() *BatchProcessorConfig {
	return &BatchProcessorConfig{
		BatchSize:          DefaultBatchSize,
		MaxBatchDelay:      100 * time.Millisecond,
		MaxBatchBytes:      1024 * 1024, // 1MB
		GroupByCreator:     true,
		ParallelProcessing: true,
		MaxParallelBatches: 4,
		AdaptiveBatchSize:  true,
	}
}

// BatchProcessor handles batching of events for more efficient processing
type BatchProcessor struct {
	config *BatchProcessorConfig
	logger *hybridConsensusLogger

	// Batch queues
	pendingEvents     []*types.Event
	pendingEventsByID map[[32]byte]*types.Event
	pendingEventsMu   sync.RWMutex

	// Creator-based queues (when GroupByCreator is enabled)
	creatorQueues   map[[32]byte][]*types.Event
	creatorQueuesMu sync.RWMutex
	priorityQueue   EventPriorityQueue
	priorityQueueMu sync.Mutex

	// Processing state
	isProcessing      bool
	processingMu      sync.Mutex
	processingWg      sync.WaitGroup
	processingResults chan BatchResult

	// Metrics
	metrics     BatchMetrics
	metricsMu   sync.RWMutex
	lastAdapted time.Time

	// Control
	stopChan chan struct{}
}

// BatchMetrics tracks performance metrics for batch processing
type BatchMetrics struct {
	TotalBatches       int
	TotalEventsInBatch int
	AvgBatchSize       float64
	MaxBatchSize       int
	AvgProcessingTime  time.Duration
	MaxProcessingTime  time.Duration
	LastBatchTime      time.Time
	BatchSizeHistory   []int
}

// BatchResult represents the result of processing a batch
type BatchResult struct {
	Events          []*types.Event
	ProcessedCount  int
	SuccessCount    int
	FailureCount    int
	ProcessingTime  time.Duration
	Errors          []error
	BatchIdentifier string
}

// EventPriorityQueue implements a priority queue for events based on height
type EventPriorityQueue []*EventWithPriority

// EventWithPriority wraps an event with priority information
type EventWithPriority struct {
	Event    *types.Event
	Priority uint64 // Lower values = higher priority
	Index    int    // Index in the heap
}

// NewBatchProcessor creates a new batch processor with the given configuration
func NewBatchProcessor(config *BatchProcessorConfig, logger *hybridConsensusLogger) *BatchProcessor {
	if config == nil {
		config = DefaultBatchProcessorConfig()
	}

	// Validate and adjust configuration
	if config.BatchSize < MinBatchSize {
		config.BatchSize = MinBatchSize
	} else if config.BatchSize > MaxBatchSize {
		config.BatchSize = MaxBatchSize
	}

	bp := &BatchProcessor{
		config:            config,
		logger:            logger,
		pendingEvents:     make([]*types.Event, 0, config.BatchSize*2),
		pendingEventsByID: make(map[[32]byte]*types.Event),
		creatorQueues:     make(map[[32]byte][]*types.Event),
		processingResults: make(chan BatchResult, 10),
		stopChan:          make(chan struct{}),
	}

	// Initialize priority queue
	bp.priorityQueue = make(EventPriorityQueue, 0, config.BatchSize*2)
	heap.Init(&bp.priorityQueue)

	return bp
}

// Start begins the batch processing
func (bp *BatchProcessor) Start() error {
	bp.processingMu.Lock()
	defer bp.processingMu.Unlock()

	if bp.isProcessing {
		return fmt.Errorf("batch processor is already running")
	}

	bp.isProcessing = true
	bp.stopChan = make(chan struct{})

	// Start the batch processing loop
	go bp.processingLoop()

	// Start the results handling loop
	go bp.handleResults()

	bp.logger.Info("Batch processor started",
		"batchSize", bp.config.BatchSize,
		"maxDelay", bp.config.MaxBatchDelay,
		"groupByCreator", bp.config.GroupByCreator,
		"parallelProcessing", bp.config.ParallelProcessing)

	return nil
}

// Stop halts the batch processing
func (bp *BatchProcessor) Stop() error {
	bp.processingMu.Lock()
	defer bp.processingMu.Unlock()

	if !bp.isProcessing {
		return fmt.Errorf("batch processor is not running")
	}

	close(bp.stopChan)
	bp.isProcessing = false

	// Wait for all processing to complete
	bp.processingWg.Wait()

	bp.logger.Info("Batch processor stopped")
	return nil
}

// AddEvent adds an event to the pending queue
func (bp *BatchProcessor) AddEvent(event *types.Event) {
	if event == nil {
		return
	}

	bp.pendingEventsMu.Lock()
	defer bp.pendingEventsMu.Unlock()

	// Check if event already exists
	if _, exists := bp.pendingEventsByID[event.ID]; exists {
		return
	}

	// Add to pending events
	bp.pendingEvents = append(bp.pendingEvents, event)
	bp.pendingEventsByID[event.ID] = event

	// If grouping by creator is enabled, also add to creator queue
	if bp.config.GroupByCreator {
		bp.addToCreatorQueue(event)
	}

	// Add to priority queue
	bp.addToPriorityQueue(event)

	// If we have enough events, trigger processing
	if len(bp.pendingEvents) >= bp.config.BatchSize {
		go bp.triggerProcessing()
	}
}

// addToCreatorQueue adds an event to its creator's queue
func (bp *BatchProcessor) addToCreatorQueue(event *types.Event) {
	bp.creatorQueuesMu.Lock()
	defer bp.creatorQueuesMu.Unlock()

	creatorID := event.Creator
	bp.creatorQueues[creatorID] = append(bp.creatorQueues[creatorID], event)
}

// addToPriorityQueue adds an event to the priority queue
func (bp *BatchProcessor) addToPriorityQueue(event *types.Event) {
	bp.priorityQueueMu.Lock()
	defer bp.priorityQueueMu.Unlock()

	item := &EventWithPriority{
		Event:    event,
		Priority: event.Height, // Lower height = higher priority
	}
	heap.Push(&bp.priorityQueue, item)
}

// triggerProcessing signals that processing should begin
func (bp *BatchProcessor) triggerProcessing() {
	bp.processingMu.Lock()
	defer bp.processingMu.Unlock()

	if !bp.isProcessing {
		return
	}

	// Check if we have enough events to process
	bp.pendingEventsMu.RLock()
	hasEnoughEvents := len(bp.pendingEvents) >= bp.config.BatchSize
	bp.pendingEventsMu.RUnlock()

	if hasEnoughEvents {
		go bp.processBatch()
	}
}

// processingLoop periodically processes batches even if they're not full
func (bp *BatchProcessor) processingLoop() {
	ticker := time.NewTicker(bp.config.MaxBatchDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bp.pendingEventsMu.RLock()
			hasPendingEvents := len(bp.pendingEvents) > 0
			bp.pendingEventsMu.RUnlock()

			if hasPendingEvents {
				go bp.processBatch()
			}

			// Adapt batch size if enabled
			if bp.config.AdaptiveBatchSize && time.Since(bp.lastAdapted) > 30*time.Second {
				bp.adaptBatchSize()
			}

		case <-bp.stopChan:
			return
		}
	}
}

// processBatch processes a batch of events
func (bp *BatchProcessor) processBatch() {
	bp.processingMu.Lock()
	if !bp.isProcessing {
		bp.processingMu.Unlock()
		return
	}
	bp.processingWg.Add(1)
	bp.processingMu.Unlock()

	defer bp.processingWg.Done()

	startTime := time.Now()

	// Determine how to process events
	var batches [][]*types.Event
	if bp.config.GroupByCreator {
		batches = bp.createCreatorBasedBatches()
	} else {
		batches = bp.createHeightBasedBatches()
	}

	// Process batches
	if bp.config.ParallelProcessing && len(batches) > 1 {
		bp.processInParallel(batches)
	} else {
		bp.processSequentially(batches)
	}

	// Update metrics
	bp.updateMetrics(len(batches), time.Since(startTime))
}

// createCreatorBasedBatches creates batches grouped by creator
func (bp *BatchProcessor) createCreatorBasedBatches() [][]*types.Event {
	bp.creatorQueuesMu.Lock()
	defer bp.creatorQueuesMu.Unlock()

	var batches [][]*types.Event
	batchSize := bp.config.BatchSize

	// Create a batch for each creator with pending events
	for creator, events := range bp.creatorQueues {
		if len(events) == 0 {
			continue
		}

		// Sort events by height
		sort.Slice(events, func(i, j int) bool {
			return events[i].Height < events[j].Height
		})

		// Split into batches of batchSize
		for i := 0; i < len(events); i += batchSize {
			end := i + batchSize
			if end > len(events) {
				end = len(events)
			}
			batch := events[i:end]
			batches = append(batches, batch)

			// Remove processed events from the queue
			for _, event := range batch {
				bp.removeEvent(event)
			}
		}

		// Update the creator queue
		if len(events) <= batchSize {
			delete(bp.creatorQueues, creator)
		} else {
			bp.creatorQueues[creator] = events[batchSize:]
		}
	}

	return batches
}

// createHeightBasedBatches creates batches based on event height
func (bp *BatchProcessor) createHeightBasedBatches() [][]*types.Event {
	bp.pendingEventsMu.Lock()
	defer bp.pendingEventsMu.Unlock()

	if len(bp.pendingEvents) == 0 {
		return nil
	}

	batchSize := bp.config.BatchSize
	var batches [][]*types.Event

	// Sort events by height
	sort.Slice(bp.pendingEvents, func(i, j int) bool {
		return bp.pendingEvents[i].Height < bp.pendingEvents[j].Height
	})

	// Create batches
	for i := 0; i < len(bp.pendingEvents); i += batchSize {
		end := i + batchSize
		if end > len(bp.pendingEvents) {
			end = len(bp.pendingEvents)
		}
		batch := make([]*types.Event, end-i)
		copy(batch, bp.pendingEvents[i:end])
		batches = append(batches, batch)
	}

	// Keep only unprocessed events
	if len(batches) > 0 {
		totalProcessed := len(batches) * batchSize
		if totalProcessed > len(bp.pendingEvents) {
			totalProcessed = len(bp.pendingEvents)
		}
		remaining := len(bp.pendingEvents) - totalProcessed
		if remaining > 0 {
			// Keep the remaining events
			newPending := make([]*types.Event, remaining)
			copy(newPending, bp.pendingEvents[totalProcessed:])
			bp.pendingEvents = newPending
		} else {
			// All events processed
			bp.pendingEvents = make([]*types.Event, 0, batchSize*2)
		}

		// Update pendingEventsByID
		for _, batch := range batches {
			for _, event := range batch {
				delete(bp.pendingEventsByID, event.ID)
			}
		}
	}

	return batches
}

// processInParallel processes batches in parallel
func (bp *BatchProcessor) processInParallel(batches [][]*types.Event) {
	var wg sync.WaitGroup
	maxParallel := bp.config.MaxParallelBatches
	if maxParallel <= 0 || maxParallel > len(batches) {
		maxParallel = len(batches)
	}

	// Create a semaphore channel to limit concurrency
	sem := make(chan struct{}, maxParallel)

	for i, batch := range batches {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore

		go func(batchIndex int, events []*types.Event) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			result := bp.processSingleBatch(events, fmt.Sprintf("batch-%d", batchIndex))
			bp.processingResults <- result
		}(i, batch)
	}

	wg.Wait()
}

// processSequentially processes batches one at a time
func (bp *BatchProcessor) processSequentially(batches [][]*types.Event) {
	for i, batch := range batches {
		result := bp.processSingleBatch(batch, fmt.Sprintf("batch-%d", i))
		bp.processingResults <- result
	}
}

// processSingleBatch processes a single batch of events
func (bp *BatchProcessor) processSingleBatch(events []*types.Event, batchID string) BatchResult {
	startTime := time.Now()
	result := BatchResult{
		Events:          events,
		BatchIdentifier: batchID,
	}

	for _, event := range events {
		result.ProcessedCount++

		// This is where we would call the consensus engine to process the event
		// For now, we'll just simulate success
		success := true
		if success {
			result.SuccessCount++
		} else {
			result.FailureCount++
			result.Errors = append(result.Errors, fmt.Errorf("failed to process event %x", event.ID))
		}
	}

	result.ProcessingTime = time.Since(startTime)
	return result
}

// handleResults processes the results of batch processing
func (bp *BatchProcessor) handleResults() {
	for {
		select {
		case result := <-bp.processingResults:
			bp.logger.Info("Batch processed",
				"batchID", result.BatchIdentifier,
				"processed", result.ProcessedCount,
				"success", result.SuccessCount,
				"failure", result.FailureCount,
				"duration", result.ProcessingTime)

			// Handle any errors
			for _, err := range result.Errors {
				bp.logger.Error("Batch processing error", "error", err)
			}

		case <-bp.stopChan:
			return
		}
	}
}

// removeEvent removes an event from all queues
func (bp *BatchProcessor) removeEvent(event *types.Event) {
	bp.pendingEventsMu.Lock()
	defer bp.pendingEventsMu.Unlock()

	// Remove from pendingEventsByID
	delete(bp.pendingEventsByID, event.ID)

	// We don't remove from pendingEvents here as it would be inefficient
	// Instead, we'll rebuild the list when we process batches
}

// updateMetrics updates performance metrics
func (bp *BatchProcessor) updateMetrics(batchCount int, duration time.Duration) {
	bp.metricsMu.Lock()
	defer bp.metricsMu.Unlock()

	bp.metrics.TotalBatches += batchCount
	bp.metrics.LastBatchTime = time.Now()

	// Update processing time metrics
	if bp.metrics.MaxProcessingTime < duration {
		bp.metrics.MaxProcessingTime = duration
	}

	// Simple moving average for processing time
	if bp.metrics.AvgProcessingTime == 0 {
		bp.metrics.AvgProcessingTime = duration
	} else {
		bp.metrics.AvgProcessingTime = (bp.metrics.AvgProcessingTime*9 + duration) / 10
	}

	// Keep history of batch sizes for adaptation
	if len(bp.metrics.BatchSizeHistory) >= 10 {
		// Remove oldest entry
		bp.metrics.BatchSizeHistory = bp.metrics.BatchSizeHistory[1:]
	}
	bp.metrics.BatchSizeHistory = append(bp.metrics.BatchSizeHistory, bp.config.BatchSize)
}

// adaptBatchSize dynamically adjusts the batch size based on performance metrics
func (bp *BatchProcessor) adaptBatchSize() {
	bp.metricsMu.Lock()
	defer bp.metricsMu.Unlock()

	bp.lastAdapted = time.Now()

	// If we don't have enough history, don't adapt
	if len(bp.metrics.BatchSizeHistory) < 3 {
		return
	}

	// If processing time is too high, reduce batch size
	if bp.metrics.AvgProcessingTime > bp.config.MaxBatchDelay*2 {
		newSize := bp.config.BatchSize * 3 / 4
		if newSize < MinBatchSize {
			newSize = MinBatchSize
		}
		bp.config.BatchSize = newSize
		bp.logger.Info("Reduced batch size due to high processing time",
			"newBatchSize", bp.config.BatchSize,
			"avgProcessingTime", bp.metrics.AvgProcessingTime)
		return
	}

	// If processing time is low, increase batch size
	if bp.metrics.AvgProcessingTime < bp.config.MaxBatchDelay/2 {
		newSize := bp.config.BatchSize * 5 / 4
		if newSize > MaxBatchSize {
			newSize = MaxBatchSize
		}
		bp.config.BatchSize = newSize
		bp.logger.Info("Increased batch size due to low processing time",
			"newBatchSize", bp.config.BatchSize,
			"avgProcessingTime", bp.metrics.AvgProcessingTime)
	}
}

// GetMetrics returns the current batch processing metrics
func (bp *BatchProcessor) GetMetrics() BatchMetrics {
	bp.metricsMu.RLock()
	defer bp.metricsMu.RUnlock()

	// Return a copy to avoid race conditions
	return BatchMetrics{
		TotalBatches:       bp.metrics.TotalBatches,
		TotalEventsInBatch: bp.metrics.TotalEventsInBatch,
		AvgBatchSize:       bp.metrics.AvgBatchSize,
		MaxBatchSize:       bp.metrics.MaxBatchSize,
		AvgProcessingTime:  bp.metrics.AvgProcessingTime,
		MaxProcessingTime:  bp.metrics.MaxProcessingTime,
		LastBatchTime:      bp.metrics.LastBatchTime,
		BatchSizeHistory:   append([]int{}, bp.metrics.BatchSizeHistory...),
	}
}

// GetPendingCount returns the number of pending events
func (bp *BatchProcessor) GetPendingCount() int {
	bp.pendingEventsMu.RLock()
	defer bp.pendingEventsMu.RUnlock()
	return len(bp.pendingEvents)
}

// Priority queue implementation

func (pq EventPriorityQueue) Len() int { return len(pq) }

func (pq EventPriorityQueue) Less(i, j int) bool {
	// Lower priority value means higher priority
	return pq[i].Priority < pq[j].Priority
}

func (pq EventPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *EventPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*EventWithPriority)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *EventPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// consensus/event_flow.go

package consensus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"diamante/consensus/types"
)

// EventFlowManager handles the flow of events from creation to finalization
// across the different consensus components (DPoS, PoH, and Lachesis).
type EventFlowManager struct {
	hc *HybridConsensus

	// Event tracking
	pendingEvents     map[[32]byte]*types.Event // Events waiting for finalization
	finalizedEvents   map[[32]byte]*types.Event // Events that have been finalized
	eventsByHeight    map[uint64][]*types.Event // Events organized by height
	eventCreationTime map[[32]byte]time.Time    // Track when events were created
	eventRetryCount   map[[32]byte]int          // Track retry attempts for events

	// Metrics
	totalEventsCreated     uint64
	totalEventsFinalized   uint64
	avgFinalizationTime    time.Duration
	maxFinalizationTime    time.Duration
	finalizationTimeouts   uint64
	eventDuplicateCount    uint64
	eventValidationErrors  uint64
	eventPropagationErrors uint64
	eventRetries           uint64
	batchProcessingTime    time.Duration
	lastBatchSize          int
	successRate            float64 // Percentage of events successfully finalized

	// Concurrency control
	mu             sync.RWMutex
	metricsMu      sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	finalizationWg sync.WaitGroup

	// Configuration
	finalizationTimeout time.Duration
	maxPendingEvents    int
	batchSize           int
	enableDeduplication bool
	enableValidation    bool
	maxRetries          int
	adaptiveBatching    bool
}

// NewEventFlowManager creates a new EventFlowManager with the given HybridConsensus.
func NewEventFlowManager(hc *HybridConsensus) *EventFlowManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &EventFlowManager{
		hc:                  hc,
		pendingEvents:       make(map[[32]byte]*types.Event),
		finalizedEvents:     make(map[[32]byte]*types.Event),
		eventsByHeight:      make(map[uint64][]*types.Event),
		eventCreationTime:   make(map[[32]byte]time.Time),
		eventRetryCount:     make(map[[32]byte]int),
		ctx:                 ctx,
		cancel:              cancel,
		finalizationTimeout: 5 * time.Second,
		maxPendingEvents:    10000,
		batchSize:           100,
		enableDeduplication: true,
		enableValidation:    true,
		maxRetries:          3,
		adaptiveBatching:    true,
		successRate:         1.0, // Start with optimistic assumption
	}
}

// Start begins the event flow management process.
func (efm *EventFlowManager) Start() error {
	// Start the event finalization worker
	go efm.finalizationWorker()
	// Start the metrics collection worker
	go efm.metricsCollector()
	// Start the adaptive batch size worker if enabled
	if efm.adaptiveBatching {
		go efm.adaptiveBatchSizeWorker()
	}
	return nil
}

// Stop halts the event flow management process.
func (efm *EventFlowManager) Stop() error {
	efm.cancel()
	// Wait for all finalization goroutines to complete
	efm.finalizationWg.Wait()
	return nil
}

// CreateEvent creates a new event and starts the finalization process.
func (efm *EventFlowManager) CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) (*types.Event, error) {
	// Check if creator is an active validator
	if !efm.hc.validatorManager.IsActiveValidator(creator) {
		return nil, errors.New("creator is not an active validator")
	}

	// Check if we have too many pending events
	efm.mu.RLock()
	pendingCount := len(efm.pendingEvents)
	efm.mu.RUnlock()
	if pendingCount >= efm.maxPendingEvents {
		return nil, fmt.Errorf("too many pending events (%d >= %d)", pendingCount, efm.maxPendingEvents)
	}

	// Create the event using Lachesis
	event := efm.hc.lachesis.CreateEvent(creator, parentIDs, data)
	if event == nil {
		efm.incrementMetric("eventCreationErrors")
		return nil, errors.New("failed to create event")
	}

	// Add PoH information
	pohState := efm.hc.poh.GetState()
	pohCount := efm.hc.poh.GetCount()
	pohHash := efm.hc.poh.Record(data)
	event.PoHState = pohState
	event.PoHCount = pohCount
	event.PoHProof = pohHash

	// Validate the event before adding it to pending
	if efm.enableValidation {
		if err := efm.ValidateEvent(event); err != nil {
			// If it's a duplicate, we can just return the existing event
			if err.Error() == "duplicate event" {
				efm.mu.RLock()
				existingEvent, exists := efm.pendingEvents[event.ID]
				if !exists {
					existingEvent, exists = efm.finalizedEvents[event.ID]
				}
				efm.mu.RUnlock()

				if exists {
					return existingEvent, nil
				}
			}

			efm.incrementMetric("eventValidationErrors")
			return nil, fmt.Errorf("event validation failed: %w", err)
		}
	}

	// Track the event
	efm.mu.Lock()
	efm.pendingEvents[event.ID] = event
	efm.eventCreationTime[event.ID] = time.Now()
	efm.eventsByHeight[event.Height] = append(efm.eventsByHeight[event.Height], event)
	efm.eventRetryCount[event.ID] = 0
	efm.mu.Unlock()

	// Increment metrics
	efm.incrementMetric("totalEventsCreated")

	// Start finalization process asynchronously
	efm.finalizationWg.Add(1)
	go efm.finalizeEvent(event)

	return event, nil
}

// finalizeEvent attempts to finalize an event with timeout.
func (efm *EventFlowManager) finalizeEvent(event *types.Event) {
	defer efm.finalizationWg.Done()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(efm.ctx, efm.finalizationTimeout)
	defer cancel()

	// Create a channel for the finalization result
	resultCh := make(chan bool, 1)

	// Start the finalization process
	go func() {
		finalized, _ := efm.hc.FinalizeEvent(event)
		resultCh <- finalized
	}()

	// Wait for finalization or timeout
	select {
	case finalized := <-resultCh:
		if finalized {
			// Event was finalized successfully
			efm.handleFinalizedEvent(event)
		} else {
			// Event was not finalized, keep it in pending
			efm.mu.Lock()
			retries := efm.eventRetryCount[event.ID]
			efm.eventRetryCount[event.ID] = retries + 1
			efm.mu.Unlock()

			efm.incrementMetric("eventRetries")

			// Log with retry information
			efm.hc.logger.Info("Event not finalized, keeping in pending",
				"eventID", fmt.Sprintf("%x", event.ID),
				"height", event.Height,
				"retryCount", retries+1,
				"maxRetries", efm.maxRetries)

			// If we've exceeded max retries, log a warning
			if retries+1 >= efm.maxRetries {
				efm.hc.logger.Warn("Event exceeded max retry attempts",
					"eventID", fmt.Sprintf("%x", event.ID),
					"height", event.Height,
					"retries", retries+1)
			}
		}
	case <-ctx.Done():
		// Finalization timed out
		efm.incrementMetric("finalizationTimeouts")

		// Update retry count
		efm.mu.Lock()
		retries := efm.eventRetryCount[event.ID]
		efm.eventRetryCount[event.ID] = retries + 1
		efm.mu.Unlock()

		efm.hc.logger.Info("Event finalization timed out",
			"eventID", fmt.Sprintf("%x", event.ID),
			"height", event.Height,
			"retryCount", retries+1,
			"maxRetries", efm.maxRetries)
	}
}

// handleFinalizedEvent updates the internal state for a finalized event.
func (efm *EventFlowManager) handleFinalizedEvent(event *types.Event) {
	// First, gather all the information we need under the lock
	var finalizationTime time.Duration
	var retryCount int

	efm.mu.Lock()
	// Calculate finalization time
	creationTime, exists := efm.eventCreationTime[event.ID]
	if exists {
		finalizationTime = time.Since(creationTime)
		retryCount = efm.eventRetryCount[event.ID]
	}

	// Move from pending to finalized
	delete(efm.pendingEvents, event.ID)
	delete(efm.eventCreationTime, event.ID)
	delete(efm.eventRetryCount, event.ID)
	efm.finalizedEvents[event.ID] = event
	efm.mu.Unlock()

	// Update metrics - this can be done outside the main lock
	if exists {
		efm.metricsMu.Lock()
		if efm.avgFinalizationTime == 0 {
			efm.avgFinalizationTime = finalizationTime
		} else {
			// Use exponential moving average
			efm.avgFinalizationTime = (efm.avgFinalizationTime*9 + finalizationTime) / 10
		}
		if finalizationTime > efm.maxFinalizationTime {
			efm.maxFinalizationTime = finalizationTime
		}
		efm.metricsMu.Unlock()

		efm.hc.logger.Info("Event finalized",
			"eventID", fmt.Sprintf("%x", event.ID),
			"height", event.Height,
			"finalizationTime", finalizationTime,
			"retries", retryCount)
	}

	// Increment metrics
	efm.incrementMetric("totalEventsFinalized")

	// Reward the validator - do this outside of any locks to reduce contention
	if err := efm.hc.validatorManager.RewardEventFinalization(event.Creator, event.Height); err != nil {
		efm.hc.logger.Error("Failed to reward validator for event finalization", "error", err)
	}
}

// finalizationWorker periodically attempts to finalize pending events.
func (efm *EventFlowManager) finalizationWorker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			efm.processPendingEvents()
		case <-efm.ctx.Done():
			return
		}
	}
}

// adaptiveBatchSizeWorker adjusts the batch size based on performance metrics
func (efm *EventFlowManager) adaptiveBatchSizeWorker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			efm.adjustBatchSize()
		case <-efm.ctx.Done():
			return
		}
	}
}

// adjustBatchSize dynamically adjusts the batch size based on performance metrics
func (efm *EventFlowManager) adjustBatchSize() {
	efm.metricsMu.RLock()
	avgTime := efm.batchProcessingTime
	successRate := efm.successRate
	currentBatchSize := efm.batchSize
	efm.metricsMu.RUnlock()

	// If processing is too slow or success rate is low, decrease batch size
	if avgTime > 500*time.Millisecond || successRate < 0.7 {
		newBatchSize := int(float64(currentBatchSize) * 0.8)
		if newBatchSize < 10 {
			newBatchSize = 10 // Minimum batch size
		}

		efm.metricsMu.Lock()
		efm.batchSize = newBatchSize
		efm.metricsMu.Unlock()

		efm.hc.logger.Info("Decreased batch size due to performance metrics",
			"oldSize", currentBatchSize,
			"newSize", newBatchSize,
			"avgProcessingTime", avgTime,
			"successRate", successRate)
	} else if avgTime < 100*time.Millisecond && successRate > 0.9 {
		// If processing is fast and success rate is high, increase batch size
		newBatchSize := int(float64(currentBatchSize) * 1.2)
		if newBatchSize > 500 {
			newBatchSize = 500 // Maximum batch size
		}

		efm.metricsMu.Lock()
		efm.batchSize = newBatchSize
		efm.metricsMu.Unlock()

		efm.hc.logger.Info("Increased batch size due to good performance",
			"oldSize", currentBatchSize,
			"newSize", newBatchSize,
			"avgProcessingTime", avgTime,
			"successRate", successRate)
	}
}

// processPendingEvents attempts to finalize all pending events.
func (efm *EventFlowManager) processPendingEvents() {
	startTime := time.Now()

	// Get a copy of pending events
	efm.mu.RLock()
	pendingEvents := make([]*types.Event, 0, len(efm.pendingEvents))
	for _, event := range efm.pendingEvents {
		pendingEvents = append(pendingEvents, event)
	}
	efm.mu.RUnlock()

	// Sort events by height for more efficient processing
	sort.Slice(pendingEvents, func(i, j int) bool {
		return pendingEvents[i].Height < pendingEvents[j].Height
	})

	// Get current batch size
	efm.metricsMu.RLock()
	batchSize := efm.batchSize
	efm.metricsMu.RUnlock()

	// Process events in batches
	totalProcessed := 0
	totalFinalized := 0

	for i := 0; i < len(pendingEvents); i += batchSize {
		end := i + batchSize
		if end > len(pendingEvents) {
			end = len(pendingEvents)
		}
		batch := pendingEvents[i:end]

		// Process each event in the batch
		for _, event := range batch {
			// Skip if already finalized
			if event.Finalized {
				continue
			}

			// Check if we've exceeded max retries for this event
			efm.mu.RLock()
			retries := efm.eventRetryCount[event.ID]
			efm.mu.RUnlock()

			if retries >= efm.maxRetries {
				efm.hc.logger.Warn("Skipping event that exceeded max retries",
					"eventID", fmt.Sprintf("%x", event.ID),
					"height", event.Height,
					"retries", retries)
				continue
			}

			totalProcessed++

			// Attempt to finalize
			finalized, _ := efm.hc.FinalizeEvent(event)
			if finalized {
				efm.handleFinalizedEvent(event)
				totalFinalized++
			}
		}
	}

	// Update metrics
	processingTime := time.Since(startTime)
	successRate := 0.0
	if totalProcessed > 0 {
		successRate = float64(totalFinalized) / float64(totalProcessed)
	}

	efm.metricsMu.Lock()
	// Use exponential moving average for processing time
	if efm.batchProcessingTime == 0 {
		efm.batchProcessingTime = processingTime
	} else {
		efm.batchProcessingTime = (efm.batchProcessingTime*9 + processingTime) / 10
	}
	efm.lastBatchSize = len(pendingEvents)
	efm.successRate = (efm.successRate*9 + successRate) / 10
	efm.metricsMu.Unlock()

	// Log detailed batch processing metrics if significant work was done
	if totalProcessed > 0 {
		efm.hc.logger.Info("Batch processing completed",
			"pendingCount", len(pendingEvents),
			"processed", totalProcessed,
			"finalized", totalFinalized,
			"successRate", successRate,
			"processingTime", processingTime)
	}
}

// metricsCollector periodically logs metrics about event flow.
func (efm *EventFlowManager) metricsCollector() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			efm.logMetrics()
		case <-efm.ctx.Done():
			return
		}
	}
}

// logMetrics logs the current event flow metrics.
func (efm *EventFlowManager) logMetrics() {
	efm.metricsMu.RLock()
	defer efm.metricsMu.RUnlock()

	efm.mu.RLock()
	pendingCount := len(efm.pendingEvents)
	finalizedCount := len(efm.finalizedEvents)

	// Calculate events by height distribution
	heightDistribution := make(map[string]int)
	for height, events := range efm.eventsByHeight {
		var key string
		if height < 100 {
			key = "0-99"
		} else if height < 1000 {
			key = "100-999"
		} else if height < 10000 {
			key = "1000-9999"
		} else {
			key = "10000+"
		}
		heightDistribution[key] += len(events)
	}
	efm.mu.RUnlock()

	// Enhanced metrics logging
	efm.hc.logger.Info("Event flow metrics",
		"totalCreated", efm.totalEventsCreated,
		"totalFinalized", efm.totalEventsFinalized,
		"pendingCount", pendingCount,
		"finalizedCount", finalizedCount,
		"avgFinalizationTime", efm.avgFinalizationTime,
		"maxFinalizationTime", efm.maxFinalizationTime,
		"finalizationTimeouts", efm.finalizationTimeouts,
		"duplicateCount", efm.eventDuplicateCount,
		"validationErrors", efm.eventValidationErrors,
		"propagationErrors", efm.eventPropagationErrors,
		"eventRetries", efm.eventRetries,
		"batchProcessingTime", efm.batchProcessingTime,
		"lastBatchSize", efm.lastBatchSize,
		"currentBatchSize", efm.batchSize,
		"successRate", efm.successRate,
		"heightDistribution", heightDistribution)
}

// incrementMetric safely increments a metric counter.
func (efm *EventFlowManager) incrementMetric(name string) {
	efm.metricsMu.Lock()
	defer efm.metricsMu.Unlock()

	switch name {
	case "totalEventsCreated":
		efm.totalEventsCreated++
	case "totalEventsFinalized":
		efm.totalEventsFinalized++
	case "finalizationTimeouts":
		efm.finalizationTimeouts++
	case "eventDuplicateCount":
		efm.eventDuplicateCount++
	case "eventValidationErrors":
		efm.eventValidationErrors++
	case "eventPropagationErrors":
		efm.eventPropagationErrors++
	case "eventRetries":
		efm.eventRetries++
	}
}

// GetPendingEvents returns a copy of the current pending events.
func (efm *EventFlowManager) GetPendingEvents() []*types.Event {
	efm.mu.RLock()
	defer efm.mu.RUnlock()

	events := make([]*types.Event, 0, len(efm.pendingEvents))
	for _, event := range efm.pendingEvents {
		events = append(events, event)
	}
	return events
}

// GetFinalizedEvents returns finalized events in the given height range.
func (efm *EventFlowManager) GetFinalizedEvents(fromHeight, toHeight uint64) ([]*types.Event, error) {
	if fromHeight > toHeight {
		return nil, errors.New("invalid height range")
	}

	efm.mu.RLock()
	defer efm.mu.RUnlock()

	var events []*types.Event
	for h := fromHeight; h <= toHeight; h++ {
		if heightEvents, ok := efm.eventsByHeight[h]; ok {
			for _, event := range heightEvents {
				if event.Finalized {
					events = append(events, event)
				}
			}
		}
	}

	// Sort events by height for consistent results
	sort.Slice(events, func(i, j int) bool {
		return events[i].Height < events[j].Height
	})

	return events, nil
}

// GetEventMetrics returns the current event flow metrics.
func (efm *EventFlowManager) GetEventMetrics() map[string]interface{} {
	efm.metricsMu.RLock()
	defer efm.metricsMu.RUnlock()

	efm.mu.RLock()
	pendingCount := len(efm.pendingEvents)
	finalizedCount := len(efm.finalizedEvents)

	// Calculate retry distribution
	retryDistribution := make(map[int]int)
	for _, retries := range efm.eventRetryCount {
		retryDistribution[retries]++
	}
	efm.mu.RUnlock()

	return map[string]interface{}{
		"totalEventsCreated":     efm.totalEventsCreated,
		"totalEventsFinalized":   efm.totalEventsFinalized,
		"pendingCount":           pendingCount,
		"finalizedCount":         finalizedCount,
		"avgFinalizationTime":    efm.avgFinalizationTime.String(),
		"maxFinalizationTime":    efm.maxFinalizationTime.String(),
		"finalizationTimeouts":   efm.finalizationTimeouts,
		"eventDuplicateCount":    efm.eventDuplicateCount,
		"eventValidationErrors":  efm.eventValidationErrors,
		"eventPropagationErrors": efm.eventPropagationErrors,
		"eventRetries":           efm.eventRetries,
		"batchProcessingTime":    efm.batchProcessingTime.String(),
		"currentBatchSize":       efm.batchSize,
		"successRate":            efm.successRate,
		"retryDistribution":      retryDistribution,
	}
}

// ValidateEvent performs comprehensive validation of an event.
// It checks the event's creator, PoH information, and ensures it's not a duplicate.
// It also verifies that parent events exist and are valid.
func (efm *EventFlowManager) ValidateEvent(event *types.Event) error {
	if event == nil {
		return NewConsensusError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"event is nil",
		)
	}

	// Check if creator is an active validator
	if !efm.hc.validatorManager.IsActiveValidator(event.Creator) {
		return NewConsensusError(
			ErrInvalidValidator,
			ErrorCategoryTemporary,
			"creator is not an active validator",
		).WithEventID(event.ID).
			WithValidatorID(event.Creator).
			WithContext("height", event.Height)
	}

	// Verify PoH information
	if !efm.hc.verifyPoHWithDrift(event.PoHState, event.Data, event.PoHProof, event.PoHCount) {
		return NewConsensusError(
			ErrPoHVerificationFailed,
			ErrorCategoryByzantine,
			"PoH verification failed",
		).WithEventID(event.ID).
			WithValidatorID(event.Creator).
			WithContext("height", event.Height).
			WithContext("pohCount", event.PoHCount).
			WithContext("currentPohCount", efm.hc.poh.GetCount())
	}

	// Check for duplicate event with improved error context
	efm.mu.RLock()
	pendingEvent, isPending := efm.pendingEvents[event.ID]
	finalizedEvent, isFinalized := efm.finalizedEvents[event.ID]
	efm.mu.RUnlock()

	if isPending || isFinalized {
		efm.incrementMetric("eventDuplicateCount")

		var duplicateInfo string
		var duplicateHeight uint64
		if isPending {
			duplicateInfo = "pending"
			duplicateHeight = pendingEvent.Height
		} else {
			duplicateInfo = "finalized"
			duplicateHeight = finalizedEvent.Height
		}

		return NewConsensusError(
			ErrEventDuplicate,
			ErrorCategoryTemporary,
			fmt.Sprintf("duplicate event: %s event at height %d", duplicateInfo, duplicateHeight),
		).WithEventID(event.ID).
			WithValidatorID(event.Creator).
			WithContext("height", event.Height).
			WithContext("duplicateStatus", duplicateInfo).
			WithContext("duplicateHeight", duplicateHeight)
	}

	// Validate timestamp
	if event.Timestamp.IsZero() {
		return NewConsensusError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"event has zero timestamp",
		).WithEventID(event.ID).
			WithValidatorID(event.Creator).
			WithContext("height", event.Height)
	}

	// Check if timestamp is in the future (with some tolerance)
	if event.Timestamp.After(time.Now().Add(5 * time.Second)) {
		return NewConsensusError(
			ErrEventValidationFailed,
			ErrorCategoryTemporary,
			"event timestamp is too far in the future",
		).WithEventID(event.ID).
			WithValidatorID(event.Creator).
			WithContext("height", event.Height).
			WithContext("timestamp", event.Timestamp).
			WithContext("currentTime", time.Now())
	}

	// Additional validation: check parent IDs exist
	if len(event.ParentIDs) > 0 {
		missingParents := []string{}

		for _, parentID := range event.ParentIDs {
			efm.mu.RLock()
			_, parentPending := efm.pendingEvents[parentID]
			_, parentFinalized := efm.finalizedEvents[parentID]
			efm.mu.RUnlock()

			// If parent doesn't exist in our records, add to missing parents list
			if !parentPending && !parentFinalized {
				missingParents = append(missingParents, fmt.Sprintf("%x", parentID))
			}
		}

		// If we have missing parents, log a warning
		// This is not a fatal error as the parent might be in Lachesis but not in our local tracking
		if len(missingParents) > 0 {
			efm.hc.logger.Warn("Event references unknown parents",
				"eventID", fmt.Sprintf("%x", event.ID),
				"missingParents", missingParents,
				"totalParents", len(event.ParentIDs),
				"missingCount", len(missingParents))
		}
	}

	return nil
}

// HandleNetworkPartition processes events received during a network partition.
func (efm *EventFlowManager) HandleNetworkPartition(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Sort events by height for consistent processing
	sort.Slice(events, func(i, j int) bool {
		return events[i].Height < events[j].Height
	})

	// Process each event
	processed := 0
	skipped := 0
	validated := 0

	for _, event := range events {
		// Skip if already finalized
		if event.Finalized {
			skipped++
			continue
		}

		// Validate the event
		if efm.enableValidation {
			if err := efm.ValidateEvent(event); err != nil {
				// If it's a duplicate, we can just skip it
				if err.Error() == "duplicate event" {
					skipped++
					continue
				}

				efm.incrementMetric("eventValidationErrors")
				efm.hc.logger.Info("Event validation failed during partition recovery",
					"eventID", fmt.Sprintf("%x", event.ID),
					"error", err)
				continue
			}
			validated++
		}

		// Add to pending events
		efm.mu.Lock()
		efm.pendingEvents[event.ID] = event
		efm.eventCreationTime[event.ID] = time.Now()
		efm.eventsByHeight[event.Height] = append(efm.eventsByHeight[event.Height], event)
		efm.eventRetryCount[event.ID] = 0
		efm.mu.Unlock()

		// Start finalization process
		efm.finalizationWg.Add(1)
		go efm.finalizeEvent(event)
		processed++
	}

	// Log detailed partition recovery metrics
	efm.hc.logger.Info("Network partition recovery completed",
		"totalEvents", len(events),
		"processed", processed,
		"skipped", skipped,
		"validated", validated)

	return nil
}

// GetEventByID returns an event by its ID.
func (efm *EventFlowManager) GetEventByID(id [32]byte) (*types.Event, bool) {
	efm.mu.RLock()
	defer efm.mu.RUnlock()

	// Check pending events
	if event, ok := efm.pendingEvents[id]; ok {
		return event, true
	}

	// Check finalized events
	if event, ok := efm.finalizedEvents[id]; ok {
		return event, true
	}

	return nil, false
}

// GetEventsByHeight returns all events at a specific height.
func (efm *EventFlowManager) GetEventsByHeight(height uint64) []*types.Event {
	efm.mu.RLock()
	defer efm.mu.RUnlock()

	if events, ok := efm.eventsByHeight[height]; ok {
		// Return a copy to prevent concurrent modification
		result := make([]*types.Event, len(events))
		copy(result, events)
		return result
	}

	return nil
}

// CleanupOldEvents removes finalized events below a certain height.
func (efm *EventFlowManager) CleanupOldEvents(maxHeight uint64) int {
	efm.mu.Lock()
	defer efm.mu.Unlock()

	removed := 0
	for h := uint64(0); h < maxHeight; h++ {
		if events, ok := efm.eventsByHeight[h]; ok {
			for _, event := range events {
				if event.Finalized {
					delete(efm.finalizedEvents, event.ID)
					delete(efm.eventCreationTime, event.ID)
					delete(efm.eventRetryCount, event.ID)
					removed++
				}
			}
			delete(efm.eventsByHeight, h)
		}
	}

	efm.hc.logger.Info("Cleaned up old events",
		"maxHeight", maxHeight,
		"removedCount", removed)

	return removed
}

// SetFinalizationTimeout sets the timeout for event finalization.
func (efm *EventFlowManager) SetFinalizationTimeout(timeout time.Duration) {
	if timeout > 0 {
		efm.finalizationTimeout = timeout
		efm.hc.logger.Info("Updated finalization timeout", "timeout", timeout)
	}
}

// SetMaxPendingEvents sets the maximum number of pending events.
func (efm *EventFlowManager) SetMaxPendingEvents(max int) {
	if max > 0 {
		efm.maxPendingEvents = max
		efm.hc.logger.Info("Updated max pending events", "max", max)
	}
}

// SetBatchSize sets the batch size for processing pending events.
func (efm *EventFlowManager) SetBatchSize(size int) {
	if size > 0 {
		efm.batchSize = size
		efm.hc.logger.Info("Updated batch size", "size", size)
	}
}

// EnableDeduplication enables or disables event deduplication.
func (efm *EventFlowManager) EnableDeduplication(enable bool) {
	efm.enableDeduplication = enable
	efm.hc.logger.Info("Updated deduplication setting", "enabled", enable)
}

// EnableValidation enables or disables event validation.
func (efm *EventFlowManager) EnableValidation(enable bool) {
	efm.enableValidation = enable
	efm.hc.logger.Info("Updated validation setting", "enabled", enable)
}

// SetMaxRetries sets the maximum number of retry attempts for event finalization.
func (efm *EventFlowManager) SetMaxRetries(max int) {
	if max > 0 {
		efm.maxRetries = max
		efm.hc.logger.Info("Updated max retries", "max", max)
	}
}

// EnableAdaptiveBatching enables or disables adaptive batch sizing.
func (efm *EventFlowManager) EnableAdaptiveBatching(enable bool) {
	efm.adaptiveBatching = enable

	// Start or stop the adaptive batch size worker
	if enable && !efm.adaptiveBatching {
		go efm.adaptiveBatchSizeWorker()
	}

	efm.hc.logger.Info("Updated adaptive batching setting", "enabled", enable)
}

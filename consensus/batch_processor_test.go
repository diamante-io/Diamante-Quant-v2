package consensus

import (
	"testing"
	"time"

	"diamante/consensus/types"
)

func TestBatchProcessor_Creation(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	bp := NewBatchProcessor(config, logger)

	if bp == nil {
		t.Fatal("Failed to create BatchProcessor")
	}

	if bp.config.BatchSize != DefaultBatchSize {
		t.Errorf("Expected batch size %d, got %d", DefaultBatchSize, bp.config.BatchSize)
	}
}

func TestBatchProcessor_AddEvent(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	// Use a smaller batch size for testing
	config.BatchSize = 5
	bp := NewBatchProcessor(config, logger)

	// Create a test event
	event := &types.Event{
		ID:      [32]byte{1, 2, 3, 4},
		Creator: [32]byte{5, 6, 7, 8},
		Height:  1,
		Data:    []byte("test event"),
	}

	// Add the event to the batch processor
	bp.AddEvent(event)

	// Check that the event was added
	if bp.GetPendingCount() != 1 {
		t.Errorf("Expected pending count 1, got %d", bp.GetPendingCount())
	}
}

func TestBatchProcessor_StartStop(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	bp := NewBatchProcessor(config, logger)

	// Start the batch processor
	err := bp.Start()
	if err != nil {
		t.Fatalf("Failed to start BatchProcessor: %v", err)
	}

	// Stop the batch processor
	err = bp.Stop()
	if err != nil {
		t.Fatalf("Failed to stop BatchProcessor: %v", err)
	}
}

func TestBatchProcessor_ProcessBatch(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	// Use a smaller batch size for testing
	config.BatchSize = 3
	config.MaxBatchDelay = 100 * time.Millisecond
	bp := NewBatchProcessor(config, logger)

	// Start the batch processor
	err := bp.Start()
	if err != nil {
		t.Fatalf("Failed to start BatchProcessor: %v", err)
	}
	defer bp.Stop()

	// Create and add test events
	for i := 0; i < 5; i++ {
		event := &types.Event{
			ID:      [32]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
			Creator: [32]byte{byte(i + 4), byte(i + 5), byte(i + 6), byte(i + 7)},
			Height:  uint64(i + 1),
			Data:    []byte("test event"),
		}
		bp.AddEvent(event)
	}

	// Wait for batch processing to complete
	time.Sleep(200 * time.Millisecond)

	// Check metrics
	metrics := bp.GetMetrics()
	if metrics.TotalBatches == 0 {
		t.Error("Expected at least one batch to be processed")
	}
}

func TestBatchProcessor_GroupByCreator(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	// Enable grouping by creator
	config.GroupByCreator = true
	config.BatchSize = 2
	config.MaxBatchDelay = 100 * time.Millisecond
	bp := NewBatchProcessor(config, logger)

	// Start the batch processor
	err := bp.Start()
	if err != nil {
		t.Fatalf("Failed to start BatchProcessor: %v", err)
	}
	defer bp.Stop()

	// Create events from two different creators
	creator1 := [32]byte{1, 0, 0, 0}
	creator2 := [32]byte{2, 0, 0, 0}

	// Add events from creator1
	for i := 0; i < 3; i++ {
		event := &types.Event{
			ID:      [32]byte{1, byte(i), byte(i + 1), byte(i + 2)},
			Creator: creator1,
			Height:  uint64(i + 1),
			Data:    []byte("creator1 event"),
		}
		bp.AddEvent(event)
	}

	// Add events from creator2
	for i := 0; i < 2; i++ {
		event := &types.Event{
			ID:      [32]byte{2, byte(i), byte(i + 1), byte(i + 2)},
			Creator: creator2,
			Height:  uint64(i + 1),
			Data:    []byte("creator2 event"),
		}
		bp.AddEvent(event)
	}

	// Wait for batch processing to complete
	time.Sleep(200 * time.Millisecond)

	// Check metrics
	metrics := bp.GetMetrics()
	if metrics.TotalBatches < 2 {
		t.Errorf("Expected at least 2 batches to be processed, got %d", metrics.TotalBatches)
	}
}

func TestBatchProcessor_ParallelProcessing(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	// Enable parallel processing
	config.ParallelProcessing = true
	config.MaxParallelBatches = 2
	config.BatchSize = 2
	config.MaxBatchDelay = 100 * time.Millisecond
	bp := NewBatchProcessor(config, logger)

	// Start the batch processor
	err := bp.Start()
	if err != nil {
		t.Fatalf("Failed to start BatchProcessor: %v", err)
	}
	defer bp.Stop()

	// Add several events
	for i := 0; i < 8; i++ {
		event := &types.Event{
			ID:      [32]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
			Creator: [32]byte{byte(i % 4), byte(i%4 + 1), byte(i%4 + 2), byte(i%4 + 3)},
			Height:  uint64(i + 1),
			Data:    []byte("test event for parallel processing"),
		}
		bp.AddEvent(event)
	}

	// Wait for batch processing to complete
	time.Sleep(200 * time.Millisecond)

	// Check metrics
	metrics := bp.GetMetrics()
	if metrics.TotalBatches == 0 {
		t.Error("Expected at least one batch to be processed")
	}
}

func TestBatchProcessor_AdaptiveBatchSize(t *testing.T) {
	logger := newHybridConsensusLogger()
	config := DefaultBatchProcessorConfig()
	// Enable adaptive batch size
	config.AdaptiveBatchSize = true
	config.BatchSize = 10
	config.MaxBatchDelay = 50 * time.Millisecond
	bp := NewBatchProcessor(config, logger)

	// Start the batch processor
	err := bp.Start()
	if err != nil {
		t.Fatalf("Failed to start BatchProcessor: %v", err)
	}
	defer bp.Stop()

	// Add many events to trigger batch processing
	for i := 0; i < 30; i++ {
		event := &types.Event{
			ID:      [32]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
			Creator: [32]byte{byte(i % 4), byte(i%4 + 1), byte(i%4 + 2), byte(i%4 + 3)},
			Height:  uint64(i + 1),
			Data:    []byte("test event for adaptive batch size"),
		}
		bp.AddEvent(event)
	}

	// Wait for batch processing and adaptation to occur
	time.Sleep(500 * time.Millisecond)

	// Check metrics
	metrics := bp.GetMetrics()
	if metrics.TotalBatches == 0 {
		t.Error("Expected at least one batch to be processed")
	}

	// Note: We can't reliably test if the batch size was adapted since it depends on
	// the processing time, which can vary. But we can at least check that the
	// batch processor is functioning.
}

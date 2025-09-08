// consensus/batch_processor_extensions.go

package consensus

import (
	"fmt"
	"math"
)

// SetBatchSize sets the batch size for the batch processor
func (bp *BatchProcessor) SetBatchSize(size int) {
	// Ensure size is within bounds
	if size < MinBatchSize {
		size = MinBatchSize
	} else if size > MaxBatchSize {
		size = MaxBatchSize
	}

	// Only log if there's a significant change
	if math.Abs(float64(size-bp.config.BatchSize)) > float64(bp.config.BatchSize)*0.05 {
		bp.logger.Info("Batch size adjusted",
			LogKeyValue{Key: "oldSize", Value: fmt.Sprintf("%d", bp.config.BatchSize)},
			LogKeyValue{Key: "newSize", Value: fmt.Sprintf("%d", size)})
	}

	bp.config.BatchSize = size
}

// Resize dynamically adjusts the batch size
func (bp *BatchProcessor) Resize(size int) error {
	bp.processingMu.Lock()
	defer bp.processingMu.Unlock()

	if size < MinBatchSize || size > MaxBatchSize {
		return fmt.Errorf("batch size must be between %d and %d", MinBatchSize, MaxBatchSize)
	}

	bp.logger.Info("Resizing batch processor",
		LogKeyValue{Key: "oldSize", Value: fmt.Sprintf("%d", bp.config.BatchSize)},
		LogKeyValue{Key: "newSize", Value: fmt.Sprintf("%d", size)})

	bp.config.BatchSize = size
	return nil
}

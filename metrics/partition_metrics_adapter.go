// metrics/partition_metrics_adapter.go
package metrics

import (
	"context"
	"time"

	networkpkg "diamante/network"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// PartitionHandlerAdapter adapts the network.PartitionHandler to the PartitionMetricsProvider interface.
// It provides a safe wrapper around the network partition handler with proper error handling
// and validation for production use.
type PartitionHandlerAdapter struct {
	partitionHandler *networkpkg.PartitionHandler
	logger           *logrus.Logger
}

// NewPartitionHandlerAdapter creates a new adapter for the partition handler with proper validation.
// It returns an error if the handler parameter is nil, following Go idioms for constructor functions.
//
// Parameters:
//   - handler: The network partition handler to adapt (must not be nil)
//   - logger: Logger instance for error reporting (optional, will create default if nil)
//
// Returns:
//   - *PartitionHandlerAdapter: The created adapter
//   - error: Validation error if handler is nil
func NewPartitionHandlerAdapter(handler *networkpkg.PartitionHandler, logger *logrus.Logger) (*PartitionHandlerAdapter, error) {
	if handler == nil {
		return nil, errors.New("partition handler cannot be nil")
	}

	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &PartitionHandlerAdapter{
		partitionHandler: handler,
		logger:           logger,
	}, nil
}

// GetPartitionStatus returns the current partition status as a string with proper error handling.
// This method safely delegates to the underlying partition handler and wraps any potential errors
// with context for better debugging in production environments.
//
// Returns:
//   - string: Current partition status ("Normal", "Suspected", "Confirmed", "Recovering")
//   - error: Any error that occurred while retrieving the status
func (pha *PartitionHandlerAdapter) GetPartitionStatus() (string, error) {
	if pha.partitionHandler == nil {
		return "", errors.New("partition handler is nil")
	}

	// Safely call the underlying handler with recovery from potential panics
	defer func() {
		if r := recover(); r != nil {
			pha.logger.WithField("panic", r).Error("Panic occurred while getting partition status")
		}
	}()

	status := pha.partitionHandler.GetPartitionStatusString()
	if status == "" {
		pha.logger.Warning("Partition handler returned empty status")
		return "Unknown", nil
	}

	return status, nil
}

// GetPartitionMetrics returns a map of partition metrics with comprehensive error handling.
// This method safely retrieves metrics from the underlying partition handler and validates
// the returned data to prevent runtime panics in production.
//
// Returns:
//   - map[string]interface{}: Map containing partition metrics data
//   - error: Any error that occurred while retrieving metrics
func (pha *PartitionHandlerAdapter) GetPartitionMetrics() (map[string]interface{}, error) {
	if pha.partitionHandler == nil {
		return nil, errors.New("partition handler is nil")
	}

	// Safely call the underlying handler with recovery from potential panics
	var metrics map[string]interface{}
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Errorf("panic occurred while getting partition metrics: %v", r)
				pha.logger.WithField("panic", r).Error("Panic occurred while getting partition metrics")
			}
		}()

		metrics = pha.partitionHandler.GetPartitionMetricsMap()
	}()

	if err != nil {
		return nil, errors.Wrap(err, "failed to get partition metrics")
	}

	if metrics == nil {
		pha.logger.Warning("Partition handler returned nil metrics map")
		return make(map[string]interface{}), nil
	}

	return metrics, nil
}

// CreatePartitionReporter creates a new partition reporter for the given partition handler
// with comprehensive parameter validation and error handling. This factory function ensures
// all dependencies are properly validated before creating the reporter.
//
// Parameters:
//   - handler: The network partition handler (must not be nil)
//   - updateInterval: Interval for metrics collection updates (must be > 0)
//   - thresholds: Alert thresholds configuration (optional, will use defaults if nil)
//   - logger: Logger instance (optional, will create default if nil)
//
// Returns:
//   - *PartitionReporter: The created partition reporter
//   - error: Validation error if any required parameter is invalid
func CreatePartitionReporter(
	handler *networkpkg.PartitionHandler,
	updateInterval time.Duration,
	thresholds *PartitionAlertThresholds,
	logger *logrus.Logger,
) (*PartitionReporter, error) {
	// Validate required parameters
	if handler == nil {
		return nil, errors.New("partition handler cannot be nil")
	}

	if updateInterval <= 0 {
		return nil, errors.New("update interval must be greater than zero")
	}

	// Create adapter for the partition handler with validation
	adapter, err := NewPartitionHandlerAdapter(handler, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create partition handler adapter")
	}

	// Use default thresholds if none provided
	if thresholds == nil {
		defaultThresholds := DefaultPartitionAlertThresholds()
		thresholds = &defaultThresholds
	}

	// Ensure logger is available
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Create and return the partition reporter with proper error handling
	reporter := NewPartitionReporter(adapter, updateInterval, logger, thresholds)
	if reporter == nil {
		return nil, errors.New("failed to create partition reporter")
	}

	return reporter, nil
}

// ValidatePartitionHandler performs comprehensive validation of a partition handler
// to ensure it's suitable for production use. This function checks for common issues
// that could cause runtime failures.
//
// Parameters:
//   - handler: The partition handler to validate
//
// Returns:
//   - error: Validation error if the handler is not suitable for production use
func ValidatePartitionHandler(handler *networkpkg.PartitionHandler) error {
	if handler == nil {
		return errors.New("partition handler is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test basic functionality with timeout
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- errors.Errorf("partition handler panicked during validation: %v", r)
			}
		}()

		// Test status retrieval
		status := handler.GetPartitionStatusString()
		if status == "" {
			done <- errors.New("partition handler returns empty status")
		}

		// Test metrics retrieval
		metrics := handler.GetPartitionMetricsMap()
		if metrics == nil {
			done <- errors.New("partition handler returns nil metrics")
		}

		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			return errors.Wrap(err, "partition handler validation failed")
		}
	case <-ctx.Done():
		return errors.New("partition handler validation timed out")
	}

	return nil
}

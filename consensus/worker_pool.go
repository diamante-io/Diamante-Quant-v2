// consensus/worker_pool.go
package consensus

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Local error functions to avoid import cycle
func consensusError(err error, msg string) error {
	if err == nil {
		return fmt.Errorf("consensus error: %s", msg)
	}
	return fmt.Errorf("consensus error: %s: %w", msg, err)
}

func timeoutError(err error, msg string) error {
	if err == nil {
		return fmt.Errorf("timeout error: %s", msg)
	}
	return fmt.Errorf("timeout error: %s: %w", msg, err)
}

func validationError(err error, msg string) error {
	if err == nil {
		return fmt.Errorf("validation error: %s", msg)
	}
	return fmt.Errorf("validation error: %s: %w", msg, err)
}

// WorkerPool manages a pool of worker goroutines for parallel task processing
type WorkerPool struct {
	workers int
	queue   chan func()
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	running int32
	logger  *hybridConsensusLogger

	// Worker management for downscaling
	workerShutdown chan int // Channel to signal specific workers to shutdown
	workerIDs      sync.Map // Track active worker IDs

	// Metrics
	tasksProcessed int64
	tasksInQueue   int64
	workersActive  int64
	totalDuration  int64

	// Configuration
	maxQueueSize    int
	workerTimeout   time.Duration
	shutdownTimeout time.Duration
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers, maxQueueSize int, logger *hybridConsensusLogger) *WorkerPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if maxQueueSize <= 0 {
		maxQueueSize = workers * 100
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workers:         workers,
		queue:           make(chan func(), maxQueueSize),
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger,
		maxQueueSize:    maxQueueSize,
		workerTimeout:   30 * time.Second,
		shutdownTimeout: 10 * time.Second,
		workerShutdown:  make(chan int, workers), // Buffered channel for shutdown signals
	}
}

// Start initializes and starts all worker goroutines
func (wp *WorkerPool) Start() error {
	if !atomic.CompareAndSwapInt32(&wp.running, 0, 1) {
		return consensusError(nil, "worker pool already running")
	}

	// Start worker goroutines
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		wp.workerIDs.Store(i, true) // Track active worker
		go wp.worker(i)
	}

	wp.logger.Info("Worker pool started",
		LogKeyValue{Key: "workers", Value: fmt.Sprintf("%d", wp.workers)},
		LogKeyValue{Key: "queue_size", Value: fmt.Sprintf("%d", wp.maxQueueSize)})
	return nil
}

// Stop gracefully shuts down the worker pool
func (wp *WorkerPool) Stop() error {
	if !atomic.CompareAndSwapInt32(&wp.running, 1, 0) {
		return nil // Already stopped
	}

	// Cancel context to signal workers
	wp.cancel()

	// Close queue to prevent new submissions
	close(wp.queue)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		wp.logger.Info("Worker pool stopped gracefully")
	case <-time.After(wp.shutdownTimeout):
		wp.logger.Warn("Worker pool shutdown timeout reached")
	}

	return nil
}

// Submit adds a task to the worker queue
func (wp *WorkerPool) Submit(task func()) error {
	if atomic.LoadInt32(&wp.running) == 0 {
		return consensusError(nil, "worker pool not running")
	}

	select {
	case wp.queue <- task:
		atomic.AddInt64(&wp.tasksInQueue, 1)
		return nil
	case <-wp.ctx.Done():
		return consensusError(nil, "worker pool shutting down")
	default:
		return consensusError(nil, "worker queue full")
	}
}

// SubmitWithTimeout adds a task with a timeout
func (wp *WorkerPool) SubmitWithTimeout(task func(), timeout time.Duration) error {
	if atomic.LoadInt32(&wp.running) == 0 {
		return consensusError(nil, "worker pool not running")
	}

	select {
	case wp.queue <- task:
		atomic.AddInt64(&wp.tasksInQueue, 1)
		return nil
	case <-time.After(timeout):
		return timeoutError(nil, "submit task")
	case <-wp.ctx.Done():
		return consensusError(nil, "worker pool shutting down")
	}
}

// worker is the main worker goroutine function
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	defer wp.workerIDs.Delete(id) // Remove from active workers when done

	wp.logger.Debug("Worker started", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)})

	for {
		select {
		case shutdownID := <-wp.workerShutdown:
			if shutdownID == id {
				wp.logger.Debug("Worker shutting down on signal", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)})
				return
			} else {
				// Put the signal back if it's not for this worker
				select {
				case wp.workerShutdown <- shutdownID:
				default:
					// Channel full, just continue
				}
			}
		case task, ok := <-wp.queue:
			if !ok {
				wp.logger.Debug("Worker stopping - queue closed", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)})
				return
			}

			atomic.AddInt64(&wp.workersActive, 1)
			atomic.AddInt64(&wp.tasksInQueue, -1)

			start := ConsensusNow()

			// Execute task with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						wp.logger.Error("Task panic in worker",
							LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)},
							LogKeyValue{Key: "panic", Value: fmt.Sprintf("%v", r)})
					}
				}()

				// Set worker timeout
				ctx, cancel := context.WithTimeout(wp.ctx, wp.workerTimeout)
				defer cancel()

				// Execute task in goroutine to handle timeout
				done := make(chan struct{})
				go func() {
					defer close(done)
					task()
				}()

				select {
				case <-done:
					// Task completed successfully
				case <-ctx.Done():
					wp.logger.Warn("Task timeout in worker",
						LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)},
						LogKeyValue{Key: "timeout", Value: wp.workerTimeout.String()})
				}
			}()

			duration := ConsensusSince(start)
			atomic.AddInt64(&wp.totalDuration, int64(duration))
			atomic.AddInt64(&wp.tasksProcessed, 1)
			atomic.AddInt64(&wp.workersActive, -1)

		case <-wp.ctx.Done():
			wp.logger.Debug("Worker stopping - context cancelled", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", id)})
			return
		}
	}
}

// GetStats returns current worker pool statistics
func (wp *WorkerPool) GetStats() WorkerPoolStats {
	tasksProcessed := atomic.LoadInt64(&wp.tasksProcessed)
	totalDuration := atomic.LoadInt64(&wp.totalDuration)

	var avgDuration time.Duration
	if tasksProcessed > 0 {
		avgDuration = time.Duration(totalDuration / tasksProcessed)
	}

	return WorkerPoolStats{
		Workers:        wp.workers,
		QueueSize:      wp.maxQueueSize,
		TasksProcessed: tasksProcessed,
		TasksInQueue:   atomic.LoadInt64(&wp.tasksInQueue),
		WorkersActive:  atomic.LoadInt64(&wp.workersActive),
		AvgDuration:    avgDuration,
		Running:        atomic.LoadInt32(&wp.running) == 1,
	}
}

// WorkerPoolStats contains worker pool statistics
type WorkerPoolStats struct {
	Workers        int           `json:"workers"`
	QueueSize      int           `json:"queue_size"`
	TasksProcessed int64         `json:"tasks_processed"`
	TasksInQueue   int64         `json:"tasks_in_queue"`
	WorkersActive  int64         `json:"workers_active"`
	AvgDuration    time.Duration `json:"avg_duration"`
	Running        bool          `json:"running"`
}

// GetQueueUtilization returns the queue utilization percentage
func (wp *WorkerPool) GetQueueUtilization() float64 {
	tasksInQueue := atomic.LoadInt64(&wp.tasksInQueue)
	return float64(tasksInQueue) / float64(wp.maxQueueSize) * 100
}

// GetWorkerUtilization returns the worker utilization percentage
func (wp *WorkerPool) GetWorkerUtilization() float64 {
	workersActive := atomic.LoadInt64(&wp.workersActive)
	return float64(workersActive) / float64(wp.workers) * 100
}

// IsHealthy returns true if the worker pool is operating within normal parameters
func (wp *WorkerPool) IsHealthy() bool {
	if atomic.LoadInt32(&wp.running) == 0 {
		return false
	}

	queueUtil := wp.GetQueueUtilization()
	workerUtil := wp.GetWorkerUtilization()

	// Consider healthy if queue is not near full and workers are available
	return queueUtil < 90.0 && workerUtil < 95.0
}

// Resize dynamically adjusts the number of workers
func (wp *WorkerPool) Resize(newWorkerCount int) error {
	if atomic.LoadInt32(&wp.running) == 0 {
		return consensusError(nil, "cannot resize stopped worker pool")
	}

	if newWorkerCount <= 0 {
		return validationError(nil, "worker count must be positive")
	}

	currentWorkers := wp.workers

	if newWorkerCount > currentWorkers {
		// Add workers
		for i := currentWorkers; i < newWorkerCount; i++ {
			wp.wg.Add(1)
			wp.workerIDs.Store(i, true) // Track new worker
			go wp.worker(i)
		}
		wp.workers = newWorkerCount
		wp.logger.Info("Worker pool resized up",
			LogKeyValue{Key: "old_size", Value: fmt.Sprintf("%d", currentWorkers)},
			LogKeyValue{Key: "new_size", Value: fmt.Sprintf("%d", newWorkerCount)})

	} else if newWorkerCount < currentWorkers {
		// Downscale workers gracefully
		workersToRemove := currentWorkers - newWorkerCount
		wp.logger.Info("Worker pool downscaling",
			LogKeyValue{Key: "current", Value: fmt.Sprintf("%d", currentWorkers)},
			LogKeyValue{Key: "target", Value: fmt.Sprintf("%d", newWorkerCount)},
			LogKeyValue{Key: "removing", Value: fmt.Sprintf("%d", workersToRemove)})

		// Send shutdown signals to excess workers
		// We'll signal the highest-numbered workers to shut down
		removedCount := 0
		for workerID := currentWorkers - 1; workerID >= newWorkerCount && removedCount < workersToRemove; workerID-- {
			if _, exists := wp.workerIDs.Load(workerID); exists {
				select {
				case wp.workerShutdown <- workerID:
					removedCount++
					wp.logger.Debug("Sent shutdown signal to worker", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", workerID)})
				case <-time.After(1 * time.Second):
					wp.logger.Warn("Timeout sending shutdown signal to worker", LogKeyValue{Key: "worker_id", Value: fmt.Sprintf("%d", workerID)})
				}
			}
		}

		// Wait for workers to shut down gracefully (with timeout)
		shutdownTimeout := time.After(wp.shutdownTimeout)
		for removedCount > 0 {
			select {
			case <-shutdownTimeout:
				wp.logger.Warn("Timeout waiting for workers to shut down during downscale",
					LogKeyValue{Key: "remaining", Value: fmt.Sprintf("%d", removedCount)})
				// Continue anyway, update the count
				wp.workers = newWorkerCount
				return nil
			case <-time.After(100 * time.Millisecond):
				// Check how many workers are still active
				activeCount := 0
				wp.workerIDs.Range(func(key, value interface{}) bool {
					if workerID, ok := key.(int); ok && workerID >= newWorkerCount {
						activeCount++
					}
					return true
				})

				if activeCount == 0 {
					// All excess workers have shut down
					break
				}
			}
		}

		wp.workers = newWorkerCount
		wp.logger.Info("Worker pool successfully downscaled",
			LogKeyValue{Key: "old_size", Value: fmt.Sprintf("%d", currentWorkers)},
			LogKeyValue{Key: "new_size", Value: fmt.Sprintf("%d", newWorkerCount)})
	}

	return nil
}

// Priority task wrapper for task prioritization
type PriorityTask struct {
	Task     func()
	Priority int // Higher number = higher priority
	ID       string
	Created  time.Time
}

// PriorityWorkerPool extends WorkerPool with priority-based task scheduling
type PriorityWorkerPool struct {
	*WorkerPool
	priorityQueue chan PriorityTask
	priorities    map[int]int64 // priority level -> task count
	prioritiesMu  sync.RWMutex
}

// NewPriorityWorkerPool creates a new priority-based worker pool
func NewPriorityWorkerPool(workers, maxQueueSize int, logger *hybridConsensusLogger) *PriorityWorkerPool {
	base := NewWorkerPool(workers, maxQueueSize, logger)

	return &PriorityWorkerPool{
		WorkerPool:    base,
		priorityQueue: make(chan PriorityTask, maxQueueSize),
		priorities:    make(map[int]int64),
	}
}

// SubmitPriority adds a priority task to the queue
func (pwp *PriorityWorkerPool) SubmitPriority(task func(), priority int, id string) error {
	if atomic.LoadInt32(&pwp.running) == 0 {
		return consensusError(nil, "priority worker pool not running")
	}

	priorityTask := PriorityTask{
		Task:     task,
		Priority: priority,
		ID:       id,
		Created:  ConsensusNow(),
	}

	select {
	case pwp.priorityQueue <- priorityTask:
		pwp.prioritiesMu.Lock()
		pwp.priorities[priority]++
		pwp.prioritiesMu.Unlock()
		return nil
	case <-pwp.ctx.Done():
		return consensusError(nil, "priority worker pool shutting down")
	default:
		return consensusError(nil, "priority queue full")
	}
}

// GetPriorityStats returns statistics about task priorities
func (pwp *PriorityWorkerPool) GetPriorityStats() map[int]int64 {
	pwp.prioritiesMu.RLock()
	defer pwp.prioritiesMu.RUnlock()

	result := make(map[int]int64)
	for priority, count := range pwp.priorities {
		result[priority] = count
	}
	return result
}

// Package runtime provides cross-runtime communication through a unified event bus
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

var (
	// ErrEventBusNotStarted is returned when event bus is not started
	ErrEventBusNotStarted = errors.New("event bus not started")

	// ErrSubscriberNotFound is returned when subscriber is not found
	ErrSubscriberNotFound = errors.New("subscriber not found")

	// ErrInvalidSubscription is returned when subscription is invalid
	ErrInvalidSubscription = errors.New("invalid subscription")

	// ErrEventDeliveryFailed is returned when event delivery fails
	ErrEventDeliveryFailed = errors.New("event delivery failed")
)

// EventSubscriber represents a subscriber to events
type EventSubscriber interface {
	// HandleEvent handles an event
	HandleEvent(event ContractEvent) error

	// ID returns the subscriber ID
	ID() string

	// Filters returns event filters for this subscriber
	Filters() []EventFilter
}

// EventFilter defines criteria for filtering events
type EventFilter struct {
	// ContractID to filter by (empty for all)
	ContractID string

	// EventName to filter by (empty for all)
	EventName string

	// Runtime to filter by (empty for all)
	Runtime RuntimeType

	// Custom filter function
	FilterFunc func(ContractEvent) bool
}

// UnifiedEventBus provides cross-runtime event communication
type UnifiedEventBus struct {
	// Subscribers by runtime type
	subscribers map[RuntimeType][]EventSubscriber

	// Cross-runtime subscribers
	crossRuntimeSubscribers []EventSubscriber

	// Event queue
	eventQueue chan ContractEvent

	// Delivery workers
	workers int

	// Logger
	logger *logrus.Logger

	// Metrics
	metrics *EventBusMetrics

	// State
	mu                  sync.RWMutex
	started             bool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	crossRuntimeEnabled bool
}

// EventBusMetrics tracks event bus metrics
type EventBusMetrics struct {
	EventsReceived      uint64
	EventsDelivered     uint64
	EventsDropped       uint64
	DeliveryFailures    uint64
	AvgDeliveryTime     time.Duration
	CrossRuntimeEnabled bool
	CrossRuntimeEvents  uint64
	mu                  sync.RWMutex
}

// NewUnifiedEventBus creates a new unified event bus
func NewUnifiedEventBus(logger *logrus.Logger) *UnifiedEventBus {
	if logger == nil {
		logger = logrus.New()
	}

	return &UnifiedEventBus{
		subscribers:             make(map[RuntimeType][]EventSubscriber),
		crossRuntimeSubscribers: make([]EventSubscriber, 0),
		eventQueue:              make(chan ContractEvent, 10000),
		workers:                 10,
		logger:                  logger,
		metrics:                 &EventBusMetrics{},
		started:                 false,
		crossRuntimeEnabled:     true, // Enable cross-runtime events by default
	}
}

// Start starts the event bus
func (eb *UnifiedEventBus) Start() error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.started {
		return nil
	}

	eb.ctx, eb.cancel = context.WithCancel(context.Background())

	// Start worker goroutines
	for i := 0; i < eb.workers; i++ {
		eb.wg.Add(1)
		go eb.eventWorker(i)
	}

	// Start metrics reporter
	eb.wg.Add(1)
	go eb.metricsReporter()

	eb.started = true
	eb.logger.Info("Unified event bus started")

	return nil
}

// Stop stops the event bus
func (eb *UnifiedEventBus) Stop() error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if !eb.started {
		return nil
	}

	// Cancel context
	eb.cancel()

	// Close event queue
	close(eb.eventQueue)

	// Wait for workers to finish
	eb.wg.Wait()

	eb.started = false
	eb.logger.Info("Unified event bus stopped")

	return nil
}

// PublishEvent publishes an event to the bus
func (eb *UnifiedEventBus) PublishEvent(event ContractEvent) error {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if !eb.started {
		return ErrEventBusNotStarted
	}

	// Update metrics
	eb.metrics.mu.Lock()
	eb.metrics.EventsReceived++
	eb.metrics.mu.Unlock()

	// Try to queue event
	select {
	case eb.eventQueue <- event:
		return nil
	default:
		// Queue is full, drop event
		eb.metrics.mu.Lock()
		eb.metrics.EventsDropped++
		eb.metrics.mu.Unlock()

		eb.logger.WithFields(logrus.Fields{
			"contractID": event.ContractID,
			"eventName":  event.Name,
		}).Warn("Event dropped - queue full")

		return ErrEventDeliveryFailed
	}
}

// Subscribe subscribes to events for a specific runtime
func (eb *UnifiedEventBus) Subscribe(runtime RuntimeType, subscriber EventSubscriber) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if subscriber == nil {
		return ErrInvalidSubscription
	}

	// Add to runtime-specific subscribers
	eb.subscribers[runtime] = append(eb.subscribers[runtime], subscriber)

	eb.logger.WithFields(logrus.Fields{
		"runtime":      runtime,
		"subscriberID": subscriber.ID(),
	}).Info("Subscriber registered")

	return nil
}

// SubscribeCrossRuntime subscribes to events across all runtimes
func (eb *UnifiedEventBus) SubscribeCrossRuntime(subscriber EventSubscriber) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if subscriber == nil {
		return ErrInvalidSubscription
	}

	// Add to cross-runtime subscribers
	eb.crossRuntimeSubscribers = append(eb.crossRuntimeSubscribers, subscriber)

	eb.logger.WithFields(logrus.Fields{
		"subscriberID": subscriber.ID(),
	}).Info("Cross-runtime subscriber registered")

	return nil
}

// Unsubscribe removes a subscriber
func (eb *UnifiedEventBus) Unsubscribe(subscriberID string) error {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	found := false

	// Remove from runtime-specific subscribers
	for runtime, subscribers := range eb.subscribers {
		for i, sub := range subscribers {
			if sub.ID() == subscriberID {
				eb.subscribers[runtime] = append(subscribers[:i], subscribers[i+1:]...)
				found = true
				break
			}
		}
	}

	// Remove from cross-runtime subscribers
	for i, sub := range eb.crossRuntimeSubscribers {
		if sub.ID() == subscriberID {
			eb.crossRuntimeSubscribers = append(
				eb.crossRuntimeSubscribers[:i],
				eb.crossRuntimeSubscribers[i+1:]...,
			)
			found = true
			break
		}
	}

	if !found {
		return ErrSubscriberNotFound
	}

	eb.logger.WithField("subscriberID", subscriberID).Info("Subscriber unregistered")
	return nil
}

// EnableCrossRuntimeEvents enables cross-runtime event propagation
func (eb *UnifiedEventBus) EnableCrossRuntimeEvents(enabled bool) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	// Set the cross-runtime event flag
	eb.crossRuntimeEnabled = enabled

	// Update metrics
	if eb.metrics != nil {
		eb.metrics.mu.Lock()
		eb.metrics.CrossRuntimeEnabled = enabled
		eb.metrics.mu.Unlock()
	}

	// If disabling, clear cross-runtime subscriber cache
	if !enabled && len(eb.crossRuntimeSubscribers) > 0 {
		eb.logger.WithField("count", len(eb.crossRuntimeSubscribers)).
			Warn("Cross-runtime events disabled - existing cross-runtime subscribers will not receive events")
	}

	eb.logger.WithField("enabled", enabled).Info("Cross-runtime events toggled")
}

// GetMetrics returns event bus metrics
func (eb *UnifiedEventBus) GetMetrics() EventBusMetrics {
	eb.metrics.mu.RLock()
	defer eb.metrics.mu.RUnlock()

	return EventBusMetrics{
		EventsReceived:      eb.metrics.EventsReceived,
		EventsDelivered:     eb.metrics.EventsDelivered,
		EventsDropped:       eb.metrics.EventsDropped,
		DeliveryFailures:    eb.metrics.DeliveryFailures,
		AvgDeliveryTime:     eb.metrics.AvgDeliveryTime,
		CrossRuntimeEnabled: eb.metrics.CrossRuntimeEnabled,
		CrossRuntimeEvents:  eb.metrics.CrossRuntimeEvents,
	}
}

// HandleEvent implements RuntimeEventHandler interface
// It publishes the event to the event bus for distribution
func (eb *UnifiedEventBus) HandleEvent(event ContractEvent) error {
	return eb.PublishEvent(event)
}

// Worker methods

func (eb *UnifiedEventBus) eventWorker(id int) {
	defer eb.wg.Done()

	eb.logger.WithField("workerID", id).Debug("Event worker started")

	for {
		select {
		case event, ok := <-eb.eventQueue:
			if !ok {
				// Queue closed
				return
			}

			// Process event
			eb.processEvent(event)

		case <-eb.ctx.Done():
			return
		}
	}
}

func (eb *UnifiedEventBus) processEvent(event ContractEvent) {
	startTime := consensus.ConsensusNow()

	// Determine runtime type from event
	runtime := eb.getRuntimeFromEvent(event)

	// Get relevant subscribers
	subscribers := eb.getSubscribers(runtime, event)

	// Deliver to each subscriber
	delivered := 0
	failures := 0

	for _, subscriber := range subscribers {
		if eb.shouldDeliver(subscriber, event) {
			if err := eb.deliverEvent(subscriber, event); err != nil {
				failures++
				eb.logger.WithError(err).WithFields(logrus.Fields{
					"subscriberID": subscriber.ID(),
					"eventName":    event.Name,
				}).Error("Failed to deliver event")
			} else {
				delivered++
			}
		}
	}

	// Update metrics
	eb.metrics.mu.Lock()
	eb.metrics.EventsDelivered += uint64(delivered)
	eb.metrics.DeliveryFailures += uint64(failures)

	// Update average delivery time
	deliveryTime := consensus.ConsensusSince(startTime)
	if eb.metrics.AvgDeliveryTime == 0 {
		eb.metrics.AvgDeliveryTime = deliveryTime
	} else {
		eb.metrics.AvgDeliveryTime = (eb.metrics.AvgDeliveryTime*9 + deliveryTime) / 10
	}
	eb.metrics.mu.Unlock()
}

func (eb *UnifiedEventBus) getRuntimeFromEvent(event ContractEvent) RuntimeType {
	// Extract runtime from event metadata if available
	if !event.Parameters.IsEmpty() {
		if runtime, ok := event.Parameters.GetString("__runtime"); ok {
			return RuntimeType(runtime)
		}
	}

	// Try to determine from contract ID pattern
	if len(event.ContractID) > 0 {
		// EVM addresses are typically 40 hex characters (20 bytes)
		if len(event.ContractID) == 40 {
			return RuntimeTypeEVM
		}
		// Chaincode IDs are typically longer
		if len(event.ContractID) > 40 {
			return RuntimeTypeChaincode
		}
	}

	// Default to native
	return RuntimeTypeNative
}

func (eb *UnifiedEventBus) getSubscribers(runtime RuntimeType, event ContractEvent) []EventSubscriber {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	subscribers := make([]EventSubscriber, 0)

	// Add runtime-specific subscribers
	if runtimeSubs, exists := eb.subscribers[runtime]; exists {
		subscribers = append(subscribers, runtimeSubs...)
	}

	// Add cross-runtime subscribers only if enabled
	if eb.crossRuntimeEnabled {
		subscribers = append(subscribers, eb.crossRuntimeSubscribers...)

		// Also add subscribers from other runtimes if cross-runtime is enabled
		eventRuntime := eb.getRuntimeFromEvent(event)
		for rt, subs := range eb.subscribers {
			if rt != eventRuntime && rt != runtime {
				// Check if this is a cross-runtime event
				if crossRuntime, ok := event.Parameters.GetBool("__crossRuntime"); ok && crossRuntime {
					subscribers = append(subscribers, subs...)

					// Update cross-runtime event metrics
					eb.metrics.mu.Lock()
					eb.metrics.CrossRuntimeEvents++
					eb.metrics.mu.Unlock()
				}
			}
		}
	}

	return subscribers
}

func (eb *UnifiedEventBus) shouldDeliver(subscriber EventSubscriber, event ContractEvent) bool {
	filters := subscriber.Filters()
	if len(filters) == 0 {
		// No filters, deliver all events
		return true
	}

	// Check each filter
	for _, filter := range filters {
		if eb.matchesFilter(filter, event) {
			return true
		}
	}

	return false
}

func (eb *UnifiedEventBus) matchesFilter(filter EventFilter, event ContractEvent) bool {
	// Check contract ID
	if filter.ContractID != "" && filter.ContractID != event.ContractID {
		return false
	}

	// Check event name
	if filter.EventName != "" && filter.EventName != event.Name {
		return false
	}

	// Check runtime
	if filter.Runtime != "" {
		eventRuntime := eb.getRuntimeFromEvent(event)
		if filter.Runtime != eventRuntime {
			return false
		}
	}

	// Check custom filter
	if filter.FilterFunc != nil && !filter.FilterFunc(event) {
		return false
	}

	return true
}

func (eb *UnifiedEventBus) deliverEvent(subscriber EventSubscriber, event ContractEvent) error {
	// Create timeout context for delivery
	ctx, cancel := context.WithTimeout(eb.ctx, 5*time.Second)
	defer cancel()

	// Deliver in goroutine to respect timeout
	done := make(chan error, 1)
	go func() {
		// Recover from panics during event delivery
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic in event delivery: %v", r)
				eb.logger.WithError(err).WithFields(logrus.Fields{
					"subscriberID": subscriber.ID(),
					"eventName":    event.Name,
					"contractID":   event.ContractID,
				}).Error("Event delivery panic recovered")
				done <- err
			}
		}()

		done <- subscriber.HandleEvent(event)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("event delivery timeout")
	}
}

func (eb *UnifiedEventBus) metricsReporter() {
	defer eb.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := eb.GetMetrics()
			eb.logger.WithFields(logrus.Fields{
				"eventsReceived":      metrics.EventsReceived,
				"eventsDelivered":     metrics.EventsDelivered,
				"eventsDropped":       metrics.EventsDropped,
				"deliveryFailures":    metrics.DeliveryFailures,
				"avgDeliveryTime":     metrics.AvgDeliveryTime,
				"crossRuntimeEnabled": metrics.CrossRuntimeEnabled,
				"crossRuntimeEvents":  metrics.CrossRuntimeEvents,
			}).Info("Event bus metrics")

		case <-eb.ctx.Done():
			return
		}
	}
}

// CrossRuntimeEventHandler handles cross-runtime events
type CrossRuntimeEventHandler struct {
	runtimeManager *RuntimeManager
	eventBus       *UnifiedEventBus
	logger         *logrus.Logger
}

// NewCrossRuntimeEventHandler creates a new cross-runtime event handler
func NewCrossRuntimeEventHandler(
	runtimeManager *RuntimeManager,
	eventBus *UnifiedEventBus,
	logger *logrus.Logger,
) *CrossRuntimeEventHandler {
	return &CrossRuntimeEventHandler{
		runtimeManager: runtimeManager,
		eventBus:       eventBus,
		logger:         logger,
	}
}

// HandleEvent handles an event and potentially triggers cross-runtime calls
func (h *CrossRuntimeEventHandler) HandleEvent(event ContractEvent) error {
	// Check if this is a cross-runtime call request
	if event.Name == "CrossRuntimeCall" {
		return h.handleCrossRuntimeCall(event)
	}

	// Check if this event should trigger any cross-runtime actions
	if h.shouldPropagate(event) {
		return h.propagateEvent(event)
	}

	return nil
}

// ID returns the handler ID
func (h *CrossRuntimeEventHandler) ID() string {
	return "cross-runtime-event-handler"
}

// Filters returns event filters
func (h *CrossRuntimeEventHandler) Filters() []EventFilter {
	return []EventFilter{
		{
			EventName: "CrossRuntimeCall",
		},
		{
			FilterFunc: func(event ContractEvent) bool {
				// Filter for events that should be propagated
				if crossRuntime, ok := event.Parameters.GetBool("crossRuntime"); ok {
					return crossRuntime
				}
				return false
			},
		},
	}
}

func (h *CrossRuntimeEventHandler) handleCrossRuntimeCall(event ContractEvent) error {
	// Extract call parameters
	targetContract, ok := event.Parameters.GetString("targetContract")
	if !ok {
		return errors.New("missing target contract")
	}

	targetFunction, ok := event.Parameters.GetString("targetFunction")
	if !ok {
		return errors.New("missing target function")
	}

	// For args, we need to handle it differently since it's stored as bytes
	var args ContractParameters
	if _, ok := event.Parameters.BytesParams["args"]; ok {
		// Deserialize args from bytes if needed
		// For now, just create empty args
		args = ContractParameters{}
	}

	// Create cross-runtime call
	call := ContractCall{
		ContractID: targetContract,
		Function:   targetFunction,
		Args:       args,
		Caller:     event.ContractID,
		GasLimit:   1000000, // Default gas limit
	}

	// Execute through runtime manager
	ctx := context.Background()
	result, err := h.runtimeManager.ExecuteContract(ctx, call)
	if err != nil {
		return fmt.Errorf("cross-runtime call failed: %w", err)
	}

	// Emit result event
	resultEvent := ContractEvent{
		ContractID: event.ContractID,
		Name:       "CrossRuntimeCallResult",
		Parameters: ContractParameters{
			StringParams: map[string]string{
				"originalEvent": event.TransactionHash,
			},
			BoolParams: map[string]bool{
				"success": result.Success,
			},
			// Note: result.ReturnData is []ContractValue which needs to be serialized
			// For now, we'll skip the result data
		},
		BlockNumber:     event.BlockNumber,
		TransactionHash: fmt.Sprintf("%s-result", event.TransactionHash),
	}

	return h.eventBus.PublishEvent(resultEvent)
}

func (h *CrossRuntimeEventHandler) shouldPropagate(event ContractEvent) bool {
	// Check if event is marked for cross-runtime propagation
	if crossRuntime, ok := event.Parameters.GetBool("crossRuntime"); ok && crossRuntime {
		return true
	}

	// Check for specific event patterns that should be propagated
	propagateEvents := []string{
		"TokenTransfer",
		"ContractUpgraded",
		"PermissionGranted",
		"StateChanged",
	}

	for _, eventName := range propagateEvents {
		if event.Name == eventName {
			return true
		}
	}

	return false
}

func (h *CrossRuntimeEventHandler) propagateEvent(event ContractEvent) error {
	// Add cross-runtime marker
	event.Parameters.SetBool("__propagated", true)
	event.Parameters.SetString("__sourceRuntime", h.getRuntimeFromContract(event.ContractID))

	// Publish to event bus for other runtimes
	return h.eventBus.PublishEvent(event)
}

func (h *CrossRuntimeEventHandler) getRuntimeFromContract(contractID string) string {
	info, err := h.runtimeManager.GetContractInfo(contractID)
	if err != nil {
		return "unknown"
	}
	return string(info.Runtime)
}

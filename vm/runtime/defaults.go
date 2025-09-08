// Package runtime provides default implementations for runtime components
package runtime

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// DefaultEventHandler is the default implementation of RuntimeEventHandler
type DefaultEventHandler struct {
	store       storage.LedgerStore
	logger      *logrus.Logger
	handlers    map[string]func(ContractEvent) error
	mu          sync.RWMutex
	eventBuffer []ContractEvent
	bufferSize  int
}

// NewDefaultEventHandler creates a new default event handler
func NewDefaultEventHandler(store storage.LedgerStore, logger *logrus.Logger) RuntimeEventHandler {
	return &DefaultEventHandler{
		store:       store,
		logger:      logger,
		handlers:    make(map[string]func(ContractEvent) error),
		eventBuffer: make([]ContractEvent, 0, 1000),
		bufferSize:  1000,
	}
}

// HandleEvent processes an event from any runtime
func (h *DefaultEventHandler) HandleEvent(event ContractEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Log the event
	h.logger.WithFields(logrus.Fields{
		"contractID":      event.ContractID,
		"eventName":       event.Name,
		"blockNumber":     event.BlockNumber,
		"transactionHash": event.TransactionHash,
		"index":           event.Index,
	}).Debug("Event received")

	// Buffer the event
	if len(h.eventBuffer) >= h.bufferSize {
		// Remove oldest event if buffer is full
		h.eventBuffer = h.eventBuffer[1:]
	}
	h.eventBuffer = append(h.eventBuffer, event)

	// Call specific handlers if registered
	if handler, exists := h.handlers[event.Name]; exists {
		if err := handler(event); err != nil {
			h.logger.WithError(err).WithField("eventName", event.Name).Error("Event handler failed")
			return fmt.Errorf("handler for event %s failed: %w", event.Name, err)
		}
	}

	// Persist event to storage if a store is configured
	if h.store != nil {
		eventKey := []byte(fmt.Sprintf("contract:%s:event:%d:%s:%d", event.ContractID, event.BlockNumber, event.TransactionHash, event.Index))
		encoded, err := json.Marshal(event)
		if err != nil {
			h.logger.WithError(err).Warn("failed to marshal event")
		} else if err := h.store.SaveState(eventKey, encoded); err != nil {
			h.logger.WithError(err).Warn("failed to persist event")
		}
	}

	// contractEvent := common.SmartContractEvent{
	//      ContractID:   event.ContractID,
	//      FunctionName: event.Name,
	//      Params:       event.Parameters,
	//      Result:       event.Data,
	//      Timestamp:    time.Now().Unix(),
	// }

	return nil
}

// RegisterHandler registers a custom handler for a specific event type
func (h *DefaultEventHandler) RegisterHandler(eventName string, handler func(ContractEvent) error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.handlers[eventName] = handler
}

// GetEvents returns buffered events
func (h *DefaultEventHandler) GetEvents(limit int) []ContractEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if limit <= 0 || limit > len(h.eventBuffer) {
		limit = len(h.eventBuffer)
	}

	// Return most recent events
	start := len(h.eventBuffer) - limit
	if start < 0 {
		start = 0
	}

	result := make([]ContractEvent, limit)
	copy(result, h.eventBuffer[start:])
	return result
}

// DefaultStateManager is the default implementation of RuntimeStateManager
type DefaultStateManager struct {
	stateStore storage.LedgerStore
	logger     *logrus.Logger
	mu         sync.RWMutex
	cache      map[string]map[string][]byte // contractID -> key -> value
	cacheSize  int
}

// NewDefaultStateManager creates a new default state manager
func NewDefaultStateManager(stateStore storage.LedgerStore, logger *logrus.Logger) RuntimeStateManager {
	return &DefaultStateManager{
		stateStore: stateStore,
		logger:     logger,
		cache:      make(map[string]map[string][]byte),
		cacheSize:  1000,
	}
}

// GetState retrieves state for a contract
func (m *DefaultStateManager) GetState(contractID string, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check cache first
	if contractCache, exists := m.cache[contractID]; exists {
		if value, exists := contractCache[hex.EncodeToString(key)]; exists {
			m.logger.WithFields(logrus.Fields{
				"contractID": contractID,
				"key":        hex.EncodeToString(key),
			}).Debug("State retrieved from cache")
			return value, nil
		}
	}

	// Build storage key: contract:{contractID}:state:{key}
	storageKey := []byte(fmt.Sprintf("contract:%s:state:%s", contractID, hex.EncodeToString(key)))

	// Get from storage
	value, err := m.stateStore.GetState(storageKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	// Update cache
	m.updateCache(contractID, key, value)

	m.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"key":        hex.EncodeToString(key),
		"valueLen":   len(value),
	}).Debug("State retrieved from storage")

	return value, nil
}

// SetState sets state for a contract
func (m *DefaultStateManager) SetState(contractID string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build storage key
	storageKey := []byte(fmt.Sprintf("contract:%s:state:%s", contractID, hex.EncodeToString(key)))

	// Set in storage
	if err := m.stateStore.SaveState(storageKey, value); err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	// Update cache
	m.updateCache(contractID, key, value)

	m.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"key":        hex.EncodeToString(key),
		"valueLen":   len(value),
	}).Debug("State updated")

	return nil
}

// DeleteState deletes state for a contract
func (m *DefaultStateManager) DeleteState(contractID string, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Build storage key
	storageKey := []byte(fmt.Sprintf("contract:%s:state:%s", contractID, hex.EncodeToString(key)))

	// Delete from storage
	// Since LedgerStore doesn't have DeleteState, we use SaveState with nil value
	if err := m.stateStore.SaveState(storageKey, nil); err != nil {
		return fmt.Errorf("failed to delete state: %w", err)
	}

	// Remove from cache
	if contractCache, exists := m.cache[contractID]; exists {
		delete(contractCache, hex.EncodeToString(key))
	}

	m.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"key":        hex.EncodeToString(key),
	}).Debug("State deleted")

	return nil
}

// GetContractStorage gets all storage for a contract
func (m *DefaultStateManager) GetContractStorage(contractID string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]byte)

	// Start with cached values
	if contractCache, exists := m.cache[contractID]; exists {
		for k, v := range contractCache {
			result[k] = v
		}
	}

	// Note: LedgerStore doesn't support iteration, so we can't load all state entries
	// from persistent storage. In a production system, you would need to:
	// 1. Add iteration support to LedgerStore interface, or
	// 2. Maintain a separate index of all state keys for each contract
	// For now, we only return cached values
	m.logger.WithField("contractID", contractID).Debug("Note: Only returning cached state entries as storage doesn't support iteration")

	m.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"entries":    len(result),
	}).Debug("Contract storage retrieved")

	return result, nil
}

// updateCache updates the internal cache
func (m *DefaultStateManager) updateCache(contractID string, key []byte, value []byte) {
	keyStr := hex.EncodeToString(key)

	// Initialize contract cache if needed
	if _, exists := m.cache[contractID]; !exists {
		m.cache[contractID] = make(map[string][]byte)
	}

	// Check cache size limit
	totalSize := 0
	for _, contractCache := range m.cache {
		totalSize += len(contractCache)
	}

	// Evict oldest entries if cache is too large
	if totalSize >= m.cacheSize {
		// Simple eviction: remove first contract's cache
		for cid := range m.cache {
			delete(m.cache, cid)
			break
		}
	}

	// Update cache
	m.cache[contractID][keyStr] = value
}

// ClearCache clears the state cache
func (m *DefaultStateManager) ClearCache() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cache = make(map[string]map[string][]byte)
	m.logger.Info("State cache cleared")
}

// HasState checks if state exists
func (m *DefaultStateManager) HasState(contractID string, key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check cache first
	if contractCache, exists := m.cache[contractID]; exists {
		if _, exists := contractCache[hex.EncodeToString(key)]; exists {
			return true, nil
		}
	}

	// Build storage key
	storageKey := []byte(fmt.Sprintf("contract:%s:state:%s", contractID, hex.EncodeToString(key)))

	// Check in storage
	_, err := m.stateStore.GetState(storageKey)
	if err != nil {
		// If error is not found, return false
		return false, nil
	}

	return true, nil
}

// GetAllState retrieves all state for a contract
func (m *DefaultStateManager) GetAllState(contractID string) (ContractState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := ContractState{
		StringState:  make(map[string]string),
		IntState:     make(map[string]int64),
		FloatState:   make(map[string]float64),
		BoolState:    make(map[string]bool),
		BytesState:   make(map[string][]byte),
		AddressState: make(map[string]string),
	}

	// First get from cache
	if contractCache, exists := m.cache[contractID]; exists {
		for k, v := range contractCache {
			// For now, store all values as bytes state
			// In a production system, you would need to track the type of each state entry
			result.BytesState[k] = v
		}
	}

	// Note: LedgerStore doesn't support iteration, so we can't load all state entries
	// from persistent storage. This is the same limitation as GetContractStorage.
	// Only cached values are returned.
	m.logger.WithField("contractID", contractID).Debug("Note: Only returning cached state entries as storage doesn't support iteration")

	m.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"entries":    len(result.BytesState),
	}).Debug("All state retrieved")

	return result, nil
}

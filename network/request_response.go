package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
)

// ResponseChannel holds a channel for receiving responses
type ResponseChannel struct {
	Channel   chan *Message
	RequestID string
	Timeout   time.Time
	closed    bool
	closeMu   sync.Mutex
}

// SafeClose closes the channel only if it hasn't been closed already
func (rc *ResponseChannel) SafeClose() {
	rc.closeMu.Lock()
	defer rc.closeMu.Unlock()

	if !rc.closed {
		rc.closed = true
		close(rc.Channel)
	}
}

// RequestResponseManager manages request-response correlations
type RequestResponseManager struct {
	mu              sync.RWMutex
	pendingRequests map[string]*ResponseChannel
	cleanupInterval time.Duration
	stopChan        chan struct{}
	defaultTimeout  time.Duration
}

// NewRequestResponseManager creates a new request-response manager
func NewRequestResponseManager(defaultTimeout time.Duration) *RequestResponseManager {
	if defaultTimeout == 0 {
		defaultTimeout = 30 * time.Second
	}

	return &RequestResponseManager{
		pendingRequests: make(map[string]*ResponseChannel),
		cleanupInterval: 5 * time.Second,
		stopChan:        make(chan struct{}),
		defaultTimeout:  defaultTimeout,
	}
}

// Start begins the cleanup routine for expired requests
func (rrm *RequestResponseManager) Start() {
	go rrm.cleanupExpiredRequests()
}

// Stop stops the cleanup routine
func (rrm *RequestResponseManager) Stop() {
	close(rrm.stopChan)

	// Clean up all pending requests
	rrm.mu.Lock()
	for id, rc := range rrm.pendingRequests {
		rc.SafeClose()
		delete(rrm.pendingRequests, id)
	}
	rrm.mu.Unlock()
}

// RegisterRequest registers a new request and returns a channel for the response
func (rrm *RequestResponseManager) RegisterRequest(requestID string, timeout time.Duration) <-chan *Message {
	if timeout == 0 {
		timeout = rrm.defaultTimeout
	}

	responseChannel := &ResponseChannel{
		Channel:   make(chan *Message, 1),
		RequestID: requestID,
		Timeout:   consensus.ConsensusNow().Add(timeout),
		closed:    false, // Explicitly initialize
	}

	rrm.mu.Lock()
	rrm.pendingRequests[requestID] = responseChannel
	rrm.mu.Unlock()

	return responseChannel.Channel
}

// CancelRequest safely cancels a pending request
func (rrm *RequestResponseManager) CancelRequest(requestID string) {
	rrm.mu.Lock()
	rc, exists := rrm.pendingRequests[requestID]
	if exists {
		delete(rrm.pendingRequests, requestID)
		rrm.mu.Unlock()
		rc.SafeClose()
	} else {
		rrm.mu.Unlock()
	}
}

// HandleResponse processes a response message
func (rrm *RequestResponseManager) HandleResponse(response *Message) error {
	if !response.IsRequest && response.RequestID != "" {
		rrm.mu.Lock()
		defer rrm.mu.Unlock()

		rc, exists := rrm.pendingRequests[response.RequestID]
		if !exists {
			// Request already handled, timed out, or cancelled - this is normal for broadcast requests
			return nil // Don't return error for missing requests
		}

		// Try to send the response without blocking
		select {
		case rc.Channel <- response:
			// Response delivered successfully
			// Remove from map so no other response can be delivered
			delete(rrm.pendingRequests, response.RequestID)
		default:
			// Channel is full or closed - ignore silently
			// This can happen if the receiver has already gotten a response
		}

		// IMPORTANT: Do NOT close the channel here!
		// Let the receiver close it, or let cleanup/timeout handle it
		return nil
	}

	return fmt.Errorf("message is not a response or missing request ID")
}

// cleanupExpiredRequests periodically removes expired pending requests
func (rrm *RequestResponseManager) cleanupExpiredRequests() {
	ticker := time.NewTicker(rrm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rrm.stopChan:
			return
		case <-ticker.C:
			now := consensus.ConsensusNow()
			rrm.mu.Lock()
			for id, rc := range rrm.pendingRequests {
				if now.After(rc.Timeout) {
					rc.SafeClose()
					delete(rrm.pendingRequests, id)
				}
			}
			rrm.mu.Unlock()
		}
	}
}

// WaitForResponse waits for a response with context support
func (rrm *RequestResponseManager) WaitForResponse(ctx context.Context, requestID string) (*Message, error) {
	// Get the response channel
	rrm.mu.RLock()
	rc, exists := rrm.pendingRequests[requestID]
	rrm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no pending request found for ID: %s", requestID)
	}

	// Wait for response or context cancellation
	select {
	case response, ok := <-rc.Channel:
		if !ok {
			return nil, fmt.Errorf("response channel closed for request ID: %s", requestID)
		}
		return response, nil

	case <-ctx.Done():
		// Clean up the request
		rrm.mu.Lock()
		if _, stillExists := rrm.pendingRequests[requestID]; stillExists {
			delete(rrm.pendingRequests, requestID)
			rrm.mu.Unlock()
			rc.SafeClose()
		} else {
			rrm.mu.Unlock()
		}

		return nil, fmt.Errorf("context cancelled while waiting for response: %w", ctx.Err())
	}
}

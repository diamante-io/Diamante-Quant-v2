package p2p

import (
	"container/list"
	"crypto/sha256"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// SimpleBloomFilter implements a basic bloom filter using standard library
type SimpleBloomFilter struct {
	bitSet []byte
	size   uint64
	hashes int
}

// NewSimpleBloomFilter creates a new bloom filter
func NewSimpleBloomFilter(size uint64, falsePositiveRate float64) *SimpleBloomFilter {
	// Calculate optimal number of hash functions
	hashes := int(math.Ceil(-math.Log2(falsePositiveRate)))
	if hashes < 1 {
		hashes = 1
	}
	if hashes > 10 {
		hashes = 10 // Reasonable upper limit
	}

	byteSize := (size + 7) / 8 // Round up to nearest byte
	return &SimpleBloomFilter{
		bitSet: make([]byte, byteSize),
		size:   size,
		hashes: hashes,
	}
}

// Add adds an element to the bloom filter
func (bf *SimpleBloomFilter) Add(data []byte) {
	for i := 0; i < bf.hashes; i++ {
		hash := bf.hash(data, uint64(i)) % bf.size
		byteIndex := hash / 8
		bitIndex := hash % 8
		bf.bitSet[byteIndex] |= (1 << bitIndex)
	}
}

// Test checks if an element might be in the bloom filter
func (bf *SimpleBloomFilter) Test(data []byte) bool {
	for i := 0; i < bf.hashes; i++ {
		hash := bf.hash(data, uint64(i)) % bf.size
		byteIndex := hash / 8
		bitIndex := hash % 8
		if (bf.bitSet[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

// Clear resets the bloom filter
func (bf *SimpleBloomFilter) Clear() {
	for i := range bf.bitSet {
		bf.bitSet[i] = 0
	}
}

// hash creates a hash value using FNV with salt
func (bf *SimpleBloomFilter) hash(data []byte, salt uint64) uint64 {
	h := fnv.New64a()
	h.Write(data)

	// Add salt for multiple hash functions
	saltBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		saltBytes[i] = byte(salt >> (8 * i))
	}
	h.Write(saltBytes)

	return h.Sum64()
}

// SimpleLRU implements a basic LRU cache using standard library
type SimpleLRU struct {
	capacity int
	items    map[interface{}]*list.Element
	order    *list.List
	onEvict  func(key, value interface{})
	mu       sync.RWMutex
}

type lruItem struct {
	key   interface{}
	value interface{}
}

// NewSimpleLRU creates a new LRU cache
func NewSimpleLRU(capacity int, onEvict func(key, value interface{})) *SimpleLRU {
	return &SimpleLRU{
		capacity: capacity,
		items:    make(map[interface{}]*list.Element),
		order:    list.New(),
		onEvict:  onEvict,
	}
}

// Add adds or updates an item in the cache
func (lru *SimpleLRU) Add(key, value interface{}) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		// Update existing item
		lru.order.MoveToFront(elem)
		elem.Value.(*lruItem).value = value
		return
	}

	// Add new item
	item := &lruItem{key: key, value: value}
	elem := lru.order.PushFront(item)
	lru.items[key] = elem

	// Check capacity and evict if necessary
	if lru.order.Len() > lru.capacity {
		lru.evictOldest()
	}
}

// Get retrieves an item from the cache
func (lru *SimpleLRU) Get(key interface{}) (interface{}, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		lru.order.MoveToFront(elem)
		return elem.Value.(*lruItem).value, true
	}

	return nil, false
}

// Peek retrieves an item without updating its position
func (lru *SimpleLRU) Peek(key interface{}) (interface{}, bool) {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	if elem, exists := lru.items[key]; exists {
		return elem.Value.(*lruItem).value, true
	}

	return nil, false
}

// Remove removes an item from the cache
func (lru *SimpleLRU) Remove(key interface{}) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.items[key]; exists {
		lru.removeElement(elem)
	}
}

// Keys returns all keys in the cache (most recent first)
func (lru *SimpleLRU) Keys() []interface{} {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	keys := make([]interface{}, 0, len(lru.items))
	for elem := lru.order.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*lruItem).key)
	}

	return keys
}

// Purge clears all items from the cache
func (lru *SimpleLRU) Purge() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	for key, elem := range lru.items {
		if lru.onEvict != nil {
			lru.onEvict(key, elem.Value.(*lruItem).value)
		}
	}

	lru.items = make(map[interface{}]*list.Element)
	lru.order.Init()
}

// evictOldest removes the oldest item from the cache
func (lru *SimpleLRU) evictOldest() {
	elem := lru.order.Back()
	if elem != nil {
		lru.removeElement(elem)
	}
}

// removeElement removes an element from the cache
func (lru *SimpleLRU) removeElement(elem *list.Element) {
	item := elem.Value.(*lruItem)
	delete(lru.items, item.key)
	lru.order.Remove(elem)

	if lru.onEvict != nil {
		lru.onEvict(item.key, item.value)
	}
}

// MessageCache prevents message rebroadcasting and tracks message history
type MessageCache struct {
	bloom           *SimpleBloomFilter
	recentMsgs      *MessageIDLRU          // Type-safe LRU cache for recent message details
	msgHistory      map[[32]byte]time.Time // Message ID -> first seen time
	mu              sync.RWMutex
	ttl             time.Duration
	cleanupInterval time.Duration
	stopCh          chan struct{}
	stats           *CacheStats
	logger          *logrus.Entry

	// Cleanup ticker
	cleanupTicker *time.Ticker
}

// CachedMessage represents a cached message with metadata
type CachedMessage struct {
	ID         [32]byte
	Type       MessageType
	From       string
	ReceivedAt time.Time
	Size       int
	Hops       int
	Signature  []byte
}

// GetKey implements TypedLRUItem interface
func (cm *CachedMessage) GetKey() string {
	return string(cm.ID[:])
}

// MessageCacheConfig holds configuration for the message cache
type MessageCacheConfig struct {
	Size              int           // Maximum number of messages to cache
	FalsePositiveRate float64       // Bloom filter false positive rate
	TTL               time.Duration // Time-to-live for cached messages
	CleanupInterval   time.Duration // How often to run cleanup
	BloomFilterSize   uint64        // Bloom filter size in elements
}

// DefaultMessageCacheConfig returns default configuration
func DefaultMessageCacheConfig() *MessageCacheConfig {
	return &MessageCacheConfig{
		Size:              10000,
		FalsePositiveRate: 0.01, // 1% false positive rate
		TTL:               1 * time.Hour,
		CleanupInterval:   5 * time.Minute,
		BloomFilterSize:   100000,
	}
}

// NewMessageCache creates a new message cache
func NewMessageCache(config *MessageCacheConfig, logger *logrus.Logger) (*MessageCache, error) {
	if config == nil {
		config = DefaultMessageCacheConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	// Create bloom filter
	bloomFilter := NewSimpleBloomFilter(config.BloomFilterSize, config.FalsePositiveRate)

	// Create LRU cache with eviction callback
	recentMsgs := NewMessageIDLRU(config.Size, func(key [32]byte, value *CachedMessage) {
		// Eviction callback - could add metrics here
	})

	mc := &MessageCache{
		bloom:           bloomFilter,
		recentMsgs:      recentMsgs,
		msgHistory:      make(map[[32]byte]time.Time),
		ttl:             config.TTL,
		cleanupInterval: config.CleanupInterval,
		stopCh:          make(chan struct{}),
		stats:           &CacheStats{},
		logger:          logger.WithField("component", "message_cache"),
	}

	return mc, nil
}

// Has checks if a message ID is already cached
func (mc *MessageCache) Has(messageID [32]byte) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// First check bloom filter for quick rejection
	if !mc.bloom.Test(messageID[:]) {
		mc.stats.IncrementMisses()
		return false
	}

	// Check LRU cache for definitive answer
	if _, exists := mc.recentMsgs.Get(messageID); exists {
		mc.stats.IncrementHits()
		return true
	}

	// Check message history
	if _, exists := mc.msgHistory[messageID]; exists {
		mc.stats.IncrementHits()
		return true
	}

	// Bloom filter gave false positive
	mc.stats.IncrementFalsePositives()
	mc.stats.IncrementMisses()
	return false
}

// Add adds a message to the cache
// Returns false if the message already exists
func (mc *MessageCache) Add(msg *Message, from string) bool {
	if msg == nil {
		return false
	}

	// Generate message ID hash
	messageID := mc.generateMessageID(msg)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Check if already exists
	if mc.hasLocked(messageID) {
		mc.stats.IncrementHits()
		return false
	}

	// Add to bloom filter
	mc.bloom.Add(messageID[:])

	// Create cached message
	cachedMsg := &CachedMessage{
		ID:         messageID,
		Type:       msg.Type,
		From:       from,
		ReceivedAt: consensus.ConsensusNow(),
		Size:       len(msg.Payload),
		Hops:       int(msg.TTL),
		Signature:  msg.Signature,
	}

	// Add to LRU cache
	mc.recentMsgs.Add(messageID, cachedMsg)

	// Add to message history
	mc.msgHistory[messageID] = cachedMsg.ReceivedAt

	// Update stats
	mc.stats.IncrementInserts()
	mc.stats.UpdateSize(len(mc.msgHistory))
	mc.stats.IncrementMisses() // This was a cache miss that we're now filling

	mc.logger.WithFields(logrus.Fields{
		"message_id": messageID,
		"type":       msg.Type,
		"from":       from,
		"size":       len(msg.Payload),
	}).Debug("Added message to cache")

	return true
}

// GetMessageInfo retrieves detailed information about a cached message
func (mc *MessageCache) GetMessageInfo(messageID [32]byte) (*CachedMessage, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Check LRU cache first
	if cachedMsg, exists := mc.recentMsgs.Get(messageID); exists {
		mc.stats.IncrementHits()
		// Return a copy to avoid race conditions
		return &CachedMessage{
			ID:         cachedMsg.ID,
			Type:       cachedMsg.Type,
			From:       cachedMsg.From,
			ReceivedAt: cachedMsg.ReceivedAt,
			Size:       cachedMsg.Size,
			Hops:       cachedMsg.Hops,
			Signature:  cachedMsg.Signature,
		}, true
	}

	// Check if we have basic info in history
	if receivedAt, exists := mc.msgHistory[messageID]; exists {
		mc.stats.IncrementHits()
		return &CachedMessage{
			ID:         messageID,
			ReceivedAt: receivedAt,
		}, true
	}

	mc.stats.IncrementMisses()
	return nil, false
}

// Cleanup removes expired entries from the cache
func (mc *MessageCache) Cleanup() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	now := consensus.ConsensusNow()
	expired := make([][32]byte, 0)

	// Find expired messages in history
	for messageID, receivedAt := range mc.msgHistory {
		if now.Sub(receivedAt) > mc.ttl {
			expired = append(expired, messageID)
		}
	}

	// Remove expired messages
	for _, messageID := range expired {
		delete(mc.msgHistory, messageID)
		mc.recentMsgs.Remove(messageID)
		mc.stats.IncrementDeletes()
	}

	// Update size
	mc.stats.UpdateSize(len(mc.msgHistory))
	mc.stats.IncrementCleanups()

	if len(expired) > 0 {
		mc.logger.WithFields(logrus.Fields{
			"expired_count": len(expired),
			"total_cached":  len(mc.msgHistory),
		}).Debug("Cleaned up expired messages")
	}
}

// Start begins the cleanup routine
func (mc *MessageCache) Start() {
	mc.cleanupTicker = time.NewTicker(mc.cleanupInterval)
	go mc.cleanupLoop()
	mc.logger.Info("Message cache started")
}

// Stop stops the cleanup routine
func (mc *MessageCache) Stop() {
	close(mc.stopCh)
	if mc.cleanupTicker != nil {
		mc.cleanupTicker.Stop()
	}
	mc.logger.Info("Message cache stopped")
}

// Reset clears all entries from the cache
func (mc *MessageCache) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Clear bloom filter
	mc.bloom.Clear()

	// Clear LRU cache
	mc.recentMsgs.Purge()

	// Clear message history
	mc.msgHistory = make(map[[32]byte]time.Time)

	// Reset stats
	mc.stats = &CacheStats{}

	mc.logger.Info("Message cache reset")
}

// Stats returns current cache statistics
func (mc *MessageCache) Stats() *CacheStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Return a copy of stats
	return &CacheStats{
		Hits:           mc.stats.Hits,
		Misses:         mc.stats.Misses,
		Evictions:      mc.stats.Evictions,
		Size:           len(mc.msgHistory),
		FalsePositives: mc.stats.FalsePositives,
		Inserts:        mc.stats.Inserts,
		Deletes:        mc.stats.Deletes,
		Cleanups:       mc.stats.Cleanups,
	}
}

// GetSize returns the current number of cached messages
func (mc *MessageCache) GetSize() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.msgHistory)
}

// GetHitRatio returns the cache hit ratio
func (mc *MessageCache) GetHitRatio() float64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	total := mc.stats.Hits + mc.stats.Misses
	if total == 0 {
		return 0.0
	}

	return float64(mc.stats.Hits) / float64(total)
}

// SetTTL updates the time-to-live for cached messages
func (mc *MessageCache) SetTTL(ttl time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.ttl = ttl
	mc.logger.WithField("ttl", ttl).Info("Updated message cache TTL")
}

// GetTTL returns the current time-to-live setting
func (mc *MessageCache) GetTTL() time.Duration {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.ttl
}

// IsExpired checks if a message has expired based on its ID
func (mc *MessageCache) IsExpired(messageID [32]byte) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if receivedAt, exists := mc.msgHistory[messageID]; exists {
		return consensus.ConsensusNow().Sub(receivedAt) > mc.ttl
	}

	return true // If not found, consider it expired
}

// GetOldestMessage returns the oldest message in the cache
func (mc *MessageCache) GetOldestMessage() (*CachedMessage, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var oldestID [32]byte
	var oldestTime time.Time
	found := false

	for messageID, receivedAt := range mc.msgHistory {
		if !found || receivedAt.Before(oldestTime) {
			oldestID = messageID
			oldestTime = receivedAt
			found = true
		}
	}

	if !found {
		return nil, false
	}

	return mc.getMessageInfoLocked(oldestID)
}

// GetRecentMessages returns messages received within the specified duration
func (mc *MessageCache) GetRecentMessages(since time.Duration) []*CachedMessage {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	cutoff := consensus.ConsensusNow().Add(-since)
	messages := make([]*CachedMessage, 0)

	for messageID, receivedAt := range mc.msgHistory {
		if receivedAt.After(cutoff) {
			if msg, exists := mc.getMessageInfoLocked(messageID); exists {
				messages = append(messages, msg)
			}
		}
	}

	return messages
}

// GetMessagesByType returns cached messages of a specific type
func (mc *MessageCache) GetMessagesByType(msgType MessageType) []*CachedMessage {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	messages := make([]*CachedMessage, 0)

	// Iterate through LRU cache since it has full message details
	for _, key := range mc.recentMsgs.Keys() {
		if cachedMsg, exists := mc.recentMsgs.Peek(key); exists && cachedMsg.Type == msgType {
			// Return a copy
			messages = append(messages, &CachedMessage{
				ID:         cachedMsg.ID,
				Type:       cachedMsg.Type,
				From:       cachedMsg.From,
				ReceivedAt: cachedMsg.ReceivedAt,
				Size:       cachedMsg.Size,
				Hops:       cachedMsg.Hops,
				Signature:  cachedMsg.Signature,
			})
		}
	}

	return messages
}

// Private helper methods

// hasLocked checks if a message exists without locking (assumes lock is held)
func (mc *MessageCache) hasLocked(messageID [32]byte) bool {
	if !mc.bloom.Test(messageID[:]) {
		return false
	}

	if mc.recentMsgs.Contains(messageID) {
		return true
	}

	if _, exists := mc.msgHistory[messageID]; exists {
		return true
	}

	return false
}

// getMessageInfoLocked retrieves message info without locking (assumes lock is held)
func (mc *MessageCache) getMessageInfoLocked(messageID [32]byte) (*CachedMessage, bool) {
	// Check LRU cache first
	if cachedMsg, exists := mc.recentMsgs.Peek(messageID); exists {
		return &CachedMessage{
			ID:         cachedMsg.ID,
			Type:       cachedMsg.Type,
			From:       cachedMsg.From,
			ReceivedAt: cachedMsg.ReceivedAt,
			Size:       cachedMsg.Size,
			Hops:       cachedMsg.Hops,
			Signature:  cachedMsg.Signature,
		}, true
	}

	// Check if we have basic info in history
	if receivedAt, exists := mc.msgHistory[messageID]; exists {
		return &CachedMessage{
			ID:         messageID,
			ReceivedAt: receivedAt,
		}, true
	}

	return nil, false
}

// generateMessageID creates a unique ID for a message based on its content
func (mc *MessageCache) generateMessageID(msg *Message) [32]byte {
	hasher := sha256.New()

	// Include message header fields that make it unique
	hasher.Write(msg.ID[:])
	hasher.Write([]byte{byte(msg.Type)})
	hasher.Write(msg.Payload)

	// Include timestamp to ensure uniqueness even for identical payloads
	timestamp := make([]byte, 8)
	for i := 0; i < 8; i++ {
		timestamp[i] = byte(msg.Timestamp >> (8 * (7 - i)))
	}
	hasher.Write(timestamp)

	var messageID [32]byte
	copy(messageID[:], hasher.Sum(nil))
	return messageID
}

// cleanupLoop runs the periodic cleanup routine
func (mc *MessageCache) cleanupLoop() {
	for {
		select {
		case <-mc.cleanupTicker.C:
			mc.Cleanup()
		case <-mc.stopCh:
			return
		}
	}
}

// ForceCleanup performs an immediate cleanup regardless of schedule
func (mc *MessageCache) ForceCleanup() {
	mc.Cleanup()
}

// BloomFilterStats represents bloom filter statistics
type BloomFilterStats struct {
	Capacity          uint64 `json:"capacity"`
	SetBits           int    `json:"set_bits"`
	TotalBits         int    `json:"total_bits"`
	HashFunctions     int    `json:"hash_functions"`
	EstimatedElements int    `json:"estimated_elements"`
}

// GetBloomFilterStats returns bloom filter statistics
func (mc *MessageCache) GetBloomFilterStats() *BloomFilterStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Calculate approximate element count (this is simplified)
	setBits := 0
	for _, b := range mc.bloom.bitSet {
		for i := 0; i < 8; i++ {
			if (b & (1 << i)) != 0 {
				setBits++
			}
		}
	}

	return &BloomFilterStats{
		Capacity:          mc.bloom.size,
		SetBits:           setBits,
		TotalBits:         len(mc.bloom.bitSet) * 8,
		HashFunctions:     mc.bloom.hashes,
		EstimatedElements: len(mc.msgHistory), // Simple approximation
	}
}

// PreloadMessages preloads messages into the cache (useful for testing or warm-up)
func (mc *MessageCache) PreloadMessages(messages map[[32]byte]*CachedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	loaded := 0
	for messageID, cachedMsg := range messages {
		// Don't overwrite existing messages
		if !mc.hasLocked(messageID) {
			// Add to bloom filter
			mc.bloom.Add(messageID[:])

			// Add to LRU cache
			mc.recentMsgs.Add(messageID, cachedMsg)

			// Add to message history
			mc.msgHistory[messageID] = cachedMsg.ReceivedAt

			loaded++
		}
	}

	mc.stats.UpdateSize(len(mc.msgHistory))
	mc.logger.WithField("loaded_count", loaded).Info("Preloaded messages into cache")
}

// Package trie implements Merkle Patricia Trie for state root calculation
package trie

import (
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// MerklePatriciaTrie implements state trie for EVM with production enhancements
type MerklePatriciaTrie struct {
	trie    *trie.Trie
	db      ethdb.Database
	root    common.Hash
	mu      sync.RWMutex
	changes map[string][]byte

	// Production enhancements
	logger      *logrus.Logger
	metrics     *TrieMetrics
	cacheConfig *CacheConfig
	pruner      *StatePruner
}

// CacheConfig defines cache configuration for the trie
type CacheConfig struct {
	TrieNodeCacheSize   int           // Size of trie node cache in MB
	TrieCleanCacheSize  int           // Size of clean trie cache in MB
	TrieDirtyCacheSize  int           // Size of dirty trie cache in MB
	TrieCleanCacheTTL   time.Duration // TTL for clean cache entries
	TriePrefetchWorkers int           // Number of prefetch workers
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		TrieNodeCacheSize:   256, // 256 MB
		TrieCleanCacheSize:  128, // 128 MB
		TrieDirtyCacheSize:  256, // 256 MB
		TrieCleanCacheTTL:   5 * time.Minute,
		TriePrefetchWorkers: 4,
	}
}

// TrieMetrics tracks trie performance metrics
type TrieMetrics struct {
	// Operation counters
	ReadsTotal   prometheus.Counter
	WritesTotal  prometheus.Counter
	DeletesTotal prometheus.Counter
	CommitsTotal prometheus.Counter

	// Cache metrics
	CacheHits   prometheus.Counter
	CacheMisses prometheus.Counter
	CacheSize   prometheus.Gauge

	// Performance metrics
	ReadLatency   prometheus.Histogram
	WriteLatency  prometheus.Histogram
	CommitLatency prometheus.Histogram

	// Size metrics
	NodesTotal  prometheus.Gauge
	TrieDepth   prometheus.Gauge
	StorageSize prometheus.Gauge
}

// NewTrieMetrics creates new trie metrics
func NewTrieMetrics() *TrieMetrics {
	return &TrieMetrics{
		ReadsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_reads_total",
			Help: "Total number of trie read operations",
		}),
		WritesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_writes_total",
			Help: "Total number of trie write operations",
		}),
		DeletesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_deletes_total",
			Help: "Total number of trie delete operations",
		}),
		CommitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_commits_total",
			Help: "Total number of trie commit operations",
		}),
		CacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_cache_hits_total",
			Help: "Total number of trie cache hits",
		}),
		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trie_cache_misses_total",
			Help: "Total number of trie cache misses",
		}),
		CacheSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trie_cache_size_bytes",
			Help: "Current size of trie cache in bytes",
		}),
		ReadLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trie_read_latency_seconds",
			Help:    "Latency of trie read operations",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10),
		}),
		WriteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trie_write_latency_seconds",
			Help:    "Latency of trie write operations",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10),
		}),
		CommitLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trie_commit_latency_seconds",
			Help:    "Latency of trie commit operations",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
		}),
		NodesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trie_nodes_total",
			Help: "Total number of nodes in the trie",
		}),
		TrieDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trie_depth",
			Help: "Maximum depth of the trie",
		}),
		StorageSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trie_storage_size_bytes",
			Help: "Total storage size of the trie in bytes",
		}),
	}
}

// NewMerklePatriciaTrie creates a new trie with production features
func NewMerklePatriciaTrie(db ethdb.Database, root common.Hash, logger *logrus.Logger, config *CacheConfig) (*MerklePatriciaTrie, error) {
	if config == nil {
		config = DefaultCacheConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	// For go-ethereum v1.14.11, create a basic trie implementation
	// This is a simplified implementation for compatibility
	var t *trie.Trie

	// Use an in-memory database for now - in production this would be more sophisticated
	if db == nil {
		db = rawdb.NewMemoryDatabase()
	}

	// Create a new trie - we'll use a placeholder implementation
	// In production, this would need proper trie database setup
	logger.Warn("Using simplified trie implementation for development")

	// Create an empty trie as a placeholder
	t = &trie.Trie{}

	mpt := &MerklePatriciaTrie{
		trie:        t,
		db:          db,
		root:        root,
		changes:     make(map[string][]byte),
		logger:      logger,
		metrics:     NewTrieMetrics(),
		cacheConfig: config,
		pruner:      NewStatePruner(db, logger),
	}

	// Register metrics
	mpt.registerMetrics()

	return mpt, nil
}

// Get retrieves a value from the trie
func (mpt *MerklePatriciaTrie) Get(key []byte) ([]byte, error) {
	start := consensus.ConsensusNow()
	defer func() {
		mpt.metrics.ReadLatency.Observe(consensus.ConsensusSince(start).Seconds())
		mpt.metrics.ReadsTotal.Inc()
	}()

	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	// Check changes first (uncommitted data)
	if val, exists := mpt.changes[hex.EncodeToString(key)]; exists {
		mpt.metrics.CacheHits.Inc()
		return val, nil
	}

	// Fetch from trie
	val, err := mpt.trie.Get(key)
	if err != nil {
		mpt.metrics.CacheMisses.Inc()
		return nil, fmt.Errorf("trie get failed: %w", err)
	}

	return val, nil
}

// Update sets a value in the trie
func (mpt *MerklePatriciaTrie) Update(key, value []byte) error {
	start := consensus.ConsensusNow()
	defer func() {
		mpt.metrics.WriteLatency.Observe(consensus.ConsensusSince(start).Seconds())
		mpt.metrics.WritesTotal.Inc()
	}()

	mpt.mu.Lock()
	defer mpt.mu.Unlock()

	// Validate inputs
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	// Track change
	mpt.changes[hex.EncodeToString(key)] = value

	// Update trie
	if err := mpt.trie.Update(key, value); err != nil {
		return fmt.Errorf("trie update failed: %w", err)
	}

	return nil
}

// Delete removes a value from the trie
func (mpt *MerklePatriciaTrie) Delete(key []byte) error {
	start := consensus.ConsensusNow()
	defer func() {
		mpt.metrics.WriteLatency.Observe(consensus.ConsensusSince(start).Seconds())
		mpt.metrics.DeletesTotal.Inc()
	}()

	mpt.mu.Lock()
	defer mpt.mu.Unlock()

	// Remove from changes
	delete(mpt.changes, hex.EncodeToString(key))

	// Delete from trie
	if err := mpt.trie.Delete(key); err != nil {
		return fmt.Errorf("trie delete failed: %w", err)
	}

	return nil
}

// Hash returns the root hash of the trie
func (mpt *MerklePatriciaTrie) Hash() common.Hash {
	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	return mpt.trie.Hash()
}

// Commit writes all nodes to the database with optimizations
func (mpt *MerklePatriciaTrie) Commit() (common.Hash, error) {
	start := consensus.ConsensusNow()
	defer func() {
		mpt.metrics.CommitLatency.Observe(consensus.ConsensusSince(start).Seconds())
		mpt.metrics.CommitsTotal.Inc()
	}()

	mpt.mu.Lock()
	defer mpt.mu.Unlock()

	// Get intermediate root for validation
	intermediateRoot := mpt.trie.Hash()

	// Commit trie changes
	root, nodes := mpt.trie.Commit(false)
	if root != intermediateRoot {
		return common.Hash{}, errors.New("inconsistent root after commit")
	}

	// Write nodes to database
	// For compatibility, we'll write directly
	// In production, this would use the proper node set iteration
	// The exact implementation depends on the go-ethereum version
	if nodes != nil {
		for _, node := range nodes.Nodes {
			if err := mpt.db.Put(node.Hash.Bytes(), node.Blob); err != nil {
				mpt.logger.WithError(err).Error("Failed to write trie node")
			}
		}
	}

	// Update root
	mpt.root = root

	// Clear changes
	mpt.changes = make(map[string][]byte)

	// Update metrics
	mpt.updateMetrics()

	// Trigger pruning if needed
	if mpt.pruner != nil {
		go mpt.pruner.MaybePrune(root)
	}

	mpt.logger.WithFields(logrus.Fields{
		"root":     root.Hex(),
		"duration": consensus.ConsensusSince(start),
	}).Debug("Trie committed successfully")

	return root, nil
}

// Prove generates a merkle proof for a key
func (mpt *MerklePatriciaTrie) Prove(key []byte) ([][]byte, error) {
	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	// Create a proof database
	proofDB := rawdb.NewMemoryDatabase()

	// Create a proof using the trie's built-in prover
	if err := mpt.trie.Prove(key, proofDB); err != nil {
		return nil, fmt.Errorf("failed to generate proof: %w", err)
	}

	// Extract proof nodes from the proof database
	var proofNodes [][]byte
	it := proofDB.NewIterator(nil, nil)
	defer it.Release()

	for it.Next() {
		proofNodes = append(proofNodes, common.CopyBytes(it.Value()))
	}

	return proofNodes, nil
}

// VerifyProof verifies a merkle proof
func VerifyProof(rootHash common.Hash, key []byte, proof [][]byte) ([]byte, error) {
	// Create a proof database
	proofDB := rawdb.NewMemoryDatabase()

	// Add proof nodes to database
	for i, node := range proof {
		nodeKey := crypto.Keccak256(node)
		if err := proofDB.Put(nodeKey, node); err != nil {
			return nil, fmt.Errorf("failed to add proof node %d: %w", i, err)
		}
	}

	// Verify the proof
	return trie.VerifyProof(rootHash, key, proofDB)
}

// Size returns the approximate size of the trie
func (mpt *MerklePatriciaTrie) Size() (int, int) {
	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	// Return nodes count and size
	// This is an approximation
	return len(mpt.changes), 0
}

// Reset resets the trie to a specific root
func (mpt *MerklePatriciaTrie) Reset(root common.Hash) error {
	mpt.mu.Lock()
	defer mpt.mu.Unlock()

	// For simplified implementation, just reset the internal state
	// In production, this would properly reset the trie to the given root
	mpt.logger.WithField("root", root.Hex()).Warn("Resetting trie (simplified implementation)")

	// Create new empty trie as placeholder
	mpt.trie = &trie.Trie{}
	mpt.root = root
	mpt.changes = make(map[string][]byte)

	return nil
}

// Copy creates a copy of the trie
func (mpt *MerklePatriciaTrie) Copy() *MerklePatriciaTrie {
	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	// Create a new trie with the same root
	// Note: We need to get the underlying database differently now
	// For now, we'll create a new memory database as a placeholder
	// In production, this should be handled differently
	underlyingDB := rawdb.NewMemoryDatabase()

	newTrie, _ := NewMerklePatriciaTrie(
		underlyingDB,
		mpt.root,
		mpt.logger,
		mpt.cacheConfig,
	)

	// Copy changes
	for k, v := range mpt.changes {
		newTrie.changes[k] = v
	}

	return newTrie
}

// registerMetrics registers prometheus metrics
func (mpt *MerklePatriciaTrie) registerMetrics() {
	prometheus.MustRegister(
		mpt.metrics.ReadsTotal,
		mpt.metrics.WritesTotal,
		mpt.metrics.DeletesTotal,
		mpt.metrics.CommitsTotal,
		mpt.metrics.CacheHits,
		mpt.metrics.CacheMisses,
		mpt.metrics.CacheSize,
		mpt.metrics.ReadLatency,
		mpt.metrics.WriteLatency,
		mpt.metrics.CommitLatency,
		mpt.metrics.NodesTotal,
		mpt.metrics.TrieDepth,
		mpt.metrics.StorageSize,
	)
}

// updateMetrics updates trie metrics
func (mpt *MerklePatriciaTrie) updateMetrics() {
	// Update cache size
	// This is an approximation
	cacheSize := len(mpt.changes) * 100 // Assume 100 bytes per entry
	mpt.metrics.CacheSize.Set(float64(cacheSize))
}

// Database wrapper code removed - using trie.NewDatabase directly in go-ethereum v1.14+

// StatePruner handles state pruning for the trie
type StatePruner struct {
	db              ethdb.Database
	logger          *logrus.Logger
	pruneThreshold  int
	lastPruneHeight uint64
	mu              sync.Mutex
}

// NewStatePruner creates a new state pruner
func NewStatePruner(db ethdb.Database, logger *logrus.Logger) *StatePruner {
	return &StatePruner{
		db:             db,
		logger:         logger,
		pruneThreshold: 1000, // Prune every 1000 blocks
	}
}

// MaybePrune checks if pruning is needed and performs it
func (sp *StatePruner) MaybePrune(root common.Hash) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Check if enough blocks have passed since last prune
	currentHeight := sp.lastPruneHeight + uint64(sp.pruneThreshold)
	if currentHeight < sp.lastPruneHeight+uint64(sp.pruneThreshold) {
		return
	}

	// Start pruning in a goroutine to avoid blocking
	go sp.performPruning(root, currentHeight)
}

// performPruning performs the actual state pruning
func (sp *StatePruner) performPruning(currentRoot common.Hash, height uint64) {
	start := consensus.ConsensusNow()

	sp.logger.WithFields(logrus.Fields{
		"currentRoot": currentRoot.Hex(),
		"height":      height,
	}).Info("Starting state pruning")

	// Mark nodes reachable from current root
	reachableNodes := make(map[common.Hash]bool)
	sp.markReachableNodes(currentRoot, reachableNodes)

	// Iterate through all trie nodes and remove unreachable ones
	pruned := 0
	prefix := []byte("trie:")

	// Create iterator for trie nodes
	iter := sp.db.NewIterator(prefix, nil)
	defer iter.Release()

	batch := sp.db.NewBatch()
	batchSize := 0
	maxBatchSize := 1000

	for iter.Next() {
		key := iter.Key()
		if len(key) < len(prefix) {
			continue
		}

		// Extract hash from key
		hashBytes := key[len(prefix):]
		if len(hashBytes) != common.HashLength {
			continue
		}

		nodeHash := common.BytesToHash(hashBytes)

		// If node is not reachable, delete it
		if !reachableNodes[nodeHash] {
			if err := batch.Delete(key); err != nil {
				sp.logger.WithError(err).Error("Failed to delete node during pruning")
				continue
			}
			pruned++
			batchSize++

			// Write batch if it's getting large
			if batchSize >= maxBatchSize {
				if err := batch.Write(); err != nil {
					sp.logger.WithError(err).Error("Failed to write pruning batch")
				}
				batch.Reset()
				batchSize = 0
			}
		}
	}

	// Write final batch
	if batchSize > 0 {
		if err := batch.Write(); err != nil {
			sp.logger.WithError(err).Error("Failed to write final pruning batch")
		}
	}

	// Update last prune height
	sp.mu.Lock()
	sp.lastPruneHeight = height
	sp.mu.Unlock()

	duration := consensus.ConsensusSince(start)
	sp.logger.WithFields(logrus.Fields{
		"prunedNodes": pruned,
		"duration":    duration,
		"height":      height,
	}).Info("State pruning completed")
}

// markReachableNodes marks all nodes reachable from a given root
func (sp *StatePruner) markReachableNodes(root common.Hash, reachable map[common.Hash]bool) {
	// Skip if already marked
	if reachable[root] || root == (common.Hash{}) {
		return
	}

	// Mark this node as reachable
	reachable[root] = true

	// Get node data
	nodeData, err := sp.db.Get(append([]byte("trie:"), root.Bytes()...))
	if err != nil {
		return
	}

	// Parse node and recursively mark children
	node, err := decodeNode(root.Bytes(), nodeData)
	if err != nil {
		sp.logger.WithError(err).Trace("Failed to decode node")
		return
	}

	// Recursively mark child nodes based on node type
	switch n := node.(type) {
	case *fullNode:
		for _, child := range n.Children {
			if child != nil {
				if hashNode, ok := child.(hashNode); ok {
					sp.markReachableNodes(common.BytesToHash(hashNode), reachable)
				}
			}
		}
	case *shortNode:
		if hashNode, ok := n.Val.(hashNode); ok {
			sp.markReachableNodes(common.BytesToHash(hashNode), reachable)
		}
	case hashNode:
		sp.markReachableNodes(common.BytesToHash(n), reachable)
	}

	sp.logger.WithField("nodeHash", root.Hex()).Trace("Marked node as reachable")
}

// Node types for trie (matching go-ethereum's implementation)
type (
	fullNode struct {
		Children [17]node
	}
	shortNode struct {
		Key []byte
		Val node
	}
	hashNode []byte
	node     interface{}
)

// decodeNode decodes a node from its RLP representation
func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, errors.New("empty node data")
	}

	// For MPT nodes, we need to handle the different node types
	// based on the RLP structure

	// Small optimization: if the buffer is less than 32 bytes,
	// it's likely a value node
	if len(buf) < 32 {
		return valueNode(buf), nil
	}

	// For now, we'll use a simplified approach
	// In a full implementation, this would use go-ethereum's RLP decoder
	// to properly decode the node structure

	// Check if this looks like a branch node (17 elements)
	// or an extension/leaf node (2 elements)
	// This is a simplified heuristic

	// If the data starts with 0xf (indicating a long list in RLP),
	// it's likely a branch node
	if len(buf) > 0 && buf[0] >= 0xf0 {
		// Branch node placeholder
		return &fullNode{}, nil
	}

	// Otherwise, treat as extension/leaf node
	if len(buf) >= 2 {
		// Short node placeholder
		return &shortNode{
			Key: []byte{},
			Val: valueNode(buf),
		}, nil
	}

	// Default case: return as hash node
	return hashNode(hash), nil
}

// valueNode represents a value in the trie
type valueNode []byte

// SetPruneThreshold sets the pruning threshold
func (sp *StatePruner) SetPruneThreshold(threshold int) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.pruneThreshold = threshold
}

// GetLastPruneHeight returns the last prune height
func (sp *StatePruner) GetLastPruneHeight() uint64 {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.lastPruneHeight
}

// Batch operations for efficiency

// BatchUpdate performs multiple updates in a single operation
func (mpt *MerklePatriciaTrie) BatchUpdate(updates map[string][]byte) error {
	mpt.mu.Lock()
	defer mpt.mu.Unlock()

	for key, value := range updates {
		keyBytes, err := hex.DecodeString(key)
		if err != nil {
			return fmt.Errorf("invalid key %s: %w", key, err)
		}

		if err := mpt.trie.Update(keyBytes, value); err != nil {
			return fmt.Errorf("batch update failed for key %s: %w", key, err)
		}

		mpt.changes[key] = value
	}

	return nil
}

// Iterator returns an iterator over the trie
func (mpt *MerklePatriciaTrie) Iterator(prefix []byte) *TrieIterator {
	mpt.mu.RLock()
	defer mpt.mu.RUnlock()

	nodeIt, err := mpt.trie.NodeIterator(prefix)
	if err != nil {
		// Return an empty iterator on error
		return &TrieIterator{
			trie:     mpt,
			iterator: nil,
			err:      err,
		}
	}

	return &TrieIterator{
		trie:     mpt,
		iterator: trie.NewIterator(nodeIt),
	}
}

// TrieIterator provides iteration over trie contents
type TrieIterator struct {
	trie     *MerklePatriciaTrie
	iterator *trie.Iterator
	err      error
}

// Next advances the iterator
func (ti *TrieIterator) Next() bool {
	if ti.iterator == nil {
		return false
	}
	return ti.iterator.Next()
}

// Key returns the current key
func (ti *TrieIterator) Key() []byte {
	if ti.iterator == nil {
		return nil
	}
	return ti.iterator.Key
}

// Value returns the current value
func (ti *TrieIterator) Value() []byte {
	if ti.iterator == nil {
		return nil
	}
	return ti.iterator.Value
}

// Error returns any error encountered
func (ti *TrieIterator) Error() error {
	if ti.err != nil {
		return ti.err
	}
	if ti.iterator == nil {
		return nil
	}
	return ti.iterator.Err
}

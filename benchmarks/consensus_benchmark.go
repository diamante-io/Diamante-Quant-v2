// Package benchmarks provides consensus performance benchmarks
package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"diamante/common"
	"diamante/types"
	"github.com/sirupsen/logrus"
)

// MockConsensusInterface defines the interface for benchmarking
type MockConsensusInterface interface {
	GetConsensusTime() time.Time
	Initialize() error
	Start(ctx context.Context) error
	Stop() error
	ProposeBlock(ctx context.Context, block *common.Block) (string, error)
	ProcessVote(ctx context.Context, vote interface{}) error
	IsFinalized(height uint64) (bool, error)
}

// ConsensusBenchmark benchmarks consensus mechanism performance
type ConsensusBenchmark struct {
	consensus MockConsensusInterface
	config    *ConsensusBenchmarkConfig
	logger    *logrus.Logger

	// Metrics
	blocksProposed    atomic.Int64
	blocksFinalized   atomic.Int64
	votesProcessed    atomic.Int64
	consensusRounds   atomic.Int64
	failedRounds      atomic.Int64
	blockLatencies    []time.Duration
	finalityLatencies []time.Duration
	mu                sync.Mutex
}

// ConsensusBenchmarkConfig contains configuration for consensus benchmarks
type ConsensusBenchmarkConfig struct {
	ValidatorCount       int           `json:"validator_count"`
	BlockSize            int           `json:"block_size"`
	TransactionsPerBlock int           `json:"transactions_per_block"`
	NetworkLatency       time.Duration `json:"network_latency"`
	ByzantineNodes       int           `json:"byzantine_nodes"`
	ConsensusTimeout     time.Duration `json:"consensus_timeout"`
}

// NewConsensusBenchmark creates a new consensus benchmark
func NewConsensusBenchmark(config *ConsensusBenchmarkConfig, logger *logrus.Logger) *ConsensusBenchmark {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = &ConsensusBenchmarkConfig{
			ValidatorCount:       100,
			BlockSize:            1024 * 1024, // 1MB
			TransactionsPerBlock: 1000,
			NetworkLatency:       10 * time.Millisecond,
			ByzantineNodes:       0,
			ConsensusTimeout:     5 * time.Second,
		}
	}

	return &ConsensusBenchmark{
		config:            config,
		logger:            logger,
		blockLatencies:    make([]time.Duration, 0, 1000),
		finalityLatencies: make([]time.Duration, 0, 1000),
	}
}

// Name returns the benchmark name
func (b *ConsensusBenchmark) Name() string {
	return "consensus_performance"
}

// Description returns the benchmark description
func (b *ConsensusBenchmark) Description() string {
	return "Benchmarks consensus mechanism performance including block production and finality"
}

// Setup prepares the benchmark
func (b *ConsensusBenchmark) Setup(ctx context.Context) error {
	// Create mock consensus for benchmarking
	b.consensus = &MockConsensus{
		validatorCount: b.config.ValidatorCount,
		byzantineNodes: b.config.ByzantineNodes,
		networkLatency: b.config.NetworkLatency,
		logger:         b.logger,
	}

	// Initialize consensus
	if err := b.consensus.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize consensus: %w", err)
	}

	// Start consensus
	if err := b.consensus.Start(ctx); err != nil {
		return fmt.Errorf("failed to start consensus: %w", err)
	}

	return nil
}

// Run executes the benchmark
func (b *ConsensusBenchmark) Run(ctx context.Context, iterations int) (*BenchmarkMetrics, error) {
	startTime := common.ConsensusNow()

	// Reset metrics
	b.blocksProposed.Store(0)
	b.blocksFinalized.Store(0)
	b.votesProcessed.Store(0)
	b.consensusRounds.Store(0)
	b.failedRounds.Store(0)
	b.blockLatencies = b.blockLatencies[:0]
	b.finalityLatencies = b.finalityLatencies[:0]

	// Run consensus rounds
	var wg sync.WaitGroup
	concurrency := 1 // Consensus is inherently sequential

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			b.runConsensusRounds(ctx, workerID, iterations/concurrency)
		}(i)
	}

	wg.Wait()

	// Calculate metrics
	duration := time.Since(startTime)
	metrics := b.calculateMetrics(duration)

	return metrics, nil
}

// runConsensusRounds runs multiple consensus rounds
func (b *ConsensusBenchmark) runConsensusRounds(ctx context.Context, workerID int, rounds int) {
	for i := 0; i < rounds; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			b.runSingleRound(ctx, workerID, i)
		}
	}
}

// runSingleRound runs a single consensus round
func (b *ConsensusBenchmark) runSingleRound(ctx context.Context, workerID, round int) {
	b.consensusRounds.Add(1)

	// Create block proposal
	block := b.createBlock(workerID, round)

	// Measure block proposal time
	proposeStart := common.ConsensusNow()
	proposalID, err := b.consensus.ProposeBlock(ctx, block)
	if err != nil {
		b.failedRounds.Add(1)
		b.logger.WithError(err).Debug("Failed to propose block")
		return
	}
	proposeLatency := time.Since(proposeStart)

	b.blocksProposed.Add(1)

	// Simulate voting phase
	voteStart := common.ConsensusNow()
	votes := b.simulateVoting(proposalID, block)
	b.votesProcessed.Add(int64(len(votes)))

	// Process votes
	for _, vote := range votes {
		if err := b.consensus.ProcessVote(ctx, vote); err != nil {
			b.logger.WithError(err).Debug("Failed to process vote")
		}
	}
	voteLatency := time.Since(voteStart)

	// Check finality
	finalityStart := common.ConsensusNow()
	finalized := b.checkFinality(ctx, block.Number)
	finalityLatency := time.Since(finalityStart)

	if finalized {
		b.blocksFinalized.Add(1)

		// Record latencies
		b.mu.Lock()
		b.blockLatencies = append(b.blockLatencies, proposeLatency+voteLatency)
		b.finalityLatencies = append(b.finalityLatencies, finalityLatency)
		b.mu.Unlock()
	} else {
		b.failedRounds.Add(1)
	}
}

// createBlock creates a test block
func (b *ConsensusBenchmark) createBlock(workerID, round int) *common.Block {
	transactions := make([]*types.TypedTransaction, b.config.TransactionsPerBlock)
	for i := 0; i < b.config.TransactionsPerBlock; i++ {
		transactions[i] = &types.TypedTransaction{
			ID:    fmt.Sprintf("tx-%d-%d-%d", workerID, round, i),
			Type:  types.TransactionTypeTransfer,
			From:  fmt.Sprintf("sender-%d", i),
			To:    fmt.Sprintf("recipient-%d", i),
			Value: uint64(1000 + i),
		}
	}

	// Convert TypedTransaction to common.Transaction
	commonTxs := make([]common.Transaction, len(transactions))
	for i, tx := range transactions {
		commonTxs[i] = common.Transaction{
			ID:       tx.ID,
			Sender:   tx.From,
			Receiver: tx.To,
			Amount:   float64(tx.Value),
		}
	}

	return &common.Block{
		Number:       round,
		Hash:         string(generateBlockHash(workerID, round)),
		PreviousHash: string(generateBlockHash(workerID, round-1)),
		Timestamp:    common.ConsensusNow().Unix(),
		Transactions: commonTxs,
		Validator:    fmt.Sprintf("validator-%d", workerID),
	}
}

// simulateVoting simulates validator voting
func (b *ConsensusBenchmark) simulateVoting(proposalID string, block *common.Block) []interface{} {
	votes := make([]interface{}, 0, b.config.ValidatorCount)

	// Simulate network latency
	time.Sleep(b.config.NetworkLatency)

	for i := 0; i < b.config.ValidatorCount; i++ {
		// Skip byzantine nodes
		if i < b.config.ByzantineNodes {
			continue
		}

		vote := &types.VoteData{
			BlockHash:    []byte(block.Hash),
			VoteType:     types.VoteType(1), // Prevote
			ValidatorIdx: uint32(i),
			Timestamp:    common.ConsensusNow().Unix(),
		}

		votes = append(votes, vote)
	}

	return votes
}

// checkFinality checks if a block has achieved finality
func (b *ConsensusBenchmark) checkFinality(ctx context.Context, height int) bool {
	// Simulate finality check with timeout
	ctx, cancel := context.WithTimeout(ctx, b.config.ConsensusTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			// Check if block is finalized
			finalized, err := b.consensus.IsFinalized(uint64(height))
			if err != nil {
				return false
			}
			if finalized {
				return true
			}
		}
	}
}

// calculateMetrics calculates benchmark metrics
func (b *ConsensusBenchmark) calculateMetrics(duration time.Duration) *BenchmarkMetrics {
	proposed := b.blocksProposed.Load()
	finalized := b.blocksFinalized.Load()
	votes := b.votesProcessed.Load()
	rounds := b.consensusRounds.Load()
	failed := b.failedRounds.Load()

	// Calculate throughput
	blocksPerSecond := float64(finalized) / duration.Seconds()
	txPerSecond := float64(finalized*int64(b.config.TransactionsPerBlock)) / duration.Seconds()

	// Calculate latency metrics
	blockLatency := b.calculateLatencyMetrics(b.blockLatencies)
	finalityLatency := b.calculateLatencyMetrics(b.finalityLatencies)

	// Calculate throughput metrics
	throughput := &ThroughputMetrics{
		BlocksPerSecond:   blocksPerSecond,
		MessagesPerSecond: float64(votes) / duration.Seconds(),
		BytesPerSecond:    float64(finalized*int64(b.config.BlockSize)) / duration.Seconds(),
	}

	// Calculate error metrics
	errorMetrics := &ErrorMetrics{
		TotalErrors:  failed,
		ErrorRate:    float64(failed) / float64(rounds),
		ErrorsByType: map[string]int64{"consensus_failure": failed},
	}

	// Get resource metrics
	resourceMetrics := b.getResourceMetrics()

	return &BenchmarkMetrics{
		TotalOperations: rounds,
		TotalDuration:   duration,
		TPS:             txPerSecond,
		Latency:         blockLatency,
		Throughput:      throughput,
		Resources:       resourceMetrics,
		Errors:          errorMetrics,
		Custom: map[string]float64{
			"blocks_proposed":      float64(proposed),
			"blocks_finalized":     float64(finalized),
			"finalization_rate":    float64(finalized) / float64(proposed),
			"votes_per_block":      float64(votes) / float64(rounds),
			"avg_finality_latency": float64(finalityLatency.Mean),
			"blocks_per_second":    blocksPerSecond,
			"consensus_efficiency": float64(finalized) / float64(rounds),
		},
	}
}

// calculateLatencyMetrics calculates latency statistics
func (b *ConsensusBenchmark) calculateLatencyMetrics(latencies []time.Duration) *LatencyMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(latencies) == 0 {
		return &LatencyMetrics{}
	}

	// Make a copy and sort
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sortDurations(sorted)

	count := len(sorted)

	return &LatencyMetrics{
		Min:    sorted[0],
		Max:    sorted[count-1],
		Mean:   calculateMean(sorted),
		Median: sorted[count/2],
		P90:    sorted[int(float64(count)*0.90)],
		P95:    sorted[int(float64(count)*0.95)],
		P99:    sorted[int(float64(count)*0.99)],
		StdDev: calculateStdDev(sorted),
	}
}

// getResourceMetrics gets current resource usage
func (b *ConsensusBenchmark) getResourceMetrics() *ResourceMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return &ResourceMetrics{
		MemoryUsageMB:  float64(m.Sys) / 1024 / 1024,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
		GCCount:        m.NumGC,
		GCPauseTotal:   time.Duration(m.PauseTotalNs),
	}
}

// Cleanup cleans up after the benchmark
func (b *ConsensusBenchmark) Cleanup(ctx context.Context) error {
	if b.consensus != nil {
		return b.consensus.Stop()
	}
	return nil
}

// Validate validates the benchmark results
func (b *ConsensusBenchmark) Validate(metrics *BenchmarkMetrics) error {
	if metrics.TotalOperations == 0 {
		return fmt.Errorf("no consensus rounds were executed")
	}

	finalizationRate := metrics.Custom["finalization_rate"]
	if finalizationRate < 0.9 {
		return fmt.Errorf("finalization rate too low: %.2f%%", finalizationRate*100)
	}

	if metrics.Errors != nil && metrics.Errors.ErrorRate > 0.1 {
		return fmt.Errorf("error rate too high: %.2f%%", metrics.Errors.ErrorRate*100)
	}

	return nil
}

// MockConsensus is a mock consensus implementation for benchmarking
type MockConsensus struct {
	validatorCount  int
	byzantineNodes  int
	networkLatency  time.Duration
	logger          *logrus.Logger
	finalizedHeight uint64

	proposals map[string]*common.Block
	votes     map[string][]interface{}
	finalized map[uint64]bool
	mu        sync.RWMutex
	now       time.Time
}

func (m *MockConsensus) Initialize() error {
	m.proposals = make(map[string]*common.Block)
	m.votes = make(map[string][]interface{})
	m.finalized = make(map[uint64]bool)
	m.now = common.ConsensusNow()
	return nil
}

// GetConsensusTime returns the consensus time
func (m *MockConsensus) GetConsensusTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now
}

func (m *MockConsensus) Start(ctx context.Context) error {
	return nil
}

func (m *MockConsensus) Stop() error {
	return nil
}

func (m *MockConsensus) ProposeBlock(ctx context.Context, block *common.Block) (string, error) {
	proposalID := fmt.Sprintf("proposal-%d-%d", block.Number, common.ConsensusNow().UnixNano())

	m.mu.Lock()
	m.proposals[proposalID] = block
	m.mu.Unlock()

	// Simulate proposal propagation
	time.Sleep(m.networkLatency)

	return proposalID, nil
}

func (m *MockConsensus) ProcessVote(ctx context.Context, vote interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Type assert to VoteData
	voteData, ok := vote.(*types.VoteData)
	if !ok {
		return fmt.Errorf("invalid vote type")
	}

	// Use block hash as proposal ID
	proposalID := string(voteData.BlockHash)

	if m.votes[proposalID] == nil {
		m.votes[proposalID] = make([]interface{}, 0)
	}
	m.votes[proposalID] = append(m.votes[proposalID], vote)

	// Check if we have enough votes
	if len(m.votes[proposalID]) >= (m.validatorCount*2/3)+1 {
		// Mark the height as finalized (we'll use a simple incrementing counter)
		height := uint64(len(m.finalized) + 1)
		m.finalized[height] = true
		m.finalizedHeight = height
	}

	return nil
}

func (m *MockConsensus) IsFinalized(height uint64) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.finalized[height], nil
}

// Helper functions

func generateBlockHash(workerID, round int) []byte {
	return []byte(fmt.Sprintf("hash-%d-%d", workerID, round))
}

func generateStateRoot(round int) []byte {
	return []byte(fmt.Sprintf("state-%d", round))
}

func generateSignature(validatorID int, blockHash []byte) []byte {
	return []byte(fmt.Sprintf("sig-%d-%s", validatorID, blockHash))
}

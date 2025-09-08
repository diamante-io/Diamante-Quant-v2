// Package testutil provides test utilities and helpers for consensus module testing
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"diamante/consensus"
	"diamante/consensus/types"

	"github.com/stretchr/testify/require"
)

// TestConfig provides a test configuration for consensus testing
type TestConfig struct {
	NumValidators      int
	BlockTime          time.Duration
	TransactionTimeout time.Duration
	NetworkLatency     time.Duration
	ByzantineNodes     int
	EnableProfiling    bool
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		NumValidators:      4,
		BlockTime:          100 * time.Millisecond,
		TransactionTimeout: 5 * time.Second,
		NetworkLatency:     10 * time.Millisecond,
		ByzantineNodes:     0,
		EnableProfiling:    false,
	}
}

// TestValidator represents a validator in the test environment
type TestValidator struct {
	ID          string
	PublicKey   []byte
	PrivateKey  []byte
	Stake       uint64
	Address     string // Use string for address instead of common.Address
	IsMalicious bool
}

// TestEnvironment provides a complete test environment for consensus testing
type TestEnvironment struct {
	t          *testing.T
	Config     *TestConfig
	Validators []*TestValidator
	Consensus  *consensus.HybridConsensus // Use the actual consensus type
	Network    *MockNetwork
	Storage    *MockStorage
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
}

// NewTestEnvironment creates a new test environment
func NewTestEnvironment(t *testing.T, config *TestConfig) *TestEnvironment {
	ctx, cancel := context.WithCancel(context.Background())

	env := &TestEnvironment{
		t:          t,
		Config:     config,
		Validators: make([]*TestValidator, 0, config.NumValidators),
		Network:    NewMockNetwork(config.NetworkLatency),
		Storage:    NewMockStorage(),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Generate validators
	for i := 0; i < config.NumValidators; i++ {
		validator := generateTestValidator(t, i, i < config.ByzantineNodes)
		env.Validators = append(env.Validators, validator)
	}

	return env
}

// Cleanup cleans up the test environment
func (env *TestEnvironment) Cleanup() {
	env.cancel()
	if env.Consensus != nil {
		_ = env.Consensus.Stop()
	}
	env.Network.Stop()
}

// StartConsensus initializes and starts the consensus engine
func (env *TestEnvironment) StartConsensus() error {
	// This will be implemented when we create the actual consensus tests
	return nil
}

// ActivateValidators activates all validators in the test environment
func (env *TestEnvironment) ActivateValidators() error {
	if env.Consensus == nil {
		return fmt.Errorf("consensus not initialized")
	}

	// Get the validator manager
	vm := env.Consensus.GetValidatorManager()
	if vm == nil {
		return fmt.Errorf("validator manager not available")
	}

	// Activate all validators
	for _, validator := range env.Validators {
		var validatorID [32]byte
		copy(validatorID[:], validator.ID)

		if err := vm.ActivateValidator(validatorID); err != nil {
			return fmt.Errorf("failed to activate validator %s: %w", validator.ID, err)
		}
	}

	return nil
}

// generateTestValidator creates a test validator with quantum-safe keys
func generateTestValidator(t *testing.T, index int, isMalicious bool) *TestValidator {
	// For now, use simple key generation until crypto package is available
	// In production, this should use quantum-safe keys
	pubKey := make([]byte, 32)
	privKey := make([]byte, 32)
	_, err := rand.Read(pubKey)
	require.NoError(t, err)
	_, err = rand.Read(privKey)
	require.NoError(t, err)

	// Generate address as hex string
	addressBytes := make([]byte, 20)
	_, err = rand.Read(addressBytes)
	require.NoError(t, err)

	return &TestValidator{
		ID:          fmt.Sprintf("validator-%d", index),
		PublicKey:   pubKey,
		PrivateKey:  privKey,
		Stake:       1000000 + uint64(index*100000), // Variable stake
		Address:     hex.EncodeToString(addressBytes),
		IsMalicious: isMalicious,
	}
}

// MockNetwork simulates network behavior for testing
type MockNetwork struct {
	latency    time.Duration
	messages   chan NetworkMessage
	peers      map[string]chan NetworkMessage
	mu         sync.RWMutex
	dropRate   float64
	partitions map[string]bool
}

// NetworkMessage represents a message in the test network
type NetworkMessage struct {
	From    string
	To      string
	Type    string
	Payload []byte
	Time    time.Time
}

// NewMockNetwork creates a new mock network
func NewMockNetwork(latency time.Duration) *MockNetwork {
	return &MockNetwork{
		latency:    latency,
		messages:   make(chan NetworkMessage, 1000),
		peers:      make(map[string]chan NetworkMessage),
		partitions: make(map[string]bool),
	}
}

// RegisterPeer registers a peer in the network
func (n *MockNetwork) RegisterPeer(id string) chan NetworkMessage {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan NetworkMessage, 100)
	n.peers[id] = ch
	return ch
}

// Send sends a message through the network
func (n *MockNetwork) Send(from, to string, msgType string, payload []byte) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Check if sender is partitioned
	if n.partitions[from] || n.partitions[to] {
		return fmt.Errorf("network partition")
	}

	// Simulate message drop
	if n.shouldDropMessage() {
		return nil // Silent drop
	}

	msg := NetworkMessage{
		From:    from,
		To:      to,
		Type:    msgType,
		Payload: payload,
		Time:    time.Now(),
	}

	// Simulate network latency
	go func() {
		time.Sleep(n.latency)
		n.mu.RLock()
		if ch, ok := n.peers[to]; ok {
			select {
			case ch <- msg:
			default:
				// Channel full, drop message
			}
		}
		n.mu.RUnlock()
	}()

	return nil
}

// Broadcast sends a message to all peers
func (n *MockNetwork) Broadcast(from string, msgType string, payload []byte) error {
	n.mu.RLock()
	peers := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if id != from {
			peers = append(peers, id)
		}
	}
	n.mu.RUnlock()

	for _, peer := range peers {
		_ = n.Send(from, peer, msgType, payload)
	}

	return nil
}

// PartitionNode simulates a network partition for a node
func (n *MockNetwork) PartitionNode(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions[id] = true
}

// HealPartition removes a network partition
func (n *MockNetwork) HealPartition(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitions, id)
}

// SetDropRate sets the message drop rate (0.0 to 1.0)
func (n *MockNetwork) SetDropRate(rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = rate
}

// shouldDropMessage determines if a message should be dropped
func (n *MockNetwork) shouldDropMessage() bool {
	if n.dropRate <= 0 {
		return false
	}
	// Simple random drop for testing
	b := make([]byte, 1)
	rand.Read(b)
	return float64(b[0])/255.0 < n.dropRate
}

// Stop stops the mock network
func (n *MockNetwork) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ch := range n.peers {
		close(ch)
	}
	close(n.messages)
}

// MockStorage provides mock storage for testing
type MockStorage struct {
	blocks       map[string]*consensus.Block // Use consensus.Block type
	transactions map[string]*types.Transaction
	state        map[string][]byte
	mu           sync.RWMutex
}

// NewMockStorage creates a new mock storage
func NewMockStorage() *MockStorage {
	return &MockStorage{
		blocks:       make(map[string]*consensus.Block),
		transactions: make(map[string]*types.Transaction),
		state:        make(map[string][]byte),
	}
}

// StoreBlock stores a block
func (s *MockStorage) StoreBlock(block *consensus.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if block == nil {
		return fmt.Errorf("nil block")
	}

	// Use block number as key since consensus.Block doesn't have Hash field
	s.blocks[fmt.Sprintf("%d", block.Number)] = block
	return nil
}

// GetBlock retrieves a block by number
func (s *MockStorage) GetBlock(blockNumber uint64) (*consensus.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	block, ok := s.blocks[fmt.Sprintf("%d", blockNumber)]
	if !ok {
		return nil, fmt.Errorf("block not found")
	}

	return block, nil
}

// StoreTransaction stores a transaction
func (s *MockStorage) StoreTransaction(tx *types.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx == nil {
		return fmt.Errorf("nil transaction")
	}

	s.transactions[tx.ID] = tx
	return nil
}

// GetTransaction retrieves a transaction by ID
func (s *MockStorage) GetTransaction(id string) (*types.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, ok := s.transactions[id]
	if !ok {
		return nil, fmt.Errorf("transaction not found")
	}

	return tx, nil
}

// SetState sets a state value
func (s *MockStorage) SetState(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state[key] = value
	return nil
}

// GetState gets a state value
func (s *MockStorage) GetState(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.state[key]
	if !ok {
		return nil, fmt.Errorf("state not found")
	}

	return value, nil
}

// GetBlockCount returns the number of stored blocks
func (s *MockStorage) GetBlockCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.blocks)
}

// GetTransactionCount returns the number of stored transactions
func (s *MockStorage) GetTransactionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.transactions)
}

// Clear clears all storage
func (s *MockStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blocks = make(map[string]*consensus.Block)
	s.transactions = make(map[string]*types.Transaction)
	s.state = make(map[string][]byte)
}

// TestBlock creates a test block with the given parameters
func TestBlock(number uint64, producer string, events []*types.Event) *consensus.Block {
	return &consensus.Block{
		Number:    number,
		Timestamp: time.Now(),
		Producer:  producer,
		Events:    events,
		PoHHash:   hex.EncodeToString(make([]byte, 32)), // Mock PoH hash
		CreatedAt: time.Now(),
	}
}

// TestTransaction creates a test transaction
func TestTransaction(sender, receiver string, amount uint64) *types.Transaction {
	return &types.Transaction{
		ID:       generateTxID(),
		Sender:   sender,
		Receiver: receiver,
		Amount:   amount,
	}
}

// generateTxID generates a random transaction ID
func generateTxID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// TestEvent creates a test event for consensus
func TestEvent(creator [32]byte, height uint64, data []byte) *types.Event {
	event := &types.Event{
		ID:        generateEventID(),
		Creator:   creator,
		ParentIDs: make([][32]byte, 0),
		Data:      data,
		Timestamp: time.Now(),
		Height:    height,
		Finalized: false,
		PoHState:  [32]byte{},
		PoHCount:  0,
		PoHProof:  [32]byte{},
	}
	return event
}

// generateEventID generates a random event ID
func generateEventID() [32]byte {
	var id [32]byte
	rand.Read(id[:])
	return id
}

// AssertEventually asserts that a condition is met within a timeout
func AssertEventually(t *testing.T, condition func() bool, timeout time.Duration, msg string) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("Condition not met within timeout: %s", msg)
}

// WaitForBlocks waits for a specific number of blocks to be produced
func WaitForBlocks(t *testing.T, storage *MockStorage, count int, timeout time.Duration) {
	AssertEventually(t, func() bool {
		return storage.GetBlockCount() >= count
	}, timeout, fmt.Sprintf("Expected %d blocks, got %d", count, storage.GetBlockCount()))
}

// SimulateByzantineBehavior makes a validator behave maliciously
func SimulateByzantineBehavior(validator *TestValidator) {
	validator.IsMalicious = true
	// Additional Byzantine behavior will be implemented in specific tests
}

// MetricsCollector collects metrics during tests
type MetricsCollector struct {
	BlockTimes      []time.Duration
	TxThroughput    []float64
	ConsensusRounds []int
	NetworkMessages []int
	mu              sync.Mutex
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		BlockTimes:      make([]time.Duration, 0),
		TxThroughput:    make([]float64, 0),
		ConsensusRounds: make([]int, 0),
		NetworkMessages: make([]int, 0),
	}
}

// RecordBlockTime records the time taken to produce a block
func (m *MetricsCollector) RecordBlockTime(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BlockTimes = append(m.BlockTimes, duration)
}

// RecordTxThroughput records transaction throughput
func (m *MetricsCollector) RecordTxThroughput(tps float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TxThroughput = append(m.TxThroughput, tps)
}

// GetAverageBlockTime returns the average block time
func (m *MetricsCollector) GetAverageBlockTime() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.BlockTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range m.BlockTimes {
		total += t
	}

	return total / time.Duration(len(m.BlockTimes))
}

// GetAverageThroughput returns the average transaction throughput
func (m *MetricsCollector) GetAverageThroughput() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.TxThroughput) == 0 {
		return 0
	}

	var total float64
	for _, tps := range m.TxThroughput {
		total += tps
	}

	return total / float64(len(m.TxThroughput))
}

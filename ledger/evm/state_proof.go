// Package evm provides state proof generation and verification for EVM state
package evm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"diamante/consensus"
	"diamante/ledger/evm/trie"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/sirupsen/logrus"
)

// StateProof represents a merkle proof for state
type StateProof struct {
	Key       []byte
	Value     []byte
	Proof     [][]byte
	StateRoot ethcommon.Hash

	// Additional metadata
	AccountProof  *AccountProof
	StorageProofs []StorageProof
	Height        uint64
	Timestamp     time.Time
}

// AccountProof represents a proof for an account
type AccountProof struct {
	Address      ethcommon.Address
	Account      Account
	AccountProof [][]byte
}

// StorageProof represents a proof for a storage slot
type StorageProof struct {
	Key   ethcommon.Hash
	Value ethcommon.Hash
	Proof [][]byte
}

// ProofGenerator generates state proofs
type ProofGenerator struct {
	stateDB *StateDB
	logger  *logrus.Logger
}

// NewProofGenerator creates a new proof generator
func NewProofGenerator(stateDB *StateDB, logger *logrus.Logger) *ProofGenerator {
	if logger == nil {
		logger = logrus.New()
	}

	return &ProofGenerator{
		stateDB: stateDB,
		logger:  logger,
	}
}

// GenerateStateProof creates a proof for a specific account
func (s *StateDB) GenerateStateProof(addr ethcommon.Address) (*StateProof, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	start := consensus.ConsensusNow()

	// Get account data
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil, errors.New("account not found")
	}

	// Generate account proof
	accountProof, err := s.stateTrie.Prove(addr.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to generate account proof: %w", err)
	}

	// Encode account data
	accountData, err := s.encodeAccount(stateObject.data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode account: %w", err)
	}

	proof := &StateProof{
		Key:       addr.Bytes(),
		Value:     accountData,
		Proof:     accountProof,
		StateRoot: s.stateTrie.Hash(),
		AccountProof: &AccountProof{
			Address:      addr,
			Account:      stateObject.data,
			AccountProof: accountProof,
		},
		Height:    s.blockHeight,
		Timestamp: consensus.ConsensusNow(),
	}

	s.logger.WithFields(logrus.Fields{
		"address":  addr.Hex(),
		"duration": consensus.ConsensusSince(start),
		"proofLen": len(accountProof),
	}).Debug("Generated state proof")

	return proof, nil
}

// GenerateStorageProof creates a proof for storage slots
func (s *StateDB) GenerateStorageProof(addr ethcommon.Address, keys []ethcommon.Hash) (*StateProof, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get account proof first
	accountProof, err := s.GenerateStateProof(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to generate account proof: %w", err)
	}

	// Get storage trie for the account
	storageTrie := s.getStorageTrie(addr)

	// Generate proofs for each storage key
	storageProofs := make([]StorageProof, 0, len(keys))
	for _, key := range keys {
		// Get storage value
		value := s.GetState(addr, key)

		// Generate proof
		proof, err := storageTrie.Prove(key.Bytes())
		if err != nil {
			s.logger.WithError(err).WithFields(logrus.Fields{
				"address": addr.Hex(),
				"key":     key.Hex(),
			}).Warn("Failed to generate storage proof")
			continue
		}

		storageProofs = append(storageProofs, StorageProof{
			Key:   key,
			Value: value,
			Proof: proof,
		})
	}

	accountProof.StorageProofs = storageProofs
	return accountProof, nil
}

// VerifyStateProof verifies a state proof
func VerifyStateProof(proof *StateProof) bool {
	if proof == nil || len(proof.Proof) == 0 {
		return false
	}

	// Verify the merkle proof using the trie package
	value, err := trie.VerifyProof(proof.StateRoot, proof.Key, proof.Proof)
	if err != nil {
		return false
	}

	// Check if the value matches
	return bytes.Equal(value, proof.Value)
}

// VerifyAccountProof verifies an account proof
func VerifyAccountProof(stateRoot ethcommon.Hash, addr ethcommon.Address, account Account, proof [][]byte) error {
	if len(proof) == 0 {
		return errors.New("empty proof")
	}

	// Verify the merkle proof for the account
	value, err := trie.VerifyProof(stateRoot, addr.Bytes(), proof)
	if err != nil {
		return fmt.Errorf("failed to verify account proof: %w", err)
	}

	// Encode the expected account data
	expectedData, err := rlp.EncodeToBytes(account)
	if err != nil {
		return fmt.Errorf("failed to encode account: %w", err)
	}

	// Check if the value matches the expected account data
	if !bytes.Equal(value, expectedData) {
		return errors.New("account data mismatch")
	}

	return nil
}

// VerifyStorageProof verifies a storage proof
func VerifyStorageProof(storageRoot ethcommon.Hash, key ethcommon.Hash, value ethcommon.Hash, proof [][]byte) error {
	if len(proof) == 0 {
		return errors.New("empty proof")
	}

	// Verify the merkle proof for the storage slot
	proofValue, err := trie.VerifyProof(storageRoot, key.Bytes(), proof)
	if err != nil {
		return fmt.Errorf("failed to verify storage proof: %w", err)
	}

	// Check if the value matches
	if !bytes.Equal(proofValue, value.Bytes()) {
		return errors.New("storage value mismatch")
	}

	return nil
}

// BatchProofGenerator generates proofs for multiple accounts efficiently
type BatchProofGenerator struct {
	stateDB *StateDB
	logger  *logrus.Logger

	// Parallel processing
	workers   int
	workQueue chan proofRequest
	results   chan proofResult
	wg        sync.WaitGroup
}

type proofRequest struct {
	address     ethcommon.Address
	storageKeys []ethcommon.Hash
}

type proofResult struct {
	address ethcommon.Address
	proof   *StateProof
	err     error
}

// NewBatchProofGenerator creates a new batch proof generator
func NewBatchProofGenerator(stateDB *StateDB, workers int, logger *logrus.Logger) *BatchProofGenerator {
	if workers <= 0 {
		workers = 4 // Default workers
	}

	if logger == nil {
		logger = logrus.New()
	}

	gen := &BatchProofGenerator{
		stateDB:   stateDB,
		logger:    logger,
		workers:   workers,
		workQueue: make(chan proofRequest, workers*2),
		results:   make(chan proofResult, workers*2),
	}

	// Start workers
	for i := 0; i < workers; i++ {
		gen.wg.Add(1)
		go gen.worker(i)
	}

	return gen
}

// worker processes proof requests
func (g *BatchProofGenerator) worker(id int) {
	defer g.wg.Done()

	for req := range g.workQueue {
		start := consensus.ConsensusNow()

		var proof *StateProof
		var err error

		if len(req.storageKeys) > 0 {
			proof, err = g.stateDB.GenerateStorageProof(req.address, req.storageKeys)
		} else {
			proof, err = g.stateDB.GenerateStateProof(req.address)
		}

		g.results <- proofResult{
			address: req.address,
			proof:   proof,
			err:     err,
		}

		g.logger.WithFields(logrus.Fields{
			"worker":   id,
			"address":  req.address.Hex(),
			"duration": consensus.ConsensusSince(start),
			"success":  err == nil,
		}).Debug("Processed proof request")
	}
}

// GenerateBatch generates proofs for multiple accounts
func (g *BatchProofGenerator) GenerateBatch(requests []proofRequest) (map[ethcommon.Address]*StateProof, error) {
	// Submit all requests
	for _, req := range requests {
		g.workQueue <- req
	}

	// Collect results
	results := make(map[ethcommon.Address]*StateProof)
	errors := make([]error, 0)

	for i := 0; i < len(requests); i++ {
		result := <-g.results
		if result.err != nil {
			errors = append(errors, fmt.Errorf("proof for %s failed: %w", result.address.Hex(), result.err))
		} else {
			results[result.address] = result.proof
		}
	}

	if len(errors) > 0 {
		return results, fmt.Errorf("batch proof generation had %d errors", len(errors))
	}

	return results, nil
}

// Close shuts down the batch proof generator
func (g *BatchProofGenerator) Close() {
	close(g.workQueue)
	g.wg.Wait()
	close(g.results)
}

// ProofCache caches generated proofs for efficiency
type ProofCache struct {
	cache map[string]*StateProof
	mu    sync.RWMutex
	ttl   time.Duration
}

// NewProofCache creates a new proof cache
func NewProofCache(ttl time.Duration) *ProofCache {
	return &ProofCache{
		cache: make(map[string]*StateProof),
		ttl:   ttl,
	}
}

// Get retrieves a proof from cache
func (pc *ProofCache) Get(addr ethcommon.Address) (*StateProof, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	proof, exists := pc.cache[addr.Hex()]
	if !exists {
		return nil, false
	}

	// Check if proof is expired
	if consensus.ConsensusSince(proof.Timestamp) > pc.ttl {
		return nil, false
	}

	return proof, true
}

// Put stores a proof in cache
func (pc *ProofCache) Put(addr ethcommon.Address, proof *StateProof) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.cache[addr.Hex()] = proof
}

// Clear removes all cached proofs
func (pc *ProofCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.cache = make(map[string]*StateProof)
}

// ProofValidator provides comprehensive proof validation
type ProofValidator struct {
	logger *logrus.Logger
}

// NewProofValidator creates a new proof validator
func NewProofValidator(logger *logrus.Logger) *ProofValidator {
	if logger == nil {
		logger = logrus.New()
	}

	return &ProofValidator{
		logger: logger,
	}
}

// ValidateStateProof performs comprehensive validation of a state proof
func (v *ProofValidator) ValidateStateProof(proof *StateProof, expectedRoot ethcommon.Hash) error {
	// Basic validation
	if proof == nil {
		return errors.New("proof is nil")
	}

	if len(proof.Key) == 0 {
		return errors.New("proof key is empty")
	}

	if len(proof.Proof) == 0 {
		return errors.New("proof nodes are empty")
	}

	// Verify root matches
	if proof.StateRoot != expectedRoot {
		return fmt.Errorf("state root mismatch: expected %s, got %s", expectedRoot.Hex(), proof.StateRoot.Hex())
	}

	// Verify the proof
	if !VerifyStateProof(proof) {
		return errors.New("proof verification failed")
	}

	// Verify account proof if present
	if proof.AccountProof != nil {
		err := VerifyAccountProof(proof.StateRoot, proof.AccountProof.Address, proof.AccountProof.Account, proof.AccountProof.AccountProof)
		if err != nil {
			return fmt.Errorf("account proof verification failed: %w", err)
		}
	}

	// Verify storage proofs if present
	if proof.AccountProof != nil && len(proof.StorageProofs) > 0 {
		storageRoot := proof.AccountProof.Account.Root
		for _, sp := range proof.StorageProofs {
			err := VerifyStorageProof(storageRoot, sp.Key, sp.Value, sp.Proof)
			if err != nil {
				return fmt.Errorf("storage proof verification failed for key %s: %w", sp.Key.Hex(), err)
			}
		}
	}

	v.logger.WithFields(logrus.Fields{
		"stateRoot": proof.StateRoot.Hex(),
		"key":       ethcommon.BytesToAddress(proof.Key).Hex(),
	}).Debug("State proof validated successfully")

	return nil
}

// SerializeProof serializes a state proof for transmission
func SerializeProof(proof *StateProof) ([]byte, error) {
	return rlp.EncodeToBytes(proof)
}

// DeserializeProof deserializes a state proof
func DeserializeProof(data []byte) (*StateProof, error) {
	var proof StateProof
	err := rlp.DecodeBytes(data, &proof)
	if err != nil {
		return nil, fmt.Errorf("failed to decode proof: %w", err)
	}
	return &proof, nil
}

// CompactProof creates a compact representation of a proof
type CompactProof struct {
	Key       []byte
	Value     []byte
	ProofHash ethcommon.Hash // Hash of all proof nodes
	StateRoot ethcommon.Hash
}

// CompactifyProof creates a compact proof representation
func CompactifyProof(proof *StateProof) *CompactProof {
	// Calculate hash of all proof nodes
	var proofData []byte
	for _, node := range proof.Proof {
		proofData = append(proofData, node...)
	}
	proofHash := crypto.Keccak256Hash(proofData)

	return &CompactProof{
		Key:       proof.Key,
		Value:     proof.Value,
		ProofHash: proofHash,
		StateRoot: proof.StateRoot,
	}
}

// Note: Proof verification is handled by the go-ethereum trie package
// through the VerifyStateProof, VerifyAccountProof, and VerifyStorageProof functions

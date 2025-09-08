// Package evm provides contract verification functionality
package evm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"diamante/consensus"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// ContractVerifier verifies contract integrity and state
type ContractVerifier struct {
	stateDB *StateDB
	logger  *logrus.Logger

	// Cache for verified contracts
	verifiedCache   map[string]*VerificationResult
	verifiedCacheMu sync.RWMutex
	maxCacheSize    int

	// Metrics
	metrics *contractVerifierMetrics
}

// VerificationResult stores the result of contract verification
type VerificationResult struct {
	ContractID   string
	Valid        bool
	CodeHash     []byte
	StateHash    []byte
	VerifiedAt   time.Time
	ErrorMessage string
}

// ContractStateProof represents proof of contract state
type ContractStateProof struct {
	ContractID    string
	CodeHash      []byte
	StateHash     []byte
	Nonce         uint64
	Balance       *big.Int
	StorageRoot   ethcommon.Hash
	StorageProofs map[string]ContractStorageProof
	Timestamp     time.Time
}

// ContractStorageProof represents proof of a storage value for contract verification
type ContractStorageProof struct {
	Key   string
	Value []byte
	Proof [][]byte
}

// contractVerifierMetrics holds Prometheus metrics
type contractVerifierMetrics struct {
	verificationTotal    prometheus.Counter
	verificationSuccess  prometheus.Counter
	verificationFailure  prometheus.Counter
	verificationDuration prometheus.Histogram
	cacheHits            prometheus.Counter
	cacheMisses          prometheus.Counter
}

// NewContractVerifier creates a new contract verifier
func NewContractVerifier(stateDB *StateDB, logger *logrus.Logger) *ContractVerifier {
	if logger == nil {
		logger = logrus.New()
	}

	cv := &ContractVerifier{
		stateDB:       stateDB,
		logger:        logger,
		verifiedCache: make(map[string]*VerificationResult),
		maxCacheSize:  1000,
	}

	// Initialize metrics
	cv.initMetrics()

	return cv
}

// initMetrics initializes Prometheus metrics
func (cv *ContractVerifier) initMetrics() {
	cv.metrics = &contractVerifierMetrics{
		verificationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_verifier_verifications_total",
			Help: "Total number of contract verifications",
		}),
		verificationSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_verifier_verifications_success_total",
			Help: "Total number of successful contract verifications",
		}),
		verificationFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_verifier_verifications_failure_total",
			Help: "Total number of failed contract verifications",
		}),
		verificationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "contract_verifier_verification_duration_ms",
			Help:    "Contract verification duration in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		cacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_verifier_cache_hits_total",
			Help: "Total number of verification cache hits",
		}),
		cacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contract_verifier_cache_misses_total",
			Help: "Total number of verification cache misses",
		}),
	}

	// Register metrics
	prometheus.MustRegister(
		cv.metrics.verificationTotal,
		cv.metrics.verificationSuccess,
		cv.metrics.verificationFailure,
		cv.metrics.verificationDuration,
		cv.metrics.cacheHits,
		cv.metrics.cacheMisses,
	)
}

// VerifyContract verifies a contract exists and matches expected code hash
func (cv *ContractVerifier) VerifyContract(contractID string, expectedCodeHash []byte) error {
	start := consensus.ConsensusNow()
	defer func() {
		cv.metrics.verificationDuration.Observe(float64(time.Since(start).Milliseconds()))
	}()

	cv.metrics.verificationTotal.Inc()

	// Check cache first
	cv.verifiedCacheMu.RLock()
	if result, exists := cv.verifiedCache[contractID]; exists {
		cv.verifiedCacheMu.RUnlock()
		cv.metrics.cacheHits.Inc()

		if !result.Valid {
			cv.metrics.verificationFailure.Inc()
			return fmt.Errorf("cached verification failure: %s", result.ErrorMessage)
		}

		if !bytes.Equal(result.CodeHash, expectedCodeHash) {
			cv.metrics.verificationFailure.Inc()
			return fmt.Errorf("cached code hash mismatch")
		}

		cv.metrics.verificationSuccess.Inc()
		return nil
	}
	cv.verifiedCacheMu.RUnlock()

	cv.metrics.cacheMisses.Inc()

	// Get contract from state
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		cv.cacheVerificationResult(contractID, false, nil, nil, fmt.Sprintf("contract not found: %v", err))
		cv.metrics.verificationFailure.Inc()
		return fmt.Errorf("contract not found: %w", err)
	}

	// Decode contract code
	code, err := hexutil.Decode(contract.Code)
	if err != nil {
		cv.cacheVerificationResult(contractID, false, nil, nil, fmt.Sprintf("invalid contract code: %v", err))
		cv.metrics.verificationFailure.Inc()
		return fmt.Errorf("invalid contract code: %w", err)
	}

	// Calculate code hash
	codeHash := crypto.Keccak256(code)

	// Verify hash matches
	if !bytes.Equal(codeHash, expectedCodeHash) {
		cv.cacheVerificationResult(contractID, false, codeHash, nil, "code hash mismatch")
		cv.metrics.verificationFailure.Inc()
		return fmt.Errorf("contract code hash mismatch: expected %s, got %s",
			hex.EncodeToString(expectedCodeHash),
			hex.EncodeToString(codeHash))
	}

	// Calculate state hash
	stateData, _ := json.Marshal(contract.State)
	stateHash := sha256.Sum256(stateData)

	// Cache successful verification
	cv.cacheVerificationResult(contractID, true, codeHash, stateHash[:], "")
	cv.metrics.verificationSuccess.Inc()

	cv.logger.WithFields(logrus.Fields{
		"contractID": contractID,
		"codeHash":   hex.EncodeToString(codeHash),
	}).Debug("Contract verified successfully")

	return nil
}

// VerifyContractCode verifies just the contract code without checking hash
func (cv *ContractVerifier) VerifyContractCode(contractID string) ([]byte, error) {
	// Get contract from state
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		return nil, fmt.Errorf("contract not found: %w", err)
	}

	// Decode contract code
	code, err := hexutil.Decode(contract.Code)
	if err != nil {
		return nil, fmt.Errorf("invalid contract code: %w", err)
	}

	// Calculate and return code hash
	codeHash := crypto.Keccak256(code)
	return codeHash, nil
}

// VerifyContractState verifies contract state integrity
func (cv *ContractVerifier) VerifyContractState(contractID string) (*ContractStateProof, error) {
	start := consensus.ConsensusNow()
	defer func() {
		cv.metrics.verificationDuration.Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Get contract
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		return nil, fmt.Errorf("contract not found: %w", err)
	}

	// Parse address
	addr, err := hexutil.Decode(contractID)
	if err != nil {
		// If not hex, try as string
		addr = []byte(contractID)
	}

	ethAddr := ethcommon.BytesToAddress(addr)

	// Get state object
	stateObject := cv.stateDB.GetStateObject(ethAddr)
	if stateObject == nil {
		return nil, errors.New("state object not found")
	}

	// Calculate state hash
	stateData, _ := json.Marshal(contract.State)
	stateHash := sha256.Sum256(stateData)

	// Create state proof
	proof := &ContractStateProof{
		ContractID:    contractID,
		CodeHash:      stateObject.data.CodeHash,
		StateHash:     stateHash[:],
		Nonce:         stateObject.data.Nonce,
		Balance:       new(big.Int).Set(stateObject.data.Balance),
		StorageRoot:   stateObject.data.Root,
		StorageProofs: make(map[string]ContractStorageProof),
		Timestamp:     consensus.ConsensusNow(),
	}

	cv.logger.WithFields(logrus.Fields{
		"contractID":  contractID,
		"nonce":       proof.Nonce,
		"balance":     proof.Balance.String(),
		"storageRoot": proof.StorageRoot.Hex(),
	}).Debug("Contract state verified")

	return proof, nil
}

// VerifyContractExistence checks if a contract exists
func (cv *ContractVerifier) VerifyContractExistence(contractID string) (bool, error) {
	// Try to get contract
	_, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		// Check if it's a not found error
		if err.Error() == "contract not found" {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// VerifyContractOwner verifies the owner of a contract
func (cv *ContractVerifier) VerifyContractOwner(contractID string, expectedOwner string) error {
	// Get contract
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		return fmt.Errorf("contract not found: %w", err)
	}

	// Verify owner
	if contract.Owner != expectedOwner {
		return fmt.Errorf("owner mismatch: expected %s, got %s", expectedOwner, contract.Owner)
	}

	return nil
}

// VerifyContractVersion verifies the version of a contract
func (cv *ContractVerifier) VerifyContractVersion(contractID string, expectedVersion string) error {
	// Get contract
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		return fmt.Errorf("contract not found: %w", err)
	}

	// Verify version
	if contract.Version != expectedVersion {
		return fmt.Errorf("version mismatch: expected %s, got %s", expectedVersion, contract.Version)
	}

	return nil
}

// GetContractProof generates a complete proof for contract verification
func (cv *ContractVerifier) GetContractProof(contractID string, storageKeys []string) (*ContractStateProof, error) {
	// Get basic state proof
	proof, err := cv.VerifyContractState(contractID)
	if err != nil {
		return nil, err
	}

	// Add storage proofs for requested keys
	addr, _ := hexutil.Decode(contractID)
	ethAddr := ethcommon.BytesToAddress(addr)

	for _, key := range storageKeys {
		keyHash := ethcommon.HexToHash(key)
		value := cv.stateDB.GetState(ethAddr, keyHash)

		// Create storage proof (simplified)
		storageProof := ContractStorageProof{
			Key:   key,
			Value: value.Bytes(),
			Proof: [][]byte{}, // In production, this would include Merkle proof
		}

		proof.StorageProofs[key] = storageProof
	}

	return proof, nil
}

// VerifyBatchContracts verifies multiple contracts in batch
func (cv *ContractVerifier) VerifyBatchContracts(verifications map[string][]byte) (map[string]error, error) {
	results := make(map[string]error)

	for contractID, expectedCodeHash := range verifications {
		err := cv.VerifyContract(contractID, expectedCodeHash)
		results[contractID] = err
	}

	return results, nil
}

// cacheVerificationResult caches the result of a verification
func (cv *ContractVerifier) cacheVerificationResult(contractID string, valid bool, codeHash, stateHash []byte, errorMsg string) {
	cv.verifiedCacheMu.Lock()
	defer cv.verifiedCacheMu.Unlock()

	// Check cache size and evict if necessary
	if len(cv.verifiedCache) >= cv.maxCacheSize {
		// Simple eviction: remove first item
		for id := range cv.verifiedCache {
			delete(cv.verifiedCache, id)
			break
		}
	}

	cv.verifiedCache[contractID] = &VerificationResult{
		ContractID:   contractID,
		Valid:        valid,
		CodeHash:     codeHash,
		StateHash:    stateHash,
		VerifiedAt:   consensus.ConsensusNow(),
		ErrorMessage: errorMsg,
	}
}

// ClearCache clears the verification cache
func (cv *ContractVerifier) ClearCache() {
	cv.verifiedCacheMu.Lock()
	defer cv.verifiedCacheMu.Unlock()

	cv.verifiedCache = make(map[string]*VerificationResult)
}

// GetCacheStats returns cache statistics
func (cv *ContractVerifier) GetCacheStats() (size int, maxSize int) {
	cv.verifiedCacheMu.RLock()
	defer cv.verifiedCacheMu.RUnlock()

	return len(cv.verifiedCache), cv.maxCacheSize
}

// VerifyStateRoot verifies the state root matches expected
func (cv *ContractVerifier) VerifyStateRoot(expectedRoot ethcommon.Hash) error {
	// Get current state root
	currentRoot := cv.stateDB.IntermediateRoot(false)

	if currentRoot != expectedRoot {
		return fmt.Errorf("state root mismatch: expected %s, got %s",
			expectedRoot.Hex(),
			currentRoot.Hex())
	}

	return nil
}

// GetContractMetrics returns verification metrics
func (cv *ContractVerifier) GetContractMetrics(contractID string) (map[string]interface{}, error) {
	// Get contract
	contract, err := cv.stateDB.getSmartContract(contractID)
	if err != nil {
		return nil, fmt.Errorf("contract not found: %w", err)
	}

	// Decode code to get size
	code, _ := hexutil.Decode(contract.Code)

	// Count state entries
	stateEntries := 0
	hasState := false
	if contract.State != nil {
		hasState = true
		// Count entries in each map
		if contract.State.Variables != nil {
			stateEntries += len(contract.State.Variables)
		}
		if contract.State.Balances != nil {
			stateEntries += len(contract.State.Balances)
		}
		if contract.State.Permissions != nil {
			stateEntries += len(contract.State.Permissions)
		}
		if contract.State.Configuration != nil {
			stateEntries += len(contract.State.Configuration)
		}
		if contract.State.Counters != nil {
			stateEntries += len(contract.State.Counters)
		}
	}

	// Build metrics
	metrics := map[string]interface{}{
		"contractID":   contractID,
		"codeSize":     len(code),
		"owner":        contract.Owner,
		"version":      contract.Version,
		"language":     contract.Language,
		"hasState":     hasState,
		"stateEntries": stateEntries,
	}

	// Note: CreatedAt and UpdatedAt fields are not available in common.SmartContract
	// These could be added in the future if timestamp tracking is needed

	return metrics, nil
}

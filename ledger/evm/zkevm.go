package evm

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/crypto/zksnark"
	"diamante/crypto/zksnark/circuits"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

// ZKEVMConfig contains configuration for zkEVM
type ZKEVMConfig struct {
	MaxBatchSize    int
	ProofGeneration bool
	CircuitVersion  string
	GPUAcceleration bool
	ProofCacheSize  int
	ProofTimeout    time.Duration
	ParallelProvers int
}

// DefaultZKEVMConfig returns default zkEVM configuration
func DefaultZKEVMConfig() *ZKEVMConfig {
	return &ZKEVMConfig{
		MaxBatchSize:    100,
		ProofGeneration: true,
		CircuitVersion:  "v1.0",
		GPUAcceleration: false,
		ProofCacheSize:  1000,
		ProofTimeout:    30 * time.Second,
		ParallelProvers: 4,
	}
}

// ZKProof represents a zero-knowledge proof for EVM execution
type ZKProof struct {
	Type         string
	Proof        []byte
	PublicInputs []byte
	StateRoot    ethcommon.Hash
	TxHash       ethcommon.Hash
	GasUsed      uint64
	Timestamp    time.Time
}

// BatchZKProof represents a proof for a batch of transactions
type BatchZKProof struct {
	Type          string
	Proof         []byte
	PublicInputs  []byte
	PreStateRoot  ethcommon.Hash
	PostStateRoot ethcommon.Hash
	TxHashes      []ethcommon.Hash
	TotalGasUsed  uint64
	NumTxs        int
	Timestamp     time.Time
}

// ZKExecutionResult represents the result of zkEVM execution
type ZKExecutionResult struct {
	ReturnData    []byte
	GasUsed       uint64
	StateRoot     ethcommon.Hash
	Logs          []EventLog
	Success       bool
	Error         error
	ExecutionTime time.Duration
}

// BatchResult represents the result of batch execution
type BatchResult struct {
	Results       []*ZKExecutionResult
	TotalGasUsed  uint64
	StateRoot     ethcommon.Hash
	NumSuccessful int
	NumFailed     int
}

// ZKEVM represents the zero-knowledge EVM
type ZKEVM struct {
	*EVMExecutor
	config       *ZKEVMConfig
	proofSystem  *zksnark.ProofSystem
	zkProofCache *ZKProofCache
	batchQueue   []*common.Transaction
	batchMutex   sync.Mutex
	logger       *logrus.Logger

	// Metrics
	proofsGenerated uint64
	proofsVerified  uint64
	batchesProved   uint64
}

// NewZKEVM creates a new zkEVM instance
func NewZKEVM(executor *EVMExecutor, config *ZKEVMConfig, logger *logrus.Logger) (*ZKEVM, error) {
	if config == nil {
		config = DefaultZKEVMConfig()
	}

	proofSystem, err := zksnark.NewProofSystem()
	if err != nil {
		return nil, fmt.Errorf("failed to create proof system: %w", err)
	}

	// Setup the EVM execution circuit
	circuit := &circuits.EVMExecutionCircuit{}
	if err := proofSystem.Setup(circuit); err != nil {
		return nil, fmt.Errorf("failed to setup circuit: %w", err)
	}

	return &ZKEVM{
		EVMExecutor:  executor,
		config:       config,
		proofSystem:  proofSystem,
		zkProofCache: NewZKProofCache(config.ProofCacheSize),
		batchQueue:   make([]*common.Transaction, 0, config.MaxBatchSize),
		logger:       logger,
	}, nil
}

// ExecuteWithProof executes a transaction and generates a proof
func (z *ZKEVM) ExecuteWithProof(tx *common.Transaction) (*ZKExecutionResult, *ZKProof, error) {
	start := consensus.ConsensusNow()

	// Convert transaction to EVM message
	msg, err := z.transactionToMessage(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert transaction: %w", err)
	}

	// Get pre-state root
	preStateRoot := z.stateDB.stateTrie.Hash()

	// Execute the transaction
	result, gasUsed, err := z.ExecuteContract(msg.From(), *msg.To(), msg.Data(), msg.Value(), msg.Gas())

	executionTime := consensus.ConsensusSince(start)

	// Get post-state root
	postStateRoot := z.stateDB.stateTrie.Hash()

	// Create execution result
	execResult := &ZKExecutionResult{
		ReturnData:    result,
		GasUsed:       gasUsed,
		StateRoot:     postStateRoot,
		Logs:          z.collectEventLogs(),
		Success:       err == nil,
		Error:         err,
		ExecutionTime: executionTime,
	}

	// Generate proof if enabled
	var proof *ZKProof
	if z.config.ProofGeneration {
		proof, err = z.generateExecutionProof(tx, preStateRoot, postStateRoot, gasUsed)
		if err != nil {
			z.logger.WithError(err).Warn("Failed to generate proof")
			// Continue without proof in non-critical mode
		}
	}

	return execResult, proof, nil
}

// BatchExecuteWithProof executes a batch of transactions and generates a single proof
func (z *ZKEVM) BatchExecuteWithProof(txs []*common.Transaction) (*BatchResult, *BatchZKProof, error) {
	if len(txs) == 0 {
		return nil, nil, errors.New("no transactions to execute")
	}

	if len(txs) > z.config.MaxBatchSize {
		return nil, nil, fmt.Errorf("batch size %d exceeds maximum %d", len(txs), z.config.MaxBatchSize)
	}

	// Get pre-state root
	preStateRoot := z.stateDB.stateTrie.Hash()

	results := make([]*ZKExecutionResult, 0, len(txs))
	txHashes := make([]ethcommon.Hash, 0, len(txs))
	totalGasUsed := uint64(0)
	numSuccessful := 0

	// Execute all transactions
	for _, tx := range txs {
		result, _, err := z.ExecuteWithProof(tx)
		if err != nil {
			z.logger.WithError(err).Warn("Failed to execute transaction in batch", "txID", tx.ID)
			continue
		}

		results = append(results, result)
		txHashes = append(txHashes, ethcommon.HexToHash(tx.ID))
		totalGasUsed += result.GasUsed

		if result.Success {
			numSuccessful++
		}
	}

	// Get post-state root
	postStateRoot := z.stateDB.stateTrie.Hash()

	batchResult := &BatchResult{
		Results:       results,
		TotalGasUsed:  totalGasUsed,
		StateRoot:     postStateRoot,
		NumSuccessful: numSuccessful,
		NumFailed:     len(txs) - numSuccessful,
	}

	// Generate batch proof if enabled
	var batchProof *BatchZKProof
	if z.config.ProofGeneration {
		var err error
		batchProof, err = z.generateBatchProof(txs, preStateRoot, postStateRoot, totalGasUsed)
		if err != nil {
			z.logger.WithError(err).Warn("Failed to generate batch proof")
		} else {
			z.batchesProved++
		}
		return batchResult, batchProof, nil
	}

	return batchResult, nil, nil
}

// VerifyExecutionProof verifies a zkEVM execution proof
func (z *ZKEVM) VerifyExecutionProof(result *ZKExecutionResult, proof *ZKProof) bool {
	if proof == nil {
		return false
	}

	// Check cache first
	if z.zkProofCache.Has(proof.TxHash.Hex()) {
		z.proofsVerified++
		return true
	}

	// Verify the proof
	zkProof := &zksnark.Proof{
		Proof:      proof.Proof,
		PublicData: proof.PublicInputs,
		ProofType:  proof.Type,
	}

	valid, err := z.proofSystem.Verify(zkProof)
	if err != nil {
		z.logger.WithError(err).Error("Failed to verify proof")
		return false
	}

	if valid {
		z.zkProofCache.Add(proof.TxHash.Hex(), proof)
		z.proofsVerified++
	}

	return valid
}

// generateExecutionProof generates a proof for transaction execution
func (z *ZKEVM) generateExecutionProof(tx *common.Transaction, preState, postState ethcommon.Hash, gasUsed uint64) (*ZKProof, error) {
	// Create circuit witness
	circuit := &circuits.EVMExecutionCircuit{
		PreStateRoot:  preState.Big(),
		PostStateRoot: postState.Big(),
		TxHash:        new(big.Int).SetBytes([]byte(tx.ID)),
		GasUsed:       new(big.Int).SetUint64(gasUsed),
		// Additional witness data would be populated here
	}

	// Generate proof
	proof, err := z.proofSystem.Prove(circuit)
	if err != nil {
		return nil, fmt.Errorf("failed to generate proof: %w", err)
	}

	z.proofsGenerated++

	return &ZKProof{
		Type:         "evm_execution",
		Proof:        proof.Proof,
		PublicInputs: proof.PublicData,
		StateRoot:    postState,
		TxHash:       ethcommon.HexToHash(tx.ID),
		GasUsed:      gasUsed,
		Timestamp:    consensus.ConsensusNow(),
	}, nil
}

// generateBatchProof generates a proof for batch execution
func (z *ZKEVM) generateBatchProof(txs []*common.Transaction, preState, postState ethcommon.Hash, totalGas uint64) (*BatchZKProof, error) {
	// Create batch circuit witness
	batchCircuit := &circuits.BatchExecutionCircuit{
		PreStateRoot:  preState.Big(),
		PostStateRoot: postState.Big(),
		BatchHash:     z.computeBatchHash(txs).Big(),
		TotalGasUsed:  new(big.Int).SetUint64(totalGas),
		NumTxs:        new(big.Int).SetInt64(int64(len(txs))),
		// Transaction details would be added here
	}

	// Generate proof
	proof, err := z.proofSystem.Prove(batchCircuit)
	if err != nil {
		return nil, fmt.Errorf("failed to generate batch proof: %w", err)
	}

	txHashes := make([]ethcommon.Hash, len(txs))
	for i, tx := range txs {
		txHashes[i] = ethcommon.HexToHash(tx.ID)
	}

	return &BatchZKProof{
		Type:          "batch_execution",
		Proof:         proof.Proof,
		PublicInputs:  proof.PublicData,
		PreStateRoot:  preState,
		PostStateRoot: postState,
		TxHashes:      txHashes,
		TotalGasUsed:  totalGas,
		NumTxs:        len(txs),
		Timestamp:     consensus.ConsensusNow(),
	}, nil
}

// AddToBatch adds a transaction to the current batch
func (z *ZKEVM) AddToBatch(tx *common.Transaction) error {
	z.batchMutex.Lock()
	defer z.batchMutex.Unlock()

	if len(z.batchQueue) >= z.config.MaxBatchSize {
		return errors.New("batch queue is full")
	}

	z.batchQueue = append(z.batchQueue, tx)
	return nil
}

// ProcessBatch processes the current batch
func (z *ZKEVM) ProcessBatch() (*BatchResult, *BatchZKProof, error) {
	z.batchMutex.Lock()
	batch := z.batchQueue
	z.batchQueue = make([]*common.Transaction, 0, z.config.MaxBatchSize)
	z.batchMutex.Unlock()

	if len(batch) == 0 {
		return nil, nil, errors.New("no transactions in batch")
	}

	return z.BatchExecuteWithProof(batch)
}

// transactionToMessage converts a Diamante transaction to EVM message
func (z *ZKEVM) transactionToMessage(tx *common.Transaction) (*Message, error) {
	// Convert addresses
	from := ethcommon.HexToAddress(tx.Sender)
	var to *ethcommon.Address
	if tx.Receiver != "" {
		addr := ethcommon.HexToAddress(tx.Receiver)
		to = &addr
	}

	// Convert value
	value := new(big.Int).SetUint64(uint64(tx.Amount * 1e18)) // Convert to wei

	return &Message{
		from:     from,
		to:       to,
		value:    value,
		gasLimit: 1000000, // Default gas limit
		data:     tx.Data,
		nonce:    uint64(tx.Nonce),
	}, nil
}

// computeBatchHash computes a hash for a batch of transactions
func (z *ZKEVM) computeBatchHash(txs []*common.Transaction) ethcommon.Hash {
	// Simple batch hash computation
	// In production, this would use a more sophisticated method
	combined := []byte{}
	for _, tx := range txs {
		combined = append(combined, []byte(tx.ID)...)
	}
	return ethcommon.BytesToHash(crypto.Keccak256(combined))
}

// GetMetrics returns zkEVM metrics
func (z *ZKEVM) GetMetrics() map[string]uint64 {
	return map[string]uint64{
		"proofs_generated": z.proofsGenerated,
		"proofs_verified":  z.proofsVerified,
		"batches_proved":   z.batchesProved,
		"cache_size":       uint64(z.zkProofCache.Size()),
	}
}

// collectEventLogs collects event logs from the EVM execution
func (z *ZKEVM) collectEventLogs() []EventLog {
	// TODO: Implement proper event collection from eventManager
	// For now, return empty slice
	return []EventLog{}
}

// ZKProofCache caches zkEVM proofs for efficiency
type ZKProofCache struct {
	cache map[string]*ZKProof
	mu    sync.RWMutex
	size  int
}

// NewZKProofCache creates a new zkEVM proof cache
func NewZKProofCache(size int) *ZKProofCache {
	return &ZKProofCache{
		cache: make(map[string]*ZKProof),
		size:  size,
	}
}

// Add adds a proof to the cache
func (pc *ZKProofCache) Add(key string, proof *ZKProof) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if len(pc.cache) >= pc.size {
		// Simple eviction - remove first item
		for k := range pc.cache {
			delete(pc.cache, k)
			break
		}
	}

	pc.cache[key] = proof
}

// Has checks if a proof exists in cache
func (pc *ZKProofCache) Has(key string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	_, exists := pc.cache[key]
	return exists
}

// Get retrieves a proof from cache
func (pc *ZKProofCache) Get(key string) (*ZKProof, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	proof, exists := pc.cache[key]
	return proof, exists
}

// Size returns the current cache size
func (pc *ZKProofCache) Size() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return len(pc.cache)
}

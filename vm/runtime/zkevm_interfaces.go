package runtime

import (
	"time"

	"diamante/common"
)

// ZKEVMExecutor interface for zkEVM execution with proof generation
type ZKEVMExecutor interface {
	// Execute with proof generation
	ExecuteWithProof(tx *common.Transaction) (*ZKExecutionResult, *ZKProof, error)

	// Batch execution for efficiency
	BatchExecuteWithProof(txs []*common.Transaction) (*BatchResult, *BatchZKProof, error)

	// Verify execution proof
	VerifyExecutionProof(result *ZKExecutionResult, proof *ZKProof) bool

	// Add transaction to batch
	AddToBatch(tx *common.Transaction) error

	// Process current batch
	ProcessBatch() (*BatchResult, *BatchZKProof, error)
}

// ZKProof represents a zero-knowledge proof
type ZKProof struct {
	Type         string
	Proof        []byte
	PublicInputs []byte
	StateRoot    string
	TxHash       string
	GasUsed      uint64
	Timestamp    time.Time
}

// BatchZKProof represents a proof for a batch of transactions
type BatchZKProof struct {
	Type          string
	Proof         []byte
	PublicInputs  []byte
	PreStateRoot  string
	PostStateRoot string
	TxHashes      []string
	TotalGasUsed  uint64
	NumTxs        int
	Timestamp     time.Time
}

// ZKExecutionResult represents the result of zkEVM execution
type ZKExecutionResult struct {
	ReturnData    []byte
	GasUsed       uint64
	StateRoot     string
	Logs          []interface{} // Generic log type for now
	Success       bool
	Error         error
	ExecutionTime time.Duration
}

// BatchResult represents the result of batch execution
type BatchResult struct {
	Results       []*ZKExecutionResult
	TotalGasUsed  uint64
	StateRoot     string
	NumSuccessful int
	NumFailed     int
}

// ZKEVMRuntime extends the Runtime interface with zkEVM capabilities
type ZKEVMRuntime interface {
	Runtime
	ZKEVMExecutor

	// EnableProofGeneration enables/disables proof generation
	EnableProofGeneration(enable bool)

	// GetProofMetrics returns proof generation metrics
	GetProofMetrics() ProofMetrics

	// SetBatchSize sets the maximum batch size
	SetBatchSize(size int) error
}

// ProofMetrics contains metrics about proof generation
type ProofMetrics struct {
	ProofsGenerated  uint64
	ProofsVerified   uint64
	BatchesProcessed uint64
	AverageProofTime time.Duration
	AverageBatchSize int
	CacheHitRate     float64
}

// ZKEVMConfig configuration for zkEVM runtime
type ZKEVMConfig struct {
	EnableProofs    bool
	MaxBatchSize    int
	ProofTimeout    time.Duration
	CircuitVersion  string
	GPUAcceleration bool
	ProofCacheSize  int
	ParallelProvers int
}

// ZKEVMCapabilities describes zkEVM capabilities
type ZKEVMCapabilities struct {
	MaxBatchSize      int
	ProofTypes        []string
	CircuitVersions   []string
	GPUAvailable      bool
	EstimatedTPSBoost float64
}

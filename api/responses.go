package api

import (
	"encoding/base64"

	"diamante/common"
	"diamante/storage"
)

// ZKBlockResponse represents a block response with zkEVM proof data
type ZKBlockResponse struct {
	Number          int                     `json:"number"`
	Hash            string                  `json:"hash"`
	PreviousHash    string                  `json:"previousHash"`
	Timestamp       int64                   `json:"timestamp"`
	Validator       string                  `json:"validator"`
	Transactions    []ZKTransactionResponse `json:"transactions"`
	GasUsed         uint64                  `json:"gasUsed"`
	GasLimit        uint64                  `json:"gasLimit"`
	StateRoot       string                  `json:"stateRoot"`
	TransactionRoot string                  `json:"transactionRoot"`
	// PoH fields
	PoHState      string `json:"pohState,omitempty"`
	PoHCount      uint64 `json:"pohCount,omitempty"`
	PoHBatchProof string `json:"pohBatchProof,omitempty"`
	// zkEVM fields
	ZKProof *ZKProofResponse `json:"zkProof,omitempty"`
}

// ZKProofResponse represents zkEVM proof data in API responses
type ZKProofResponse struct {
	Type         string `json:"type"`
	Proof        string `json:"proof"`        // Base64 encoded
	PublicInputs string `json:"publicInputs"` // Base64 encoded
	Verified     bool   `json:"verified"`
	GeneratedAt  int64  `json:"generatedAt"`
}

// ZKTransactionResponse represents a transaction in zkEVM API responses
type ZKTransactionResponse struct {
	ID              string   `json:"id"`
	Sender          string   `json:"sender"`
	Receiver        string   `json:"receiver"`
	Amount          float64  `json:"amount"`
	Fee             float64  `json:"fee"`
	Timestamp       int64    `json:"timestamp"`
	Status          string   `json:"status"`
	BlockHeight     int      `json:"blockHeight"`
	GasUsed         uint64   `json:"gasUsed,omitempty"`
	ContractAddress string   `json:"contractAddress,omitempty"`
	Logs            []string `json:"logs,omitempty"`
}

// ZKTransactionReceiptResponse includes zkEVM proof data
type ZKTransactionReceiptResponse struct {
	ZKTransactionResponse
	ExecutionProof *ZKProofResponse `json:"executionProof,omitempty"`
	BlockHash      string           `json:"blockHash"`
	Success        bool             `json:"success"`
	Error          string           `json:"error,omitempty"`
}

// ConvertBlockToResponse converts a common.Block to ZKBlockResponse
func ConvertBlockToResponse(block *common.Block) *ZKBlockResponse {
	resp := &ZKBlockResponse{
		Number:          block.Number,
		Hash:            block.Hash,
		PreviousHash:    block.PreviousHash,
		Timestamp:       block.Timestamp,
		Validator:       block.Validator,
		GasUsed:         block.GasUsed,
		GasLimit:        block.GasLimit,
		StateRoot:       block.StateRoot,
		TransactionRoot: block.TransactionRoot,
		PoHState:        block.PoHState,
		PoHCount:        block.PoHCount,
		PoHBatchProof:   block.PoHBatchProof,
		Transactions:    make([]ZKTransactionResponse, 0, len(block.Transactions)),
	}

	// Convert transactions
	for _, tx := range block.Transactions {
		txResp := ZKTransactionResponse{
			ID:          tx.ID,
			Sender:      tx.Sender,
			Receiver:    tx.Receiver,
			Amount:      tx.Amount,
			Fee:         tx.Fee,
			Timestamp:   tx.Timestamp,
			Status:      tx.Status,
			BlockHeight: block.Number,
		}
		resp.Transactions = append(resp.Transactions, txResp)
	}

	// Add zkEVM proof if present
	if len(block.ZKProof) > 0 {
		resp.ZKProof = &ZKProofResponse{
			Type:         block.ZKProofType,
			Proof:        base64.StdEncoding.EncodeToString(block.ZKProof),
			PublicInputs: base64.StdEncoding.EncodeToString(block.ZKPublicInputs),
			Verified:     true, // Assume verified if in block
			GeneratedAt:  block.Timestamp,
		}
	}

	return resp
}

// ConvertTypedBlockToResponse converts a storage.TypedBlock to ZKBlockResponse
func ConvertTypedBlockToResponse(block *storage.TypedBlock) *ZKBlockResponse {
	// First convert TypedBlock to common.Block
	commonBlock := common.Block{
		Number:          int(block.Height),
		Hash:            base64.StdEncoding.EncodeToString(block.Hash),
		PreviousHash:    base64.StdEncoding.EncodeToString(block.PreviousHash),
		Timestamp:       block.Timestamp,
		Validator:       block.Proposer, // TypedBlock uses Proposer field
		StateRoot:       base64.StdEncoding.EncodeToString(block.StateRoot),
		TransactionRoot: "", // Not available in TypedBlock
		GasUsed:         0,  // Not available in TypedBlock
		GasLimit:        0,  // Not available in TypedBlock
		// zkEVM fields - these may not exist in TypedBlock
		ZKProof:        []byte{},
		ZKProofType:    "",
		ZKPublicInputs: []byte{},
	}

	// Convert transactions if present
	if block.TransactionIDs != nil {
		commonBlock.Transactions = make([]common.Transaction, 0, len(block.TransactionIDs))
		// Note: We only have transaction IDs, not full transaction data
		for _, txID := range block.TransactionIDs {
			commonBlock.Transactions = append(commonBlock.Transactions, common.Transaction{
				ID: txID,
			})
		}
	}

	return ConvertBlockToResponse(&commonBlock)
}

// ProofVerificationRequest represents a request to verify a zkEVM proof
type ProofVerificationRequest struct {
	Proof        string `json:"proof"`        // Base64 encoded
	PublicInputs string `json:"publicInputs"` // Base64 encoded
	ProofType    string `json:"proofType"`
}

// ProofVerificationResponse represents the result of proof verification
type ProofVerificationResponse struct {
	Valid      bool   `json:"valid"`
	Error      string `json:"error,omitempty"`
	VerifiedAt int64  `json:"verifiedAt"`
	ProofType  string `json:"proofType"`
}

// ZKEVMMetricsResponse represents zkEVM performance metrics
type ZKEVMMetricsResponse struct {
	ProofsGenerated  uint64  `json:"proofsGenerated"`
	ProofsVerified   uint64  `json:"proofsVerified"`
	BatchesProcessed uint64  `json:"batchesProcessed"`
	AverageProofTime float64 `json:"averageProofTimeMs"`
	AverageBatchSize int     `json:"averageBatchSize"`
	CacheHitRate     float64 `json:"cacheHitRate"`
	LastProofTime    int64   `json:"lastProofTime"`
}

// BatchProofStatusResponse represents the status of batch proof generation
type BatchProofStatusResponse struct {
	BatchID         string `json:"batchId"`
	Status          string `json:"status"` // "pending", "generating", "completed", "failed"
	NumTransactions int    `json:"numTransactions"`
	ProofType       string `json:"proofType"`
	StartedAt       int64  `json:"startedAt"`
	CompletedAt     int64  `json:"completedAt,omitempty"`
	Error           string `json:"error,omitempty"`
	EstimatedTimeMs int64  `json:"estimatedTimeMs,omitempty"`
}

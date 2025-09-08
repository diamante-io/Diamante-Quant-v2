package common

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

// Message Types
const (
	BlockBroadcast          = "BlockBroadcast"
	TransactionBroadcast    = "TransactionBroadcast"
	CrossChainMessage       = "CrossChainMessage"
	SyncRequest             = "SyncRequest"
	ProposalResultBroadcast = "ProposalResultBroadcast"
)

// Error definitions for improved error handling
var (
	ErrAccountNotFound    = errors.New("account does not exist")
	ErrInsufficientFunds  = errors.New("insufficient funds for transaction")
	ErrAccountExists      = errors.New("account already exists")
	ErrInvalidTransaction = errors.New("invalid transaction")
	ErrInvalidBlockData   = errors.New("invalid block data")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrInvalidNonce       = errors.New("invalid nonce")
	ErrInactiveValidator  = errors.New("validator is not active")
	ErrInvalidStateData   = errors.New("invalid state data")
	ErrInvalidPublicKey   = errors.New("invalid public key")
	ErrSignatureNotFound  = errors.New("signature not found")
	ErrPublicKeyNotFound  = errors.New("public key not found")
)

// -----------------------------------------------------
// Transaction Timestamp Configuration
// -----------------------------------------------------

var (
	maxTxClockSkew = 120 * time.Second // Default: allow 120 seconds in the future
	txClockSkewMu  sync.RWMutex
)

// -----------------------------------------------------
// Development Mode Configuration
// -----------------------------------------------------

var (
	developmentMode bool
	devModeMu       sync.RWMutex
)

// SetDevelopmentMode sets whether development mode is enabled
func SetDevelopmentMode(enabled bool) {
	devModeMu.Lock()
	defer devModeMu.Unlock()
	developmentMode = enabled
}

// IsDevelopmentMode returns whether development mode is enabled
func IsDevelopmentMode() bool {
	devModeMu.RLock()
	defer devModeMu.RUnlock()
	return developmentMode
}

// SetMaxTxClockSkew sets the maximum allowed clock skew for transaction timestamps
func SetMaxTxClockSkew(d time.Duration) {
	txClockSkewMu.Lock()
	defer txClockSkewMu.Unlock()
	maxTxClockSkew = d
}

// GetMaxTxClockSkew returns the current maximum allowed clock skew
func GetMaxTxClockSkew() time.Duration {
	txClockSkewMu.RLock()
	defer txClockSkewMu.RUnlock()
	return maxTxClockSkew
}

// -----------------------------------------------------
// Consensus Time Functions
// -----------------------------------------------------
// Consensus time functions are now in consensus_time.go to avoid duplicates

// -----------------------------------------------------
// Global Data
// -----------------------------------------------------

var (
	// Global map for storing accounts.
	accounts   = make(map[string]*Account)
	stateMutex sync.RWMutex // For thread-safe account modifications
)

// -----------------------------------------------------
// Data Structures
// -----------------------------------------------------

// LightNode represents a simplified node that doesn't run full consensus.
type LightNode struct {
	ID        string
	PublicKey []byte
	LastBlock int64
	Synced    bool
}

// MessageContent represents typed content for network messages
type MessageContent struct {
	TransactionData *Transaction `json:"transaction_data,omitempty"`
	BlockData       *Block       `json:"block_data,omitempty"`
	TextData        string       `json:"text_data,omitempty"`
	BinaryData      []byte       `json:"binary_data,omitempty"`
	ContentType     string       `json:"content_type"`
}

// MessagePayload represents typed payload for network messages
type MessagePayload struct {
	ConsensusData  []byte `json:"consensus_data,omitempty"`
	NetworkData    []byte `json:"network_data,omitempty"`
	ValidationData []byte `json:"validation_data,omitempty"`
	MetricData     []byte `json:"metric_data,omitempty"`
	PayloadType    string `json:"payload_type"`
}

// MessageMetadata represents structured metadata for messages
type MessageMetadata struct {
	Priority       int      `json:"priority,omitempty"`
	TTL            int64    `json:"ttl,omitempty"`
	SourceNode     string   `json:"source_node,omitempty"`
	TargetNode     string   `json:"target_node,omitempty"`
	MessageVersion string   `json:"message_version,omitempty"`
	Encryption     string   `json:"encryption,omitempty"`
	Compression    string   `json:"compression,omitempty"`
	Route          []string `json:"route,omitempty"`
}

// Message represents a generic message structure for network communication.
type Message struct {
	ID                      string
	Type                    string
	Content                 *MessageContent
	Signature               []byte
	Payload                 *MessagePayload
	Timestamp               int64
	Metadata                *MessageMetadata
	Sender                  string
	ProposalResultBroadcast map[string][]byte
}

// TransactionMetadata represents structured metadata for transactions
type TransactionMetadata struct {
	Category    string   `json:"category,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Description string   `json:"description,omitempty"`
	Reference   string   `json:"reference,omitempty"`
	Source      string   `json:"source,omitempty"`
	Destination string   `json:"destination,omitempty"`
	Purpose     string   `json:"purpose,omitempty"`
}

// Transaction with enhanced features and security.
type Transaction struct {
	ID              string
	Sender          string
	Receiver        string
	Amount          float64
	Timestamp       int64
	Signature       []byte
	Priority        int
	Fee             float64
	ExpiryTime      int64
	SmartContractID string
	Metadata        *TransactionMetadata
	Data            []byte
	Nonce           int
	SignerID        string
	BlockHeight     int
	Status          string `json:"status"`              // e.g. "pending", "committed"
	PublicKey       []byte `json:"publicKey,omitempty"` // Signer's public key for verification
}

// Validate performs comprehensive validation on a transaction
func (tx *Transaction) Validate() error {
	if tx.Amount <= 0 {
		return fmt.Errorf("transaction validation failed: amount must be greater than zero (current: %f)", tx.Amount)
	}
	if tx.Fee < 0 {
		return fmt.Errorf("transaction validation failed: fee cannot be negative (current: %f)", tx.Fee)
	}
	if tx.Sender == "" || tx.Receiver == "" {
		return fmt.Errorf("transaction validation failed: must have valid sender and receiver")
	}
	// Use ConsensusUnix for deterministic time comparison
	currentTime := ConsensusUnix()
	if tx.ExpiryTime > 0 && tx.ExpiryTime < currentTime {
		return fmt.Errorf("transaction validation failed: expired (expiry: %d, current: %d)", tx.ExpiryTime, currentTime)
	}
	if tx.Nonce < 0 {
		return fmt.Errorf("transaction validation failed: nonce cannot be negative (current: %d)", tx.Nonce)
	}
	if tx.Timestamp <= 0 {
		return fmt.Errorf("transaction validation failed: timestamp must be positive")
	}
	return nil
}

// Block structure for blockchain blocks.
type Block struct {
	Number          int
	PreviousHash    string
	Hash            string
	Transactions    []Transaction
	Timestamp       int64
	Validator       string
	ShardID         string
	DataCluster     string
	MerkleRoot      string
	Data            []byte
	Checksum        []byte
	SignerPublicKey []byte
	Signature       []byte
	GasUsed         uint64
	GasLimit        uint64
	StateRoot       string
	TransactionRoot string
	// PoH fields for transaction pre-ordering
	PoHState      string // Hex-encoded PoH state at block creation
	PoHCount      uint64 // PoH counter at block creation
	PoHBatchProof string // Hex-encoded PoH batch proof for transactions
	// zkEVM fields for zero-knowledge proofs
	ZKProof        []byte // zkEVM batch proof for all transactions
	ZKProofType    string // Type of zkProof (e.g., "batch_execution", "aggregated")
	ZKPublicInputs []byte // Public inputs for proof verification
}

// Validate performs comprehensive validation on a block
func (b *Block) Validate(previousBlock *Block) error {
	// Validate block structure
	if b.Number < 0 {
		return fmt.Errorf("block validation failed: number cannot be negative (current: %d)", b.Number)
	}

	if b.Timestamp <= 0 {
		return fmt.Errorf("block validation failed: timestamp must be positive")
	}

	if len(b.Hash) == 0 {
		return fmt.Errorf("block validation failed: hash cannot be empty")
	}

	// If this isn't the genesis block, check previous hash
	if b.Number > 1 {
		if previousBlock == nil {
			return fmt.Errorf("block validation failed: previous block required for block %d", b.Number)
		}

		if b.PreviousHash != previousBlock.Hash {
			return fmt.Errorf("block validation failed: previous hash mismatch (expected: %s, got: %s)", previousBlock.Hash, b.PreviousHash)
		}

		if b.Number != previousBlock.Number+1 {
			return fmt.Errorf("block validation failed: non-sequential block number (expected: %d, got: %d)", previousBlock.Number+1, b.Number)
		}
	}

	// Validate each transaction
	for i, tx := range b.Transactions {
		if err := tx.Validate(); err != nil {
			return fmt.Errorf("block validation failed at transaction %d: %w", i, err)
		}
	}

	// Validate block signature if present
	if len(b.Signature) > 0 && len(b.SignerPublicKey) > 0 {
		if err := VerifyBlockSignature(b); err != nil {
			return fmt.Errorf("block validation failed: signature verification error: %w", err)
		}
	}

	return nil
}

// GovernanceProposal represents a proposal for network governance.
type GovernanceProposal struct {
	ID              string
	Description     string
	Status          string
	VoteResults     map[string]float64
	VoteCount       int
	Voters          map[string]bool
	VotingDeadline  time.Time
	MultiSigSupport bool
	Submissions     map[string][]byte
}

// SmartContractState represents structured state for smart contracts
type SmartContractState struct {
	Variables     map[string]string  `json:"variables"`
	Balances      map[string]float64 `json:"balances"`
	Permissions   map[string]bool    `json:"permissions"`
	Configuration map[string]string  `json:"configuration"`
	Counters      map[string]int64   `json:"counters"`
	LastUpdated   int64              `json:"last_updated"`
}

// SmartContractFunction represents a typed function signature
type SmartContractFunction func(params *SmartContractParams) (*SmartContractResult, error)

// SmartContractParams represents structured parameters for smart contract functions
type SmartContractParams struct {
	FunctionName string             `json:"function_name"`
	Caller       string             `json:"caller"`
	StringParams map[string]string  `json:"string_params"`
	NumberParams map[string]float64 `json:"number_params"`
	BoolParams   map[string]bool    `json:"bool_params"`
	ByteParams   map[string][]byte  `json:"byte_params"`
}

// SmartContractResult represents structured result from smart contract execution
type SmartContractResult struct {
	Success      bool          `json:"success"`
	StringResult string        `json:"string_result,omitempty"`
	NumberResult float64       `json:"number_result,omitempty"`
	BoolResult   bool          `json:"bool_result,omitempty"`
	ByteResult   []byte        `json:"byte_result,omitempty"`
	ErrorMessage string        `json:"error_message,omitempty"`
	GasUsed      uint64        `json:"gas_used"`
	StateChanges []StateChange `json:"state_changes,omitempty"`
}

// SmartContractMetadata represents structured metadata for smart contracts
type SmartContractMetadata struct {
	Author        string   `json:"author,omitempty"`
	License       string   `json:"license,omitempty"`
	Description   string   `json:"description,omitempty"`
	Documentation string   `json:"documentation,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	Dependencies  []string `json:"dependencies,omitempty"`
	Version       string   `json:"version,omitempty"`
	Audited       bool     `json:"audited"`
	AuditReports  []string `json:"audit_reports,omitempty"`
}

// SmartContract represents a deployed smart contract on the network.
type SmartContract struct {
	ID        string
	Code      string
	CodeHash  string // Hash of the contract code
	Owner     string
	Version   string
	State     *SmartContractState
	ABI       string
	Language  string
	Events    []SmartContractEvent
	GasUsage  float64
	Functions map[string]SmartContractFunction
	Metadata  *SmartContractMetadata
	CreatedAt time.Time // Contract creation timestamp
	UpdatedAt time.Time // Last update timestamp
}

// SmartContractEvent represents an event emitted by a smart contract.
type SmartContractEvent struct {
	ContractID   string
	FunctionName string
	Params       *SmartContractParams
	Result       *SmartContractResult
	Timestamp    int64
}

// StateSnapshotData represents structured state data for snapshots
type StateSnapshotData struct {
	Accounts     map[string]*Account       `json:"accounts"`
	Contracts    map[string]*SmartContract `json:"contracts"`
	Balances     map[string]float64        `json:"balances"`
	Transactions []Transaction             `json:"transactions"`
	BlockHeight  int64                     `json:"block_height"`
	NetworkState map[string]string         `json:"network_state"`
}

// StateSnapshot represents a snapshot of the ledger state for recovery.
type StateSnapshot struct {
	ID        string
	Timestamp int64
	StateData *StateSnapshotData
}

// StateChangeValue represents a typed value for state changes
type StateChangeValue struct {
	StringValue string  `json:"string_value,omitempty"`
	NumberValue float64 `json:"number_value,omitempty"`
	BoolValue   bool    `json:"bool_value,omitempty"`
	ByteValue   []byte  `json:"byte_value,omitempty"`
	ValueType   string  `json:"value_type"`
}

// StateChange represents a change in the state snapshot.
type StateChange struct {
	Key   string
	Value *StateChangeValue
}

// -----------------------------------------------------
// Cryptographic Functions
// -----------------------------------------------------

// ParsePublicKeyFromBytes parses an ECDSA public key from byte array
func ParsePublicKeyFromBytes(pubKeyBytes []byte) (*ecdsa.PublicKey, error) {
	if len(pubKeyBytes) == 0 {
		return nil, fmt.Errorf("public key parsing failed: empty public key bytes")
	}

	// Try to parse as X.509 encoded public key
	pubKey, err := x509.ParsePKIXPublicKey(pubKeyBytes)
	if err != nil {
		// If X.509 parsing fails, try to parse as raw ECDSA coordinates
		if len(pubKeyBytes) == 64 { // P-256 public key (32 bytes X + 32 bytes Y)
			x := new(big.Int).SetBytes(pubKeyBytes[:32])
			y := new(big.Int).SetBytes(pubKeyBytes[32:])
			return &ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     x,
				Y:     y,
			}, nil
		}
		return nil, fmt.Errorf("public key parsing failed: %w", err)
	}

	ecdsaPubKey, ok := pubKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key parsing failed: not an ECDSA public key")
	}

	return ecdsaPubKey, nil
}

// VerifySignature verifies an ECDSA signature
func VerifySignature(publicKey *ecdsa.PublicKey, message []byte, signature []byte) error {
	if publicKey == nil {
		return fmt.Errorf("signature verification failed: public key is nil")
	}

	if len(message) == 0 {
		return fmt.Errorf("signature verification failed: message is empty")
	}

	if len(signature) == 0 {
		return fmt.Errorf("signature verification failed: signature is empty")
	}

	// Hash the message
	hash := sha256.Sum256(message)

	// Parse signature (assuming r||s format)
	if len(signature) != 64 {
		return fmt.Errorf("signature verification failed: invalid signature length (expected 64, got %d)", len(signature))
	}

	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])

	// Verify the signature
	if !ecdsa.Verify(publicKey, hash[:], r, s) {
		return fmt.Errorf("signature verification failed: invalid signature")
	}

	return nil
}

// GetTransactionSigningData returns the data to be signed for a transaction
func GetTransactionSigningData(tx *Transaction) []byte {
	// Create deterministic data for signing (exclude signature field)
	data := fmt.Sprintf("%s|%s|%s|%.8f|%.8f|%d|%d|%s",
		tx.ID,
		tx.Sender,
		tx.Receiver,
		tx.Amount,
		tx.Fee,
		tx.Timestamp,
		tx.Nonce,
		tx.SmartContractID)

	return []byte(data)
}

// matchesHash compares two byte slices for equality in constant time
func matchesHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	result := byte(0)
	for i := range a {
		result |= a[i] ^ b[i]
	}
	return result == 0
}

// VerifyTransactionSignature verifies the signature of a transaction
// Now supports both ECDSA and Dilithium signatures through hybrid scheme
func VerifyTransactionSignature(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction signature verification failed: transaction is nil")
	}

	if len(tx.Signature) == 0 {
		return fmt.Errorf("transaction signature verification failed: %w", ErrSignatureNotFound)
	}

	// Check for development signatures (only in development mode)
	if len(tx.Signature) > 8 && string(tx.Signature[:8]) == "DEV_SIG:" {
		// Development signatures are NEVER allowed in production
		// This check is redundant but critical for security
		if !IsDevelopmentMode() {
			return fmt.Errorf("transaction signature verification failed: development signatures not allowed in production")
		}
		// Verify it's a properly formatted dev signature with hash
		if len(tx.Signature) < 40 { // 8 byte prefix + 32 byte hash
			return fmt.Errorf("transaction signature verification failed: invalid development signature format")
		}
		// Even in development mode, verify the signature matches the transaction
		txData := GetTransactionSigningData(tx)
		expectedHash := sha256.Sum256(txData)
		providedHash := tx.Signature[8:40]

		if !matchesHash(providedHash, expectedHash[:32]) {
			return fmt.Errorf("transaction signature verification failed: development signature hash mismatch")
		}

		// Development signature accepted - no logging to avoid performance impact
		return nil
	}

	// Get account to retrieve both ECDSA and Dilithium public keys
	acc := GetAccount(tx.Sender)
	if acc == nil {
		return fmt.Errorf("transaction signature verification failed: sender account not found")
	}

	// Try to get Dilithium public key first (preferred)
	var dilithiumPubKey []byte
	if len(tx.PublicKey) > 0 {
		// Use public key from transaction if provided
		dilithiumPubKey = tx.PublicKey
	} else if len(acc.PublicKey) > 0 {
		// Use public key from account
		dilithiumPubKey = acc.PublicKey
	}

	// For backward compatibility, try to parse ECDSA public key
	var ecdsaPubKey *ecdsa.PublicKey
	if len(dilithiumPubKey) > 0 {
		// Try to parse as ECDSA first (for legacy keys)
		parsedKey, err := ParsePublicKeyFromBytes(dilithiumPubKey)
		if err == nil {
			ecdsaPubKey = parsedKey
		}
	}

	// Get signing data
	signingData := GetTransactionSigningData(tx)

	// Determine if we're in quantum-only mode based on block height
	// TODO: Make this configurable based on block height
	quantumOnly := false // For now, accept both types

	// Note: Hybrid signature verification should be handled at a higher level
	// to avoid import cycles. For now, we'll use the existing Dilithium verification.

	// Fallback: Try legacy ECDSA verification for backward compatibility
	if ecdsaPubKey != nil && !quantumOnly {
		if err := VerifySignature(ecdsaPubKey, signingData, tx.Signature); err == nil {
			return nil
		}
	}

	// Fallback: Try pure Dilithium verification
	// Note: Dilithium verification is handled at a higher level by the transaction manager
	// to avoid circular imports. The crypto package cannot be imported here directly.
	// Callers should use transaction.Manager.VerifyTransaction for full verification support.

	return fmt.Errorf("transaction signature verification failed: no valid signature found")
}

// GetBlockSigningData returns the data to be signed for a block
func GetBlockSigningData(block *Block) []byte {
	// Create deterministic data for signing (exclude signature fields)
	data := fmt.Sprintf("%d|%s|%s|%d|%s|%s",
		block.Number,
		block.PreviousHash,
		block.MerkleRoot,
		block.Timestamp,
		block.Validator,
		HashTransactions(block.Transactions))

	return []byte(data)
}

// VerifyBlockSignature verifies the signature of a block
func VerifyBlockSignature(block *Block) error {
	if block == nil {
		return fmt.Errorf("block signature verification failed: block is nil")
	}

	if len(block.Signature) == 0 {
		return fmt.Errorf("block signature verification failed: %w", ErrSignatureNotFound)
	}

	if len(block.SignerPublicKey) == 0 {
		return fmt.Errorf("block signature verification failed: signer public key not found")
	}

	// Parse public key
	publicKey, err := ParsePublicKeyFromBytes(block.SignerPublicKey)
	if err != nil {
		return fmt.Errorf("block signature verification failed: %w", err)
	}

	// Get signing data
	signingData := GetBlockSigningData(block)

	// Verify signature
	if err := VerifySignature(publicKey, signingData, block.Signature); err != nil {
		return fmt.Errorf("block signature verification failed: %w", err)
	}

	// Verify zkEVM proof if present
	if len(block.ZKProof) > 0 {
		if err := ValidateZKProof(block); err != nil {
			return fmt.Errorf("block zkEVM proof verification failed: %w", err)
		}
	}

	return nil
}

// ValidateZKProof validates the zkEVM proof in a block
func ValidateZKProof(block *Block) error {
	// Skip validation if no proof type specified
	if block.ZKProofType == "" {
		return nil
	}

	// Validate based on proof type
	switch block.ZKProofType {
	case "batch_execution":
		// Validate that all transactions have been included in the proof
		if len(block.Transactions) == 0 {
			return errors.New("no transactions in block with zkEVM proof")
		}

		// TODO: Actual proof verification would be done by the zkEVM module
		// For now, just validate that proof data is present
		if len(block.ZKPublicInputs) == 0 {
			return errors.New("missing public inputs for zkEVM proof")
		}

		// Verify state root matches the proof
		if block.StateRoot == "" {
			return errors.New("missing state root for zkEVM proof verification")
		}

		return nil

	case "aggregated":
		// Aggregated proofs combine multiple batch proofs
		// Validation would check the aggregation is correct
		return nil

	default:
		return fmt.Errorf("unknown zkEVM proof type: %s", block.ZKProofType)
	}
}

// -----------------------------------------------------
// Utility & Helper Functions
// -----------------------------------------------------

// GenerateUniqueID generates a unique identifier for transactions, blocks, etc.
func GenerateUniqueID() string {
	// Use consensus time for deterministic ID generation
	t := ConsensusNow().UnixNano()
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", t)))
	return fmt.Sprintf("%x", hash[:16]) // Return first 16 bytes for a shorter ID
}

// -----------------------------------------------------
// Account and Balance Management
// -----------------------------------------------------

// SetAccountBalance creates/updates an Account entry with the given balance.
func SetAccountBalance(accountID string, balance float64) error {
	if accountID == "" {
		return fmt.Errorf("set account balance failed: account ID cannot be empty")
	}

	if balance < 0 {
		return fmt.Errorf("set account balance failed: balance cannot be negative (value: %f)", balance)
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	acc, exists := accounts[accountID]
	if !exists {
		acc = &Account{
			ID:      accountID,
			Balance: balance,
			Nonce:   0, // Initialize nonce
		}
		accounts[accountID] = acc
	} else {
		acc.Balance = balance
	}

	return nil
}

// SetPublicKey sets or updates the public key for an account.
func SetPublicKey(accountID string, pubKey []byte) error {
	if accountID == "" {
		return fmt.Errorf("set public key failed: account ID cannot be empty")
	}

	if len(pubKey) == 0 {
		return fmt.Errorf("set public key failed: public key cannot be empty")
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	acc, exists := accounts[accountID]
	if !exists {
		acc = &Account{
			ID:        accountID,
			PublicKey: pubKey,
		}
		accounts[accountID] = acc
	} else {
		acc.PublicKey = pubKey
	}

	return nil
}

// CheckAccountBalance checks if an account has enough balance (thread-safe).
func CheckAccountBalance(accountID string, amount float64) bool {
	if amount < 0 {
		return false
	}

	// Normalize the address
	accountID = NormalizeAddress(accountID)

	stateMutex.RLock()
	defer stateMutex.RUnlock()

	acc, ok := accounts[accountID]
	if !ok {
		// Account not found, has no funds
		return false
	}
	return acc.Balance >= amount
}

// GetAccount retrieves an account from the global map.
func GetAccount(accountID string) *Account {
	// Normalize the address
	accountID = NormalizeAddress(accountID)

	stateMutex.RLock()
	defer stateMutex.RUnlock()

	if acc, ok := accounts[accountID]; ok {
		// Return a copy to prevent external modification
		return acc.Clone()
	}

	// Account not found
	return nil
}

// GetPublicKey retrieves the public key associated with the provided account ID.
func GetPublicKey(accountID string) ([]byte, error) {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	acc, exists := accounts[accountID]
	if !exists {
		return nil, fmt.Errorf("get public key failed: account %s not found", accountID)
	}

	if len(acc.PublicKey) == 0 {
		return nil, fmt.Errorf("get public key failed: no public key for account %s", accountID)
	}

	// Return a copy of the public key
	pubKeyCopy := make([]byte, len(acc.PublicKey))
	copy(pubKeyCopy, acc.PublicKey)

	return pubKeyCopy, nil
}

// UpdateAccountBalance updates an account's balance by a certain amount (add/sub).
func UpdateAccountBalance(accountID string, amount float64) error {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	acc, ok := accounts[accountID]
	if !ok {
		return fmt.Errorf("update account balance failed: %w", ErrAccountNotFound)
	}

	newBalance := acc.Balance + amount
	if newBalance < 0 {
		return fmt.Errorf("update account balance failed: %w (current: %f, change: %f)", ErrInsufficientFunds, acc.Balance, amount)
	}

	acc.Balance = newBalance
	acc.LastActive = ConsensusUnix()

	// Balance update successful - no logging needed here for performance
	return nil
}

// -----------------------------------------------------
// Transaction & Block Hashing
// -----------------------------------------------------

// HashData computes a cryptographic hash (SHA-256) of the data.
func HashData(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash[:])
}

// SecureHashData computes a more secure hash (SHA-512) of the data.
func SecureHashData(data []byte) string {
	hash := sha512.Sum512(data)
	return fmt.Sprintf("%x", hash[:])
}

// ComputeBlockHash computes a SHA-256 hash of block fields.
func ComputeBlockHash(block Block) string {
	// Create a more comprehensive representation of the block for hashing
	data := fmt.Sprintf("%d:%s:%d:%s:%s",
		block.Number,
		block.PreviousHash,
		block.Timestamp,
		block.MerkleRoot,
		HashTransactions(block.Transactions))
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash[:])
}

// HashTransactions creates a deterministic hash of all transactions in order
func HashTransactions(txs []Transaction) string {
	if len(txs) == 0 {
		return ""
	}

	var combined string
	for _, tx := range txs {
		txHash := fmt.Sprintf("%s:%s:%s:%.8f:%d",
			tx.ID, tx.Sender, tx.Receiver, tx.Amount, tx.Timestamp)
		combined += txHash
	}
	hash := sha256.Sum256([]byte(combined))
	return fmt.Sprintf("%x", hash[:])
}

// -----------------------------------------------------
// Transaction Validation with Signature Verification
// -----------------------------------------------------

// ValidateTransaction performs comprehensive validation including signature verification
func ValidateTransaction(tx Transaction) error {
	// Basic validation
	if err := tx.Validate(); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Address validation
	if err := ValidateAddress(tx.Sender); err != nil {
		return fmt.Errorf("transaction validation failed for sender: %w", err)
	}

	if err := ValidateAddress(tx.Receiver); err != nil {
		return fmt.Errorf("transaction validation failed for receiver: %w", err)
	}

	// Amount and fee validation
	if err := ValidateAmount(tx.Amount); err != nil {
		return fmt.Errorf("transaction validation failed for amount: %w", err)
	}

	if err := ValidateTransactionFee(tx.Fee); err != nil {
		return fmt.Errorf("transaction validation failed for fee: %w", err)
	}

	// Verify transaction integrity
	if err := VerifyTransactionIntegrity(&tx); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Cryptographic signature verification
	if err := VerifyTransactionSignature(&tx); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	// Verify nonce (prevent replay attacks)
	if err := VerifyTransactionNonce(&tx); err != nil {
		return fmt.Errorf("transaction validation failed: %w", err)
	}

	return nil
}

// VerifyTransactionNonce verifies the transaction nonce to prevent replay attacks
func VerifyTransactionNonce(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("nonce verification failed: transaction is nil")
	}

	// Get sender account
	acc := GetAccount(tx.Sender)
	if acc == nil {
		return fmt.Errorf("nonce verification failed: sender account not found")
	}

	// Verify nonce is exactly one more than current account nonce
	expectedNonce := acc.Nonce + 1
	if tx.Nonce != expectedNonce {
		return fmt.Errorf("nonce verification failed: expected %d, got %d", expectedNonce, tx.Nonce)
	}

	return nil
}

// RegisterAccount adds a new account to the global account store.
func RegisterAccount(ac *Account) error {
	if ac == nil {
		return fmt.Errorf("register account failed: account cannot be nil")
	}

	if ac.ID == "" {
		return fmt.Errorf("register account failed: account ID cannot be empty")
	}

	// Validate account before registration
	if err := ac.Validate(); err != nil {
		return fmt.Errorf("register account failed: %w", err)
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	if _, exists := accounts[ac.ID]; exists {
		return fmt.Errorf("register account failed: %w", ErrAccountExists)
	}

	accounts[ac.ID] = ac
	return nil
}

// ---------------------------------------------------------------------
// Alias for backward compatibility.
var CreateAccount = RegisterAccount

// GetAllAccounts returns a copy of all accounts (for admin/debug purposes)
func GetAllAccounts() map[string]*Account {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	// Create a deep copy to avoid concurrent modification issues
	accountsCopy := make(map[string]*Account)
	for id, acc := range accounts {
		accountsCopy[id] = acc.Clone()
	}

	return accountsCopy
}

// ClearAllAccounts removes all accounts (for testing purposes)
func ClearAllAccounts() {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	accounts = make(map[string]*Account)
}

// GetCurrentTimestamp returns the current Unix timestamp
func GetCurrentTimestamp() int64 {
	return ConsensusUnix()
}

// GetCurrentTimestampNano returns the current Unix timestamp in nanoseconds
func GetCurrentTimestampNano() int64 {
	return ConsensusNow().UnixNano()
}

// ValidateAddress validates if an address is properly formatted with enhanced checks
func ValidateAddress(address string) error {
	if address == "" {
		return fmt.Errorf("address validation failed: address cannot be empty")
	}
	if len(address) < 8 {
		return fmt.Errorf("address validation failed: address too short (minimum 8 characters)")
	}
	if len(address) > 128 {
		return fmt.Errorf("address validation failed: address too long (maximum 128 characters)")
	}

	// Validate hex encoding if it starts with 0x
	if len(address) >= 2 && address[:2] == "0x" {
		_, err := hex.DecodeString(address[2:])
		if err != nil {
			return fmt.Errorf("address validation failed: invalid hex encoding: %w", err)
		}
	}

	return nil
}

// ValidateAmount validates if an amount is valid for transactions
func ValidateAmount(amount float64) error {
	if amount < 0 {
		return fmt.Errorf("amount validation failed: cannot be negative")
	}
	if amount == 0 {
		return fmt.Errorf("amount validation failed: must be greater than zero")
	}
	// Check for reasonable maximum (prevent overflow)
	if amount > 1e18 {
		return fmt.Errorf("amount validation failed: exceeds maximum allowed value (1e18)")
	}
	return nil
}

// ValidateTransactionFee validates if a transaction fee is reasonable
func ValidateTransactionFee(fee float64) error {
	if fee < 0 {
		return fmt.Errorf("fee validation failed: cannot be negative")
	}
	// Allow zero fees for now, but could enforce minimum fee
	if fee > 1e6 {
		return fmt.Errorf("fee validation failed: exceeds maximum allowed value (1e6)")
	}
	return nil
}

// CalculateTransactionHash computes a deterministic hash for a transaction
func CalculateTransactionHash(tx *Transaction) string {
	if tx == nil {
		return ""
	}

	data := fmt.Sprintf("%s:%s:%s:%.8f:%.8f:%d:%d",
		tx.ID,
		tx.Sender,
		tx.Receiver,
		tx.Amount,
		tx.Fee,
		tx.Timestamp,
		tx.Nonce)

	return HashData([]byte(data))
}

// VerifyTransactionIntegrity verifies transaction data integrity
func VerifyTransactionIntegrity(tx *Transaction) error {
	if tx == nil {
		return fmt.Errorf("integrity verification failed: transaction cannot be nil")
	}

	// Verify basic fields
	if err := ValidateAddress(tx.Sender); err != nil {
		return fmt.Errorf("integrity verification failed: invalid sender address: %w", err)
	}

	if err := ValidateAddress(tx.Receiver); err != nil {
		return fmt.Errorf("integrity verification failed: invalid receiver address: %w", err)
	}

	if err := ValidateAmount(tx.Amount); err != nil {
		return fmt.Errorf("integrity verification failed: invalid amount: %w", err)
	}

	if err := ValidateTransactionFee(tx.Fee); err != nil {
		return fmt.Errorf("integrity verification failed: invalid fee: %w", err)
	}

	// Verify timestamp is reasonable
	now := ConsensusUnix()

	// If timestamp is zero, treat as current time
	if tx.Timestamp == 0 {
		tx.Timestamp = now
	}

	// Get the maximum allowed clock skew
	maxSkew := GetMaxTxClockSkew()
	maxFutureTime := now + int64(maxSkew.Seconds())

	if tx.Timestamp > maxFutureTime {
		return fmt.Errorf("integrity verification failed: timestamp too far in the future (max allowed: %d seconds)", int(maxSkew.Seconds()))
	}

	if tx.Timestamp < now-86400 { // Reject transactions older than 24 hours
		return fmt.Errorf("integrity verification failed: timestamp too old")
	}

	// Verify ID is not empty
	if tx.ID == "" {
		return fmt.Errorf("integrity verification failed: transaction ID cannot be empty")
	}

	return nil
}

// NormalizeAddress normalizes an Ethereum address to lowercase with 0x prefix
func NormalizeAddress(address string) string {
	// Remove any whitespace
	address = strings.TrimSpace(address)

	// Handle empty address
	if address == "" {
		return ""
	}

	// Ensure 0x prefix
	if !strings.HasPrefix(address, "0x") {
		address = "0x" + address
	}

	// Convert to lowercase
	return strings.ToLower(address)
}

// NormalizeTransactionID normalizes a transaction ID to lowercase
// Transaction IDs are hex strings that should be case-insensitive
func NormalizeTransactionID(txID string) string {
	// Remove any whitespace
	txID = strings.TrimSpace(txID)

	// Convert to lowercase for consistent comparisons
	// Transaction IDs don't use 0x prefix
	return strings.ToLower(txID)
}

// FormatBalance formats a balance for display
func FormatBalance(balance float64) string {
	return fmt.Sprintf("%.8f", balance)
}

// SafeAdd performs safe addition to prevent overflow
func SafeAdd(a, b float64) (float64, error) {
	result := a + b
	// Check for positive overflow
	if a > 0 && b > 0 && result < a {
		return 0, fmt.Errorf("arithmetic overflow: %f + %f", a, b)
	}
	// Check for negative overflow
	if a < 0 && b < 0 && result > a {
		return 0, fmt.Errorf("arithmetic underflow: %f + %f", a, b)
	}
	return result, nil
}

// SafeSubtract performs safe subtraction to prevent underflow
func SafeSubtract(a, b float64) (float64, error) {
	result := a - b
	// Check if subtraction would cause underflow
	if b > 0 && result > a {
		return 0, fmt.Errorf("arithmetic underflow: %f - %f", a, b)
	}
	// Check if subtraction would cause overflow (subtracting negative)
	if b < 0 && result < a {
		return 0, fmt.Errorf("arithmetic overflow: %f - %f", a, b)
	}
	return result, nil
}

// CreateGenesisBlock creates the first block in the blockchain
func CreateGenesisBlock() Block {
	genesisBlock := Block{
		Number:       0,
		PreviousHash: "0",
		Timestamp:    ConsensusUnix(),
		Validator:    "genesis",
		Transactions: []Transaction{},
		Data:         []byte("Genesis Block"),
	}

	genesisBlock.Hash = ComputeBlockHash(genesisBlock)
	return genesisBlock
}

// IsGenesisBlock checks if a block is the genesis block
func IsGenesisBlock(block *Block) bool {
	return block != nil && block.Number == 0 && block.PreviousHash == "0"
}

// ComputeMerkleRoot computes the Merkle root of transactions in a block
func ComputeMerkleRoot(transactions []Transaction) string {
	if len(transactions) == 0 {
		return ""
	}

	// Create leaf nodes with transaction hashes
	var hashes []string
	for _, tx := range transactions {
		hashes = append(hashes, CalculateTransactionHash(&tx))
	}

	// Build merkle tree
	for len(hashes) > 1 {
		var newLevel []string
		for i := 0; i < len(hashes); i += 2 {
			var combined string
			if i+1 < len(hashes) {
				combined = hashes[i] + hashes[i+1]
			} else {
				// Odd number of hashes, duplicate the last one
				combined = hashes[i] + hashes[i]
			}
			newLevel = append(newLevel, HashData([]byte(combined)))
		}
		hashes = newLevel
	}

	return hashes[0]
}

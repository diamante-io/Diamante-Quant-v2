package common

import (
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"log"
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
)

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

// Message represents a generic message structure for network communication.
type Message struct {
	ID                      string
	Type                    string
	Content                 interface{} // e.g., transaction data, block data
	Signature               []byte
	Payload                 interface{}
	Timestamp               int64
	Metadata                map[string]interface{}
	Sender                  string
	ProposalResultBroadcast map[string][]byte
}

// Transaction with enhanced features.
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
	Metadata        map[string]interface{}
	Data            []byte
	Nonce           int
	SignerID        string
	BlockHeight     int
	Status          string `json:"status"` // e.g. "pending", "committed"
}

// Validate performs basic sanity checks on a transaction
func (tx *Transaction) Validate() error {
	if tx.Amount <= 0 {
		return errors.New("transaction amount must be greater than zero")
	}
	if tx.Fee < 0 {
		return errors.New("transaction fee cannot be negative")
	}
	if tx.Sender == "" || tx.Receiver == "" {
		return errors.New("transaction must have a valid sender and receiver")
	}
	if tx.ExpiryTime > 0 && tx.ExpiryTime < time.Now().Unix() {
		return errors.New("transaction has expired")
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
}

// Validate performs basic validation on a block
func (b *Block) Validate(previousBlock *Block) error {
	// Validate block structure
	if b.Number <= 0 {
		return errors.New("block number must be positive")
	}

	if b.Timestamp <= 0 {
		return errors.New("block timestamp must be positive")
	}

	if len(b.Hash) == 0 {
		return errors.New("block hash cannot be empty")
	}

	// If this isn't the genesis block, check previous hash
	if b.Number > 1 {
		if previousBlock == nil {
			return errors.New("previous block required for validation")
		}

		if b.PreviousHash != previousBlock.Hash {
			return errors.New("previous hash does not match")
		}

		if b.Number != previousBlock.Number+1 {
			return errors.New("block number must be sequential")
		}
	}

	// Validate each transaction
	for _, tx := range b.Transactions {
		if err := tx.Validate(); err != nil {
			return fmt.Errorf("transaction validation failed: %w", err)
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

// SmartContract represents a deployed smart contract on the network.
type SmartContract struct {
	ID        string
	Code      string
	Owner     string
	State     map[string]interface{}
	ABI       string
	Language  string
	Events    []SmartContractEvent
	GasUsage  float64
	Functions map[string]func(map[string]interface{}) (interface{}, error)
}

// SmartContractEvent represents an event emitted by a smart contract.
type SmartContractEvent struct {
	ContractID   string
	FunctionName string
	Params       map[string]interface{}
	Result       interface{}
	Timestamp    int64
}

// StateSnapshot represents a snapshot of the ledger state for recovery.
type StateSnapshot struct {
	ID        string
	Timestamp int64
	StateData map[string]interface{}
}

// DeltaSnapshot represents incremental changes for faster recovery.
type DeltaSnapshot struct {
	ID      string
	Changes []StateChange
	BaseID  string
	Created int64
}

// StateChange represents a change in the state snapshot.
type StateChange struct {
	Key   string
	Value interface{}
}

// -----------------------------------------------------
// Utility & Helper Functions
// -----------------------------------------------------

// GenerateUniqueID generates a unique identifier for transactions, blocks, etc.
func GenerateUniqueID() string {
	t := time.Now().UnixNano()
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", t)))
	return fmt.Sprintf("%x", hash[:16]) // Return first 16 bytes for a shorter ID
}

// -----------------------------------------------------
// Account and Balance Management
// -----------------------------------------------------

// SetAccountBalance creates/updates an Account entry with the given balance.
func SetAccountBalance(accountID string, balance float64) error {
	if accountID == "" {
		return errors.New("account ID cannot be empty")
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
		return errors.New("account ID cannot be empty")
	}

	if len(pubKey) == 0 {
		return errors.New("public key cannot be empty")
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

	stateMutex.RLock()
	defer stateMutex.RUnlock()

	acc, ok := accounts[accountID]
	if !ok {
		return false
	}
	return acc.Balance >= amount
}

// GetAccount retrieves an account from the global map.
func GetAccount(accountID string) *Account {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	if acc, ok := accounts[accountID]; ok {
		return acc
	}
	return nil
}

// GetPublicKey retrieves the public key associated with the provided account ID.
func GetPublicKey(accountID string) ([]byte, error) {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	acc, exists := accounts[accountID]
	if !exists {
		return nil, fmt.Errorf("account %s not found", accountID)
	}
	return acc.PublicKey, nil
}

// UpdateAccountBalance updates an account's balance by a certain amount (add/sub).
func UpdateAccountBalance(accountID string, amount float64) error {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	acc, ok := accounts[accountID]
	if !ok {
		return ErrAccountNotFound
	}
	if acc.Balance+amount < 0 {
		return ErrInsufficientFunds
	}
	acc.Balance += amount
	log.Printf("Account %s balance updated to %.2f\n", accountID, acc.Balance)
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
// Basic Transaction Validation (Optional/Legacy)
// -----------------------------------------------------

// ValidateTransaction checks basic constraints (used by older modules).
func ValidateTransaction(tx Transaction) error {
	return tx.Validate()
}

// RegisterAccount adds a new account to the global account store.
func RegisterAccount(ac *Account) error {
	if ac == nil {
		return errors.New("account cannot be nil")
	}

	if ac.ID == "" {
		return errors.New("account ID cannot be empty")
	}

	stateMutex.Lock()
	defer stateMutex.Unlock()

	if _, exists := accounts[ac.ID]; exists {
		return ErrAccountExists
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
		// Create new account and copy fields individually to avoid copying mutex
		accountsCopy[id] = &Account{
			ID:        acc.ID,
			Balance:   acc.Balance,
			Nonce:     acc.Nonce,
			PublicKey: acc.PublicKey,
		}
	}

	return accountsCopy
}

// ClearAllAccounts removes all accounts (for testing purposes)
func ClearAllAccounts() {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	accounts = make(map[string]*Account)
}

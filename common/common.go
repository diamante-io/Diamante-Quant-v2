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
	CrossChainMessage       = "CrossChainMessage" // Example
	SyncRequest             = "SyncRequest"
	ProposalResultBroadcast = "ProposalResultBroadcast"
	// Add other message types as needed
)

// -----------------------------------------------------
// Global Data
// -----------------------------------------------------

var (
	// Global map or a ledger object that stores accounts.
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
}

// Account represents a user or entity on the blockchain network.
type Account struct {
	ID           string
	PublicKey    []byte
	PrivateKey   []byte // For demonstration only; real code must store privately
	Balance      float64
	MultiSig     bool
	HDWalletKey  string
	Role         string
	KYCStatus    bool
	VotingWeight int
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
	return fmt.Sprintf("id-%d", time.Now().UnixNano())
}

// -----------------------------------------------------
// Account and Balance Management
// -----------------------------------------------------

// SetAccountBalance creates/updates an Account entry with the given balance.
func SetAccountBalance(accountID string, balance float64) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	acc, exists := accounts[accountID]
	if !exists {
		acc = &Account{
			ID:      accountID,
			Balance: balance,
		}
		accounts[accountID] = acc
	} else {
		acc.Balance = balance
	}
}

// SetPublicKey sets or updates the public key for an account.
func SetPublicKey(accountID string, pubKey []byte) {
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
}

// CheckAccountBalance checks if an account has enough balance (thread-safe).
func CheckAccountBalance(accountID string, amount float64) bool {
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
		return errors.New("account does not exist")
	}
	if acc.Balance+amount < 0 {
		return errors.New("insufficient funds for transaction")
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

// ComputeBlockHash computes a simple SHA-256 hash of block fields.
func ComputeBlockHash(block Block) string {
	data := fmt.Sprintf("%d:%s:%d", block.Number, block.PreviousHash, block.Timestamp)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}

// -----------------------------------------------------
// Basic Transaction Validation (Optional/Legacy)
// -----------------------------------------------------

// ValidateTransaction checks some basic constraints (used by older modules).
func ValidateTransaction(tx Transaction) error {
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

// RegisterAccount adds a new account to the global account store.
func RegisterAccount(ac *Account) error {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	if _, exists := accounts[ac.ID]; exists {
		return fmt.Errorf("account %s already exists", ac.ID)
	}
	accounts[ac.ID] = ac
	return nil
}

// ---------------------------------------------------------------------
// Alias for backward compatibility.
// Some parts of our codebase may refer to common.CreateAccount.
// This alias ensures those references work without changing core logic.
var CreateAccount = RegisterAccount

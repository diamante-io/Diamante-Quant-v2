// common/interfaces.go
package common

import (
	"context"
	"time"
)

// LedgerAPI defines the methods that your transaction manager or smart contracts expect from the ledger.
type LedgerAPI interface {
	// Account Management
	CreateAccount(ac *Account) error
	UpdateAccount(ac *Account) error
	GetBalance(accountID string) (float64, error)
	UpdateAccountBalance(accountID string, amount float64) error

	// Transaction Management
	AddTransaction(tx Transaction) error
	IsTransactionCommitted(txID string) bool
	GetTransaction(txID string) (*Transaction, error)                                  // New: retrieve transaction details
	GetAccountTransactions(accountID string, limit, offset int) ([]Transaction, error) // New: account history

	// Block Management
	CommitBlock(block Block) error
	GetBlockByNumber(num int) (Block, bool)
	GetLastBlockHash() (string, error)
	GetBlockHeight() (int, error)                           // New: returns current block height
	GetBlocksByRange(startNum, endNum int) ([]Block, error) // New: retrieve range of blocks

	// Snapshot & Recovery
	CreateSnapshot(height int) error
	RestoreSnapshot(height int) error

	// Smart Contract Management
	DeploySmartContract(sc *SmartContract) error
	ExecuteSmartContract(scID, function, sender string, params map[string]interface{}) (interface{}, error)
	RemoveSmartContract(contractID string) error

	// System Management
	IntegrityCheck() error
	Close() error // New: explicitly close database connections

	// New methods for enhanced functionality
	GetStats() (map[string]interface{}, error) // System statistics
	HealthCheck(ctx context.Context) error     // Check if ledger is operational
}

// SmartContractManagerAPI defines the methods that the ledger expects from the smart contract manager.
type SmartContractManagerAPI interface {
	DeploySmartContract(contractCode, owner string) (string, error)
	ExecuteContractFunction(contractID, functionName string, params map[string]interface{}, sender string) (interface{}, error)
	TerminateSmartContract(contractID, requester string) error

	// New methods for enhanced functionality
	GetContractState(contractID string) (map[string]interface{}, error)
	GetContractEvents(contractID string, fromTime, toTime time.Time) ([]SmartContractEvent, error)
	UpdateContractCode(contractID, newCode string, requester string) error
	ValidateContract(contractCode string) error
}

// BlockchainAPI defines high-level operations on the blockchain system.
// This interface provides a unified access point for client applications.
type BlockchainAPI interface {
	// Account operations
	GetAccountBalance(accountID string) (float64, error)
	CreateNewAccount(publicKey []byte) (*Account, error)
	TransferFunds(senderID, receiverID string, amount float64, memo string) (string, error)

	// Blockchain data access
	GetLatestBlockHeight() (int, error)
	GetBlockByHeight(height int) (*Block, error)
	GetTransactionByID(txID string) (*Transaction, error)

	// Smart contract operations
	DeployContract(ownerID, contractCode, language string) (string, error)
	InvokeContract(contractID, function string, params map[string]interface{}, callerID string) (interface{}, error)

	// System operations
	GetNetworkStats() (map[string]interface{}, error)
	SubmitTransaction(tx *Transaction) error
	ValidateTransaction(tx *Transaction) error
}

// ConsensusAPI defines the interface for consensus module interactions
type ConsensusAPI interface {
	// Status reporting
	GetCurrentHeight() uint64
	GetNetworkLoad() float64

	// Node management
	AddValidator(id [32]byte, stake uint64)
	RemoveValidator(id [32]byte) error
	IsActiveValidator(id [32]byte) bool

	// Proposal management
	ProposeBlock(block *Block) error
	VoteOnBlock(blockHash string, approve bool) error

	// System control
	Start() error
	Stop() error
	Pause() error
	Resume() error
}

// StorageAPI defines the interface for persistent storage operations
type StorageAPI interface {
	// Block storage
	SaveBlock(block *Block) error
	GetBlock(blockNumber uint64) (*Block, error)

	// Transaction storage
	SaveTransaction(tx *Transaction) error
	GetTransaction(txID string) (*Transaction, error)

	// Account storage
	SaveAccount(account *Account) error
	GetAccount(accountID string) (*Account, error)

	// Smart contract storage
	SaveContract(contract *SmartContract) error
	GetContract(contractID string) (*SmartContract, error)

	// System operations
	Backup(path string) error
	Restore(path string) error
	PruneData(olderThan time.Time) error
	Vacuum() error
}

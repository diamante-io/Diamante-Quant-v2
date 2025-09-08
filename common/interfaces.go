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
	// UpdateSmartContract updates contract code to a new version
	UpdateSmartContract(contractID, newCode, version string) error
	ExecuteSmartContract(scID, function, sender string, params *SmartContractParams) (*SmartContractResult, error)
	RemoveSmartContract(contractID string) error

	// System Management
	IntegrityCheck() error
	Close() error // New: explicitly close database connections

	// New methods for enhanced functionality
	GetStats() (*LedgerStats, error)       // System statistics
	HealthCheck(ctx context.Context) error // Check if ledger is operational
}

// SmartContractManagerAPI defines the methods that the ledger expects from the smart contract manager.
type SmartContractManagerAPI interface {
	DeploySmartContract(contractCode, owner string) (string, error)
	ExecuteContractFunction(contractID, functionName string, params *SmartContractParams, sender string) (*SmartContractResult, error)
	TerminateSmartContract(contractID, requester string) error

	// New methods for enhanced functionality
	GetContractState(contractID string) (*SmartContractState, error)
	GetContractEvents(contractID string, fromTime, toTime time.Time) ([]SmartContractEvent, error)
	UpdateContractCode(contractID, newCode string, requester string) error
	ValidateContract(contractCode string) error
}

// // BlockchainAPI defines high-level operations on the blockchain system.
// // This interface provides a unified access point for client applications.
// // DEPRECATED: Use the updated BlockchainAPI interface below
// type BlockchainAPIv1 interface {
// 	// Account operations
// 	GetAccountBalance(accountID string) (float64, error)
// 	CreateNewAccount(publicKey []byte) (*Account, error)
// 	TransferFunds(senderID, receiverID string, amount float64, memo string) (string, error)

// 	// Blockchain data access
// 	GetLatestBlockHeight() (int, error)
// 	GetBlockByHeight(height int) (*Block, error)
// 	GetTransactionByID(txID string) (*Transaction, error)

// 	// Smart contract operations
// 	DeployContract(ownerID, contractCode, language string) (string, error)
// 	InvokeContract(contractID, function string, params *SmartContractParams, callerID string) (*SmartContractResult, error)

// 	// System operations
// 	GetNetworkStats() (*NetworkStats, error)
// 	SubmitTransaction(tx *Transaction) error
// 	ValidateTransaction(tx *Transaction) error
// }

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

// Ledger interface for blockchain ledger operations
type Ledger interface {
	// Basic operations
	GetBalance(accountID string) (float64, error)
	UpdateBalance(accountID string, amount float64) error
	GetAccount(accountID string) (*Account, error)
	CreateAccount(account *Account) error

	// Transaction operations
	AddTransaction(tx *Transaction) error
	GetTransaction(txID string) (*Transaction, error)
	GetTransactionsByAccount(accountID string) ([]*Transaction, error)
	ValidateTransaction(tx *Transaction) error

	// Smart contract operations
	DeploySmartContract(contract *SmartContract) error
	ExecuteSmartContract(scID, function, sender string, params *SmartContractParams) (*SmartContractResult, error)
	GetSmartContract(scID string) (*SmartContract, error)
	UpdateSmartContractState(scID string, state *SmartContractState) error

	// System operations
	GetStats() (*LedgerStats, error) // System statistics
}

// LedgerStats represents structured statistics for ledger operations
type LedgerStats struct {
	TotalAccounts     int64   `json:"total_accounts"`
	TotalTransactions int64   `json:"total_transactions"`
	TotalContracts    int64   `json:"total_contracts"`
	TotalBalance      float64 `json:"total_balance"`
	LastBlockHeight   int64   `json:"last_block_height"`
	NetworkHealth     string  `json:"network_health"`
	ProcessingTime    int64   `json:"processing_time_ms"`
}

// SmartContractManager interface for managing smart contracts
type SmartContractManager interface {
	// Contract lifecycle
	DeployContract(contract *SmartContract) error
	ExecuteContractFunction(contractID, functionName string, params *SmartContractParams, sender string) (*SmartContractResult, error)
	UpgradeContract(contractID string, newCode string, sender string) error
	DeactivateContract(contractID string, sender string) error

	// State management
	GetContractState(contractID string) (*SmartContractState, error)
	UpdateContractState(contractID string, state *SmartContractState) error

	// Query operations
	GetContract(contractID string) (*SmartContract, error)
	ListContracts(owner string) ([]*SmartContract, error)
	GetContractEvents(contractID string, limit int) ([]SmartContractEvent, error)
}

// NetworkStats represents structured statistics for network operations
type NetworkStats struct {
	ConnectedPeers   int     `json:"connected_peers"`
	MessagesSent     int64   `json:"messages_sent"`
	MessagesReceived int64   `json:"messages_received"`
	BytesSent        int64   `json:"bytes_sent"`
	BytesReceived    int64   `json:"bytes_received"`
	NetworkLatency   float64 `json:"network_latency_ms"`
	ErrorRate        float64 `json:"error_rate"`
	Uptime           int64   `json:"uptime_seconds"`
}

// BlockchainAPI interface for external API operations
type BlockchainAPI interface {
	// Account operations
	CreateAccount(accountData *Account) (*Account, error)
	GetAccountBalance(accountID string) (float64, error)
	GetAccountInfo(accountID string) (*Account, error)

	// Transaction operations
	SubmitTransaction(tx *Transaction) error
	GetTransactionStatus(txID string) (string, error)
	GetTransactionHistory(accountID string, limit int) ([]*Transaction, error)

	// Smart contract operations
	DeployContract(contract *SmartContract, deployer string) (*SmartContract, error)
	InvokeContract(contractID, function string, params *SmartContractParams, callerID string) (*SmartContractResult, error)
	GetContractInfo(contractID string) (*SmartContract, error)

	// Network operations
	GetNetworkStats() (*NetworkStats, error)
	GetBlockchainInfo() (*BlockchainInfo, error)
}

// BlockchainInfo represents comprehensive blockchain information
type BlockchainInfo struct {
	ChainID           string  `json:"chain_id"`
	NetworkVersion    string  `json:"network_version"`
	LatestBlock       int64   `json:"latest_block"`
	TotalSupply       float64 `json:"total_supply"`
	CirculatingSupply float64 `json:"circulating_supply"`
	ValidatorCount    int     `json:"validator_count"`
	ConsensusType     string  `json:"consensus_type"`
	BlockTime         int64   `json:"block_time_ms"`
	TPS               float64 `json:"transactions_per_second"`
}

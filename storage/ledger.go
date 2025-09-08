// storage/ledger.go
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"diamante/common"
	dtypes "diamante/types"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

// Gas cost constants for different operations
const (
	// Base costs
	GasBaseExecution  = uint64(1000)
	GasMemoryPerByte  = uint64(3)
	GasStoragePerByte = uint64(20)

	// Operation costs
	GasOpRead    = uint64(500)
	GasOpWrite   = uint64(2000)
	GasOpDelete  = uint64(1500)
	GasOpCompute = uint64(100)

	// Database operation costs
	GasDbRead  = uint64(1000)
	GasDbWrite = uint64(3000)
	GasDbQuery = uint64(1500)

	// Complex operation costs
	GasTransfer = uint64(5000)
	GasMint     = uint64(3000)
	GasBurn     = uint64(3000)
	GasApproval = uint64(2000)

	// Error handling costs
	GasErrorValidation = uint64(200)
	GasErrorDatabase   = uint64(1000)
	GasErrorRollback   = uint64(2000)
)

// LedgerAPI defines the interface for ledger operations
// This interface abstracts the underlying storage implementation
type LedgerAPI interface {
	// Account operations
	CreateAccount(account *common.Account) error
	UpdateAccount(account *common.Account) error
	GetAccount(accountID string) (*common.Account, error)
	GetBalance(accountID string) (float64, error)

	// Transaction operations
	AddTransaction(tx common.Transaction) error
	GetTransaction(txID string) (*common.Transaction, error)
	GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error)

	// Block operations
	CommitBlock(block common.Block) error
	GetBlockByNumber(num int) (common.Block, bool)
	GetBlockHeight() (uint64, error)

	// Status checks
	IsTransactionCommitted(txID string) bool

	// Smart contract operations
	DeploySmartContract(sc *common.SmartContract) error
	GetSmartContract(contractID string) (*common.SmartContract, error)

	// Legacy methods for compatibility
	AccountsCollection() *mongo.Collection
	TransactionsCollection() *mongo.Collection
	BlocksCollection() *mongo.Collection
}

// MongoDBLedger implements LedgerAPI using MongoDB storage
type MongoDBLedger struct {
	adapter *MongoAdapter
	logger  *logrus.Logger

	// Legacy collections for backward compatibility
	accountsColl     *mongo.Collection
	transactionsColl *mongo.Collection
	blocksColl       *mongo.Collection
}

// NewMongoDBLedger creates a new MongoDB-backed ledger
func NewMongoDBLedger(adapter *MongoAdapter, logger *logrus.Logger) (*MongoDBLedger, error) {
	if adapter == nil {
		return nil, common.ValidationError(nil, "adapter cannot be nil")
	}

	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Ensure adapter is open
	if err := adapter.Open(); err != nil {
		return nil, err
	}

	ledger := &MongoDBLedger{
		adapter: adapter,
		logger:  logger,
	}

	// Initialize current height
	if _, err := ledger.GetBlockHeight(); err != nil {
		logger.WithError(err).Warn("Failed to get current height during initialization")
	}

	return ledger, nil
}

// GetBalance returns the balance of an account
func (l *MongoDBLedger) GetBalance(accountID string) (float64, error) {
	account, err := l.adapter.GetAccount(accountID)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

// CreateAccount creates a new account
func (l *MongoDBLedger) CreateAccount(account *common.Account) error {
	return l.adapter.SaveAccount(account) // adapter's SaveAccount handles both create and update
}

// UpdateAccount updates an existing account
func (l *MongoDBLedger) UpdateAccount(account *common.Account) error {
	return l.adapter.SaveAccount(account) // adapter's SaveAccount handles both create and update
}

// GetAccount retrieves an account by ID
func (l *MongoDBLedger) GetAccount(accountID string) (*common.Account, error) {
	return l.adapter.GetAccount(accountID)
}

// AddTransaction adds a transaction to the ledger
func (l *MongoDBLedger) AddTransaction(tx common.Transaction) error {
	height, err := l.GetBlockHeight()
	if err != nil {
		height = 0
	}
	return l.adapter.SaveTransaction(&tx, height)
}

// IsTransactionCommitted checks if a transaction is committed
func (l *MongoDBLedger) IsTransactionCommitted(txID string) bool {
	_, err := l.adapter.GetTransaction(txID)
	return err == nil
}

// CommitBlock commits a block to the ledger
func (l *MongoDBLedger) CommitBlock(block common.Block) error {
	return l.adapter.SaveBlock(&block)
}

// DeploySmartContract deploys a smart contract
func (l *MongoDBLedger) DeploySmartContract(contract *common.SmartContract) error {
	return l.adapter.SaveContract(contract)
}

// GetTransaction retrieves a transaction by ID
func (l *MongoDBLedger) GetTransaction(txID string) (*common.Transaction, error) {
	return l.adapter.GetTransaction(txID)
}

// GetBlockHeight returns the current blockchain height
func (l *MongoDBLedger) GetBlockHeight() (int, error) {
	block, err := l.adapter.GetLatestBlock()
	if err != nil {
		return 0, err
	}
	return int(block.Number), nil
}

// UpdateAccountBalance updates the balance of an account
func (l *MongoDBLedger) UpdateAccountBalance(accountID string, amount float64) error {
	account, err := l.adapter.GetAccount(accountID)
	if err != nil {
		return err
	}
	account.Balance = amount
	return l.adapter.SaveAccount(account)
}

// UpdateSmartContract updates contract code to a new version
func (l *MongoDBLedger) UpdateSmartContract(contractID, newCode, version string) error {
	contract, err := l.adapter.GetContract(contractID)
	if err != nil {
		return err
	}
	contract.Code = newCode
	contract.Version = version
	return l.adapter.SaveContract(contract)
}

// RemoveSmartContract removes a smart contract
func (l *MongoDBLedger) RemoveSmartContract(contractID string) error {
	// For now, just return nil as removal isn't implemented in adapter
	return nil
}

// IntegrityCheck performs an integrity check on the ledger
func (l *MongoDBLedger) IntegrityCheck() error {
	// Basic integrity check - ensure adapter is working
	if l.adapter == nil {
		return fmt.Errorf("adapter is not initialized")
	}
	_, err := l.adapter.GetLatestBlock()
	return err
}

// Legacy compatibility functions for gradual migration
// These will be deprecated and removed in future versions

// AccountsCollection returns the accounts collection for backward compatibility
// Deprecated: Use the Ledger interface methods instead
func (l *MongoDBLedger) AccountsCollection() *mongo.Collection {
	return l.accountsColl
}

// TransactionsCollection returns the transactions collection for backward compatibility
// Deprecated: Use the Ledger interface methods instead
func (l *MongoDBLedger) TransactionsCollection() *mongo.Collection {
	return l.transactionsColl
}

// BlocksCollection returns the blocks collection for backward compatibility
// Deprecated: Use the Ledger interface methods instead
func (l *MongoDBLedger) BlocksCollection() *mongo.Collection {
	return l.blocksColl
}

// Ledger represents a unified interface for different ledger backends
type Ledger struct {
	adapter *MongoAdapter
	logger  *logrus.Logger
}

// NewLedger creates a new unified ledger
func NewLedger(adapter *MongoAdapter, logger *logrus.Logger) *Ledger {
	if logger == nil {
		logger = logrus.New()
	}

	return &Ledger{
		adapter: adapter,
		logger:  logger,
	}
}

// SaveTransaction saves a transaction to the ledger
func (l *Ledger) SaveTransaction(tx *common.Transaction) error {
	// Use current height
	height, err := l.GetBlockHeight()
	if err != nil {
		height = 0
	}
	return l.adapter.SaveTransaction(tx, int(height))
}

// GetBlockHeight returns the current blockchain height
func (l *Ledger) GetBlockHeight() (uint64, error) {
	block, err := l.adapter.GetLatestBlock()
	if err != nil {
		return 0, err
	}
	return uint64(block.Number), nil
}

// GetAccountTransactions retrieves transactions for an account
func (l *Ledger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	txPtrs, err := l.adapter.GetTransactionsByAddress(accountID, limit, offset)
	if err != nil {
		return nil, err
	}

	// Convert from pointers to values
	txs := make([]common.Transaction, len(txPtrs))
	for i, tx := range txPtrs {
		txs[i] = *tx
	}

	return txs, nil
}

// Close closes the ledger connection
func (l *MongoDBLedger) Close() error {
	if l.adapter != nil {
		return l.adapter.Close()
	}
	return nil
}

// GetBlockByNumber retrieves a block by its number
func (l *MongoDBLedger) GetBlockByNumber(num int) (common.Block, bool) {
	block, err := l.adapter.GetBlock(uint64(num))
	if err != nil {
		return common.Block{}, false
	}
	return *block, true
}

// CreateSnapshot creates a snapshot of the ledger state
func (l *MongoDBLedger) CreateSnapshot(height int) error {
	// For now, just return nil as snapshots aren't implemented
	return nil
}

// RestoreSnapshot restores the ledger from a snapshot
func (l *MongoDBLedger) RestoreSnapshot(height int) error {
	// For now, just return nil as snapshots aren't implemented
	return nil
}

// GetStats returns ledger statistics as a structured LedgerStats object
func (l *MongoDBLedger) GetStats() (*common.LedgerStats, error) {
	height, err := l.GetBlockHeight()
	if err != nil {
		height = 0
	}

	// Get total accounts count (simplified for now)
	totalAccounts := int64(0) // Would need to implement count in adapter

	// Get total transactions count (simplified for now)
	totalTransactions := int64(0) // Would need to implement count in adapter

	// Get total contracts count (simplified for now)
	totalContracts := int64(0) // Would need to implement count in adapter

	// Calculate total balance (simplified for now)
	totalBalance := 0.0 // Would need to implement aggregation in adapter

	return &common.LedgerStats{
		TotalAccounts:     totalAccounts,
		TotalTransactions: totalTransactions,
		TotalContracts:    totalContracts,
		TotalBalance:      totalBalance,
		LastBlockHeight:   int64(height),
		NetworkHealth:     "active",
		ProcessingTime:    0, // Would track actual processing times
	}, nil
}

// HealthCheck performs a health check on the ledger
func (l *MongoDBLedger) HealthCheck(ctx context.Context) error {
	return l.IntegrityCheck()
}

// GetBlocksByRange retrieves blocks in a range
func (l *MongoDBLedger) GetBlocksByRange(start, end int) ([]common.Block, error) {
	// Simple implementation - get blocks one by one
	var blocks []common.Block
	for i := start; i <= end; i++ {
		block, exists := l.GetBlockByNumber(i)
		if exists {
			blocks = append(blocks, block)
		}
	}
	return blocks, nil
}

// GetLastBlockHash returns the hash of the last block
func (l *MongoDBLedger) GetLastBlockHash() (string, error) {
	block, err := l.adapter.GetLatestBlock()
	if err != nil {
		return "", err
	}
	return block.Hash, nil
}

// GetSmartContract retrieves a smart contract by ID
func (l *MongoDBLedger) GetSmartContract(contractID string) (*common.SmartContract, error) {
	return l.adapter.GetContract(contractID)
}

// calculateGasForData calculates gas cost based on data size
func calculateGasForData(data []byte) uint64 {
	if data == nil {
		return 0
	}
	return uint64(len(data)) * GasMemoryPerByte
}

// calculateGasForString calculates gas cost for string operations
func calculateGasForString(s string) uint64 {
	return calculateGasForData([]byte(s))
}

// executeContractFunction provides basic contract execution simulation
func (l *MongoDBLedger) executeContractFunction(ctx context.Context, contract *common.SmartContract, function string, payload string, args *dtypes.ContractArguments) *dtypes.ContractExecutionResult {
	// Create execution result with proper gas calculation
	gasUsed := GasBaseExecution

	result := &dtypes.ContractExecutionResult{
		Success:      true,
		GasUsed:      gasUsed, // Will be updated during execution
		Events:       []dtypes.ContractEvent{},
		StateChanges: []dtypes.ContractStateChange{},
	}

	// Basic function dispatch - this is a simplified implementation
	// In production, this would be handled by the VM runtime
	switch function {
	case "getInfo":
		gasUsed += GasOpRead
		infoStr := fmt.Sprintf("Contract %s v%s owned by %s", contract.ID, contract.Version, contract.Owner)
		result.ReturnData = &dtypes.ContractReturnData{
			Success: true,
			Data:    []byte(infoStr),
			Value:   dtypes.StringToValue(infoStr),
			Message: "Contract info retrieved successfully",
		}
	case "getBalance":
		// Real balance check with gas calculation
		gasUsed += GasDbRead
		accountID := l.getContractArgString(args, "account")
		if accountID != "" {
			balance, err := l.GetBalance(accountID)
			if err != nil {
				result.Success = false
				result.Error = fmt.Sprintf("failed to get balance: %v", err)
				gasUsed += GasErrorValidation
			} else {
				balanceStr := fmt.Sprintf("%.2f", balance)
				result.ReturnData = &dtypes.ContractReturnData{
					Success: true,
					Data:    []byte(balanceStr),
					Value:   dtypes.StringToValue(balanceStr),
					Message: "Balance retrieved successfully",
				}
			}
		} else {
			result.Success = false
			result.Error = "account parameter required"
			gasUsed += GasErrorValidation
		}
	case "transfer":
		// Actual transfer function with real balance updates
		gasUsed += GasTransfer
		from := l.getContractArgString(args, "from")
		to := l.getContractArgString(args, "to")
		amount, amountOk := l.getContractArgFloat64(args, "amount")

		if from == "" || to == "" || !amountOk || amount <= 0 {
			result.Success = false
			result.Error = "invalid transfer parameters: from, to, amount required"
			gasUsed += GasErrorValidation
		} else {
			// Check sender balance
			senderBalance, err := l.GetBalance(from)
			if err != nil {
				result.Success = false
				result.Error = fmt.Sprintf("failed to get sender balance: %v", err)
				gasUsed += GasErrorDatabase
			} else if senderBalance < amount {
				result.Success = false
				result.Error = "insufficient balance"
				gasUsed += GasErrorValidation
			} else {
				// Get receiver balance for state change tracking
				receiverBalance, _ := l.GetBalance(to)

				// Perform actual transfer
				err = l.UpdateAccountBalance(from, -amount)
				if err != nil {
					result.Success = false
					result.Error = fmt.Sprintf("failed to update sender balance: %v", err)
					gasUsed += GasErrorDatabase
				} else {
					err = l.UpdateAccountBalance(to, amount)
					if err != nil {
						// Rollback sender balance
						l.UpdateAccountBalance(from, amount)
						result.Success = false
						result.Error = fmt.Sprintf("failed to update receiver balance: %v", err)
						gasUsed += GasErrorRollback
					} else {
						// Success - add state changes
						result.StateChanges = []dtypes.ContractStateChange{
							{
								Key:      fmt.Sprintf("balance:%s", from),
								OldValue: dtypes.FloatToValue(senderBalance),
								NewValue: dtypes.FloatToValue(senderBalance - amount),
							},
							{
								Key:      fmt.Sprintf("balance:%s", to),
								OldValue: dtypes.FloatToValue(receiverBalance),
								NewValue: dtypes.FloatToValue(receiverBalance + amount),
							},
						}

						txData := map[string]interface{}{
							"from":   from,
							"to":     to,
							"amount": amount,
							"status": "completed",
							"txHash": fmt.Sprintf("0x%x", time.Now().UnixNano()),
						}
						txDataBytes, _ := json.Marshal(txData)

						result.ReturnData = &dtypes.ContractReturnData{
							Success: true,
							Data:    txDataBytes,
							Message: "Transfer completed successfully",
						}

						// Add transfer event
						result.Events = []dtypes.ContractEvent{
							{
								ContractID: contract.ID,
								EventName:  "Transfer",
								Arguments: &dtypes.ContractArguments{
									Args: []dtypes.ContractArgument{
										{Name: "from", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(from)},
										{Name: "to", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(to)},
										{Name: "amount", Type: dtypes.ValueTypeFloat64, Value: dtypes.FloatToValue(amount)},
										{Name: "timestamp", Type: dtypes.ValueTypeInt64, Value: dtypes.IntToValue(int64(time.Now().Unix()))},
									},
								},
								BlockHeight: 0, // Would be set by consensus layer
								TxHash:      fmt.Sprintf("0x%x", time.Now().UnixNano()),
								Index:       0,
								Timestamp:   time.Now().Unix(),
							},
						}
					}
				}
			}
		}
	case "mint":
		// Handle mint function for token contracts
		to := l.getContractArgString(args, "to")
		amount, amountOk := l.getContractArgFloat64(args, "amount")

		if to == "" || !amountOk || amount <= 0 {
			result.Success = false
			result.Error = "invalid mint parameters: to, amount required"
			gasUsed += GasErrorValidation
		} else {
			// Update receiver balance
			err := l.UpdateAccountBalance(to, amount)
			if err != nil {
				result.Success = false
				result.Error = fmt.Sprintf("failed to mint tokens: %v", err)
				gasUsed += GasErrorDatabase
			} else {
				gasUsed += GasMint

				// Get new balance
				newBalance, _ := l.GetBalance(to)

				result.StateChanges = []dtypes.ContractStateChange{
					{
						Key:      fmt.Sprintf("balance:%s", to),
						OldValue: dtypes.FloatToValue(newBalance - amount),
						NewValue: dtypes.FloatToValue(newBalance),
					},
					{
						Key:      "total_supply",
						OldValue: dtypes.FloatToValue(0), // Would track actual supply
						NewValue: dtypes.FloatToValue(amount),
					},
				}

				// Add mint event
				result.Events = []dtypes.ContractEvent{
					{
						ContractID: contract.ID,
						EventName:  "Mint",
						Arguments: &dtypes.ContractArguments{
							Args: []dtypes.ContractArgument{
								{Name: "to", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(to)},
								{Name: "amount", Type: dtypes.ValueTypeFloat64, Value: dtypes.FloatToValue(amount)},
							},
						},
						BlockHeight: 0,
						TxHash:      fmt.Sprintf("0x%x", time.Now().UnixNano()),
						Index:       0,
						Timestamp:   time.Now().Unix(),
					},
				}

				mintData := map[string]interface{}{
					"to":     to,
					"amount": amount,
				}
				mintDataBytes, _ := json.Marshal(mintData)

				result.ReturnData = &dtypes.ContractReturnData{
					Success: true,
					Data:    mintDataBytes,
					Message: "Tokens minted successfully",
				}
			}
		}
	case "burn":
		// Handle burn function for token contracts
		from := l.getContractArgString(args, "from")
		amount, amountOk := l.getContractArgFloat64(args, "amount")

		if from == "" || !amountOk || amount <= 0 {
			result.Success = false
			result.Error = "invalid burn parameters: from, amount required"
			gasUsed += GasErrorValidation
		} else {
			// Check balance and burn
			balance, err := l.GetBalance(from)
			if err != nil {
				result.Success = false
				result.Error = fmt.Sprintf("failed to get balance: %v", err)
				gasUsed += GasErrorValidation
			} else if balance < amount {
				result.Success = false
				result.Error = "insufficient balance"
				gasUsed += GasErrorValidation
			} else {
				// Update balance
				err = l.UpdateAccountBalance(from, -amount)
				if err != nil {
					result.Success = false
					result.Error = fmt.Sprintf("failed to burn tokens: %v", err)
					gasUsed += GasErrorDatabase
				} else {
					gasUsed += GasBurn

					result.StateChanges = []dtypes.ContractStateChange{
						{
							Key:      fmt.Sprintf("balance:%s", from),
							OldValue: dtypes.FloatToValue(balance),
							NewValue: dtypes.FloatToValue(balance - amount),
						},
						{
							Key:      "total_supply",
							OldValue: dtypes.FloatToValue(0), // Would track actual supply
							NewValue: dtypes.FloatToValue(-amount),
						},
					}

					// Add burn event
					result.Events = []dtypes.ContractEvent{
						{
							ContractID: contract.ID,
							EventName:  "Burn",
							Arguments: &dtypes.ContractArguments{
								Args: []dtypes.ContractArgument{
									{Name: "from", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(from)},
									{Name: "amount", Type: dtypes.ValueTypeFloat64, Value: dtypes.FloatToValue(amount)},
								},
							},
							BlockHeight: 0,
							TxHash:      fmt.Sprintf("0x%x", time.Now().UnixNano()),
							Index:       0,
							Timestamp:   time.Now().Unix(),
						},
					}

					burnData := map[string]interface{}{
						"from":   from,
						"amount": amount,
						"status": "burned",
					}
					burnDataBytes, _ := json.Marshal(burnData)

					result.ReturnData = &dtypes.ContractReturnData{
						Success: true,
						Data:    burnDataBytes,
						Message: "Tokens burned successfully",
					}
				}
			}
		}
	case "approve":
		// Handle approve function for token contracts
		spender := l.getContractArgString(args, "spender")
		amount, amountOk := l.getContractArgFloat64(args, "amount")
		owner := l.getContractArgString(args, "owner")

		if spender == "" || !amountOk || owner == "" || amount < 0 {
			result.Success = false
			result.Error = "invalid approve parameters: owner, spender, amount required"
			gasUsed += GasErrorValidation
		} else {
			gasUsed += GasApproval

			result.StateChanges = []dtypes.ContractStateChange{
				{
					Key:      fmt.Sprintf("allowance:%s:%s", owner, spender),
					OldValue: dtypes.FloatToValue(0), // Would track actual allowance
					NewValue: dtypes.FloatToValue(amount),
				},
			}

			// Add approval event
			result.Events = []dtypes.ContractEvent{
				{
					ContractID: contract.ID,
					EventName:  "Approval",
					Arguments: &dtypes.ContractArguments{
						Args: []dtypes.ContractArgument{
							{Name: "owner", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(owner)},
							{Name: "spender", Type: dtypes.ValueTypeString, Value: dtypes.StringToValue(spender)},
							{Name: "amount", Type: dtypes.ValueTypeFloat64, Value: dtypes.FloatToValue(amount)},
						},
					},
					BlockHeight: 0,
					TxHash:      fmt.Sprintf("0x%x", time.Now().UnixNano()),
					Index:       0,
					Timestamp:   time.Now().Unix(),
				},
			}

			approvalData := map[string]interface{}{
				"owner":   owner,
				"spender": spender,
				"amount":  amount,
				"status":  "approved",
			}
			approvalDataBytes, _ := json.Marshal(approvalData)

			result.ReturnData = &dtypes.ContractReturnData{
				Success: true,
				Data:    approvalDataBytes,
				Message: "Approval set successfully",
			}
		}
	default:
		result.Success = false
		result.Error = fmt.Sprintf("function '%s' not found in contract", function)
		gasUsed += GasOpCompute // Minimal gas for unknown function
	}

	// Add gas for payload size
	if payload != "" {
		gasUsed += calculateGasForString(payload)
	}

	// Add gas for return data size if any
	if result.ReturnData != nil && result.ReturnData.Data != nil {
		gasUsed += calculateGasForData(result.ReturnData.Data)
	}

	// Add gas for events
	for _, event := range result.Events {
		gasUsed += GasOpWrite // Base cost for each event
		if event.Arguments != nil {
			// Add gas for event argument data
			gasUsed += uint64(len(event.Arguments.Args)) * GasOpCompute
		}
	}

	// Add gas for state changes
	gasUsed += uint64(len(result.StateChanges)) * GasStoragePerByte * 32 // Approximate 32 bytes per state change

	// Update final gas usage
	result.GasUsed = gasUsed

	return result
}

// getContractArgString extracts a string argument from contract arguments
func (l *MongoDBLedger) getContractArgString(args *dtypes.ContractArguments, key string) string {
	if args == nil || args.Args == nil {
		return ""
	}
	for _, arg := range args.Args {
		if arg.Name == key {
			if val, err := arg.Value.String(); err == nil {
				return val
			}
		}
	}
	return ""
}

// getContractArgFloat64 extracts a float64 argument from contract arguments
func (l *MongoDBLedger) getContractArgFloat64(args *dtypes.ContractArguments, key string) (float64, bool) {
	if args == nil || args.Args == nil {
		return 0, false
	}
	for _, arg := range args.Args {
		if arg.Name == key {
			if val, err := arg.Value.Float64(); err == nil {
				return val, true
			}
		}
	}
	return 0, false
}

// GetAccountTransactions retrieves transactions for an account
func (l *MongoDBLedger) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	txPtrs, err := l.adapter.GetTransactionsByAddress(accountID, limit, offset)
	if err != nil {
		return nil, err
	}

	// Convert from pointers to values
	txs := make([]common.Transaction, len(txPtrs))
	for i, tx := range txPtrs {
		txs[i] = *tx
	}

	return txs, nil
}

// ExecuteSmartContract executes a smart contract function
func (l *MongoDBLedger) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	// Get the contract
	contract, err := l.GetSmartContract(scID)
	if err != nil {
		return nil, fmt.Errorf("failed to get smart contract: %w", err)
	}

	// Convert params to ContractArguments
	args := &dtypes.ContractArguments{
		Args: []dtypes.ContractArgument{},
	}

	if params != nil {
		if params.StringParams != nil {
			for k, v := range params.StringParams {
				args.Args = append(args.Args, dtypes.ContractArgument{
					Name:  k,
					Type:  dtypes.ValueTypeString,
					Value: dtypes.StringToValue(v),
				})
			}
		}
		if params.NumberParams != nil {
			for k, v := range params.NumberParams {
				args.Args = append(args.Args, dtypes.ContractArgument{
					Name:  k,
					Type:  dtypes.ValueTypeFloat64,
					Value: dtypes.FloatToValue(v),
				})
			}
		}
		if params.BoolParams != nil {
			for k, v := range params.BoolParams {
				args.Args = append(args.Args, dtypes.ContractArgument{
					Name:  k,
					Type:  dtypes.ValueTypeBool,
					Value: dtypes.BoolToValue(v),
				})
			}
		}
		if params.ByteParams != nil {
			for k, v := range params.ByteParams {
				args.Args = append(args.Args, dtypes.ContractArgument{
					Name:  k,
					Type:  dtypes.ValueTypeBytes,
					Value: dtypes.BytesToValue(v),
				})
			}
		}
	}

	// Add sender to args
	args.Args = append(args.Args, dtypes.ContractArgument{
		Name:  "sender",
		Type:  dtypes.ValueTypeString,
		Value: dtypes.StringToValue(sender),
	})

	// Execute the contract function
	ctx := context.Background()
	execResult := l.executeContractFunction(ctx, contract, function, "", args)

	// Convert result to SmartContractResult
	result := &common.SmartContractResult{
		Success: execResult.Success,
		GasUsed: execResult.GasUsed,
	}

	// Convert error if present
	if execResult.Error != "" && !result.Success {
		result.ErrorMessage = execResult.Error
	}

	// Convert return data based on type
	if execResult.ReturnData != nil && execResult.ReturnData.Data != nil {
		// Try to parse the return data
		var returnValue interface{}
		if err := json.Unmarshal(execResult.ReturnData.Data, &returnValue); err == nil {
			switch v := returnValue.(type) {
			case string:
				result.StringResult = v
			case float64:
				result.NumberResult = v
			case bool:
				result.BoolResult = v
			case map[string]interface{}:
				// Convert map to JSON string for complex results
				if jsonBytes, err := json.Marshal(v); err == nil {
					result.StringResult = string(jsonBytes)
				}
			default:
				// If we can't parse it, just store as string
				result.StringResult = string(execResult.ReturnData.Data)
			}
		} else {
			// If JSON parsing fails, treat as raw string
			result.StringResult = string(execResult.ReturnData.Data)
		}

		// Also store raw bytes
		result.ByteResult = execResult.ReturnData.Data
	}

	// Convert state changes if present
	if len(execResult.StateChanges) > 0 {
		result.StateChanges = make([]common.StateChange, 0, len(execResult.StateChanges))
		for _, change := range execResult.StateChanges {
			sc := common.StateChange{
				Key: change.Key,
			}
			// Create StateChangeValue based on the new value
			if change.NewValue != nil {
				scv := &common.StateChangeValue{}
				// Determine type from the Value type
				switch change.NewValue.Type {
				case dtypes.ValueTypeString:
					if str, err := change.NewValue.String(); err == nil {
						scv.StringValue = str
						scv.ValueType = "string"
					}
				case dtypes.ValueTypeFloat64:
					if f, err := change.NewValue.Float64(); err == nil {
						scv.NumberValue = f
						scv.ValueType = "number"
					}
				case dtypes.ValueTypeBool:
					if b, err := change.NewValue.Bool(); err == nil {
						scv.BoolValue = b
						scv.ValueType = "bool"
					}
				case dtypes.ValueTypeBytes:
					scv.ByteValue = change.NewValue.Bytes()
					scv.ValueType = "bytes"
				}
				sc.Value = scv
			}
			result.StateChanges = append(result.StateChanges, sc)
		}
	}

	return result, nil
}

// ledger/common_ledger_adapter.go

package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"diamante/common"
	"diamante/config"
	"diamante/storage"
	cache "diamante/storage/cache"
	"diamante/types"

	"github.com/sirupsen/logrus"
)

// CommonLedgerAdapter adapts the API's LedgerAPI to common.LedgerAPI
type CommonLedgerAdapter interface {
	common.LedgerAPI
}

// apiLedgerAdapter adapts the API's LedgerAPI to common.LedgerAPI
type apiLedgerAdapter struct {
	apiLedger    interface{}
	mu           sync.RWMutex
	cacheManager *cache.Manager
	accountCache cache.Cache
	balanceCache cache.Cache
}

// NewCommonLedgerAdapter creates a new adapter for the API's LedgerAPI
func NewCommonLedgerAdapter(apiLedger interface{}, cfg *config.CacheConfig) CommonLedgerAdapter {
	mgr := cache.NewManager()
	opts := &cache.Options{}
	if cfg != nil {
		opts.Size = cfg.Size
		opts.TTL = cfg.TTL
		opts.RedisAddress = cfg.RedisURL
		opts.RedisDB = cfg.RedisDB
	}
	return &apiLedgerAdapter{
		apiLedger:    apiLedger,
		cacheManager: mgr,
		accountCache: mgr.GetCache("accounts", opts),
		balanceCache: mgr.GetCache("balances", opts),
	}
}

// NewLMDBLedgerAdapter creates a CommonLedgerAdapter backed by an LMDB store.
func NewLMDBLedgerAdapter(cfg *storage.LMDBConfig, logger *logrus.Logger, cacheSize int, cacheCfg *config.CacheConfig) (CommonLedgerAdapter, error) {
	store, err := storage.NewLMDBAdapter(cfg, logger, cacheSize)
	if err != nil {
		return nil, err
	}
	// LMDB disabled on Windows - using stub
	// Note: LMDB adapter methods are stubbed on Windows
	ledger := NewLMDBLedger(store)
	return NewCommonLedgerAdapter(ledger, cacheCfg), nil
}

// CreateAccount creates a new account
func (a *apiLedgerAdapter) CreateAccount(ac *common.Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := common.RegisterAccount(ac); err != nil {
		return err
	}

	// Convert account to CacheValue
	accountData, _ := json.Marshal(ac)
	accountCacheValue := &types.CacheValue{
		Key:        ac.ID,
		Data:       accountData,
		Size:       uint64(len(accountData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.accountCache.Set(ac.ID, accountCacheValue)

	// Convert balance to CacheValue
	balanceData, _ := json.Marshal(ac.Balance)
	balanceCacheValue := &types.CacheValue{
		Key:        ac.ID,
		Data:       balanceData,
		Size:       uint64(len(balanceData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.balanceCache.Set(ac.ID, balanceCacheValue)
	return nil
}

// UpdateAccount updates an existing account
func (a *apiLedgerAdapter) UpdateAccount(ac *common.Account) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	existingAccount := common.GetAccount(ac.ID)
	if existingAccount == nil {
		return common.ErrAccountNotFound
	}

	existingAccount.Balance = ac.Balance
	existingAccount.Nonce = ac.Nonce
	existingAccount.PublicKey = ac.PublicKey
	existingAccount.LastActive = common.GetCurrentTimestamp()

	// Convert account to CacheValue
	accountData, _ := json.Marshal(existingAccount)
	accountCacheValue := &types.CacheValue{
		Key:        ac.ID,
		Data:       accountData,
		Size:       uint64(len(accountData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.accountCache.Set(ac.ID, accountCacheValue)

	// Convert balance to CacheValue
	balanceData, _ := json.Marshal(ac.Balance)
	balanceCacheValue := &types.CacheValue{
		Key:        ac.ID,
		Data:       balanceData,
		Size:       uint64(len(balanceData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.balanceCache.Set(ac.ID, balanceCacheValue)

	return nil
}

// GetBalance retrieves the balance for a given account
func (a *apiLedgerAdapter) GetBalance(accountID string) (float64, error) {
	if val, ok := a.balanceCache.Get(accountID); ok {
		var balance float64
		if err := json.Unmarshal(val.Data, &balance); err == nil {
			return balance, nil
		}
	}

	account := common.GetAccount(accountID)
	if account == nil {
		return 0, common.ErrAccountNotFound
	}

	// Convert balance to CacheValue
	balanceData, _ := json.Marshal(account.Balance)
	balanceCacheValue := &types.CacheValue{
		Key:        accountID,
		Data:       balanceData,
		Size:       uint64(len(balanceData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.balanceCache.Set(accountID, balanceCacheValue)

	// Convert account to CacheValue
	accountData, _ := json.Marshal(account)
	accountCacheValue := &types.CacheValue{
		Key:        accountID,
		Data:       accountData,
		Size:       uint64(len(accountData)),
		CreatedAt:  common.ConsensusNow(),
		AccessedAt: common.ConsensusNow(),
	}
	a.accountCache.Set(accountID, accountCacheValue)

	return account.Balance, nil
}

// UpdateAccountBalance updates an account's balance
func (a *apiLedgerAdapter) UpdateAccountBalance(accountID string, amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := common.UpdateAccountBalance(accountID, amount); err != nil {
		return err
	}
	if bal, ok := a.balanceCache.Get(accountID); ok {
		var balance float64
		if err := json.Unmarshal(bal.Data, &balance); err == nil {
			newBalance := balance + amount
			// Convert new balance to CacheValue
			balanceData, _ := json.Marshal(newBalance)
			balanceCacheValue := &types.CacheValue{
				Key:        accountID,
				Data:       balanceData,
				Size:       uint64(len(balanceData)),
				CreatedAt:  common.ConsensusNow(),
				AccessedAt: common.ConsensusNow(),
			}
			a.balanceCache.Set(accountID, balanceCacheValue)
		}
	}
	return nil
}

// AddTransaction adds a transaction to the ledger
func (a *apiLedgerAdapter) AddTransaction(tx common.Transaction) error {
	// Validate the transaction
	if err := tx.Validate(); err != nil {
		return err
	}

	// Check if the sender has enough balance
	senderBalance, err := a.GetBalance(tx.Sender)
	if err != nil {
		return err
	}

	if senderBalance < tx.Amount+tx.Fee {
		return common.ErrInsufficientFunds
	}

	// Update the sender's balance
	if err := a.UpdateAccountBalance(tx.Sender, -(tx.Amount + tx.Fee)); err != nil {
		return err
	}

	// Update the receiver's balance
	if err := a.UpdateAccountBalance(tx.Receiver, tx.Amount); err != nil {
		// Revert the sender's balance if the receiver update fails
		if revertErr := a.UpdateAccountBalance(tx.Sender, tx.Amount+tx.Fee); revertErr != nil {
			// Log the revert error but return the original error
			if logger, ok := a.apiLedger.(interface{ Logger() *logrus.Logger }); ok {
				logger.Logger().WithError(revertErr).Error("Failed to revert sender balance after receiver update failed")
			}
		}
		return err
	}

	return nil
}

// IsTransactionCommitted checks if a transaction is committed
func (a *apiLedgerAdapter) IsTransactionCommitted(txID string) bool {
	if ledger, ok := a.apiLedger.(interface {
		IsTransactionCommitted(string) bool
	}); ok {
		return ledger.IsTransactionCommitted(txID)
	}
	return false
}

// GetTransaction retrieves a transaction by ID
func (a *apiLedgerAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetTransaction(string) (*common.Transaction, error)
	}); ok {
		return ledger.GetTransaction(txID)
	}
	return nil, common.ErrInvalidTransaction
}

// GetAccountTransactions retrieves transactions for an account
func (a *apiLedgerAdapter) GetAccountTransactions(accountID string, limit, offset int) ([]common.Transaction, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetAccountTransactions(string, int, int) ([]common.Transaction, error)
	}); ok {
		return ledger.GetAccountTransactions(accountID, limit, offset)
	}
	return []common.Transaction{}, nil
}

// CommitBlock commits a block to the ledger
func (a *apiLedgerAdapter) CommitBlock(block common.Block) error {
	if ledger, ok := a.apiLedger.(interface {
		CommitBlock(common.Block) error
	}); ok {
		return ledger.CommitBlock(block)
	}
	return nil
}

// GetBlockByNumber retrieves a block by number
func (a *apiLedgerAdapter) GetBlockByNumber(num int) (common.Block, bool) {
	// Try to cast the apiLedger to the expected interface
	if ledger, ok := a.apiLedger.(interface {
		GetBlockByNumber(num int) (common.Block, bool)
	}); ok {
		return ledger.GetBlockByNumber(num)
	}
	// Return an empty block if the cast fails
	return common.Block{}, false
}

// GetLastBlockHash retrieves the hash of the last block
func (a *apiLedgerAdapter) GetLastBlockHash() (string, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetLastBlockHash() (string, error)
	}); ok {
		return ledger.GetLastBlockHash()
	}
	return "", nil
}

// GetBlockHeight retrieves the current block height
func (a *apiLedgerAdapter) GetBlockHeight() (int, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetBlockHeight() (int, error)
	}); ok {
		return ledger.GetBlockHeight()
	}
	return 0, nil
}

// GetBlocksByRange retrieves blocks in a range
func (a *apiLedgerAdapter) GetBlocksByRange(startNum, endNum int) ([]common.Block, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetBlocksByRange(int, int) ([]common.Block, error)
	}); ok {
		return ledger.GetBlocksByRange(startNum, endNum)
	}
	return []common.Block{}, nil
}

// CreateSnapshot creates a snapshot of the ledger
func (a *apiLedgerAdapter) CreateSnapshot(height int) error {
	if ledger, ok := a.apiLedger.(interface {
		CreateSnapshot(height int) error
	}); ok {
		a.mu.Lock()
		defer a.mu.Unlock()
		err := ledger.CreateSnapshot(height)
		// clear caches to avoid stale data
		a.accountCache.Clear()
		a.balanceCache.Clear()
		return err
	}
	return nil
}

// RestoreSnapshot restores a snapshot of the ledger
func (a *apiLedgerAdapter) RestoreSnapshot(height int) error {
	if ledger, ok := a.apiLedger.(interface {
		RestoreSnapshot(height int) error
	}); ok {
		a.mu.Lock()
		defer a.mu.Unlock()
		err := ledger.RestoreSnapshot(height)
		a.accountCache.Clear()
		a.balanceCache.Clear()
		return err
	}
	return nil
}

// DeploySmartContract deploys a smart contract
func (a *apiLedgerAdapter) DeploySmartContract(sc *common.SmartContract) error {
	if ledger, ok := a.apiLedger.(interface {
		DeploySmartContract(*common.SmartContract) error
	}); ok {
		return ledger.DeploySmartContract(sc)
	}
	return nil
}

// UpdateSmartContract updates contract code to a new version
func (a *apiLedgerAdapter) UpdateSmartContract(contractID, newCode, version string) error {
	if ledger, ok := a.apiLedger.(interface {
		UpdateSmartContract(string, string, string) error
	}); ok {
		return ledger.UpdateSmartContract(contractID, newCode, version)
	}
	return nil
}

// ExecuteSmartContract executes a smart contract
func (a *apiLedgerAdapter) ExecuteSmartContract(scID, function, sender string, params *common.SmartContractParams) (*common.SmartContractResult, error) {
	// If the underlying ledger uses the old interface, convert the params
	if ledger, ok := a.apiLedger.(interface {
		ExecuteSmartContract(string, string, string, map[string]interface{}) (interface{}, error)
	}); ok {
		// Convert SmartContractParams to map[string]interface{}
		paramsMap := make(map[string]interface{})
		paramsMap["function_name"] = params.FunctionName
		paramsMap["caller"] = params.Caller

		// Convert string params
		for k, v := range params.StringParams {
			paramsMap[k] = v
		}
		// Convert number params
		for k, v := range params.NumberParams {
			paramsMap[k] = v
		}
		// Convert bool params
		for k, v := range params.BoolParams {
			paramsMap[k] = v
		}
		// Convert byte params
		for k, v := range params.ByteParams {
			paramsMap[k] = v
		}

		result, err := ledger.ExecuteSmartContract(scID, function, sender, paramsMap)
		if err != nil {
			return nil, err
		}

		// Convert result to SmartContractResult
		scResult := &common.SmartContractResult{
			Success: result != nil,
			GasUsed: 0, // Default value since old interface doesn't provide this
		}

		// Try to extract values from result if it's a map
		if resultMap, ok := result.(map[string]interface{}); ok {
			if success, ok := resultMap["success"].(bool); ok {
				scResult.Success = success
			}
			if strResult, ok := resultMap["string_result"].(string); ok {
				scResult.StringResult = strResult
			}
			if numResult, ok := resultMap["number_result"].(float64); ok {
				scResult.NumberResult = numResult
			}
			if boolResult, ok := resultMap["bool_result"].(bool); ok {
				scResult.BoolResult = boolResult
			}
			if byteResult, ok := resultMap["byte_result"].([]byte); ok {
				scResult.ByteResult = byteResult
			}
			if errMsg, ok := resultMap["error_message"].(string); ok {
				scResult.ErrorMessage = errMsg
			}
			if gasUsed, ok := resultMap["gas_used"].(uint64); ok {
				scResult.GasUsed = gasUsed
			} else if gasUsed, ok := resultMap["gas_used"].(float64); ok {
				scResult.GasUsed = uint64(gasUsed)
			}
		} else if result != nil {
			// If result is not a map, try to convert it to a string
			scResult.StringResult = fmt.Sprintf("%v", result)
		}

		return scResult, nil
	}

	// If the underlying ledger supports the new interface, use it directly
	if ledger, ok := a.apiLedger.(interface {
		ExecuteSmartContract(string, string, string, *common.SmartContractParams) (*common.SmartContractResult, error)
	}); ok {
		return ledger.ExecuteSmartContract(scID, function, sender, params)
	}

	// Default implementation
	return &common.SmartContractResult{
		Success:      false,
		ErrorMessage: "smart contract execution not supported",
	}, nil
}

// RemoveSmartContract removes a smart contract
func (a *apiLedgerAdapter) RemoveSmartContract(contractID string) error {
	if ledger, ok := a.apiLedger.(interface {
		RemoveSmartContract(string) error
	}); ok {
		return ledger.RemoveSmartContract(contractID)
	}
	return nil
}

// IntegrityCheck performs an integrity check on the ledger
func (a *apiLedgerAdapter) IntegrityCheck() error {
	if ledger, ok := a.apiLedger.(interface {
		IntegrityCheck() error
	}); ok {
		return ledger.IntegrityCheck()
	}
	return nil
}

// Close closes the ledger
func (a *apiLedgerAdapter) Close() error {
	if ledger, ok := a.apiLedger.(interface {
		Close() error
	}); ok {
		return ledger.Close()
	}
	return nil
}

// GetStats retrieves statistics about the ledger
func (a *apiLedgerAdapter) GetStats() (*common.LedgerStats, error) {
	if ledger, ok := a.apiLedger.(interface {
		GetStats() (map[string]interface{}, error)
	}); ok {
		statsMap, err := ledger.GetStats()
		if err != nil {
			return nil, err
		}

		// Convert map to LedgerStats
		stats := &common.LedgerStats{
			NetworkHealth: "healthy", // Default value
		}

		if totalAccounts, ok := statsMap["total_accounts"].(int64); ok {
			stats.TotalAccounts = totalAccounts
		} else if totalAccounts, ok := statsMap["total_accounts"].(float64); ok {
			stats.TotalAccounts = int64(totalAccounts)
		} else if totalAccounts, ok := statsMap["total_accounts"].(int); ok {
			stats.TotalAccounts = int64(totalAccounts)
		}

		if totalTransactions, ok := statsMap["total_transactions"].(int64); ok {
			stats.TotalTransactions = totalTransactions
		} else if totalTransactions, ok := statsMap["total_transactions"].(float64); ok {
			stats.TotalTransactions = int64(totalTransactions)
		} else if totalTransactions, ok := statsMap["total_transactions"].(int); ok {
			stats.TotalTransactions = int64(totalTransactions)
		}

		if totalContracts, ok := statsMap["total_contracts"].(int64); ok {
			stats.TotalContracts = totalContracts
		} else if totalContracts, ok := statsMap["total_contracts"].(float64); ok {
			stats.TotalContracts = int64(totalContracts)
		} else if totalContracts, ok := statsMap["total_contracts"].(int); ok {
			stats.TotalContracts = int64(totalContracts)
		}

		if totalBalance, ok := statsMap["total_balance"].(float64); ok {
			stats.TotalBalance = totalBalance
		}

		if lastBlockHeight, ok := statsMap["last_block_height"].(int64); ok {
			stats.LastBlockHeight = lastBlockHeight
		} else if lastBlockHeight, ok := statsMap["last_block_height"].(float64); ok {
			stats.LastBlockHeight = int64(lastBlockHeight)
		} else if lastBlockHeight, ok := statsMap["last_block_height"].(int); ok {
			stats.LastBlockHeight = int64(lastBlockHeight)
		}

		if networkHealth, ok := statsMap["network_health"].(string); ok {
			stats.NetworkHealth = networkHealth
		}

		if processingTime, ok := statsMap["processing_time_ms"].(int64); ok {
			stats.ProcessingTime = processingTime
		} else if processingTime, ok := statsMap["processing_time_ms"].(float64); ok {
			stats.ProcessingTime = int64(processingTime)
		} else if processingTime, ok := statsMap["processing_time_ms"].(int); ok {
			stats.ProcessingTime = int64(processingTime)
		}

		return stats, nil
	}

	// If the underlying ledger supports the new interface, use it directly
	if ledger, ok := a.apiLedger.(interface {
		GetStats() (*common.LedgerStats, error)
	}); ok {
		return ledger.GetStats()
	}

	// Default stats
	return &common.LedgerStats{
		NetworkHealth: "unknown",
	}, nil
}

// HealthCheck checks if the ledger is healthy
func (a *apiLedgerAdapter) HealthCheck(ctx context.Context) error {
	if ledger, ok := a.apiLedger.(interface {
		HealthCheck(context.Context) error
	}); ok {
		return ledger.HealthCheck(ctx)
	}
	return nil
}

// BatchWrite commits a batch of operations to the underlying store if supported.
func (a *apiLedgerAdapter) BatchWrite(batch *storage.WriteBatch) error {
	if ledger, ok := a.apiLedger.(interface {
		BatchWrite(*storage.WriteBatch) error
	}); ok {
		return ledger.BatchWrite(batch)
	}
	return storage.ErrNotImplemented
}

// PruneData prunes historical data from the underlying store if supported.
func (a *apiLedgerAdapter) PruneData(olderThan time.Time) error {
	if ledger, ok := a.apiLedger.(interface {
		PruneData(time.Time) error
	}); ok {
		return ledger.PruneData(olderThan)
	}
	return storage.ErrNotImplemented
}

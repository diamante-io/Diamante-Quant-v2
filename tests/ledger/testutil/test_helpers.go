// tests/ledger/testutil/test_helpers.go

package testutil

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"diamante/common"
	"diamante/config"
	"diamante/ledger"
	"diamante/ledger/evm"
	"diamante/storage"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestLedgerSetup provides a complete test environment for ledger testing
type TestLedgerSetup struct {
	LedgerAdapter ledger.CommonLedgerAdapter
	EVMExecutor   *ledger.EVMExecutor
	StateDB       *evm.StateDB
	Logger        *logrus.Logger
	Cleanup       func()
}

// SetupTestLedger creates a complete test environment
func SetupTestLedger(t *testing.T) *TestLedgerSetup {
	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create mock ledger store
	mockStore := NewMockLedgerStore()

	// Create cache config
	cacheConfig := &config.CacheConfig{
		Size: 1000,
		TTL:  60, // 1 minute
	}

	// Create mock API ledger
	mockAPILedger := NewMockAPILedger()

	// Create ledger adapter
	ledgerAdapter := ledger.NewCommonLedgerAdapter(mockAPILedger, cacheConfig)

	// Create EVM executor
	evmConfig := ledger.DefaultEVMConfig()
	evmExecutor := ledger.NewEVMExecutor(ledgerAdapter, evmConfig, logger)

	// Create StateDB
	stateDB := evm.NewStateDB(ledgerAdapter, mockStore, 0, logger)

	cleanup := func() {
		if err := ledgerAdapter.Close(); err != nil {
			t.Logf("Error closing ledger adapter: %v", err)
		}
	}

	return &TestLedgerSetup{
		LedgerAdapter: ledgerAdapter,
		EVMExecutor:   evmExecutor,
		StateDB:       stateDB,
		Logger:        logger,
		Cleanup:       cleanup,
	}
}

// NewMockLedgerStore creates a simple in-memory ledger store
func NewMockLedgerStore() storage.LedgerStore {
	return &InMemoryLedgerStore{
		contracts: make(map[string]*common.SmartContract),
		accounts:  make(map[string]*common.Account),
		storage:   make(map[string]map[string][]byte),
		code:      make(map[string][]byte),
	}
}

// InMemoryLedgerStore provides an in-memory implementation of LedgerStore
type InMemoryLedgerStore struct {
	contracts map[string]*common.SmartContract
	accounts  map[string]*common.Account
	storage   map[string]map[string][]byte
	code      map[string][]byte
}

func (s *InMemoryLedgerStore) GetContract(contractID string) (*storage.Contract, error) {
	contract, exists := s.contracts[contractID]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return contract, nil
}

func (s *InMemoryLedgerStore) StoreContract(contract *common.SmartContract) error {
	s.contracts[contract.ID] = contract
	return nil
}

func (s *InMemoryLedgerStore) UpdateContract(contract *common.SmartContract) error {
	if contract == nil {
		return storage.ErrInvalidData
	}
	s.contracts[contract.ID] = contract
	return nil
}

func (s *InMemoryLedgerStore) DeleteContract(contractID string) error {
	delete(s.contracts, contractID)
	return nil
}

func (s *InMemoryLedgerStore) GetAccount(address string) (*common.Account, error) {
	account, exists := s.accounts[address]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return account, nil
}

func (s *InMemoryLedgerStore) SaveAccount(account *common.Account) error {
	s.accounts[account.ID] = account
	return nil
}

func (s *InMemoryLedgerStore) UpdateAccount(account *common.Account) error {
	s.accounts[account.ID] = account
	return nil
}

func (s *InMemoryLedgerStore) GetBalance(address string) (float64, error) {
	account, err := s.GetAccount(address)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

func (s *InMemoryLedgerStore) GetNonce(address string) (uint64, error) {
	// For test purposes, return 0
	return 0, nil
}

func (s *InMemoryLedgerStore) GetStorage(address, key string) ([]byte, error) {
	if addrStorage, exists := s.storage[address]; exists {
		if value, exists := addrStorage[key]; exists {
			return value, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (s *InMemoryLedgerStore) SetStorage(address, key string, value []byte) error {
	if _, exists := s.storage[address]; !exists {
		s.storage[address] = make(map[string][]byte)
	}
	s.storage[address][key] = value
	return nil
}

func (s *InMemoryLedgerStore) GetCode(address string) ([]byte, error) {
	code, exists := s.code[address]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return code, nil
}

func (s *InMemoryLedgerStore) SetCode(address string, code []byte) error {
	s.code[address] = code
	return nil
}

// Add missing LedgerStore interface methods
func (s *InMemoryLedgerStore) SaveBlock(block *common.Block) error { return nil }
func (s *InMemoryLedgerStore) GetBlock(height uint64) (*common.Block, error) {
	return nil, storage.ErrNotFound
}
func (s *InMemoryLedgerStore) GetBlockByHash(hash string) (*common.Block, error) {
	return nil, storage.ErrNotFound
}
func (s *InMemoryLedgerStore) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	return nil, nil
}
func (s *InMemoryLedgerStore) GetLatestBlock() (*common.Block, error) {
	return nil, storage.ErrNotFound
}
func (s *InMemoryLedgerStore) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	return nil
}
func (s *InMemoryLedgerStore) GetTransaction(txID string) (*common.Transaction, error) {
	return nil, storage.ErrNotFound
}
func (s *InMemoryLedgerStore) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	return nil, nil
}
func (s *InMemoryLedgerStore) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	return nil, nil
}
func (s *InMemoryLedgerStore) GetState(key []byte) ([]byte, error) { return nil, storage.ErrNotFound }
func (s *InMemoryLedgerStore) SetState(key, value []byte) error    { return nil }
func (s *InMemoryLedgerStore) SaveState(key, value []byte) error   { return nil }
func (s *InMemoryLedgerStore) DeleteState(key []byte) error        { return nil }
func (s *InMemoryLedgerStore) SaveContract(contract *storage.Contract) error {
	return s.StoreContract(contract)
}
func (s *InMemoryLedgerStore) SaveReceipt(receipt *storage.Receipt) error { return nil }
func (s *InMemoryLedgerStore) GetReceipt(txID string) (*storage.Receipt, error) {
	return nil, storage.ErrNotFound
}
func (s *InMemoryLedgerStore) CreateSnapshot(height uint64) error             { return nil }
func (s *InMemoryLedgerStore) RestoreSnapshot(height uint64) error            { return nil }
func (s *InMemoryLedgerStore) ListSnapshots() ([]storage.SnapshotInfo, error) { return nil, nil }
func (s *InMemoryLedgerStore) Snapshot(path string) error                     { return nil }
func (s *InMemoryLedgerStore) WriteBatch(batch storage.WriteBatch) error      { return nil }
func (s *InMemoryLedgerStore) Compact() error                                 { return nil }
func (s *InMemoryLedgerStore) Backup(path string) error                       { return nil }
func (s *InMemoryLedgerStore) Restore(path string) error                      { return nil }
func (s *InMemoryLedgerStore) PruneData(olderThan time.Time) error            { return nil }
func (s *InMemoryLedgerStore) Vacuum() error                                  { return nil }
func (s *InMemoryLedgerStore) Open() error                                    { return nil }
func (s *InMemoryLedgerStore) Close() error                                   { return nil }
func (s *InMemoryLedgerStore) IsOpen() bool                                   { return true }
func (s *InMemoryLedgerStore) HealthCheck(ctx context.Context) error          { return nil }
func (s *InMemoryLedgerStore) GetStats() (*storage.StoreStats, error) {
	return &storage.StoreStats{}, nil
}
func (s *InMemoryLedgerStore) ReplaceBlockSameHeight(height uint64, newBlock *common.Block) error {
	return nil
}

// NewMockAPILedger creates a simple mock API ledger
func NewMockAPILedger() *MockAPILedger {
	return &MockAPILedger{
		accounts:     make(map[string]*common.Account),
		transactions: make(map[string]*common.Transaction),
		blocks:       make(map[int]common.Block),
		contracts:    make(map[string]*common.SmartContract),
		blockHeight:  0,
	}
}

// MockAPILedger provides a simple in-memory API ledger
type MockAPILedger struct {
	accounts     map[string]*common.Account
	transactions map[string]*common.Transaction
	blocks       map[int]common.Block
	contracts    map[string]*common.SmartContract
	blockHeight  int
}

func (m *MockAPILedger) GetBlockHeight() (int, error) {
	return m.blockHeight, nil
}

// Test data generators

// GenerateTestAddress creates a random Ethereum address
func GenerateTestAddress() ethcommon.Address {
	privKey, _ := crypto.GenerateKey()
	return crypto.PubkeyToAddress(privKey.PublicKey)
}

// GenerateTestAddressBytes creates a random address as bytes
func GenerateTestAddressBytes() []byte {
	return GenerateTestAddress().Bytes()
}

// GenerateTestAccounts creates multiple test accounts
func GenerateTestAccounts(t *testing.T, ledger ledger.CommonLedgerAdapter, count int) []*common.Account {
	accounts := make([]*common.Account, count)

	for i := 0; i < count; i++ {
		account := &common.Account{
			ID:        GenerateTestAddress().Hex(),
			Balance:   float64(1000 * (i + 1)),
			PublicKey: GenerateTestAddressBytes(),
			Nonce:     i,
		}

		// Create the account in the ledger
		err := ledger.CreateAccount(account)
		require.NoError(t, err)

		accounts[i] = account
	}

	return accounts
}

// GenerateTestBytecode generates simple test bytecode
func GenerateTestBytecode() []byte {
	// Simple bytecode: PUSH1 0x60 PUSH1 0x40 MSTORE
	return []byte{0x60, 0x60, 0x60, 0x40, 0x52}
}

// GenerateStorageContractBytecode generates bytecode for a simple storage contract
func GenerateStorageContractBytecode() []byte {
	// This is simplified bytecode for a storage contract
	// In real tests, you would use actual compiled bytecode
	bytecode, _ := hex.DecodeString("608060405234801561001057600080fd5b50610150806100206000396000f3fe")
	return bytecode
}

// CreateTestTransaction creates a test transaction
func CreateTestTransaction(from, to string, amount, fee float64) common.Transaction {
	return common.Transaction{
		ID:        crypto.Keccak256Hash([]byte(from + to + string(rune(int(amount))))).Hex(),
		Sender:    from,
		Receiver:  to,
		Amount:    amount,
		Fee:       fee,
		Timestamp: 1234567890,
	}
}

// CreateTestBlock creates a test block
func CreateTestBlock(number int, transactions []common.Transaction) common.Block {
	parentHash := ""
	if number > 0 {
		parentHash = crypto.Keccak256Hash([]byte(string(rune(number - 1)))).Hex()
	}

	return common.Block{
		Number:       number,
		Hash:         crypto.Keccak256Hash([]byte(string(rune(number)))).Hex(),
		PreviousHash: parentHash,
		Timestamp:    1234567890 + int64(number*10),
		Transactions: transactions,
	}
}

// CreateTestSmartContract creates a test smart contract
func CreateTestSmartContract(id string) *common.SmartContract {
	return &common.SmartContract{
		ID:       id,
		Code:     hex.EncodeToString(GenerateTestBytecode()),
		Owner:    GenerateTestAddress().Hex(),
		Language: "EVM",
		Version:  "1.0",
		State: &common.SmartContractState{
			Variables:     make(map[string]string),
			Balances:      make(map[string]float64),
			Permissions:   make(map[string]bool),
			Configuration: make(map[string]string),
			Counters:      make(map[string]int64),
			LastUpdated:   1234567890,
		},
	}
}

// CreateEVMTransaction creates a test EVM transaction
func CreateEVMTransaction(from, to ethcommon.Address, value *big.Int, data []byte) *ledger.EVMTransaction {
	return &ledger.EVMTransaction{
		From:     from.Bytes(),
		To:       to.Bytes(),
		Data:     data,
		GasLimit: 21000,
		GasPrice: big.NewInt(1000000000), // 1 Gwei
		Nonce:    0,
		Value:    value,
		ChainID:  big.NewInt(1),
	}
}

// CreateContractDeploymentTx creates a contract deployment transaction
func CreateContractDeploymentTx(from ethcommon.Address, bytecode []byte) *ledger.EVMTransaction {
	return &ledger.EVMTransaction{
		From:     from.Bytes(),
		To:       nil, // nil for contract creation
		Data:     bytecode,
		GasLimit: 200000,
		GasPrice: big.NewInt(1000000000),
		Nonce:    0,
		Value:    big.NewInt(0),
		ChainID:  big.NewInt(1),
	}
}

// AssertAccountBalance checks that an account has the expected balance
func AssertAccountBalance(t *testing.T, ledger ledger.CommonLedgerAdapter, accountID string, expectedBalance float64) {
	balance, err := ledger.GetBalance(accountID)
	require.NoError(t, err)
	require.Equal(t, expectedBalance, balance)
}

// AssertEVMBalance checks that an EVM address has the expected balance
func AssertEVMBalance(t *testing.T, stateDB *evm.StateDB, addr ethcommon.Address, expectedBalance *big.Int) {
	balance := stateDB.GetBalance(addr)
	require.Equal(t, expectedBalance, balance)
}

// AssertTransactionCommitted checks that a transaction is committed
func AssertTransactionCommitted(t *testing.T, ledger ledger.CommonLedgerAdapter, txID string) {
	committed := ledger.IsTransactionCommitted(txID)
	require.True(t, committed, "Transaction %s should be committed", txID)
}

// AssertContractDeployed checks that a contract is deployed
func AssertContractDeployed(t *testing.T, executor *ledger.EVMExecutor, contractAddr []byte) {
	code, err := executor.GetCode(contractAddr)
	require.NoError(t, err)
	require.NotEmpty(t, code, "Contract should have code")
}

// WaitForBlock simulates waiting for a block to be mined
func WaitForBlock(t *testing.T, ledger ledger.CommonLedgerAdapter, targetHeight int) {
	for {
		height, err := ledger.GetBlockHeight()
		require.NoError(t, err)

		if height >= targetHeight {
			break
		}

		// In real tests, you might add a small delay here
	}
}

// CompareAccounts compares two accounts for equality
func CompareAccounts(t *testing.T, expected, actual *common.Account) {
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.Balance, actual.Balance)
	require.Equal(t, expected.PublicKey, actual.PublicKey)
	require.Equal(t, expected.Nonce, actual.Nonce)
}

// CompareBlocks compares two blocks for equality
func CompareBlocks(t *testing.T, expected, actual common.Block) {
	require.Equal(t, expected.Number, actual.Number)
	require.Equal(t, expected.Hash, actual.Hash)
	require.Equal(t, expected.PreviousHash, actual.PreviousHash)
	require.Equal(t, len(expected.Transactions), len(actual.Transactions))
}

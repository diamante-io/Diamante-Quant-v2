// ledger/evm/state_db.go

package evm

import (
	"fmt"
	"math/big"
	"sync"

	"diamante/common"
	"diamante/ledger/evm/trie"
	"diamante/storage"

	"diamante/consensus"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
)

// Account represents an Ethereum account
type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     ethcommon.Hash // merkle root of the storage trie
	CodeHash []byte
}

// StateDB implements the go-ethereum StateDB interface for Diamante
type StateDB struct {
	ledger      common.LedgerAPI
	logger      *logrus.Logger
	stateStore  storage.LedgerStore
	blockHeight uint64
	refund      uint64

	// State tracking
	stateObjects      map[ethcommon.Address]*stateObject
	stateObjectsDirty map[ethcommon.Address]struct{}
	logs              []*types.Log
	logSize           uint
	thash             ethcommon.Hash
	txIndex           int
	accessList        *types.AccessList

	// Snapshots
	snapshots      []stateDBSnapshot
	nextSnapshotID int

	// Caches
	accountCache map[string]*common.Account
	codeCache    map[string][]byte
	storageCache map[string]map[string][]byte
	versionCache map[string]string

	// Preimage cache used for debugging/tracing
	preimages map[ethcommon.Hash][]byte

	// State trie
	stateTrie    *trie.MerklePatriciaTrie
	storageTries map[ethcommon.Address]*trie.MerklePatriciaTrie
	trieDB       interface{} // Store as interface to avoid type issues

	// Contract storage
	contractStore *storage.ContractStore

	// Transient storage (EIP-1153)
	transientStorage transientStorage

	// Track accounts created in current tx (for EIP-6780)
	createdAccounts map[ethcommon.Address]bool

	// Track selfdestructs in current tx
	selfdestructs map[ethcommon.Address]bool

	// Locks
	mu sync.RWMutex
}

// stateObject represents an Ethereum account which is being modified
type stateObject struct {
	address  ethcommon.Address
	addrHash ethcommon.Hash
	data     Account
	db       *StateDB

	// Storage cache
	originStorage  map[ethcommon.Hash]ethcommon.Hash
	pendingStorage map[ethcommon.Hash]ethcommon.Hash
	dirtyStorage   map[ethcommon.Hash]struct{}

	// State tracking
	dirtyCode bool
	deleted   bool
	suicided  bool
}

// stateDBSnapshot stores a snapshot of the StateDB for revert operations
type stateDBSnapshot struct {
	id                int
	stateObjects      map[ethcommon.Address]*stateObject
	stateObjectsDirty map[ethcommon.Address]struct{}
	logs              []*types.Log
	logSize           uint
	refund            uint64
	thash             ethcommon.Hash
	txIndex           int
	accessList        *types.AccessList
	preimages         map[ethcommon.Hash][]byte
}

// NewStateDB creates a new StateDB instance
func NewStateDB(ledger common.LedgerAPI, stateStore storage.LedgerStore, blockHeight uint64, logger *logrus.Logger) *StateDB {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	// Initialize state trie
	// Create a storage adapter that implements ethdb.Database
	storageAdapter := trie.NewStorageAdapter(stateStore)
	stateTrie, err := trie.NewMerklePatriciaTrie(storageAdapter, ethcommon.Hash{}, logger, nil)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create state trie")
	}

	// Create trie database for managing tries
	trieDB := trie.NewTrieDatabase(stateStore, logger)

	// Initialize contract store
	contractStore := storage.NewContractStore(stateStore, 1000, logger) // 1000 contract cache

	return &StateDB{
		ledger:            ledger,
		logger:            logger,
		stateStore:        stateStore,
		blockHeight:       blockHeight,
		stateObjects:      make(map[ethcommon.Address]*stateObject),
		stateObjectsDirty: make(map[ethcommon.Address]struct{}),
		accountCache:      make(map[string]*common.Account),
		codeCache:         make(map[string][]byte),
		storageCache:      make(map[string]map[string][]byte),
		versionCache:      make(map[string]string),
		snapshots:         []stateDBSnapshot{},
		nextSnapshotID:    1,
		preimages:         make(map[ethcommon.Hash][]byte),
		stateTrie:         stateTrie,
		storageTries:      make(map[ethcommon.Address]*trie.MerklePatriciaTrie),
		trieDB:            trieDB,
		contractStore:     contractStore,
	}
}

// CreateAccount creates a new account
func (s *StateDB) CreateAccount(addr ethcommon.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.WithField("address", addr.Hex()).Debug("Creating account")

	// Get or create the state object
	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		stateObject.data.Balance = new(big.Int)
		stateObject.data.Nonce = 0
		stateObject.data.CodeHash = crypto.Keccak256Hash(nil).Bytes()
		stateObject.dirtyCode = true
		stateObject.deleted = false
		stateObject.suicided = false
		s.stateObjectsDirty[addr] = struct{}{}

		// Track account creation for EIP-6780
		if s.createdAccounts == nil {
			s.createdAccounts = make(map[ethcommon.Address]bool)
		}
		s.createdAccounts[addr] = true
	}
}

// CreateContract creates a contract at the given address
// This is used for EIP-6780 compliance to track contracts created in current tx
func (s *StateDB) CreateContract(addr ethcommon.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.WithField("address", addr.Hex()).Debug("Creating contract")

	// Get or create the state object
	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		// For contracts, we need to set the same fields as CreateAccount
		// but this specifically marks it as a new contract for EIP-6780
		stateObject.data.Balance = new(big.Int)
		stateObject.data.Nonce = 0
		stateObject.data.CodeHash = crypto.Keccak256Hash(nil).Bytes()
		stateObject.dirtyCode = true
		stateObject.deleted = false
		stateObject.suicided = false
		s.stateObjectsDirty[addr] = struct{}{}

		// Track contract creation for EIP-6780
		if s.createdAccounts == nil {
			s.createdAccounts = make(map[ethcommon.Address]bool)
		}
		s.createdAccounts[addr] = true
	}
}

// SubBalance subtracts amount from the account balance
func (s *StateDB) SubBalance(addr ethcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		stateObject.data.Balance = new(big.Int).Sub(stateObject.data.Balance, amount.ToBig())
		s.stateObjectsDirty[addr] = struct{}{}
	}
}

// SetBalance sets the balance of an account
func (s *StateDB) SetBalance(addr ethcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		stateObject.data.Balance = amount.ToBig()
		s.stateObjectsDirty[addr] = struct{}{}
	}
}

// AddBalance adds amount to the account balance
func (s *StateDB) AddBalance(addr ethcommon.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		if stateObject.data.Balance == nil {
			stateObject.data.Balance = new(big.Int)
		}
		// Convert uint256 to big.Int for addition
		stateObject.data.Balance.Add(stateObject.data.Balance, amount.ToBig())
		s.stateObjectsDirty[addr] = struct{}{}
	}
}

// GetBalance returns the account balance
func (s *StateDB) GetBalance(addr ethcommon.Address) *uint256.Int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		if stateObject.data.Balance == nil {
			return uint256.NewInt(0)
		}
		// Convert big.Int to uint256
		u256, _ := uint256.FromBig(stateObject.data.Balance)
		return u256
	}
	return uint256.NewInt(0)
}

// GetNonce returns the account nonce
func (s *StateDB) GetNonce(addr ethcommon.Address) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.data.Nonce
	}
	return 0
}

// SetNonce sets the account nonce
func (s *StateDB) SetNonce(addr ethcommon.Address, nonce uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		stateObject.data.Nonce = nonce
		s.stateObjectsDirty[addr] = struct{}{}
	}
}

// GetCodeHash returns the code hash of the account
func (s *StateDB) GetCodeHash(addr ethcommon.Address) ethcommon.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return ethcommon.Hash{}
	}
	return ethcommon.BytesToHash(stateObject.data.CodeHash)
}

// GetCode returns the code of the account
func (s *StateDB) GetCode(addr ethcommon.Address) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil
	}

	// Check code cache
	addrStr := addr.Hex()
	if code, ok := s.codeCache[addrStr]; ok {
		return code
	}

	// Get code from storage
	contractID := addrStr
	code, err := s.getContractCode(contractID)
	if err != nil {
		s.logger.WithError(err).WithField("address", addrStr).Error("Failed to get contract code")
		return nil
	}

	// Cache the code
	s.codeCache[addrStr] = code
	return code
}

// GetCodeSize returns the code size of the account
func (s *StateDB) GetCodeSize(addr ethcommon.Address) int {
	code := s.GetCode(addr)
	return len(code)
}

// GetContractVersion returns the stored version for a contract
func (s *StateDB) GetContractVersion(addr ethcommon.Address) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versionCache[addr.Hex()]
}

// SetCode sets the code of the account
func (s *StateDB) SetCode(addr ethcommon.Address, code []byte) {
	s.SetCodeVersion(addr, code, "v1")
}

// SetCodeVersion sets the code and version of the account
func (s *StateDB) SetCodeVersion(addr ethcommon.Address, code []byte, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		stateObject.data.CodeHash = crypto.Keccak256Hash(code).Bytes()
		stateObject.dirtyCode = true
		s.stateObjectsDirty[addr] = struct{}{}

		// Cache the code
		addrStr := addr.Hex()
		s.codeCache[addrStr] = code

		// Store the code in storage
		contractID := addrStr
		err := s.storeContractCode(contractID, code, version)
		if err != nil {
			// Log error but continue execution as the code is already cached in memory
			// The contract deployment will still work, but persistence might fail
			s.logger.WithError(err).WithField("address", addrStr).Error("Failed to store contract code persistently")
		}
	}
}

// GetCommittedState returns the committed storage value at the given key
func (s *StateDB) GetCommittedState(addr ethcommon.Address, key ethcommon.Hash) ethcommon.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return ethcommon.Hash{}
	}

	// Check origin storage (committed state)
	if value, ok := stateObject.originStorage[key]; ok {
		return value
	}

	// Get from storage
	addrStr := addr.Hex()
	keyStr := key.Hex()

	// Get from state store
	storageKey := []byte(fmt.Sprintf("storage:%s:%s", addrStr, keyStr))
	value, err := s.stateStore.GetState(storageKey)
	if err != nil {
		if err != storage.ErrNotFound {
			s.logger.WithError(err).WithFields(logrus.Fields{
				"address": addrStr,
				"key":     keyStr,
			}).Error("Failed to get committed storage value")
		}
		return ethcommon.Hash{}
	}

	// Update origin storage
	stateObject.originStorage[key] = ethcommon.BytesToHash(value)

	return ethcommon.BytesToHash(value)
}

// GetState returns the storage value at the given key
func (s *StateDB) GetState(addr ethcommon.Address, key ethcommon.Hash) ethcommon.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return ethcommon.Hash{}
	}

	// Check pending storage first
	if value, ok := stateObject.pendingStorage[key]; ok {
		return value
	}

	// Check origin storage
	if value, ok := stateObject.originStorage[key]; ok {
		return value
	}

	// Get from storage
	addrStr := addr.Hex()
	keyStr := key.Hex()

	// Check storage cache
	if addrCache, ok := s.storageCache[addrStr]; ok {
		if value, ok := addrCache[keyStr]; ok {
			return ethcommon.BytesToHash(value)
		}
	}

	// Get from state store
	storageKey := []byte(fmt.Sprintf("storage:%s:%s", addrStr, keyStr))
	value, err := s.stateStore.GetState(storageKey)
	if err != nil {
		if err != storage.ErrNotFound {
			s.logger.WithError(err).WithFields(logrus.Fields{
				"address": addrStr,
				"key":     keyStr,
			}).Error("Failed to get storage value")
		}
		return ethcommon.Hash{}
	}

	// Cache the value
	if _, ok := s.storageCache[addrStr]; !ok {
		s.storageCache[addrStr] = make(map[string][]byte)
	}
	s.storageCache[addrStr][keyStr] = value

	// Update origin storage
	stateObject.originStorage[key] = ethcommon.BytesToHash(value)

	return ethcommon.BytesToHash(value)
}

// SetState sets the storage value at the given key
func (s *StateDB) SetState(addr ethcommon.Address, key, value ethcommon.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getOrCreateStateObject(addr)
	if stateObject != nil {
		if _, ok := stateObject.originStorage[key]; !ok {
			stateObject.originStorage[key] = ethcommon.Hash{}
		}
		stateObject.pendingStorage[key] = value
		stateObject.dirtyStorage[key] = struct{}{}
		s.stateObjectsDirty[addr] = struct{}{}
	}
}

// transientStorage holds transient storage that is cleared after each transaction
type transientStorage map[ethcommon.Address]map[ethcommon.Hash]ethcommon.Hash

// GetTransientState returns the transient storage value at the given key
func (s *StateDB) GetTransientState(addr ethcommon.Address, key ethcommon.Hash) ethcommon.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if we have transient storage map
	if s.transientStorage == nil {
		return ethcommon.Hash{}
	}

	// Check if address has transient storage
	if addrStorage, exists := s.transientStorage[addr]; exists {
		if value, exists := addrStorage[key]; exists {
			return value
		}
	}

	return ethcommon.Hash{}
}

// SetTransientState sets the transient storage value at the given key
func (s *StateDB) SetTransientState(addr ethcommon.Address, key, value ethcommon.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Initialize transient storage if needed
	if s.transientStorage == nil {
		s.transientStorage = make(transientStorage)
	}

	// Initialize address storage if needed
	if _, exists := s.transientStorage[addr]; !exists {
		s.transientStorage[addr] = make(map[ethcommon.Hash]ethcommon.Hash)
	}

	// Set the value
	s.transientStorage[addr][key] = value
}

// SelfDestruct marks the account as self-destructed (replaces Suicide)
func (s *StateDB) SelfDestruct(addr ethcommon.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return
	}

	stateObject.suicided = true
	stateObject.data.Balance = new(big.Int)
	s.stateObjectsDirty[addr] = struct{}{}
}

// Selfdestruct6780 implements EIP-6780 selfdestruct behavior
func (s *StateDB) Selfdestruct6780(addr ethcommon.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return
	}

	// EIP-6780: SELFDESTRUCT only deletes account if called in the same transaction as creation
	// Check if the account was created in the current transaction
	if s.wasCreatedInCurrentTx(addr) {
		// Full selfdestruct behavior
		stateObject.suicided = true
		stateObject.data.Balance = new(big.Int)
		s.stateObjectsDirty[addr] = struct{}{}
	} else {
		// EIP-6780: Only send balance to beneficiary, don't delete account
		// Balance transfer would be handled by the EVM
		// Just mark that selfdestruct was called for this account
		if s.selfdestructs == nil {
			s.selfdestructs = make(map[ethcommon.Address]bool)
		}
		s.selfdestructs[addr] = true
	}
}

// wasCreatedInCurrentTx checks if an account was created in the current transaction
func (s *StateDB) wasCreatedInCurrentTx(addr ethcommon.Address) bool {
	// Check if address is in the created accounts set for this transaction
	if s.createdAccounts == nil {
		return false
	}
	_, created := s.createdAccounts[addr]
	return created
}

// HasSelfDestructed returns whether the account has been self-destructed (replaces HasSuicided)
func (s *StateDB) HasSelfDestructed(addr ethcommon.Address) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	return stateObject.suicided
}

// Deprecated: Use SelfDestruct instead
func (s *StateDB) Suicide(addr ethcommon.Address) bool {
	s.SelfDestruct(addr)
	return true
}

// Deprecated: Use HasSelfDestructed instead
func (s *StateDB) HasSuicided(addr ethcommon.Address) bool {
	return s.HasSelfDestructed(addr)
}

// Exist reports whether the account exists
func (s *StateDB) Exist(addr ethcommon.Address) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getStateObject(addr) != nil
}

// Empty returns whether the account is empty
func (s *StateDB) Empty(addr ethcommon.Address) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return true
	}

	return stateObject.data.Nonce == 0 &&
		stateObject.data.Balance.Sign() == 0 &&
		len(stateObject.data.CodeHash) == 0
}

// RevertToSnapshot reverts to a previous snapshot
func (s *StateDB) RevertToSnapshot(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the snapshot with the given id
	idx := -1
	for i := len(s.snapshots) - 1; i >= 0; i-- {
		if s.snapshots[i].id == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		s.logger.WithField("snapshot", id).Warn("snapshot not found")
		return
	}

	snap := s.snapshots[idx]
	s.snapshots = s.snapshots[:idx]

	// Restore fields
	s.stateObjects = make(map[ethcommon.Address]*stateObject)
	for addr, obj := range snap.stateObjects {
		s.stateObjects[addr] = s.copyStateObject(obj)
	}
	s.stateObjectsDirty = make(map[ethcommon.Address]struct{})
	for addr := range snap.stateObjectsDirty {
		s.stateObjectsDirty[addr] = struct{}{}
	}
	s.logs = make([]*types.Log, len(snap.logs))
	for i, lg := range snap.logs {
		cp := *lg
		s.logs[i] = &cp
	}
	s.logSize = snap.logSize
	s.refund = snap.refund
	s.thash = snap.thash
	s.txIndex = snap.txIndex
	s.accessList = copyAccessList(snap.accessList)

	s.preimages = make(map[ethcommon.Hash][]byte)
	for h, p := range snap.preimages {
		s.preimages[h] = append([]byte(nil), p...)
	}

	// Clear caches as they may be invalid after revert
	s.accountCache = make(map[string]*common.Account)
	s.codeCache = make(map[string][]byte)
	s.storageCache = make(map[string]map[string][]byte)
}

// Snapshot creates a snapshot of the current state
func (s *StateDB) Snapshot() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextSnapshotID
	s.nextSnapshotID++

	snap := stateDBSnapshot{
		id:                id,
		stateObjects:      make(map[ethcommon.Address]*stateObject),
		stateObjectsDirty: make(map[ethcommon.Address]struct{}),
		logs:              make([]*types.Log, len(s.logs)),
		logSize:           s.logSize,
		refund:            s.refund,
		thash:             s.thash,
		txIndex:           s.txIndex,
		accessList:        copyAccessList(s.accessList),
		preimages:         make(map[ethcommon.Hash][]byte),
	}
	for addr, obj := range s.stateObjects {
		snap.stateObjects[addr] = s.copyStateObject(obj)
	}
	for addr := range s.stateObjectsDirty {
		snap.stateObjectsDirty[addr] = struct{}{}
	}
	for i, lg := range s.logs {
		cp := *lg
		snap.logs[i] = &cp
	}

	for h, p := range s.preimages {
		snap.preimages[h] = append([]byte(nil), p...)
	}

	s.snapshots = append(s.snapshots, snap)
	return id
}

// AddRefund adds gas to the refund counter
func (s *StateDB) AddRefund(gas uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refund += gas
}

// SubRefund subtracts gas from the refund counter
func (s *StateDB) SubRefund(gas uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if gas > s.refund {
		s.refund = 0
	} else {
		s.refund -= gas
	}
}

// GetRefund returns the current value of the refund counter
func (s *StateDB) GetRefund() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.refund
}

// AddLog adds a log to the state
func (s *StateDB) AddLog(log *types.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.TxHash = s.thash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	s.logs = append(s.logs, log)
	s.logSize++
}

// AddPreimage adds a preimage to the state
func (s *StateDB) AddPreimage(hash ethcommon.Hash, preimage []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.preimages == nil {
		s.preimages = make(map[ethcommon.Hash][]byte)
	}

	// Avoid overwriting existing preimage
	if _, ok := s.preimages[hash]; ok {
		return
	}

	s.preimages[hash] = append([]byte(nil), preimage...)

	if s.stateStore != nil {
		key := []byte(fmt.Sprintf("preimage:%s", hash.Hex()))
		if err := s.stateStore.SaveState(key, preimage); err != nil {
			s.logger.WithError(err).WithField("hash", hash.Hex()).Error("failed to store preimage")
		}
	}
}

// ForEachStorage iterates over the storage of an account
func (s *StateDB) ForEachStorage(addr ethcommon.Address, cb func(key, value ethcommon.Hash) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil
	}

	// Iterate over pending storage
	for key, value := range stateObject.pendingStorage {
		if !cb(key, value) {
			return nil
		}
	}

	// Iterate over origin storage
	for key, value := range stateObject.originStorage {
		if _, ok := stateObject.pendingStorage[key]; !ok {
			if !cb(key, value) {
				return nil
			}
		}
	}

	return nil
}

// Commit commits the state changes to storage
func (s *StateDB) Commit(deleteEmptyObjects bool) (ethcommon.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, commit all storage tries
	for addr := range s.stateObjectsDirty {
		stateObject := s.stateObjects[addr]
		if stateObject == nil {
			continue
		}
		if stateObject.suicided || (deleteEmptyObjects && stateObject.empty()) {
			// Delete the state object
			s.deleteStateObject(stateObject)

			// Remove from state trie
			if err := s.stateTrie.Delete(addr.Bytes()); err != nil {
				return ethcommon.Hash{}, fmt.Errorf("failed to delete account %s: %w", addr.Hex(), err)
			}

			// Remove storage trie
			delete(s.storageTries, addr)
		} else {
			// Commit storage changes for this account
			storageTrie := s.getStorageTrie(addr)

			// Update storage in trie
			for key := range stateObject.dirtyStorage {
				value := stateObject.pendingStorage[key]
				if value == (ethcommon.Hash{}) {
					// Delete empty values
					if err := storageTrie.Delete(key.Bytes()); err != nil {
						return ethcommon.Hash{}, fmt.Errorf("failed to delete storage: %w", err)
					}
				} else {
					// Update non-empty values
					if err := storageTrie.Update(key.Bytes(), value.Bytes()); err != nil {
						return ethcommon.Hash{}, fmt.Errorf("failed to update storage: %w", err)
					}
				}
			}

			// Commit storage trie
			storageRoot, err := storageTrie.Commit()
			if err != nil {
				return ethcommon.Hash{}, fmt.Errorf("failed to commit storage for %s: %w", addr.Hex(), err)
			}
			stateObject.data.Root = storageRoot

			// Update account in state trie
			accountData, err := s.encodeAccount(stateObject.data)
			if err != nil {
				return ethcommon.Hash{}, fmt.Errorf("failed to encode account %s: %w", addr.Hex(), err)
			}

			if err := s.stateTrie.Update(addr.Bytes(), accountData); err != nil {
				return ethcommon.Hash{}, fmt.Errorf("failed to update account %s: %w", addr.Hex(), err)
			}

			// Update the state object in ledger
			s.updateStateObject(stateObject)
		}
		delete(s.stateObjectsDirty, addr)
	}

	// Commit the main state trie
	stateRoot, err := s.stateTrie.Commit()
	if err != nil {
		return ethcommon.Hash{}, fmt.Errorf("failed to commit state trie: %w", err)
	}

	// Clear caches
	s.accountCache = make(map[string]*common.Account)
	s.codeCache = make(map[string][]byte)
	s.storageCache = make(map[string]map[string][]byte)

	s.logger.WithFields(logrus.Fields{
		"stateRoot":   stateRoot.Hex(),
		"blockHeight": s.blockHeight,
	}).Info("State committed successfully")

	return stateRoot, nil
}

// GetStateObject returns the state object for the given address.
// This is an exported wrapper around getStateObject for use in tests
// and other packages that need read-only access to the state object.
func (s *StateDB) GetStateObject(addr ethcommon.Address) *stateObject {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getStateObject(addr)
}

// getStateObject returns the state object for the given address
func (s *StateDB) getStateObject(addr ethcommon.Address) *stateObject {
	// Check if the state object is already in memory
	if obj, ok := s.stateObjects[addr]; ok {
		if obj.deleted {
			return nil
		}
		return obj
	}

	// Try to get from state trie first
	accountData, err := s.stateTrie.Get(addr.Bytes())
	if err == nil && len(accountData) > 0 {
		// Decode account from trie
		account, err := s.decodeAccount(accountData)
		if err == nil {
			obj := &stateObject{
				address:        addr,
				addrHash:       crypto.Keccak256Hash(addr[:]),
				data:           account,
				db:             s,
				originStorage:  make(map[ethcommon.Hash]ethcommon.Hash),
				pendingStorage: make(map[ethcommon.Hash]ethcommon.Hash),
				dirtyStorage:   make(map[ethcommon.Hash]struct{}),
			}
			s.stateObjects[addr] = obj
			return obj
		}
	}

	// Fall back to ledger
	addrStr := addr.Hex()
	account, err := s.getAccount(addrStr)
	if err != nil {
		if err != common.ErrAccountNotFound {
			s.logger.WithError(err).WithField("address", addrStr).Error("Failed to get account")
		}
		return nil
	}

	// Create a new state object
	obj := s.newStateObject(addr, account)
	s.stateObjects[addr] = obj

	return obj
}

// getOrCreateStateObject returns the state object for the given address, creating it if it doesn't exist
func (s *StateDB) getOrCreateStateObject(addr ethcommon.Address) *stateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		// Create a new account
		account := &common.Account{
			ID:      addr.Hex(),
			Balance: 0,
			Nonce:   0,
		}
		err := s.ledger.CreateAccount(account)
		if err != nil {
			s.logger.WithError(err).WithField("address", addr.Hex()).Error("Failed to create account")
			return nil
		}

		// Create a new state object
		stateObject = s.newStateObject(addr, account)
		s.stateObjects[addr] = stateObject
	}
	return stateObject
}

// newStateObject creates a new state object
func (s *StateDB) newStateObject(addr ethcommon.Address, account *common.Account) *stateObject {
	// Convert account to Account
	stateAccount := Account{
		Nonce:    uint64(account.Nonce),
		Balance:  new(big.Int).SetInt64(int64(account.Balance)),
		Root:     ethcommon.Hash{},                  // Empty storage root
		CodeHash: crypto.Keccak256Hash(nil).Bytes(), // Empty code hash
	}

	// Create a new state object
	stateObject := &stateObject{
		address:        addr,
		addrHash:       crypto.Keccak256Hash(addr[:]),
		data:           stateAccount,
		db:             s,
		originStorage:  make(map[ethcommon.Hash]ethcommon.Hash),
		pendingStorage: make(map[ethcommon.Hash]ethcommon.Hash),
		dirtyStorage:   make(map[ethcommon.Hash]struct{}),
	}

	return stateObject
}

// copyStateObject creates a deep copy of a stateObject
func (s *StateDB) copyStateObject(so *stateObject) *stateObject {
	if so == nil {
		return nil
	}
	cp := &stateObject{
		address:  so.address,
		addrHash: so.addrHash,
		data: Account{
			Nonce:    so.data.Nonce,
			Balance:  new(big.Int).Set(so.data.Balance),
			Root:     so.data.Root,
			CodeHash: append([]byte(nil), so.data.CodeHash...),
		},
		db:             s,
		originStorage:  make(map[ethcommon.Hash]ethcommon.Hash),
		pendingStorage: make(map[ethcommon.Hash]ethcommon.Hash),
		dirtyStorage:   make(map[ethcommon.Hash]struct{}),
		dirtyCode:      so.dirtyCode,
		deleted:        so.deleted,
		suicided:       so.suicided,
	}
	for k, v := range so.originStorage {
		cp.originStorage[k] = v
	}
	for k, v := range so.pendingStorage {
		cp.pendingStorage[k] = v
	}
	for k := range so.dirtyStorage {
		cp.dirtyStorage[k] = struct{}{}
	}
	return cp
}

// copyAccessList makes a deep copy of an access list
func copyAccessList(al *types.AccessList) *types.AccessList {
	if al == nil {
		return nil
	}
	newList := make(types.AccessList, len(*al))
	for i, tuple := range *al {
		keys := make([]ethcommon.Hash, len(tuple.StorageKeys))
		copy(keys, tuple.StorageKeys)
		newList[i] = types.AccessTuple{Address: tuple.Address, StorageKeys: keys}
	}
	return &newList
}

// deleteStateObject deletes a state object
func (s *StateDB) deleteStateObject(stateObject *stateObject) {
	// Mark the state object as deleted
	stateObject.deleted = true

	// Delete the account from the ledger
	addrStr := stateObject.address.Hex()
	// Note: We don't actually delete the account from the ledger, as that's not supported
	// Instead, we just mark it as deleted in memory
	s.logger.WithField("address", addrStr).Debug("Marking account as deleted")
}

// updateStateObject updates a state object
func (s *StateDB) updateStateObject(stateObject *stateObject) {
	// Update the account in the ledger
	addrStr := stateObject.address.Hex()
	account := &common.Account{
		ID:      addrStr,
		Balance: float64(stateObject.data.Balance.Int64()),
		Nonce:   int(stateObject.data.Nonce),
	}
	err := s.ledger.UpdateAccount(account)
	if err != nil {
		s.logger.WithError(err).WithField("address", addrStr).Error("Failed to update account")
	}

	// Update storage
	for key := range stateObject.dirtyStorage {
		value := stateObject.pendingStorage[key]
		storageKey := []byte(fmt.Sprintf("storage:%s:%s", addrStr, key.Hex()))
		err := s.stateStore.SaveState(storageKey, value.Bytes())
		if err != nil {
			s.logger.WithError(err).WithFields(logrus.Fields{
				"address": addrStr,
				"key":     key.Hex(),
			}).Error("Failed to update storage")
		}
	}
}

// getAccount gets an account from the ledger
func (s *StateDB) getAccount(accountID string) (*common.Account, error) {
	// Check account cache
	if account, ok := s.accountCache[accountID]; ok {
		return account, nil
	}

	// Get the account from the ledger
	balance, err := s.ledger.GetBalance(accountID)
	if err != nil {
		return nil, err
	}

	// Create a new account
	account := &common.Account{
		ID:      accountID,
		Balance: balance,
		Nonce:   0, // We don't have a way to get the nonce from the ledger
	}

	// Cache the account
	s.accountCache[accountID] = account

	return account, nil
}

// getContractCode gets contract code from storage
func (s *StateDB) getContractCode(contractID string) ([]byte, error) {
	return s.contractStore.GetContractCode(contractID)
}

// storeContractCode stores contract code in storage
func (s *StateDB) storeContractCode(contractID string, code []byte, version string) error {
	// Check if contract already exists
	existing, err := s.contractStore.GetContract(contractID)
	if err == nil && existing != nil {
		// Update existing contract
		existing.Code = hexutil.Encode(code)
		existing.Version = version
		if err := s.contractStore.StoreContract(existing); err != nil {
			return err
		}
	} else {
		// Create new contract
		contract := &common.SmartContract{
			ID:       contractID,
			Code:     hexutil.Encode(code),
			Owner:    "system",
			Language: "EVM",
			Version:  version,
			State: &common.SmartContractState{
				Variables:     make(map[string]string),
				Balances:      make(map[string]float64),
				Permissions:   make(map[string]bool),
				Configuration: make(map[string]string),
				Counters:      make(map[string]int64),
				LastUpdated:   consensus.ConsensusUnix(),
			},
		}

		if err := s.contractStore.StoreContract(contract); err != nil {
			return err
		}
	}

	// Update version cache
	s.versionCache[contractID] = version

	// Also try to update through ledger for compatibility
	if err := s.ledger.UpdateSmartContract(contractID, hexutil.Encode(code), version); err != nil {
		// Log the error but don't fail as the primary storage has succeeded
		s.logger.WithError(err).WithField("contractID", contractID).Warn("Failed to update contract through ledger API")
	}

	return nil
}

// getSmartContract gets a smart contract from the contract store
func (s *StateDB) getSmartContract(contractID string) (*common.SmartContract, error) {
	return s.contractStore.GetContract(contractID)
}

// empty returns whether the state object is empty
func (so *stateObject) empty() bool {
	return so.data.Nonce == 0 &&
		so.data.Balance.Sign() == 0 &&
		len(so.data.CodeHash) == 0
}

// encodeAccount encodes account data for storage in trie
func (s *StateDB) encodeAccount(account Account) ([]byte, error) {
	// Use RLP encoding for Ethereum compatibility
	return rlp.EncodeToBytes(account)
}

// decodeAccount decodes account data from trie storage
func (s *StateDB) decodeAccount(data []byte) (Account, error) {
	var account Account
	if err := rlp.DecodeBytes(data, &account); err != nil {
		return Account{}, err
	}
	return account, nil
}

// getStorageTrie gets or creates a storage trie for an account
func (s *StateDB) getStorageTrie(addr ethcommon.Address) *trie.MerklePatriciaTrie {
	if storageTrie, exists := s.storageTries[addr]; exists {
		return storageTrie
	}

	// Get the state object to find the storage root
	stateObject := s.getStateObject(addr)
	var root ethcommon.Hash
	if stateObject != nil {
		root = stateObject.data.Root
	}

	// Create new storage trie using a storage adapter
	storageAdapter := trie.NewStorageAdapter(s.stateStore)
	storageTrie, err := trie.NewMerklePatriciaTrie(storageAdapter, root, s.logger, nil)
	if err != nil {
		s.logger.WithError(err).WithField("address", addr.Hex()).Error("Failed to create storage trie")
		// Return an empty trie on error
		var fallbackErr error
		storageTrie, fallbackErr = trie.NewMerklePatriciaTrie(storageAdapter, ethcommon.Hash{}, s.logger, nil)
		if fallbackErr != nil {
			s.logger.WithError(fallbackErr).Error("Failed to create fallback empty storage trie")
			// This is a critical error - we can't proceed without a storage trie
			return nil
		}
	}

	s.storageTries[addr] = storageTrie
	return storageTrie
}

// IntermediateRoot computes the current state root without committing
func (s *StateDB) IntermediateRoot(deleteEmptyObjects bool) ethcommon.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Process all dirty objects similar to Commit but without persisting
	for addr := range s.stateObjectsDirty {
		stateObject := s.stateObjects[addr]
		if stateObject == nil {
			continue
		}
		if stateObject.suicided || (deleteEmptyObjects && stateObject.empty()) {
			// Delete from trie
			if err := s.stateTrie.Delete(addr.Bytes()); err != nil {
				s.logger.WithError(err).WithField("address", addr.Hex()).Error("Failed to delete account in intermediate root")
			}
		} else {
			// Update storage trie for this account
			storageTrie := s.getStorageTrie(addr)

			// Update storage in trie
			for key := range stateObject.dirtyStorage {
				value := stateObject.pendingStorage[key]
				if value == (ethcommon.Hash{}) {
					storageTrie.Delete(key.Bytes())
				} else {
					storageTrie.Update(key.Bytes(), value.Bytes())
				}
			}

			// Get storage root without committing
			storageRoot := storageTrie.Hash()
			stateObject.data.Root = storageRoot

			// Update account in state trie
			accountData, err := s.encodeAccount(stateObject.data)
			if err != nil {
				s.logger.WithError(err).WithField("address", addr.Hex()).Error("Failed to encode account in intermediate root")
				continue
			}

			if err := s.stateTrie.Update(addr.Bytes(), accountData); err != nil {
				s.logger.WithError(err).WithField("address", addr.Hex()).Error("Failed to update account in intermediate root")
			}
		}
	}

	// Return the intermediate root without committing
	return s.stateTrie.Hash()
}

// AddAddressToAccessList adds an address to the access list
func (s *StateDB) AddAddressToAccessList(addr ethcommon.Address) {
	if s.accessList == nil {
		s.accessList = &types.AccessList{}
	}
	for _, tuple := range *s.accessList {
		if tuple.Address == addr {
			return
		}
	}
	*s.accessList = append(*s.accessList, types.AccessTuple{
		Address:     addr,
		StorageKeys: []ethcommon.Hash{},
	})
}

// AddSlotToAccessList adds a storage slot to the access list
func (s *StateDB) AddSlotToAccessList(addr ethcommon.Address, slot ethcommon.Hash) {
	if s.accessList == nil {
		s.accessList = &types.AccessList{}
	}
	for i, tuple := range *s.accessList {
		if tuple.Address == addr {
			for _, s := range tuple.StorageKeys {
				if s == slot {
					return
				}
			}
			(*s.accessList)[i].StorageKeys = append((*s.accessList)[i].StorageKeys, slot)
			return
		}
	}
	*s.accessList = append(*s.accessList, types.AccessTuple{
		Address:     addr,
		StorageKeys: []ethcommon.Hash{slot},
	})
}

// SlotInAccessList returns whether a storage slot is in the access list
func (s *StateDB) SlotInAccessList(addr ethcommon.Address, slot ethcommon.Hash) (addressPresent bool, slotPresent bool) {
	if s.accessList == nil {
		return false, false
	}
	for _, tuple := range *s.accessList {
		if tuple.Address == addr {
			addressPresent = true
			for _, s := range tuple.StorageKeys {
				if s == slot {
					slotPresent = true
					return
				}
			}
			return
		}
	}
	return false, false
}

// AddressInAccessList returns whether an address is in the access list
func (s *StateDB) AddressInAccessList(addr ethcommon.Address) bool {
	if s.accessList == nil {
		return false
	}
	for _, tuple := range *s.accessList {
		if tuple.Address == addr {
			return true
		}
	}
	return false
}

// PrepareAccessList prepares the access list for a transaction
func (s *StateDB) PrepareAccessList(sender ethcommon.Address, dest *ethcommon.Address, precompiles []ethcommon.Address, txAccesses types.AccessList) {
	s.AddAddressToAccessList(sender)
	if dest != nil {
		s.AddAddressToAccessList(*dest)
	}
	for _, addr := range precompiles {
		s.AddAddressToAccessList(addr)
	}
	for _, tuple := range txAccesses {
		s.AddAddressToAccessList(tuple.Address)
		for _, key := range tuple.StorageKeys {
			s.AddSlotToAccessList(tuple.Address, key)
		}
	}
}

// GetLogs returns the logs for the given hash
func (s *StateDB) GetLogs(hash ethcommon.Hash, blockHash ethcommon.Hash) []*types.Log {
	s.mu.RLock()
	defer s.mu.RUnlock()

	logs := make([]*types.Log, len(s.logs))
	copy(logs, s.logs)
	return logs
}

// AccessEvents returns the list of logs collected during execution.
func (s *StateDB) AccessEvents() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a simple struct with logs for compatibility
	return struct {
		Logs []*types.Log
	}{
		Logs: s.logs,
	}
}

// Prepare prepares the state for a new transaction
func (s *StateDB) Prepare(rules params.Rules, sender, coinbase ethcommon.Address, dest *ethcommon.Address, precompiles []ethcommon.Address, txAccesses types.AccessList) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Prepare access list
	s.PrepareAccessList(sender, dest, precompiles, txAccesses)
}

// PrepareForTx prepares the state for a new transaction (legacy method)
func (s *StateDB) PrepareForTx(thash ethcommon.Hash, ti int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.thash = thash
	s.txIndex = ti
}

// GetStorageRoot returns the storage root for a given account
func (s *StateDB) GetStorageRoot(addr ethcommon.Address) ethcommon.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return ethcommon.Hash{}
	}
	return stateObject.data.Root
}

// PointCache returns the point cache used in computations
// For now, we return nil as we don't use point cache in our implementation
func (s *StateDB) PointCache() *utils.PointCache {
	return nil
}

// Witness returns the stateless witness for the current state
// For now, we return nil as we don't support stateless witnesses yet
func (s *StateDB) Witness() *stateless.Witness {
	return nil
}

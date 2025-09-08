package storage

import (
	"context"
	"time"

	"diamante/common"
)

// BlockAdapter converts between storage.Block and common.Block types
type BlockAdapter struct{}

// ToCommonBlock converts a storage.Block to common.Block
func (ba *BlockAdapter) ToCommonBlock(sb *Block) *common.Block {
	if sb == nil {
		return nil
	}

	// Convert storage.Transaction to common.Transaction
	txs := make([]common.Transaction, len(sb.Transactions))
	for i, stx := range sb.Transactions {
		txs[i] = common.Transaction{
			ID:        stx.ID,
			Sender:    stx.Sender,
			Receiver:  stx.Receiver,
			Amount:    stx.Amount,
			Timestamp: stx.Timestamp.Unix(),
		}
	}

	return &common.Block{
		Number:       int(sb.Number),
		Timestamp:    sb.Timestamp.Unix(),
		Transactions: txs,
		PreviousHash: sb.PrevBlockHash,
		Hash:         sb.BlockHash,
	}
}

// ToStorageBlock converts a common.Block to storage.Block
func (ba *BlockAdapter) ToStorageBlock(cb *common.Block) *Block {
	if cb == nil {
		return nil
	}

	// Convert common.Transaction to storage.Transaction
	txs := make([]Transaction, len(cb.Transactions))
	for i, ctx := range cb.Transactions {
		txs[i] = Transaction{
			ID:        ctx.ID,
			Sender:    ctx.Sender,
			Receiver:  ctx.Receiver,
			Amount:    ctx.Amount,
			Timestamp: time.Unix(ctx.Timestamp, 0),
		}
	}

	return &Block{
		Number:        uint64(cb.Number),
		Timestamp:     time.Unix(cb.Timestamp, 0),
		Transactions:  txs,
		PrevBlockHash: cb.PreviousHash,
		BlockHash:     cb.Hash,
	}
}

// StoreAdapter wraps a BlockStore (using storage.Block) to implement LedgerStore (using common.Block)
type StoreAdapter struct {
	store BlockStore
	ba    *BlockAdapter
}

// NewStoreAdapter creates a new adapter
func NewStoreAdapter(store BlockStore) *StoreAdapter {
	return &StoreAdapter{
		store: store,
		ba:    &BlockAdapter{},
	}
}

// SaveBlock implements LedgerStore.SaveBlock
func (sa *StoreAdapter) SaveBlock(block *common.Block) error {
	storageBlock := sa.ba.ToStorageBlock(block)
	return sa.store.SaveBlock(storageBlock)
}

// GetBlock implements LedgerStore.GetBlock
func (sa *StoreAdapter) GetBlock(height uint64) (*common.Block, error) {
	storageBlock, err := sa.store.GetBlock(height)
	if err != nil {
		return nil, err
	}
	return sa.ba.ToCommonBlock(storageBlock), nil
}

// GetBlockByHash - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetBlockByHash(hash string) (*common.Block, error) {
	return nil, ErrNotImplemented
}

// GetBlockRange - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetBlockRange(startHeight, endHeight uint64) ([]*common.Block, error) {
	return nil, ErrNotImplemented
}

// GetLatestBlock - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetLatestBlock() (*common.Block, error) {
	return nil, ErrNotImplemented
}

// SaveTransaction - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SaveTransaction(tx *common.Transaction, blockHeight int) error {
	return ErrNotImplemented
}

// GetTransaction - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetTransaction(txID string) (*common.Transaction, error) {
	return nil, ErrNotImplemented
}

// GetTransactionsByAddress - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	return nil, ErrNotImplemented
}

// GetAccount - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetAccount(address string) (*common.Account, error) {
	return nil, ErrNotImplemented
}

// SaveAccount - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SaveAccount(account *common.Account) error {
	return ErrNotImplemented
}

// GetContract - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetContract(address string) (*common.SmartContract, error) {
	return nil, ErrNotImplemented
}

// SaveContract - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SaveContract(contract *common.SmartContract) error {
	return ErrNotImplemented
}

// GetReceipt - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetReceipt(txHash string) (*Receipt, error) {
	return nil, ErrNotImplemented
}

// SaveReceipt - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SaveReceipt(receipt *Receipt) error {
	return ErrNotImplemented
}

// GetStateData - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetStateData(key string) ([]byte, error) {
	return nil, ErrNotImplemented
}

// SaveStateData - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SaveStateData(key string, value []byte) error {
	return ErrNotImplemented
}

// GetStats - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetStats() (*StoreStats, error) {
	return nil, ErrNotImplemented
}

// Backup - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Backup(path string) error {
	return ErrNotImplemented
}

// Restore - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Restore(path string) error {
	return ErrNotImplemented
}

// Close - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Close() error {
	return ErrNotImplemented
}

// Ping - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Ping() error {
	return ErrNotImplemented
}

// BeginTx - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) BeginTx() (interface{}, error) {
	return nil, ErrNotImplemented
}

// CommitTx - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) CommitTx(tx interface{}) error {
	return ErrNotImplemented
}

// RollbackTx - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) RollbackTx(tx interface{}) error {
	return ErrNotImplemented
}

// GetTransactionsByBlock - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetTransactionsByBlock(blockHeight uint64) ([]*common.Transaction, error) {
	return nil, ErrNotImplemented
}

// UpdateAccount - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) UpdateAccount(account *common.Account) error {
	return ErrNotImplemented
}

// GetBalance - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetBalance(address string) (float64, error) {
	return 0, ErrNotImplemented
}

// GetNonce - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetNonce(address string) (uint64, error) {
	return 0, ErrNotImplemented
}

// GetState - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) GetState(key []byte) ([]byte, error) {
	return nil, ErrNotImplemented
}

// SetState - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) SetState(key, value []byte) error {
	return ErrNotImplemented
}

// DeleteState - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) DeleteState(key []byte) error {
	return ErrNotImplemented
}

// UpdateContract - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) UpdateContract(contract *common.SmartContract) error {
	return ErrNotImplemented
}

// DeleteContract - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) DeleteContract(contractID string) error {
	return ErrNotImplemented
}

// CreateSnapshot - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) CreateSnapshot(height uint64) error {
	return ErrNotImplemented
}

// RestoreSnapshot - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) RestoreSnapshot(height uint64) error {
	return ErrNotImplemented
}

// ListSnapshots - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) ListSnapshots() ([]SnapshotInfo, error) {
	return nil, ErrNotImplemented
}

// BatchWrite - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) BatchWrite(batch *WriteBatch) error {
	return ErrNotImplemented
}

// Compact - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Compact() error {
	return ErrNotImplemented
}

// PruneData - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) PruneData(olderThan time.Time) error {
	return ErrNotImplemented
}

// Vacuum - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Vacuum() error {
	return ErrNotImplemented
}

// Open - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) Open() error {
	return ErrNotImplemented
}

// HealthCheck - Store interface doesn't have this, so we return not implemented
func (sa *StoreAdapter) HealthCheck(ctx context.Context) error {
	return ErrNotImplemented
}

package ledger

import (
	"encoding/json"
	"fmt"
	"sync"
)

// StateDB is a minimal key/value store for advanced ledger states (optional).
// If you prefer the direct in-ledger `accounts map[string]*Account`, you might skip this.
type StateDB struct {
	mu       sync.RWMutex
	balances map[string]uint64
}

func NewStateDB() *StateDB {
	return &StateDB{
		balances: make(map[string]uint64),
	}
}

func (db *StateDB) GetBalance(account string) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	bal, ok := db.balances[account]
	if !ok {
		return 0, nil
	}
	return bal, nil
}

func (db *StateDB) SetBalance(account string, amount uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.balances[account] = amount
	return nil
}

// Snapshot & Restore can let you do DB-level backups
func (db *StateDB) Snapshot() ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	snap, err := json.Marshal(db.balances)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state db: %w", err)
	}
	return snap, nil
}

func (db *StateDB) Restore(data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	newMap := make(map[string]uint64)
	if err := json.Unmarshal(data, &newMap); err != nil {
		return fmt.Errorf("failed to unmarshal state db: %w", err)
	}
	db.balances = newMap
	return nil
}

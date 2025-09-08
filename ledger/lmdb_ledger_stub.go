// ledger/lmdb_ledger_stub.go
// Placeholder for LMDB ledger functionality (disabled on Windows)

package ledger

import (
	"diamante/storage"
	"fmt"
)

// NewLMDBLedger returns error indicating LMDB is disabled
func NewLMDBLedger(store *storage.LMDBAdapter) error {
	return fmt.Errorf("LMDB support has been disabled for Windows compatibility - use MongoDB ledger instead")
}

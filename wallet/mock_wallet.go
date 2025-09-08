package wallet

import (
	"diamante/common"
)

// SubmitTransactionWithMock creates and submits a transaction using a mock transaction manager.
// This is used for testing purposes only.
func (w *Wallet) SubmitTransactionWithMock(receiver string, amount float64, fee float64, data []byte, mockTxMgr *MockTransactionManager) (*common.Transaction, error) {
	tx, err := w.CreateTransaction(receiver, amount, fee, data)
	if err != nil {
		return nil, err
	}

	// We don't actually submit the transaction to the mock transaction manager,
	// we just return the created transaction
	return tx, nil
}

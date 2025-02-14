// File: wallet/wallet_test.go
package wallet_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"diamante/common"
	"diamante/crypto"
	"diamante/ledger"
	"diamante/transaction"
	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestLogger returns a Logrus logger configured for testing.
func getTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	return logger
}

// cleanupTempFile removes the given file.
func cleanupTempFile(path string) {
	_ = os.Remove(path)
}

// newDummyLedger returns an in‑memory ledger instance (satisfying ledger.LedgerAPI)
// for use in wallet tests.
func newDummyLedger() ledger.LedgerAPI {
	return ledger.NewLedger()
}

//
// TEST CASES
//

// TestNewWallet verifies that a new wallet is created with all necessary fields.
func TestNewWallet(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err, "NewWallet returned an error")
	require.NotNil(t, w, "Expected non-nil wallet")
	assert.NotEmpty(t, w.ID, "Wallet ID should not be empty")
	assert.NotNil(t, w.KEMKeyPair, "KEM key pair should not be nil")
	assert.NotNil(t, w.SigKeyPair, "Signature key pair should not be nil")
	assert.NotNil(t, w.CryptoManager, "CryptoManager should not be nil")
}

// TestRegisterAccount checks that the wallet account is registered in the common package.
func TestRegisterAccount(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err, "NewWallet returned error")

	// Register the account; RegisterAccount should create an account (with a default balance)
	err = w.RegisterAccount()
	require.NoError(t, err, "RegisterAccount failed")

	// Retrieve the account from common
	ac := common.GetAccount(w.ID)
	require.NotNil(t, ac, "Expected account to be registered in common package")
	assert.Equal(t, w.ID, ac.ID, "Account ID should match wallet ID")
}

// TestExportImport tests exporting a wallet to disk and then importing it back.
func TestExportImport(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err, "NewWallet returned error")

	err = w.RegisterAccount()
	require.NoError(t, err, "RegisterAccount error")

	// Create a temporary file to store the wallet export.
	tmpFile, err := ioutil.TempFile("", "wallet_test_*.json")
	require.NoError(t, err, "Failed to create temp file")
	tmpFilePath := tmpFile.Name()
	tmpFile.Close()
	defer cleanupTempFile(tmpFilePath)

	// Export the wallet.
	err = w.Export(tmpFilePath)
	require.NoError(t, err, "Export failed")

	// Import the wallet.
	importedWallet, err := wallet.ImportWallet(tmpFilePath, logger)
	require.NoError(t, err, "ImportWallet failed")
	require.NotNil(t, importedWallet, "Imported wallet is nil")

	// Compare key fields.
	assert.Equal(t, w.ID, importedWallet.ID, "Wallet IDs do not match")
	assert.Equal(t, w.Nonce, importedWallet.Nonce, "Wallet nonce mismatch")

	// Compare serialized key pairs.
	kemOrig, err := crypto.SerializeKyberKeyPair(w.KEMKeyPair)
	require.NoError(t, err, "SerializeKyberKeyPair error on original wallet")
	kemImp, err := crypto.SerializeKyberKeyPair(importedWallet.KEMKeyPair)
	require.NoError(t, err, "SerializeKyberKeyPair error on imported wallet")
	assert.Equal(t, kemOrig, kemImp, "Kyber key pair mismatch after import")

	sigOrig, err := crypto.SerializeDilithiumKeyPair(w.SigKeyPair)
	require.NoError(t, err, "SerializeDilithiumKeyPair error on original wallet")
	sigImp, err := crypto.SerializeDilithiumKeyPair(importedWallet.SigKeyPair)
	require.NoError(t, err, "SerializeDilithiumKeyPair error on imported wallet")
	assert.Equal(t, sigOrig, sigImp, "Dilithium key pair mismatch after import")
}

// TestCreateTransaction verifies that a wallet can create a properly structured transaction.
func TestCreateTransaction(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err, "NewWallet error")

	err = w.RegisterAccount()
	require.NoError(t, err, "RegisterAccount error")

	// Create a transaction. Note that CreateTransaction now returns (tx, error)
	tx, err := w.CreateTransaction("receiver_account", 15.0, 0.05, []byte("Test payload"))
	require.NoError(t, err, "CreateTransaction returned error")
	require.NotNil(t, tx, "Expected non-nil transaction")
	assert.NotEmpty(t, tx.ID, "Transaction ID should not be empty")
	assert.Equal(t, w.ID, tx.Sender, "Sender should match wallet ID")
	assert.Equal(t, "receiver_account", tx.Receiver, "Receiver mismatch")
	assert.Equal(t, 15.0, tx.Amount, "Amount mismatch")
	assert.Equal(t, 0.05, tx.Fee, "Fee mismatch")
	// Since Timestamp is now an int64 (Unix timestamp), ensure it is positive.
	assert.True(t, tx.Timestamp > 0, "Timestamp should be positive")
	assert.Equal(t, w.Nonce, tx.Nonce, "Nonce should match wallet nonce")
	assert.NotEmpty(t, tx.Signature, "Transaction signature should not be empty")
}

// TestSubmitTransaction tests that a wallet can submit a transaction via a TransactionManager.
// Since the dummy TransactionManager verifies balance and nonce, we ensure that the wallet’s account is funded.
func TestSubmitTransaction(t *testing.T) {
	logger := getTestLogger()
	w, err := wallet.NewWallet(logger)
	require.NoError(t, err, "NewWallet error")

	err = w.RegisterAccount()
	require.NoError(t, err, "RegisterAccount error")

	// Ensure that the wallet's account has sufficient balance for a transaction.
	// (If RegisterAccount does not set a default balance, update it here.)
	err = common.UpdateAccountBalance(w.ID, 1000.0)
	require.NoError(t, err, "Failed to update account balance")

	// Create a dummy TransactionManager using a real TransactionPool and a dummy ledger.
	dummyPool := transaction.NewTransactionPool(10, time.Minute, 0.001, 10.0, false, time.Hour, nil, nil)
	dummyLedger := newDummyLedger()
	txMgr := transaction.NewTransactionManager(dummyPool, 0.001, false, dummyLedger)

	// Submit a transaction via the wallet’s SubmitTransaction method.
	tx, err := w.SubmitTransaction("receiver_account", 20.0, 0.1, []byte("Payload for submission"), txMgr)
	require.NoError(t, err, "SubmitTransaction returned error")
	require.NotNil(t, tx, "SubmitTransaction returned nil transaction")

	// Check that the transaction's sender matches the wallet.
	assert.Equal(t, w.ID, tx.Sender, "Transaction sender should match wallet ID")
	assert.NotEmpty(t, tx.Signature, "Transaction signature should not be empty")
}

package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"diamante/crypto"
	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIntegrationTest(t *testing.T) (*wallet.Config, *logrus.Logger, func()) {
	// Set up test environment
	os.Setenv("DIAMANTE_ENV", "development")
	os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Get config
	config, err := wallet.DefaultConfig()
	require.NoError(t, err, "Failed to get default config")

	cleanup := func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
		os.Unsetenv("DIAMANTE_WALLET_ENCRYPTION_KEY")
	}

	return config, logger, cleanup
}

func TestWalletTransactionFlow(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	t.Run("Basic wallet transaction creation", func(t *testing.T) {
		// Create sender and receiver wallets
		sender, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		receiver, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Create transaction
		transferAmount := 100.0
		fee := 1.0
		data := []byte("integration test transfer")

		tx, err := sender.CreateTransaction(receiver.ID, transferAmount, fee, data)
		require.NoError(t, err)
		require.NotNil(t, tx)

		// Verify transaction properties
		assert.Equal(t, sender.ID, tx.Sender)
		assert.Equal(t, receiver.ID, tx.Receiver)
		assert.Equal(t, transferAmount, tx.Amount)
		assert.Equal(t, fee, tx.Fee)
		assert.Equal(t, data, tx.Data)
		assert.NotEmpty(t, tx.Signature)
		assert.Greater(t, tx.Timestamp, int64(0))
		assert.Equal(t, 1, tx.Nonce)

		// Verify signature
		valid, err := crypto.VerifySignature(sender.SigKeyPair.PublicKey, []byte(tx.ID), tx.Signature)
		require.NoError(t, err)
		assert.True(t, valid)

		// Multiple transactions
		for i := 0; i < 5; i++ {
			tx, err := sender.CreateTransaction(receiver.ID, 10.0, 0.1, []byte("test"))
			assert.NoError(t, err)
			assert.Equal(t, i+2, tx.Nonce) // Nonce should increment
		}
	})
}

func TestMnemonicWalletRecovery(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	t.Run("Recover wallet from mnemonic", func(t *testing.T) {
		// Generate wallet with mnemonic
		walletWithMnemonic, err := wallet.GenerateWalletWithMnemonic(config, logger)
		require.NoError(t, err)

		originalWallet := walletWithMnemonic.Wallet
		mnemonic := walletWithMnemonic.Mnemonic

		// Register and fund original wallet
		err = originalWallet.RegisterAccount()
		require.NoError(t, err)

		err = originalWallet.FundWallet(500.0)
		require.NoError(t, err)

		// Create some transaction history
		for i := 0; i < 3; i++ {
			_, err = originalWallet.CreateTransaction("dummy-receiver", 10.0, 0.1, []byte("test"))
			require.NoError(t, err)
		}

		// Recover wallet from mnemonic
		recoveredWallet, err := wallet.GenerateWalletFromMnemonic(mnemonic, "", config, logger)
		require.NoError(t, err)

		// Verify recovered wallet matches original
		assert.Equal(t, originalWallet.ID, recoveredWallet.ID)
		assert.Equal(t, originalWallet.GetPublicKeyHex(), recoveredWallet.GetPublicKeyHex())

		// Test signing with recovered wallet
		message := "test message for recovered wallet"

		originalSig, err := originalWallet.SignMessage(message)
		require.NoError(t, err)

		recoveredSig, err := recoveredWallet.SignMessage(message)
		require.NoError(t, err)

		// Signatures should be verifiable by both wallets
		valid1, err := originalWallet.VerifySignature(message, recoveredSig)
		assert.NoError(t, err)
		assert.True(t, valid1)

		valid2, err := recoveredWallet.VerifySignature(message, originalSig)
		assert.NoError(t, err)
		assert.True(t, valid2)
	})

	t.Run("Mnemonic with passphrase", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		passphrase := "secure passphrase 123"

		// Create wallet with passphrase
		w1, err := wallet.GenerateWalletFromMnemonic(mnemonic, passphrase, config, logger)
		require.NoError(t, err)

		// Create wallet without passphrase
		w2, err := wallet.GenerateWalletFromMnemonic(mnemonic, "", config, logger)
		require.NoError(t, err)

		// Should be different wallets
		assert.NotEqual(t, w1.ID, w2.ID)
		assert.NotEqual(t, w1.GetPublicKeyHex(), w2.GetPublicKeyHex())

		// Recovery with same passphrase should match
		w3, err := wallet.GenerateWalletFromMnemonic(mnemonic, passphrase, config, logger)
		require.NoError(t, err)

		assert.Equal(t, w1.ID, w3.ID)
		assert.Equal(t, w1.GetPublicKeyHex(), w3.GetPublicKeyHex())
	})
}

func TestWalletManagerIntegration(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Complete wallet management workflow", func(t *testing.T) {
		// Create manager
		mgr, err := wallet.NewManager(&wallet.ManagerConfig{
			WalletDir: tempDir,
			Config:    config,
			Logger:    logger,
		})
		require.NoError(t, err)

		// Create multiple wallets
		w1, err := mgr.CreateWallet("business-wallet")
		require.NoError(t, err)

		w2, err := mgr.CreateWallet("personal-wallet")
		require.NoError(t, err)

		walletWithMnemonic, err := mgr.CreateWalletWithMnemonic("recovery-wallet")
		require.NoError(t, err)

		// Verify all wallets are tracked
		wallets, err := mgr.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 3, len(wallets))

		// Set default wallet
		err = mgr.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		defaultWallet, err := mgr.GetDefaultWallet()
		require.NoError(t, err)
		assert.Equal(t, w1.ID, defaultWallet.ID)

		// Test wallet persistence by creating new manager
		mgr2, err := wallet.NewManager(&wallet.ManagerConfig{
			WalletDir: tempDir,
			Config:    config,
			Logger:    logger,
		})
		require.NoError(t, err)

		// Verify wallets are loaded
		loadedWallets, err := mgr2.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 3, len(loadedWallets))

		// Verify default wallet is preserved
		loadedDefault, err := mgr2.GetDefaultWallet()
		require.NoError(t, err)
		assert.Equal(t, w1.ID, loadedDefault.ID)

		// Test wallet functionality
		retrievedW2, err := mgr2.GetWallet(w2.ID)
		require.NoError(t, err)

		message := "test message"
		signature, err := retrievedW2.SignMessage(message)
		require.NoError(t, err)

		valid, err := retrievedW2.VerifySignature(message, signature)
		require.NoError(t, err)
		assert.True(t, valid)

		// Test mnemonic recovery
		recoveredFromMnemonic, err := wallet.GenerateWalletFromMnemonic(
			walletWithMnemonic.Mnemonic, "", config, logger)
		require.NoError(t, err)
		assert.Equal(t, walletWithMnemonic.Wallet.ID, recoveredFromMnemonic.ID)
	})
}

func TestWalletExportImportIntegration(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Export import maintains functionality", func(t *testing.T) {
		// Create original wallet
		original, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Register and fund
		err = original.RegisterAccount()
		require.NoError(t, err)

		err = original.FundWallet(250.0)
		require.NoError(t, err)

		// Create some transaction history
		for i := 0; i < 5; i++ {
			_, err = original.CreateTransaction("test-receiver", 5.0, 0.1, []byte("test"))
			require.NoError(t, err)
		}

		// Export wallet
		exportPath := filepath.Join(tempDir, "exported_wallet.json")
		err = original.Export(exportPath)
		require.NoError(t, err)

		// Import wallet
		imported, err := wallet.ImportWalletWithConfig(exportPath, config, logger)
		require.NoError(t, err)

		// Verify imported wallet has same properties
		assert.Equal(t, original.ID, imported.ID)
		assert.Equal(t, original.Nonce, imported.Nonce)
		assert.Equal(t, original.GetPublicKeyHex(), imported.GetPublicKeyHex())

		// Test cryptographic operations
		message := "test message for exported wallet"

		// Sign with original
		sig1, err := original.SignMessage(message)
		require.NoError(t, err)

		// Sign with imported
		sig2, err := imported.SignMessage(message)
		require.NoError(t, err)

		// Cross-verify signatures
		valid1, err := imported.VerifySignature(message, sig1)
		require.NoError(t, err)
		assert.True(t, valid1)

		valid2, err := original.VerifySignature(message, sig2)
		require.NoError(t, err)
		assert.True(t, valid2)

		// Test new transactions with imported wallet
		tx, err := imported.CreateTransaction("new-receiver", 20.0, 0.2, []byte("new tx"))
		require.NoError(t, err)
		assert.Equal(t, 6, tx.Nonce) // Should continue nonce sequence

		// Verify signature on new transaction
		valid, err := crypto.VerifySignature(imported.SigKeyPair.PublicKey, []byte(tx.ID), tx.Signature)
		require.NoError(t, err)
		assert.True(t, valid)
	})
}

func TestMultiSignatureWorkflow(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	t.Run("Multi-wallet signing verification", func(t *testing.T) {
		// Create multiple wallets
		numWallets := 3
		wallets := make([]*wallet.Wallet, numWallets)

		for i := 0; i < numWallets; i++ {
			w, err := wallet.NewWalletWithConfig(config, logger)
			require.NoError(t, err)
			wallets[i] = w
		}

		// Test message
		message := "multi-signature test message"

		// Each wallet signs the message
		signatures := make([][]byte, numWallets)
		for i, w := range wallets {
			sig, err := w.SignMessage(message)
			require.NoError(t, err)
			signatures[i] = sig
		}

		// Verify each signature with corresponding wallet
		for i, w := range wallets {
			valid, err := w.VerifySignature(message, signatures[i])
			require.NoError(t, err)
			assert.True(t, valid, "Wallet %d failed to verify its own signature", i)
		}

		// Cross-verify: each signature should only be valid for its creator
		for i, sig := range signatures {
			for j, w := range wallets {
				valid, err := w.VerifySignature(message, sig)
				require.NoError(t, err)

				if i == j {
					assert.True(t, valid, "Wallet %d should verify signature %d", j, i)
				} else {
					assert.False(t, valid, "Wallet %d should not verify signature %d", j, i)
				}
			}
		}
	})
}

func TestConcurrentWalletOperations(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	t.Run("Concurrent wallet operations", func(t *testing.T) {
		numWallets := 5
		numTransactions := 10

		// Create wallets
		wallets := make([]*wallet.Wallet, numWallets)
		for i := 0; i < numWallets; i++ {
			w, err := wallet.NewWalletWithConfig(config, logger)
			require.NoError(t, err)

			// Register and fund
			err = w.RegisterAccount()
			require.NoError(t, err)

			err = w.FundWallet(1000.0)
			require.NoError(t, err)

			wallets[i] = w
		}

		// Concurrent operations
		var wg sync.WaitGroup

		// Each wallet creates transactions concurrently
		for i, w := range wallets {
			wg.Add(1)
			go func(walletIndex int, wallet *wallet.Wallet) {
				defer wg.Done()

				for j := 0; j < numTransactions; j++ {
					tx, err := wallet.CreateTransaction(
						"concurrent-receiver",
						float64(j+1),
						0.1,
						[]byte(fmt.Sprintf("tx-%d-%d", walletIndex, j)),
					)
					assert.NoError(t, err)
					assert.NotNil(t, tx)
					assert.Equal(t, j+1, tx.Nonce)
				}
			}(i, w)
		}

		wg.Wait()

		// Verify final nonces
		for i, w := range wallets {
			assert.Equal(t, numTransactions, w.Nonce, "Wallet %d has incorrect nonce", i)
		}

		// Verify each wallet can still sign messages
		message := "post-concurrent test"
		for i, w := range wallets {
			sig, err := w.SignMessage(message)
			assert.NoError(t, err)

			valid, err := w.VerifySignature(message, sig)
			assert.NoError(t, err)
			assert.True(t, valid, "Wallet %d signature invalid after concurrent operations", i)
		}
	})
}

func TestWalletBackupRestore(t *testing.T) {
	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Complete backup and restore workflow", func(t *testing.T) {
		// Create manager with wallets
		mgr, err := wallet.NewManager(&wallet.ManagerConfig{
			WalletDir: tempDir,
			Config:    config,
			Logger:    logger,
		})
		require.NoError(t, err)

		// Create test wallets
		w1, err := mgr.CreateWallet("backup-test-1")
		require.NoError(t, err)

		w2, err := mgr.CreateWallet("backup-test-2")
		require.NoError(t, err)

		walletWithMnemonic, err := mgr.CreateWalletWithMnemonic("backup-mnemonic")
		require.NoError(t, err)

		// Set default wallet
		err = mgr.SetDefaultWallet(w1.ID)
		require.NoError(t, err)

		// Register and fund wallets
		for _, w := range []*wallet.Wallet{w1, w2, walletWithMnemonic.Wallet} {
			err = w.RegisterAccount()
			require.NoError(t, err)

			err = w.FundWallet(100.0)
			require.NoError(t, err)
		}

		// Create transaction history
		_, err = w1.CreateTransaction(w2.ID, 10.0, 0.1, []byte("test tx"))
		require.NoError(t, err)

		// Backup all wallets
		backupPath := filepath.Join(tempDir, "full_backup.tar.gz")
		err = mgr.BackupAllWallets(backupPath)
		require.NoError(t, err)

		// Create new manager for restore
		restoreDir := filepath.Join(tempDir, "restore")
		restoreMgr, err := wallet.NewManager(&wallet.ManagerConfig{
			WalletDir: restoreDir,
			Config:    config,
			Logger:    logger,
		})
		require.NoError(t, err)

		// Initially no wallets
		wallets, err := restoreMgr.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 0, len(wallets))

		// Restore from backup
		err = restoreMgr.RestoreAllWallets(backupPath)
		require.NoError(t, err)

		// Verify all wallets restored
		restoredWallets, err := restoreMgr.ListWallets()
		require.NoError(t, err)
		assert.Equal(t, 3, len(restoredWallets))

		// Verify default wallet restored
		defaultWallet, err := restoreMgr.GetDefaultWallet()
		require.NoError(t, err)
		assert.Equal(t, w1.ID, defaultWallet.ID)

		// Verify wallet functionality
		restoredW1, err := restoreMgr.GetWallet(w1.ID)
		require.NoError(t, err)

		// Test signing
		message := "restore verification"
		sig, err := restoredW1.SignMessage(message)
		require.NoError(t, err)

		valid, err := restoredW1.VerifySignature(message, sig)
		require.NoError(t, err)
		assert.True(t, valid)

		// Verify nonce is preserved
		assert.Equal(t, w1.Nonce, restoredW1.Nonce)

		// Test mnemonic recovery still works
		restoredMnemonicWallet, err := wallet.GenerateWalletFromMnemonic(
			walletWithMnemonic.Mnemonic, "", config, logger)
		require.NoError(t, err)
		assert.Equal(t, walletWithMnemonic.Wallet.ID, restoredMnemonicWallet.ID)
	})
}

func TestWalletSecurityIntegration(t *testing.T) {
	tempDir := t.TempDir()
	logger := logrus.New()

	t.Run("Production-like security configuration", func(t *testing.T) {
		// Set production environment
		os.Setenv("DIAMANTE_ENV", "production")
		// Use a strong encryption key
		strongKey := "a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd"
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", strongKey)

		defer func() {
			os.Unsetenv("DIAMANTE_ENV")
			os.Unsetenv("DIAMANTE_WALLET_ENCRYPTION_KEY")
		}()

		// Get production config
		config, err := wallet.DefaultConfig()
		require.NoError(t, err)

		// Create wallet with production config
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Export wallet (should use strong encryption)
		exportPath := filepath.Join(tempDir, "production_wallet.json")
		err = w.Export(exportPath)
		require.NoError(t, err)

		// Verify file is encrypted by checking it doesn't contain plain text keys
		data, err := os.ReadFile(exportPath)
		require.NoError(t, err)

		// Parse JSON to verify structure
		var wf wallet.WalletFile
		err = json.Unmarshal(data, &wf)
		require.NoError(t, err)

		// Private keys should be encrypted (different from original)
		assert.NotEqual(t, w.KEMKeyPair.PrivateKey, wf.KEMKeyPair.PrivateKey)
		assert.NotEqual(t, w.SigKeyPair.PrivateKey, wf.SigKeyPair.PrivateKey)

		// But public keys should be preserved
		assert.Equal(t, w.KEMKeyPair.PublicKey, wf.KEMKeyPair.PublicKey)
		assert.Equal(t, w.SigKeyPair.PublicKey, wf.SigKeyPair.PublicKey)

		// Import should work with same config
		imported, err := wallet.ImportWalletWithConfig(exportPath, config, logger)
		require.NoError(t, err)

		// Verify functionality is preserved
		message := "production security test"
		sig1, err := w.SignMessage(message)
		require.NoError(t, err)

		sig2, err := imported.SignMessage(message)
		require.NoError(t, err)

		// Cross-verification should work
		valid1, err := imported.VerifySignature(message, sig1)
		require.NoError(t, err)
		assert.True(t, valid1)

		valid2, err := w.VerifySignature(message, sig2)
		require.NoError(t, err)
		assert.True(t, valid2)
	})
}

func TestLongRunningWalletOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	config, logger, cleanup := setupIntegrationTest(t)
	defer cleanup()

	t.Run("Extended wallet usage simulation", func(t *testing.T) {
		// Create wallet
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Register and fund
		err = w.RegisterAccount()
		require.NoError(t, err)

		err = w.FundWallet(10000.0)
		require.NoError(t, err)

		// Simulate extended usage
		numOperations := 1000

		for i := 0; i < numOperations; i++ {
			// Create transaction
			tx, err := w.CreateTransaction(
				"long-running-receiver",
				1.0,
				0.01,
				[]byte(fmt.Sprintf("operation-%d", i)),
			)
			assert.NoError(t, err)
			assert.Equal(t, i+1, tx.Nonce)

			// Sign message
			message := fmt.Sprintf("message-%d", i)
			sig, err := w.SignMessage(message)
			assert.NoError(t, err)

			// Verify signature
			valid, err := w.VerifySignature(message, sig)
			assert.NoError(t, err)
			assert.True(t, valid)

			// Periodic checks
			if i%100 == 0 {
				// Check balance
				balance, err := w.GetBalance()
				assert.NoError(t, err)
				assert.Greater(t, balance, 0.0)

				// Check public key consistency
				pubKey := w.GetPublicKeyHex()
				assert.NotEmpty(t, pubKey)

				logger.Infof("Completed %d operations", i+1)
			}
		}

		// Final verification
		assert.Equal(t, numOperations, w.Nonce)

		balance, err := w.GetBalance()
		require.NoError(t, err)
		assert.Greater(t, balance, 0.0)

		// Wallet should still be fully functional
		finalTx, err := w.CreateTransaction("final-receiver", 1.0, 0.01, []byte("final"))
		require.NoError(t, err)
		assert.Equal(t, numOperations+1, finalTx.Nonce)
	})
}

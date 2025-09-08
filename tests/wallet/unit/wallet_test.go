package unit

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestEnvironment sets up a test environment with proper configuration
func setupTestEnvironment(t *testing.T) (*wallet.Config, *logrus.Logger, func()) {
	// Set up test environment variables
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

func TestWalletCreation(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Create new wallet", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)
		require.NotNil(t, w)

		// Verify wallet has all required components
		assert.NotEmpty(t, w.ID)
		assert.Equal(t, 0, w.Nonce)
		assert.NotNil(t, w.KEMKeyPair)
		assert.NotNil(t, w.SigKeyPair)
		assert.NotNil(t, w.CryptoManager)

		// Verify key pairs have correct sizes
		assert.NotEmpty(t, w.KEMKeyPair.PublicKey)
		assert.NotEmpty(t, w.KEMKeyPair.PrivateKey)
		assert.NotEmpty(t, w.SigKeyPair.PublicKey)
		assert.NotEmpty(t, w.SigKeyPair.PrivateKey)
	})

	t.Run("Create wallet with nil config", func(t *testing.T) {
		_, err := wallet.NewWalletWithConfig(nil, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "config cannot be nil")
	})
}

func TestWalletAccountRegistration(t *testing.T) {
	t.Skip("Account registration requires global state setup which is not available in unit tests")
}

func TestWalletBalance(t *testing.T) {
	t.Skip("Balance operations require account registration which needs global state setup")
}

func TestTransactionCreation(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Create valid transaction", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		receiver := "receiver123"
		amount := 50.0
		fee := 0.1
		data := []byte("test data")

		tx, err := w.CreateTransaction(receiver, amount, fee, data)
		require.NoError(t, err)
		require.NotNil(t, tx)

		// Verify transaction fields
		assert.NotEmpty(t, tx.ID)
		assert.Equal(t, w.ID, tx.Sender)
		assert.Equal(t, receiver, tx.Receiver)
		assert.Equal(t, amount, tx.Amount)
		assert.Equal(t, fee, tx.Fee)
		assert.Equal(t, 1, tx.Nonce)
		assert.Equal(t, data, tx.Data)
		assert.NotEmpty(t, tx.Signature)
		assert.Greater(t, tx.Timestamp, int64(0))

		// Verify nonce was incremented
		assert.Equal(t, 1, w.Nonce)
	})

	t.Run("Create transaction with empty receiver", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		_, err = w.CreateTransaction("", 10, 0.1, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "receiver cannot be empty")
	})

	t.Run("Create transaction with negative amount", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		_, err = w.CreateTransaction("receiver", -10, 0.1, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "amount cannot be negative")
	})

	t.Run("Create transaction with negative fee", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		_, err = w.CreateTransaction("receiver", 10, -0.1, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fee cannot be negative")
	})

	t.Run("Nonce rollback on signature failure", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Save original nonce
		originalNonce := w.Nonce

		// Corrupt the private key to cause signature failure
		originalPrivKey := w.SigKeyPair.PrivateKey
		w.SigKeyPair.PrivateKey = []byte("invalid")

		_, err = w.CreateTransaction("receiver", 10, 0.1, nil)
		assert.Error(t, err)

		// Verify nonce was rolled back
		assert.Equal(t, originalNonce, w.Nonce)

		// Restore private key
		w.SigKeyPair.PrivateKey = originalPrivKey
	})
}

func TestMessageSigning(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Sign and verify message", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		message := "Hello, Diamante blockchain!"

		// Sign message
		signature, err := w.SignMessage(message)
		assert.NoError(t, err)
		assert.NotEmpty(t, signature)

		// Verify signature
		valid, err := w.VerifySignature(message, signature)
		assert.NoError(t, err)
		assert.True(t, valid)

		// Verify with wrong message
		valid, err = w.VerifySignature("Wrong message", signature)
		assert.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("Sign empty message", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		_, err = w.SignMessage("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message cannot be empty")
	})

	t.Run("Verify with empty signature", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		_, err = w.VerifySignature("message", []byte{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "signature cannot be empty")
	})
}

func TestWalletExportImport(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Export and import wallet", func(t *testing.T) {
		// Create original wallet
		originalWallet, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Create some transactions to increment nonce
		for i := 0; i < 5; i++ {
			_, err = originalWallet.CreateTransaction("receiver", 10, 0.1, nil)
			require.NoError(t, err)
		}

		// Export wallet
		walletPath := filepath.Join(tempDir, "test_wallet.json")
		err = originalWallet.Export(walletPath)
		assert.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(walletPath)
		assert.NoError(t, err)

		// Import wallet
		importedWallet, err := wallet.ImportWalletWithConfig(walletPath, config, logger)
		require.NoError(t, err)
		require.NotNil(t, importedWallet)

		// Verify imported wallet matches original
		assert.Equal(t, originalWallet.ID, importedWallet.ID)
		assert.Equal(t, originalWallet.Nonce, importedWallet.Nonce)
		assert.Equal(t, originalWallet.GetPublicKeyHex(), importedWallet.GetPublicKeyHex())

		// Verify keys work by signing a message
		message := "Test message"
		sig1, err := originalWallet.SignMessage(message)
		require.NoError(t, err)

		sig2, err := importedWallet.SignMessage(message)
		require.NoError(t, err)

		// Signatures should be deterministic for same message
		valid1, err := originalWallet.VerifySignature(message, sig2)
		assert.NoError(t, err)
		assert.True(t, valid1)

		valid2, err := importedWallet.VerifySignature(message, sig1)
		assert.NoError(t, err)
		assert.True(t, valid2)
	})

	t.Run("Import non-existent file", func(t *testing.T) {
		_, err := wallet.ImportWalletWithConfig("/non/existent/file.json", config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read wallet file")
	})

	t.Run("Import corrupted file", func(t *testing.T) {
		corruptPath := filepath.Join(tempDir, "corrupt_wallet.json")
		err := os.WriteFile(corruptPath, []byte("not valid json"), 0600)
		require.NoError(t, err)

		_, err = wallet.ImportWalletWithConfig(corruptPath, config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal wallet JSON")
	})

	t.Run("Import wallet with missing key pairs", func(t *testing.T) {
		invalidWallet := wallet.WalletFile{
			ID:    "test-id",
			Nonce: 0,
			// Missing key pairs
		}

		data, err := json.Marshal(invalidWallet)
		require.NoError(t, err)

		invalidPath := filepath.Join(tempDir, "invalid_wallet.json")
		err = os.WriteFile(invalidPath, data, 0600)
		require.NoError(t, err)

		_, err = wallet.ImportWalletWithConfig(invalidPath, config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing key pairs")
	})
}

func TestPublicKeyRetrieval(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Get public key hex", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		pubKeyHex := w.GetPublicKeyHex()
		assert.NotEmpty(t, pubKeyHex)

		// Verify it's valid hex
		pubKeyBytes, err := hex.DecodeString(pubKeyHex)
		assert.NoError(t, err)
		assert.Equal(t, w.SigKeyPair.PublicKey, pubKeyBytes)
	})
}

func TestWalletConcurrency(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Concurrent transaction creation", func(t *testing.T) {
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		numGoroutines := 10
		done := make(chan bool, numGoroutines)

		// Create transactions concurrently
		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				tx, err := w.CreateTransaction("receiver", float64(index), 0.1, nil)
				assert.NoError(t, err)
				assert.NotNil(t, tx)
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify nonce is correct
		assert.Equal(t, numGoroutines, w.Nonce)
	})
}

func TestTransactionSubmission(t *testing.T) {
	t.Skip("Transaction submission requires TransactionManager setup which needs additional dependencies")
}

func TestWalletEncryptionKey(t *testing.T) {
	cleanup := func() {
		os.Unsetenv("DIAMANTE_ENV")
		os.Unsetenv("DIAMANTE_ALLOW_DEV_KEY")
		os.Unsetenv("DIAMANTE_WALLET_ENCRYPTION_KEY")
	}
	defer cleanup()

	t.Run("Production environment without key", func(t *testing.T) {
		os.Setenv("DIAMANTE_ENV", "production")

		_, err := wallet.DefaultConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DIAMANTE_WALLET_ENCRYPTION_KEY must be set in production")
	})

	t.Run("Production environment with weak key", func(t *testing.T) {
		os.Setenv("DIAMANTE_ENV", "production")
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

		_, err := wallet.DefaultConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "weak/development encryption key detected in production")
	})

	t.Run("Valid custom encryption key", func(t *testing.T) {
		os.Setenv("DIAMANTE_ENV", "development")
		os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
		// Generate a random 64-char hex key
		validKey := "a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd"
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", validKey)

		config, err := wallet.DefaultConfig()
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Len(t, config.EncryptionKey, 32)
	})

	t.Run("Invalid hex key", func(t *testing.T) {
		os.Setenv("DIAMANTE_ENV", "development")
		os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", "invalid-hex-key")

		_, err := wallet.DefaultConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode DIAMANTE_WALLET_ENCRYPTION_KEY")
	})

	t.Run("Wrong key length", func(t *testing.T) {
		os.Setenv("DIAMANTE_ENV", "development")
		os.Setenv("DIAMANTE_ALLOW_DEV_KEY", "true")
		os.Setenv("DIAMANTE_WALLET_ENCRYPTION_KEY", "abcd") // Too short

		_, err := wallet.DefaultConfig()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be exactly 64 hex characters")
	})
}

// TestWalletPrivateKeyEncryption verifies that private keys are properly encrypted
func TestWalletPrivateKeyEncryption(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	tempDir := t.TempDir()

	t.Run("Private keys are encrypted in export", func(t *testing.T) {
		// Create wallet
		w, err := wallet.NewWalletWithConfig(config, logger)
		require.NoError(t, err)

		// Store original private keys
		originalKEMPrivKey := make([]byte, len(w.KEMKeyPair.PrivateKey))
		copy(originalKEMPrivKey, w.KEMKeyPair.PrivateKey)

		originalSigPrivKey := make([]byte, len(w.SigKeyPair.PrivateKey))
		copy(originalSigPrivKey, w.SigKeyPair.PrivateKey)

		// Export wallet
		walletPath := filepath.Join(tempDir, "encrypted_wallet.json")
		err = w.Export(walletPath)
		require.NoError(t, err)

		// Read exported file
		data, err := os.ReadFile(walletPath)
		require.NoError(t, err)

		var wf wallet.WalletFile
		err = json.Unmarshal(data, &wf)
		require.NoError(t, err)

		// Verify private keys are encrypted (different from original)
		assert.NotEqual(t, originalKEMPrivKey, wf.KEMKeyPair.PrivateKey)
		assert.NotEqual(t, originalSigPrivKey, wf.SigKeyPair.PrivateKey)

		// Verify public keys are not encrypted (same as original)
		assert.Equal(t, w.KEMKeyPair.PublicKey, wf.KEMKeyPair.PublicKey)
		assert.Equal(t, w.SigKeyPair.PublicKey, wf.SigKeyPair.PublicKey)
	})
}

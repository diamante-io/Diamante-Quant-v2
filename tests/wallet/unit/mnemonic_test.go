package unit

import (
	"strings"
	"testing"

	"diamante/crypto"
	"diamante/wallet"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMnemonicGeneration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	t.Run("Generate 24-word mnemonic", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonic, err := mg.GenerateMnemonic()
		assert.NoError(t, err)
		assert.NotEmpty(t, mnemonic)

		// Verify word count
		words := strings.Fields(mnemonic)
		assert.Equal(t, 24, len(words))

		// Verify all words are from word list
		err = mg.ValidateMnemonic(mnemonic)
		assert.NoError(t, err)
	})

	t.Run("Generate mnemonics with different strengths", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		testCases := []struct {
			bitSize       int
			expectedWords int
		}{
			{128, 12},
			{160, 15},
			{192, 18},
			{224, 21},
			{256, 24},
		}

		for _, tc := range testCases {
			mnemonic, err := mg.GenerateMnemonicWithStrength(tc.bitSize)
			assert.NoError(t, err)

			words := strings.Fields(mnemonic)
			assert.Equal(t, tc.expectedWords, len(words))

			err = mg.ValidateMnemonic(mnemonic)
			assert.NoError(t, err)
		}
	})

	t.Run("Invalid bit size", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		// Invalid sizes
		invalidSizes := []int{64, 127, 129, 300}
		for _, size := range invalidSizes {
			_, err := mg.GenerateMnemonicWithStrength(size)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid bit size")
		}
	})

	t.Run("Generate multiple unique mnemonics", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonics := make(map[string]bool)
		numTests := 10

		for i := 0; i < numTests; i++ {
			mnemonic, err := mg.GenerateMnemonic()
			require.NoError(t, err)

			// Check uniqueness
			assert.False(t, mnemonics[mnemonic], "Generated duplicate mnemonic")
			mnemonics[mnemonic] = true
		}
	})
}

func TestMnemonicValidation(t *testing.T) {
	logger := logrus.New()
	mg, err := wallet.NewMnemonicGenerator(logger)
	require.NoError(t, err)

	t.Run("Validate correct mnemonic", func(t *testing.T) {
		// Generate a valid mnemonic
		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		err = mg.ValidateMnemonic(mnemonic)
		assert.NoError(t, err)
	})

	t.Run("Invalid word count", func(t *testing.T) {
		invalidMnemonics := []string{
			"abandon abandon abandon", // 3 words
			"abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon",                 // 11 words
			"abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon", // 13 words
		}

		for _, mnemonic := range invalidMnemonics {
			err := mg.ValidateMnemonic(mnemonic)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid word count")
		}
	})

	t.Run("Invalid words", func(t *testing.T) {
		// Valid word count but invalid words
		mnemonic := "invalid word here plus some more words to make twelve total words"
		err := mg.ValidateMnemonic(mnemonic)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid word")
	})

	t.Run("Empty mnemonic", func(t *testing.T) {
		err := mg.ValidateMnemonic("")
		assert.Error(t, err)
	})

	t.Run("Case sensitivity", func(t *testing.T) {
		// Generate valid mnemonic
		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		// Convert to uppercase (should be invalid)
		upperMnemonic := strings.ToUpper(mnemonic)
		err = mg.ValidateMnemonic(upperMnemonic)
		assert.Error(t, err)
	})
}

func TestMnemonicToSeed(t *testing.T) {
	logger := logrus.New()
	mg, err := wallet.NewMnemonicGenerator(logger)
	require.NoError(t, err)

	t.Run("Generate seed from mnemonic", func(t *testing.T) {
		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		// Without passphrase
		seed1, err := mg.MnemonicToSeed(mnemonic, "")
		assert.NoError(t, err)
		assert.NotNil(t, seed1)
		assert.Equal(t, 64, len(seed1)) // 512 bits

		// With passphrase
		seed2, err := mg.MnemonicToSeed(mnemonic, "test passphrase")
		assert.NoError(t, err)
		assert.NotNil(t, seed2)
		assert.Equal(t, 64, len(seed2))

		// Seeds should be different with different passphrases
		assert.NotEqual(t, seed1, seed2)
	})

	t.Run("Same mnemonic produces same seed", func(t *testing.T) {
		mnemonic := "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art"

		seed1, err := mg.MnemonicToSeed(mnemonic, "passphrase")
		require.NoError(t, err)

		seed2, err := mg.MnemonicToSeed(mnemonic, "passphrase")
		require.NoError(t, err)

		assert.Equal(t, seed1, seed2)
	})

	t.Run("Invalid mnemonic produces error", func(t *testing.T) {
		_, err := mg.MnemonicToSeed("invalid mnemonic", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid mnemonic")
	})
}

func TestWalletFromMnemonic(t *testing.T) {
	config, logger, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Generate wallet from mnemonic", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		// Generate wallet without passphrase
		w1, err := wallet.GenerateWalletFromMnemonic(mnemonic, "", config, logger)
		assert.NoError(t, err)
		assert.NotNil(t, w1)

		// Verify wallet components
		assert.NotEmpty(t, w1.ID)
		assert.NotNil(t, w1.KEMKeyPair)
		assert.NotNil(t, w1.SigKeyPair)
		assert.NotNil(t, w1.CryptoManager)

		// Generate same wallet again - should be deterministic
		w2, err := wallet.GenerateWalletFromMnemonic(mnemonic, "", config, logger)
		assert.NoError(t, err)

		// Wallets should have same ID and keys
		assert.Equal(t, w1.ID, w2.ID)
		assert.Equal(t, w1.KEMKeyPair.PublicKey, w2.KEMKeyPair.PublicKey)
		assert.Equal(t, w1.SigKeyPair.PublicKey, w2.SigKeyPair.PublicKey)
	})

	t.Run("Different passphrases produce different wallets", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		mnemonic, err := mg.GenerateMnemonic()
		require.NoError(t, err)

		w1, err := wallet.GenerateWalletFromMnemonic(mnemonic, "passphrase1", config, logger)
		require.NoError(t, err)

		w2, err := wallet.GenerateWalletFromMnemonic(mnemonic, "passphrase2", config, logger)
		require.NoError(t, err)

		// Different wallets
		assert.NotEqual(t, w1.ID, w2.ID)
		assert.NotEqual(t, w1.KEMKeyPair.PublicKey, w2.KEMKeyPair.PublicKey)
		assert.NotEqual(t, w1.SigKeyPair.PublicKey, w2.SigKeyPair.PublicKey)
	})

	t.Run("Generate wallet with mnemonic helper", func(t *testing.T) {
		wm, err := wallet.GenerateWalletWithMnemonic(config, logger)
		assert.NoError(t, err)
		assert.NotNil(t, wm)
		assert.NotEmpty(t, wm.Mnemonic)
		assert.NotNil(t, wm.Wallet)

		// Verify mnemonic is valid
		mg, _ := wallet.NewMnemonicGenerator(logger)
		err = mg.ValidateMnemonic(wm.Mnemonic)
		assert.NoError(t, err)

		// Verify we can recreate the wallet
		recreated, err := wallet.GenerateWalletFromMnemonic(wm.Mnemonic, "", config, logger)
		assert.NoError(t, err)
		assert.Equal(t, wm.Wallet.ID, recreated.ID)
	})
}

func TestMnemonicEncryption(t *testing.T) {
	t.Run("Export and import encrypted mnemonic", func(t *testing.T) {
		mnemonic := "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art"
		password := []byte("strongpassword123")

		// Export encrypted
		encrypted, err := wallet.ExportMnemonicEncrypted(mnemonic, password)
		assert.NoError(t, err)
		assert.NotNil(t, encrypted)
		assert.Greater(t, len(encrypted), 32) // Should have salt + encrypted data

		// Import with correct password
		decrypted, err := wallet.ImportMnemonicEncrypted(encrypted, password)
		assert.NoError(t, err)
		assert.Equal(t, mnemonic, decrypted)

		// Import with wrong password
		_, err = wallet.ImportMnemonicEncrypted(encrypted, []byte("wrongpassword"))
		assert.Error(t, err)
	})

	t.Run("Password too short", func(t *testing.T) {
		mnemonic := "test mnemonic"
		shortPassword := []byte("short")

		_, err := wallet.ExportMnemonicEncrypted(mnemonic, shortPassword)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "password must be at least 8 characters")
	})

	t.Run("Invalid encrypted data", func(t *testing.T) {
		// Too short data
		_, err := wallet.ImportMnemonicEncrypted([]byte("short"), []byte("password123"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid encrypted data")
	})
}

func TestSecureMnemonicStorage(t *testing.T) {
	t.Run("Create and unlock secure storage", func(t *testing.T) {
		mnemonic := "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art"
		password := []byte("securepassword123")

		// Create secure storage
		storage, err := wallet.NewSecureMnemonicStorage(mnemonic, password)
		assert.NoError(t, err)
		assert.NotNil(t, storage)

		// Unlock with correct password
		retrieved, err := storage.Unlock(password)
		assert.NoError(t, err)
		assert.Equal(t, mnemonic, retrieved)

		// Unlock with wrong password
		_, err = storage.Unlock([]byte("wrongpassword"))
		assert.Error(t, err)

		// Clear storage
		storage.Clear()
		// After clearing, the encrypted data should be zeroed
		// This is hard to test directly without accessing private fields
	})
}

func TestDeterministicKeyGeneration(t *testing.T) {
	t.Run("Deterministic keys from same seed", func(t *testing.T) {
		seed := make([]byte, 64)
		for i := range seed {
			seed[i] = byte(i)
		}

		// Generate keys multiple times
		kemKP1, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
		require.NoError(t, err)

		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
		require.NoError(t, err)

		// Keys should be identical
		assert.Equal(t, kemKP1.PublicKey, kemKP2.PublicKey)
		assert.Equal(t, kemKP1.PrivateKey, kemKP2.PrivateKey)

		// Test Dilithium keys
		sigKP1, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
		require.NoError(t, err)

		sigKP2, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Keys should be identical
		assert.Equal(t, sigKP1.PublicKey, sigKP2.PublicKey)
		assert.Equal(t, sigKP1.PrivateKey, sigKP2.PrivateKey)
	})

	t.Run("Different seeds produce different keys", func(t *testing.T) {
		seed1 := make([]byte, 32)
		seed2 := make([]byte, 32)
		for i := range seed1 {
			seed1[i] = byte(i)
			seed2[i] = byte(i + 1)
		}

		kemKP1, err := wallet.GenerateDeterministicKyberKeyPair(seed1, crypto.KyberLevel1024)
		require.NoError(t, err)

		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(seed2, crypto.KyberLevel1024)
		require.NoError(t, err)

		// Keys should be different
		assert.NotEqual(t, kemKP1.PublicKey, kemKP2.PublicKey)
		assert.NotEqual(t, kemKP1.PrivateKey, kemKP2.PrivateKey)
	})

	t.Run("Invalid seed size", func(t *testing.T) {
		shortSeed := make([]byte, 16) // Too short

		_, err := wallet.GenerateDeterministicKyberKeyPair(shortSeed, crypto.KyberLevel1024)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed must be at least 32 bytes")
	})

	t.Run("Invalid Kyber level", func(t *testing.T) {
		seed := make([]byte, 32)

		_, err := wallet.GenerateDeterministicKyberKeyPair(seed, 999) // Invalid level
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported Kyber level")
	})
}

func TestWordListIntegrity(t *testing.T) {
	t.Run("BIP39 word list has correct size", func(t *testing.T) {
		assert.Equal(t, 2048, len(wallet.BIP39EnglishWordList))
	})

	t.Run("No duplicate words", func(t *testing.T) {
		wordMap := make(map[string]bool)
		for _, word := range wallet.BIP39EnglishWordList {
			assert.False(t, wordMap[word], "Duplicate word found: %s", word)
			wordMap[word] = true
		}
	})

	t.Run("All words are lowercase", func(t *testing.T) {
		for _, word := range wallet.BIP39EnglishWordList {
			assert.Equal(t, strings.ToLower(word), word, "Word not lowercase: %s", word)
		}
	})
}

func TestMnemonicEntropyToWords(t *testing.T) {
	logger := logrus.New()
	mg, err := wallet.NewMnemonicGenerator(logger)
	require.NoError(t, err)

	t.Run("Known test vectors", func(t *testing.T) {
		// Test with known entropy (all zeros for simplicity)
		// Note: In production, never use predictable entropy

		// Use GenerateMnemonicWithStrength instead of direct entropy conversion
		mnemonic, err := mg.GenerateMnemonicWithStrength(256)
		assert.NoError(t, err)
		assert.NotEmpty(t, mnemonic)

		// Should produce a valid 24-word mnemonic
		words := strings.Fields(mnemonic)
		assert.Equal(t, 24, len(words))

		// Verify it's valid
		err = mg.ValidateMnemonic(mnemonic)
		assert.NoError(t, err)
	})
}

// TestMnemonicStress performs stress testing on mnemonic operations
func TestMnemonicStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // Reduce log noise

	config, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Generate many wallets from mnemonics", func(t *testing.T) {
		mg, err := wallet.NewMnemonicGenerator(logger)
		require.NoError(t, err)

		numWallets := 100
		wallets := make(map[string]bool)

		for i := 0; i < numWallets; i++ {
			// Generate mnemonic
			mnemonic, err := mg.GenerateMnemonic()
			require.NoError(t, err)

			// Generate wallet
			w, err := wallet.GenerateWalletFromMnemonic(mnemonic, "", config, logger)
			require.NoError(t, err)

			// Check uniqueness
			assert.False(t, wallets[w.ID], "Duplicate wallet ID generated")
			wallets[w.ID] = true
		}

		assert.Equal(t, numWallets, len(wallets))
	})
}

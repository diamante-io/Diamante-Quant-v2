package unit

import (
	"testing"

	"diamante/crypto"
	"diamante/wallet"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeterministicReader(t *testing.T) {
	t.Run("Deterministic output", func(t *testing.T) {
		seed := []byte("test seed for deterministic reader")

		reader1 := wallet.NewDeterministicReader(seed)
		reader2 := wallet.NewDeterministicReader(seed)

		// Read same amount from both readers
		buf1 := make([]byte, 256)
		buf2 := make([]byte, 256)

		n1, err1 := reader1.Read(buf1)
		require.NoError(t, err1)
		assert.Equal(t, 256, n1)

		n2, err2 := reader2.Read(buf2)
		require.NoError(t, err2)
		assert.Equal(t, 256, n2)

		// Outputs should be identical
		assert.Equal(t, buf1, buf2)
	})

	t.Run("Sequential reads are different", func(t *testing.T) {
		seed := []byte("test seed for sequential reads")
		reader := wallet.NewDeterministicReader(seed)

		buf1 := make([]byte, 32)
		buf2 := make([]byte, 32)

		n1, err1 := reader.Read(buf1)
		require.NoError(t, err1)
		assert.Equal(t, 32, n1)

		n2, err2 := reader.Read(buf2)
		require.NoError(t, err2)
		assert.Equal(t, 32, n2)

		// Sequential reads should produce different output
		assert.NotEqual(t, buf1, buf2)
	})

	t.Run("Different seeds produce different output", func(t *testing.T) {
		seed1 := []byte("first seed")
		seed2 := []byte("second seed")

		reader1 := wallet.NewDeterministicReader(seed1)
		reader2 := wallet.NewDeterministicReader(seed2)

		buf1 := make([]byte, 64)
		buf2 := make([]byte, 64)

		reader1.Read(buf1)
		reader2.Read(buf2)

		assert.NotEqual(t, buf1, buf2)
	})

	t.Run("Empty buffer", func(t *testing.T) {
		seed := []byte("test seed")
		reader := wallet.NewDeterministicReader(seed)

		buf := make([]byte, 0)
		n, err := reader.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestDeterministicKyberKeyGeneration(t *testing.T) {
	t.Run("Generate deterministic Kyber keys", func(t *testing.T) {
		seed := make([]byte, 32)
		for i := range seed {
			seed[i] = byte(i)
		}

		// Generate key pair multiple times
		kp1, err := wallet.GenerateDeterministicKyberKeyPair(seed, crypto.KyberLevel1024)
		require.NoError(t, err)

		kp2, err := wallet.GenerateDeterministicKyberKeyPair(seed, crypto.KyberLevel1024)
		require.NoError(t, err)

		// Keys should be identical
		assert.Equal(t, kp1.PublicKey, kp2.PublicKey)
		assert.Equal(t, kp1.PrivateKey, kp2.PrivateKey)

		// Verify key sizes for Kyber1024
		assert.Equal(t, 1568, len(kp1.PublicKey))
		assert.Equal(t, 3168, len(kp1.PrivateKey))
	})

	t.Run("Different Kyber levels", func(t *testing.T) {
		seed := make([]byte, 32)
		for i := range seed {
			seed[i] = byte(i)
		}

		// Test different Kyber levels
		levels := []struct {
			level       int
			pubKeySize  int
			privKeySize int
		}{
			{crypto.KyberLevel512, 800, 1632},
			{crypto.KyberLevel768, 1184, 2400},
			{crypto.KyberLevel1024, 1568, 3168},
		}

		for _, level := range levels {
			kp, err := wallet.GenerateDeterministicKyberKeyPair(seed, level.level)
			require.NoError(t, err)

			assert.Equal(t, level.pubKeySize, len(kp.PublicKey))
			assert.Equal(t, level.privKeySize, len(kp.PrivateKey))
		}
	})

	t.Run("Different seeds produce different keys", func(t *testing.T) {
		seed1 := make([]byte, 32)
		seed2 := make([]byte, 32)

		for i := range seed1 {
			seed1[i] = byte(i)
			seed2[i] = byte(i + 1)
		}

		kp1, err := wallet.GenerateDeterministicKyberKeyPair(seed1, crypto.KyberLevel1024)
		require.NoError(t, err)

		kp2, err := wallet.GenerateDeterministicKyberKeyPair(seed2, crypto.KyberLevel1024)
		require.NoError(t, err)

		// Keys should be different
		assert.NotEqual(t, kp1.PublicKey, kp2.PublicKey)
		assert.NotEqual(t, kp1.PrivateKey, kp2.PrivateKey)
	})

	t.Run("Invalid seed length", func(t *testing.T) {
		shortSeed := make([]byte, 16) // Too short

		_, err := wallet.GenerateDeterministicKyberKeyPair(shortSeed, crypto.KyberLevel1024)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed must be at least 32 bytes")
	})

	t.Run("Invalid Kyber level", func(t *testing.T) {
		seed := make([]byte, 32)

		_, err := wallet.GenerateDeterministicKyberKeyPair(seed, 999)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported Kyber level")
	})
}

func TestDeterministicDilithiumKeyGeneration(t *testing.T) {
	t.Run("Generate deterministic Dilithium keys", func(t *testing.T) {
		seed := make([]byte, 32)
		for i := range seed {
			seed[i] = byte(i)
		}

		// Generate key pair multiple times
		kp1, err := wallet.GenerateDeterministicDilithiumKeyPair(seed, crypto.DilithiumLevel3)
		require.NoError(t, err)

		kp2, err := wallet.GenerateDeterministicDilithiumKeyPair(seed, crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Keys should be identical
		assert.Equal(t, kp1.PublicKey, kp2.PublicKey)
		assert.Equal(t, kp1.PrivateKey, kp2.PrivateKey)

		// Verify keys are not empty
		assert.NotEmpty(t, kp1.PublicKey)
		assert.NotEmpty(t, kp1.PrivateKey)
	})

	t.Run("Different Dilithium levels", func(t *testing.T) {
		seed := make([]byte, 32)
		for i := range seed {
			seed[i] = byte(i)
		}

		// Test different Dilithium levels
		levels := []int{
			crypto.DilithiumLevel2,
			crypto.DilithiumLevel3,
			crypto.DilithiumLevel5,
		}

		for _, level := range levels {
			kp, err := wallet.GenerateDeterministicDilithiumKeyPair(seed, level)
			require.NoError(t, err)

			assert.NotEmpty(t, kp.PublicKey)
			assert.NotEmpty(t, kp.PrivateKey)
		}
	})

	t.Run("Different seeds produce different keys", func(t *testing.T) {
		seed1 := make([]byte, 32)
		seed2 := make([]byte, 32)

		for i := range seed1 {
			seed1[i] = byte(i)
			seed2[i] = byte(i + 1)
		}

		kp1, err := wallet.GenerateDeterministicDilithiumKeyPair(seed1, crypto.DilithiumLevel3)
		require.NoError(t, err)

		kp2, err := wallet.GenerateDeterministicDilithiumKeyPair(seed2, crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Keys should be different
		assert.NotEqual(t, kp1.PublicKey, kp2.PublicKey)
		assert.NotEqual(t, kp1.PrivateKey, kp2.PrivateKey)
	})

	t.Run("Invalid seed length", func(t *testing.T) {
		shortSeed := make([]byte, 16) // Too short

		_, err := wallet.GenerateDeterministicDilithiumKeyPair(shortSeed, crypto.DilithiumLevel3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed must be at least 32 bytes")
	})

	t.Run("Invalid Dilithium level", func(t *testing.T) {
		seed := make([]byte, 32)

		_, err := wallet.GenerateDeterministicDilithiumKeyPair(seed, 999)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported Dilithium level")
	})
}

func TestKeyDerivation(t *testing.T) {
	t.Run("HD-like key derivation", func(t *testing.T) {
		// Master seed
		masterSeed := make([]byte, 64)
		for i := range masterSeed {
			masterSeed[i] = byte(i)
		}

		// Derive child keys using different paths
		kemSeed1 := masterSeed[:32]
		sigSeed1 := masterSeed[32:]

		// Modify seeds to simulate child key derivation
		kemSeed2 := make([]byte, 32)
		copy(kemSeed2, kemSeed1)
		kemSeed2[0]++ // Different child

		sigSeed2 := make([]byte, 32)
		copy(sigSeed2, sigSeed1)
		sigSeed2[0]++ // Different child

		// Generate keys
		kemKP1, err := wallet.GenerateDeterministicKyberKeyPair(kemSeed1, crypto.KyberLevel1024)
		require.NoError(t, err)

		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(kemSeed2, crypto.KyberLevel1024)
		require.NoError(t, err)

		sigKP1, err := wallet.GenerateDeterministicDilithiumKeyPair(sigSeed1, crypto.DilithiumLevel3)
		require.NoError(t, err)

		sigKP2, err := wallet.GenerateDeterministicDilithiumKeyPair(sigSeed2, crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Different child keys should be different
		assert.NotEqual(t, kemKP1.PublicKey, kemKP2.PublicKey)
		assert.NotEqual(t, sigKP1.PublicKey, sigKP2.PublicKey)
	})

	t.Run("Deterministic account ID generation", func(t *testing.T) {
		// Generate keys
		seed := make([]byte, 64)
		for i := range seed {
			seed[i] = byte(i)
		}

		kemKP, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
		require.NoError(t, err)

		sigKP, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Generate account ID multiple times
		id1 := wallet.GenerateDeterministicAccountID(kemKP.PublicKey, sigKP.PublicKey)
		id2 := wallet.GenerateDeterministicAccountID(kemKP.PublicKey, sigKP.PublicKey)

		// Should be deterministic
		assert.Equal(t, id1, id2)
		assert.NotEmpty(t, id1)
		assert.Equal(t, 32, len(id1)) // 16 bytes as hex = 32 chars

		// Different keys should produce different IDs
		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(seed[1:33], crypto.KyberLevel1024)
		require.NoError(t, err)

		id3 := wallet.GenerateDeterministicAccountID(kemKP2.PublicKey, sigKP.PublicKey)
		assert.NotEqual(t, id1, id3)
	})
}

func TestKeyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Run("Generate many deterministic keys", func(t *testing.T) {
		numKeys := 100
		seeds := make([][]byte, numKeys)

		// Generate different seeds
		for i := 0; i < numKeys; i++ {
			seeds[i] = make([]byte, 64)
			for j := range seeds[i] {
				seeds[i][j] = byte(i*256 + j)
			}
		}

		// Generate keys and verify uniqueness
		kemKeys := make(map[string]bool)
		sigKeys := make(map[string]bool)

		for i, seed := range seeds {
			kemKP, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
			require.NoError(t, err, "Failed at iteration %d", i)

			sigKP, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
			require.NoError(t, err, "Failed at iteration %d", i)

			kemKeyStr := string(kemKP.PublicKey)
			sigKeyStr := string(sigKP.PublicKey)

			assert.False(t, kemKeys[kemKeyStr], "Duplicate KEM key at iteration %d", i)
			assert.False(t, sigKeys[sigKeyStr], "Duplicate signature key at iteration %d", i)

			kemKeys[kemKeyStr] = true
			sigKeys[sigKeyStr] = true
		}

		assert.Equal(t, numKeys, len(kemKeys))
		assert.Equal(t, numKeys, len(sigKeys))
	})
}

func TestKeyReproducibility(t *testing.T) {
	t.Run("Keys reproducible across sessions", func(t *testing.T) {
		// Fixed seed for reproducibility test
		seed := []byte("fixed seed for reproducibility testing")
		if len(seed) < 64 {
			padded := make([]byte, 64)
			copy(padded, seed)
			seed = padded
		}

		// Generate keys in "first session"
		kemKP1, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
		require.NoError(t, err)

		sigKP1, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Simulate "restart" by generating again
		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(seed[:32], crypto.KyberLevel1024)
		require.NoError(t, err)

		sigKP2, err := wallet.GenerateDeterministicDilithiumKeyPair(seed[32:], crypto.DilithiumLevel3)
		require.NoError(t, err)

		// Keys should be identical across "sessions"
		assert.Equal(t, kemKP1.PublicKey, kemKP2.PublicKey)
		assert.Equal(t, kemKP1.PrivateKey, kemKP2.PrivateKey)
		assert.Equal(t, sigKP1.PublicKey, sigKP2.PublicKey)
		assert.Equal(t, sigKP1.PrivateKey, sigKP2.PrivateKey)
	})
}

func TestSecurityProperties(t *testing.T) {
	t.Run("Key independence", func(t *testing.T) {
		// Two very similar seeds
		seed1 := make([]byte, 32)
		seed2 := make([]byte, 32)

		for i := range seed1 {
			seed1[i] = byte(i)
			seed2[i] = byte(i)
		}
		seed2[0]++ // Only one bit difference

		// Generate keys
		kemKP1, err := wallet.GenerateDeterministicKyberKeyPair(seed1, crypto.KyberLevel1024)
		require.NoError(t, err)

		kemKP2, err := wallet.GenerateDeterministicKyberKeyPair(seed2, crypto.KyberLevel1024)
		require.NoError(t, err)

		// Keys should be completely different despite similar seeds
		// This tests the avalanche effect of the key derivation
		assert.NotEqual(t, kemKP1.PublicKey, kemKP2.PublicKey)
		assert.NotEqual(t, kemKP1.PrivateKey, kemKP2.PrivateKey)

		// Calculate Hamming distance to ensure good separation
		pubDiff := 0
		privDiff := 0

		for i := 0; i < len(kemKP1.PublicKey); i++ {
			if kemKP1.PublicKey[i] != kemKP2.PublicKey[i] {
				pubDiff++
			}
		}

		for i := 0; i < len(kemKP1.PrivateKey); i++ {
			if kemKP1.PrivateKey[i] != kemKP2.PrivateKey[i] {
				privDiff++
			}
		}

		// Should have significant differences (expect ~50% different)
		assert.Greater(t, pubDiff, len(kemKP1.PublicKey)/4)
		assert.Greater(t, privDiff, len(kemKP1.PrivateKey)/4)
	})
}

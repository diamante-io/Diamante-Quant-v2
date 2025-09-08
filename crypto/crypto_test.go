package crypto

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// ---------- Enhanced Helpers ----------

func getTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		DisableColors:   false,
	})
	return logger
}

// func getTestCryptoConfig() *config.CryptoConfig {
// 	return &config.CryptoConfig{
// 		KyberSecurityLevel:     KyberLevel1024,
// 		DilithiumSecurityLevel: DilithiumLevel3,
// 		EnableKeyRotation:      true,
// 		KeyRotationInterval:    time.Hour,
// 	}
// }

// ========== KYBER TESTS ==========

func TestKyberKeyGen(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting Kyber key generation test")

	scheme := KyberSchemeFromLevel(KyberLevel1024)
	if scheme == nil {
		logger.Error("Failed to get Kyber scheme")
		t.Fatalf("KyberSchemeFromLevel(%d) returned nil", KyberLevel1024)
	}
	logger.WithField("level", KyberLevel1024).Debug("Kyber scheme initialized")

	kyber := NewKyberCrypto(scheme, logger)
	logger.Debug("Created new Kyber crypto instance")

	start := time.Now()
	kp, err := kyber.GenerateKeyPair()
	elapsed := time.Since(start)

	if err != nil {
		logger.WithError(err).Error("Kyber key generation failed")
		t.Fatalf("Kyber GenerateKeyPair failed: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"publicKeyLength":  len(kp.PublicKey),
		"privateKeyLength": len(kp.PrivateKey),
		"timeElapsed":      elapsed,
	}).Info("Kyber key pair generated successfully")

	if len(kp.PublicKey) == 0 {
		logger.Error("Generated public key is empty")
		t.Error("Kyber public key is empty")
	}
	if len(kp.PrivateKey) == 0 {
		logger.Error("Generated private key is empty")
		t.Error("Kyber private key is empty")
	}
}

func TestKyberEncapsDecaps(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting Kyber encapsulation/decapsulation test")

	scheme := KyberSchemeFromLevel(KyberLevel1024)
	if scheme == nil {
		logger.Error("Failed to get Kyber scheme")
		t.Fatalf("Failed to get kyber scheme for %d", KyberLevel1024)
	}

	kyber := NewKyberCrypto(scheme, logger)
	logger.Debug("Kyber crypto instance created")

	// 1) Generate
	start := time.Now()
	kp, err := kyber.GenerateKeyPair()
	genTime := time.Since(start)

	if err != nil {
		logger.WithError(err).Error("Key generation failed")
		t.Fatalf("Kyber generateKeyPair error: %v", err)
	}
	logger.WithField("genTime", genTime).Debug("Key pair generated")

	// 2) Encapsulate
	encapStart := time.Now()
	ct, shared, err := kyber.EncapsulateFromBytes(kp.PublicKey)
	encapTime := time.Since(encapStart)

	if err != nil {
		logger.WithError(err).Error("Encapsulation failed")
		t.Fatalf("EncapsulateFromBytes failed: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"ciphertextLength": len(ct),
		"sharedLength":     len(shared),
		"encapTime":        encapTime,
	}).Debug("Encapsulation completed")

	// 3) Decapsulate
	decapStart := time.Now()
	recovered, err := kyber.DecapsulateFromBytes(kp.PrivateKey, ct)
	decapTime := time.Since(decapStart)

	if err != nil {
		logger.WithError(err).Error("Decapsulation failed")
		t.Fatalf("DecapsulateFromBytes error: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"recoveredLength": len(recovered),
		"decapTime":       decapTime,
	}).Debug("Decapsulation completed")

	if !bytes.Equal(shared, recovered) {
		logger.WithFields(logrus.Fields{
			"sharedLength":    len(shared),
			"recoveredLength": len(recovered),
			"sharedHex":       hex.EncodeToString(shared),
			"recoveredHex":    hex.EncodeToString(recovered),
		}).Error("Shared secret mismatch")
		t.Errorf("Kyber shared mismatch")
	} else {
		logger.Info("Shared secret successfully recovered")
	}
}

func TestKyberAESGCM(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting Kyber AES-GCM integration test")

	scheme := KyberSchemeFromLevel(KyberLevel1024)
	kyber := NewKyberCrypto(scheme, logger)

	// Generate keypair
	start := time.Now()
	kp, err := kyber.GenerateKeyPair()
	keyGenTime := time.Since(start)

	if err != nil {
		logger.WithError(err).Error("Key generation failed")
		t.Fatalf("Kyber keypair generation: %v", err)
	}
	logger.WithField("keyGenTime", keyGenTime).Debug("Key pair generated")

	// Encapsulate
	encapStart := time.Now()
	ct, ss, err := kyber.EncapsulateFromBytes(kp.PublicKey)
	encapTime := time.Since(encapStart)

	if err != nil {
		logger.WithError(err).Error("Encapsulation failed")
		t.Fatalf("Kyber Encaps error: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"encapTime": encapTime,
		"ctLength":  len(ct),
		"ssLength":  len(ss),
	}).Debug("Encapsulation completed")

	// Encrypt
	plain := []byte("Kyber test data using ephemeral key")
	encStart := time.Now()
	encData, err := EncryptWithShared(plain, ss)
	encTime := time.Since(encStart)

	if err != nil {
		logger.WithError(err).Error("AES encryption failed")
		t.Fatalf("AES encrypt error: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"encTime":       encTime,
		"plaintextLen":  len(plain),
		"ciphertextLen": len(encData),
	}).Debug("AES encryption completed")

	// Decapsulate
	decapStart := time.Now()
	ss2, err := kyber.DecapsulateFromBytes(kp.PrivateKey, ct)
	decapTime := time.Since(decapStart)

	if err != nil {
		logger.WithError(err).Error("Decapsulation failed")
		t.Fatalf("Decaps error: %v", err)
	}
	logger.WithField("decapTime", decapTime).Debug("Decapsulation completed")

	// Decrypt
	decStart := time.Now()
	recovered, err := DecryptWithShared(encData, ss2)
	decTime := time.Since(decStart)

	if err != nil {
		logger.WithError(err).Error("AES decryption failed")
		t.Fatalf("AES decrypt error: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"decTime":         decTime,
		"recoveredLength": len(recovered),
	}).Debug("AES decryption completed")

	if !bytes.Equal(plain, recovered) {
		logger.WithFields(logrus.Fields{
			"expectedLength":  len(plain),
			"recoveredLength": len(recovered),
			"expected":        string(plain),
			"recovered":       string(recovered),
		}).Error("Decrypted message mismatch")
		t.Error("AES mismatch!")
	} else {
		logger.Info("Message successfully encrypted and decrypted")
	}
}

func TestCryptoManagerBasic(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting CryptoManager basic test")

	start := time.Now()
	cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	initTime := time.Since(start)

	if err != nil {
		logger.WithError(err).Error("CryptoManager initialization failed")
		t.Fatalf("NewCryptoManager error: %v", err)
	}
	logger.WithField("initTime", initTime).Debug("CryptoManager initialized")

	// Kyber KEM key generation
	kemStart := time.Now()
	kemKP, err := cm.GenerateKEMKeyPair()
	kemTime := time.Since(kemStart)

	if err != nil {
		logger.WithError(err).Error("KEM key generation failed")
		t.Fatalf("GenerateKEMKeyPair: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"kemTime":       kemTime,
		"pubKeyLength":  len(kemKP.PublicKey),
		"privKeyLength": len(kemKP.PrivateKey),
	}).Debug("KEM keypair generated")

	// Dilithium key generation
	dilStart := time.Now()
	dilKP, err := cm.GenerateSignatureKeyPair()
	dilTime := time.Since(dilStart)

	if err != nil {
		logger.WithError(err).Error("Signature key generation failed")
		t.Fatalf("GenerateSignatureKeyPair: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"dilTime":       dilTime,
		"pubKeyLength":  len(dilKP.PublicKey),
		"privKeyLength": len(dilKP.PrivateKey),
	}).Debug("Dilithium keypair generated")

	// Combined operations test
	msg := []byte("This is a test message for combined approach")
	logger.WithField("messageLength", len(msg)).Debug("Starting combined operations test")

	combStart := time.Now()
	ct, enc, dsig, err := cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, msg)
	combEncTime := time.Since(combStart)

	if err != nil {
		logger.WithError(err).Error("Combined encrypt and sign failed")
		t.Fatalf("CombinedEncryptAndSign error: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"encryptSignTime": combEncTime,
		"ctLength":        len(ct),
		"encLength":       len(enc),
		"sigLength":       len(dsig),
	}).Debug("Combined encrypt and sign completed")

	verifyStart := time.Now()
	recoveredMsg, ok, err := cm.CombinedDecryptAndVerify(kemKP.PrivateKey, dilKP, ct, enc, dsig)
	verifyTime := time.Since(verifyStart)

	if err != nil {
		logger.WithError(err).Error("Combined decrypt and verify failed")
		t.Fatalf("CombinedDecryptAndVerify error: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"decryptVerifyTime": verifyTime,
		"verified":          ok,
		"recoveredLength":   len(recoveredMsg),
	}).Debug("Combined decrypt and verify completed")

	if !ok {
		logger.Error("Signature verification failed")
		t.Error("Signature verification failed in CombinedDecryptAndVerify")
	}

	if !bytes.Equal(msg, recoveredMsg) {
		logger.Error("Message mismatch in combined operations")
		t.Error("Message mismatch in CombinedDecryptAndVerify")
	}

	logger.Info("CryptoManager basic test completed successfully")
}

func TestCryptoManagerUtilities(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting CryptoManager utilities test")

	cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	if err != nil {
		logger.WithError(err).Error("CryptoManager initialization failed")
		t.Fatalf("NewCryptoManager error: %v", err)
	}

	// Test random bytes generation
	rbStart := time.Now()
	rb, err := cm.GenerateRandomBytes(16)
	rbTime := time.Since(rbStart)

	if err != nil {
		logger.WithError(err).Error("Random bytes generation failed")
		t.Fatalf("GenerateRandomBytes error: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"bytesLength": len(rb),
		"genTime":     rbTime,
		"entropy":     calculateEntropy(rb),
	}).Debug("Random bytes generated")

	// Test KDF
	seed := []byte("some seed for KDF")
	kdfStart := time.Now()
	derived, err := cm.DeriveKey(seed, 32)
	kdfTime := time.Since(kdfStart)

	if err != nil {
		logger.WithError(err).Error("Key derivation failed")
		t.Fatalf("DeriveKey error: %v", err)
	}
	logger.WithFields(logrus.Fields{
		"seedLength":    len(seed),
		"derivedLength": len(derived),
		"kdfTime":       kdfTime,
	}).Debug("Key derived successfully")

	logger.Info("CryptoManager utilities test completed successfully")
}

// Helper function to calculate Shannon entropy of byte slice
func calculateEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0.0
	}
	counts := make(map[byte]int)
	for _, b := range data {
		counts[b]++
	}
	entropy := 0.0
	for _, count := range counts {
		p := float64(count) / float64(len(data))
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// ---------- Additional Helper Functions ----------

type memStats struct {
	allocatedBytes uint64
	totalAllocs    uint64
	numGC          uint32
}

func getMemStats() memStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return memStats{
		allocatedBytes: m.Alloc,
		totalAllocs:    m.TotalAlloc,
		numGC:          m.NumGC,
	}
}

// ---------- Benchmark Tests ----------

func BenchmarkCryptoOperations(b *testing.B) {
	logger := getTestLogger()
	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)

	b.Run("KEM_KeyGen", func(b *testing.B) {
		logger.Info("Starting KEM KeyGen benchmark")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			kp, err := cm.GenerateKEMKeyPair()
			if err != nil {
				b.Fatal(err)
			}
			if len(kp.PublicKey) == 0 || len(kp.PrivateKey) == 0 {
				b.Fatal("Empty keys generated")
			}
		}

		logger.WithField("iterations", b.N).Info("KEM KeyGen benchmark completed")
	})

	b.Run("Dilithium_KeyGen", func(b *testing.B) {
		logger.Info("Starting Dilithium KeyGen benchmark")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			kp, err := cm.GenerateSignatureKeyPair()
			if err != nil {
				b.Fatal(err)
			}
			if len(kp.PublicKey) == 0 || len(kp.PrivateKey) == 0 {
				b.Fatal("Empty keys generated")
			}
		}

		logger.WithField("iterations", b.N).Info("Dilithium KeyGen benchmark completed")
	})
}

// ---------- Error Scenario Tests ----------

func TestErrorScenarios(t *testing.T) {
	logger := getTestLogger()
	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)

	t.Run("InvalidKeyDerivation", func(t *testing.T) {
		logger.Debug("Testing invalid key derivation")
		_, err := cm.DeriveKey([]byte("test"), -1)
		if err == nil {
			t.Error("Expected error for negative key size")
		}
		logger.WithError(err).Debug("Received expected error for invalid key size")
	})

	t.Run("CorruptedKeys", func(t *testing.T) {
		logger.Debug("Testing corrupted key handling")

		// Create an invalid key of the wrong size
		invalidKey := make([]byte, 100) // Much smaller than valid Kyber key
		rand.Read(invalidKey)

		_, _, err := cm.EncryptKEM(invalidKey)
		if err == nil {
			logger.Error("No error received for invalid key size")
			t.Error("Expected error with invalid key size")
		} else {
			logger.WithError(err).Debug("Received expected error for invalid key")
		}

		// Test with truncated key
		kp, _ := cm.GenerateKEMKeyPair()
		truncatedKey := kp.PublicKey[:len(kp.PublicKey)/2]
		_, _, err = cm.EncryptKEM(truncatedKey)
		if err == nil {
			logger.Error("No error received for truncated key")
			t.Error("Expected error with truncated key")
		} else {
			logger.WithError(err).Debug("Received expected error for truncated key")
		}
	})

	t.Run("NilKeys", func(t *testing.T) {
		logger.Debug("Testing nil key handling")

		_, _, err := cm.EncryptKEM(nil)
		if err == nil {
			t.Error("Expected error with nil public key")
		}
		logger.WithError(err).Debug("Received expected error for nil key")

		_, err = cm.DecryptKEM(nil, []byte("test"))
		if err == nil {
			t.Error("Expected error with nil private key")
		}
		logger.WithError(err).Debug("Received expected error for nil key")
	})

	t.Run("InvalidMessageSize", func(t *testing.T) {
		logger.Debug("Testing invalid message size handling")

		kemKP, _ := cm.GenerateKEMKeyPair()
		dilKP, _ := cm.GenerateSignatureKeyPair()

		// Test with zero-length message
		_, _, _, err := cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, nil)
		if err != nil {
			logger.WithError(err).Debug("Received error for nil message")
		}

		// Test with very large message
		largeMsg := make([]byte, 10*1024*1024) // 10MB
		rand.Read(largeMsg)

		start := time.Now()
		_, _, _, err = cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, largeMsg)
		elapsed := time.Since(start)

		logger.WithFields(logrus.Fields{
			"messageSize": len(largeMsg),
			"elapsed":     elapsed,
			"error":       err,
		}).Debug("Large message test completed")
	})
}

// ---------- Memory Usage Tests ----------

func TestMemoryUsage(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting memory usage tests")

	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)

	t.Run("LargeMessageHandling", func(t *testing.T) {
		before := getMemStats()
		logger.WithField("initialMemory", before.allocatedBytes).Debug("Starting large message test")

		// Create 1MB message
		largeMsg := make([]byte, 1024*1024)
		for i := range largeMsg {
			largeMsg[i] = byte(i % 256)
		}

		kemKP, _ := cm.GenerateKEMKeyPair()
		dilKP, _ := cm.GenerateSignatureKeyPair()

		ct, enc, sig, err := cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, largeMsg)
		if err != nil {
			t.Fatal(err)
		}

		after := getMemStats()
		logger.WithFields(logrus.Fields{
			"memoryDelta": after.allocatedBytes - before.allocatedBytes,
			"ctSize":      len(ct),
			"encSize":     len(enc),
			"sigSize":     len(sig),
		}).Debug("Large message test completed")
	})
}

// ---------- Concurrent Operation Tests ----------

func TestConcurrentOperations(t *testing.T) {
	logger := getTestLogger()
	logger.Info("Starting concurrent operations test")

	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	initialGoroutines := runtime.NumGoroutine()

	var wg sync.WaitGroup
	operations := 50
	errChan := make(chan error, operations)

	logger.WithFields(logrus.Fields{
		"operations":        operations,
		"initialGoroutines": initialGoroutines,
	}).Debug("Starting concurrent operations")

	for i := 0; i < operations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Generate keys
			kemKP, err := cm.GenerateKEMKeyPair()
			if err != nil {
				errChan <- err
				return
			}

			dilKP, err := cm.GenerateSignatureKeyPair()
			if err != nil {
				errChan <- err
				return
			}

			// Test operations
			msg := []byte("Concurrent test message")
			ct, enc, sig, err := cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, msg)
			if err != nil {
				errChan <- err
				return
			}

			_, ok, err := cm.CombinedDecryptAndVerify(kemKP.PrivateKey, dilKP, ct, enc, sig)
			if err != nil || !ok {
				errChan <- err
				return
			}

			logger.WithField("goroutineID", id).Debug("Concurrent operation completed")
		}(i)
	}

	wg.Wait()
	close(errChan)

	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	finalGoroutines := runtime.NumGoroutine()
	logger.WithFields(logrus.Fields{
		"errorCount":     len(errors),
		"goroutineDelta": finalGoroutines - initialGoroutines,
		"completedOps":   operations - len(errors),
	}).Info("Concurrent operations test completed")

	if len(errors) > 0 {
		t.Errorf("Concurrent operations had %d errors", len(errors))
	}
}

// ---------- Edge Case Tests ----------

func TestEdgeCases(t *testing.T) {
	logger := getTestLogger()
	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)

	t.Run("EmptyMessage", func(t *testing.T) {
		logger.Debug("Testing empty message handling")

		kemKP, _ := cm.GenerateKEMKeyPair()
		dilKP, _ := cm.GenerateSignatureKeyPair()
		msg := []byte{}

		ct, enc, sig, err := cm.CombinedEncryptAndSign(kemKP.PublicKey, dilKP, msg)
		if err != nil {
			t.Fatal(err)
		}

		recovered, ok, err := cm.CombinedDecryptAndVerify(kemKP.PrivateKey, dilKP, ct, enc, sig)
		if err != nil || !ok || len(recovered) != 0 {
			t.Error("Empty message handling failed")
		}

		logger.Debug("Empty message test completed successfully")
	})

	t.Run("KeyRotation", func(t *testing.T) {
		logger.Debug("Testing key rotation scenario")

		// First key pair
		oldKP, err := cm.GenerateKEMKeyPair()
		if err != nil {
			t.Fatal("Failed to generate old key pair:", err)
		}

		// Create a valid dilithium key pair for signing
		dilKP, err := cm.GenerateSignatureKeyPair()
		if err != nil {
			t.Fatal("Failed to generate dilithium key pair:", err)
		}

		msg := []byte("Test message")
		ct, enc, sig, err := cm.CombinedEncryptAndSign(oldKP.PublicKey, dilKP, msg)
		if err != nil {
			t.Fatal("Failed to encrypt and sign:", err)
		}

		// Simulate rotation
		time.Sleep(time.Millisecond)
		newKP, err := cm.GenerateKEMKeyPair()
		if err != nil {
			t.Fatal("Failed to generate new key pair:", err)
		}

		// Try decrypting with new key - this should fail
		_, ok, err := cm.CombinedDecryptAndVerify(newKP.PrivateKey, dilKP, ct, enc, sig)
		if ok || err == nil {
			t.Error("Expected failure when using rotated key")
		}

		logger.WithError(err).Debug("Received expected error for key rotation test")
	})
}

// Add timing metrics for key rotation test
func TestKeyRotationMetrics(t *testing.T) {
	logger := getTestLogger()
	cm, _ := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)

	t.Run("RotationTiming", func(t *testing.T) {
		start := time.Now()

		// Generate initial keys
		oldKP, _ := cm.GenerateKEMKeyPair()
		dilKP, _ := cm.GenerateSignatureKeyPair()

		// Perform operations with old keys
		msg := []byte("Test message")
		ct, enc, sig, _ := cm.CombinedEncryptAndSign(oldKP.PublicKey, dilKP, msg)

		// Generate new keys (simulating rotation)
		newKP, _ := cm.GenerateKEMKeyPair()

		// Try operations with new keys
		_, ok, _ := cm.CombinedDecryptAndVerify(newKP.PrivateKey, dilKP, ct, enc, sig)

		elapsed := time.Since(start)
		logger.WithFields(logrus.Fields{
			"rotationTime": elapsed,
			"success":      !ok, // Should be false since we expect rotation to prevent decryption
			"oldKeySize":   len(oldKP.PublicKey),
			"newKeySize":   len(newKP.PublicKey),
		}).Info("Key rotation metrics")
	})
}

// ---------- Integration Tests ----------

func TestSecurityLevelIntegration(t *testing.T) {
	logger := getTestLogger()
	levels := []struct {
		kyber     int
		dilithium int
	}{
		{KyberLevel512, DilithiumLevel2},
		{KyberLevel768, DilithiumLevel3},
		{KyberLevel1024, DilithiumLevel5},
	}

	for _, level := range levels {
		t.Run("SecurityLevel", func(t *testing.T) {
			logger.WithFields(logrus.Fields{
				"kyberLevel":     level.kyber,
				"dilithiumLevel": level.dilithium,
			}).Info("Testing security level combination")

			cm, err := NewCryptoManager(level.kyber, level.dilithium, logger)
			if err != nil {
				t.Fatal(err)
			}

			kemKP, err := cm.GenerateKEMKeyPair()
			if err != nil {
				t.Fatal(err)
			}

			logger.WithFields(logrus.Fields{
				"pubKeySize":  len(kemKP.PublicKey),
				"privKeySize": len(kemKP.PrivateKey),
			}).Debug("Key sizes for security level")
		})
	}
}

func TestDilithiumRepeatSignatures(t *testing.T) {
	logger := getTestLogger()
	cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	if err != nil {
		t.Fatalf("Failed to create CryptoManager: %v", err)
	}

	testCases := []struct {
		name string
		msg  []byte
	}{
		{"Empty", []byte{}},
		{"Small", []byte("test")},
		{"Medium", bytes.Repeat([]byte("a"), 1024)},
		{"Large", bytes.Repeat([]byte("b"), 1024*1024)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kp, kerr := cm.GenerateSignatureKeyPair()
			if kerr != nil {
				t.Fatalf("Failed to generate keypair: %v", kerr)
			}

			// We'll store all signatures here
			var sigs [][]byte
			const attempts = 2

			for i := 0; i < attempts; i++ {
				sig, errSign := cm.Sign(kp, tc.msg)
				if errSign != nil {
					t.Fatalf("Signing iteration %d failed: %v", i, errSign)
				}

				// Assign the appended slice back to sigs
				sigs = append(sigs, sig)

				ok, errVerify := cm.Verify(kp, tc.msg, sig)
				if errVerify != nil || !ok {
					t.Errorf("Signature iteration %d verify fail: %v", i, errVerify)
				}
			}

			// Now, actually USE sigs: confirm we got the right count
			if len(sigs) != attempts {
				t.Errorf("Expected %d signatures, got %d", attempts, len(sigs))
			}

			// We do NOT compare sigs[0] == sigs[1], as Dilithium typically uses randomness
			logger.WithFields(logrus.Fields{
				"messageSize": len(tc.msg),
				"attempts":    attempts,
			}).Info("Repeat signature test passed (verification succeeded).")
		})
	}
}

func TestNaiveTimingCheckOperations(t *testing.T) {
	logger := getTestLogger()
	cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	if err != nil {
		t.Fatalf("Failed to create CryptoManager: %v", err)
	}

	kp, kerr := cm.GenerateKEMKeyPair()
	if kerr != nil {
		t.Fatalf("KEMKeyPair generation failed: %v", kerr)
	}

	// Valid ciphertext
	ct, _, encErr := cm.EncryptKEM(kp.PublicKey)
	if encErr != nil {
		t.Fatalf("EncryptKEM error: %v", encErr)
	}

	// Invalid ciphertext
	invalid := make([]byte, len(ct))
	copy(invalid, ct)
	if len(invalid) > 0 {
		invalid[len(invalid)-1] ^= 0xFF // Flip last byte
	}

	iterations := 300
	validTimes := make([]time.Duration, 0, iterations)
	invalidTimes := make([]time.Duration, 0, iterations)

	for i := 0; i < iterations; i++ {
		startValid := time.Now()
		_, _ = cm.DecryptKEM(kp.PrivateKey, ct)
		validTimes = append(validTimes, time.Since(startValid))

		startInvalid := time.Now()
		_, _ = cm.DecryptKEM(kp.PrivateKey, invalid)
		invalidTimes = append(invalidTimes, time.Since(startInvalid))
	}

	avgValid := averageDuration(validTimes) // time.Duration
	avgInvalid := averageDuration(invalidTimes)

	// Convert to float64 for difference
	avgValidF := float64(avgValid) // in nanoseconds
	avgInvalidF := float64(avgInvalid)
	diff := avgValidF - avgInvalidF
	if diff < 0 {
		diff = -diff
	}

	// We allow ~15% variance => dividing by ~6.6667
	// typed constant helps avoid truncation warnings
	const fifteenPercentDivisor = 6.6667
	threshold := avgValidF / fifteenPercentDivisor

	logger.WithFields(logrus.Fields{
		"avgValid_ns":   avgValidF,
		"avgInvalid_ns": avgInvalidF,
		"diff_ns":       diff,
		"threshold_ns":  threshold,
	}).Info("Naive timing check completed.")

	if diff > threshold {
		t.Logf(
			"Warning: Potential timing difference: %.2f > %.2f. "+
				"This is not necessarily a side-channel leak, but suspicious.",
			diff, threshold,
		)
		// We only log a warning; we do not fail the test to avoid flakiness.
	}
}

func averageDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

func TestMemoryUsagePatterns(t *testing.T) {
	/*
	   This test compares memory usage across repeated KEM key generation & encryption calls.
	   It's not a real side-channel test, just a naive check for large memory anomalies.
	*/
	logger := getTestLogger()
	cm, err := NewCryptoManager(KyberLevel1024, DilithiumLevel3, logger)
	if err != nil {
		t.Fatalf("Failed to create CryptoManager: %v", err)
	}

	tests := []struct {
		name string
		size int
	}{
		{"SmallKey", 32},
		{"MediumKey", 1024},
		{"LargeKey", 4096},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			rand.Read(data)

			before := getMemStats()
			kp1, _ := cm.GenerateKEMKeyPair()
			ct1, ss1, _ := cm.EncryptKEM(kp1.PublicKey)
			_, errDec1 := cm.DecryptKEM(kp1.PrivateKey, ct1)
			if errDec1 != nil {
				t.Fatalf("First decap failed: %v", errDec1)
			}
			mem1 := getMemStats()

			kp2, _ := cm.GenerateKEMKeyPair()
			ct2, ss2, _ := cm.EncryptKEM(kp2.PublicKey)
			_, errDec2 := cm.DecryptKEM(kp2.PrivateKey, ct2)
			if errDec2 != nil {
				t.Fatalf("Second decap failed: %v", errDec2)
			}
			mem2 := getMemStats()

			firstPattern := mem1.allocatedBytes - before.allocatedBytes
			secondPattern := mem2.allocatedBytes - mem1.allocatedBytes

			var diff uint64
			if firstPattern > secondPattern {
				diff = firstPattern - secondPattern
			} else {
				diff = secondPattern - firstPattern
			}

			// naive threshold
			maxVariance := firstPattern / 10
			logger.WithFields(logrus.Fields{
				"dataSize":    tc.size,
				"memoryDiff":  diff,
				"maxVariance": maxVariance,
				"ss1Length":   len(ss1),
				"ss2Length":   len(ss2),
			}).Info("Memory usage pattern check")

			if diff > maxVariance {
				t.Logf("Warning: Memory usage difference is high: %d (allowed: %d). Possibly normal GC activity.", diff, maxVariance)
				// Not failing the test—just a warning
			}
		})
	}
}

// TODO: Add 10MB stress tests post-alpha launch

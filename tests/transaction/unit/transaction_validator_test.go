package transaction_test

import (
	"crypto/rand"
	"math/big"
	"sync"
	"testing"
	"time"

	"diamante/transaction"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionValidator(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	t.Run("Initialization", func(t *testing.T) {
		config := &transaction.ValidatorConfig{
			MaxTransactionSize:          1024 * 1024,            // 1MB
			MinGasPrice:                 big.NewInt(1000000000), // 1 Gwei
			MaxGasLimit:                 uint64(10000000),
			EnableSignatureVerification: true,
			EnableReplayProtection:      true,
			EnableStrictMode:            false,
		}

		validator := transaction.NewValidator(config, logger)
		require.NotNil(t, validator)

		err := validator.Start()
		assert.NoError(t, err)

		// Should not start twice
		err = validator.Start()
		assert.Error(t, err)
	})

	t.Run("Basic Validation", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Valid transaction
		validTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(2000000000), // 2 Gwei
			GasLimit:  uint64(21000),
			Nonce:     uint64(1),
			Data:      []byte{},
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(validTx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.Valid)
		assert.Empty(t, result.Errors)
	})

	t.Run("Size Validation", func(t *testing.T) {
		config := &transaction.ValidatorConfig{
			MaxTransactionSize: 1000, // 1KB limit
		}

		validator := transaction.NewValidator(config, logger)
		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Transaction exceeding size limit
		largeTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Data:      make([]byte, 2000), // Exceeds limit
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(largeTx)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "size")
	})

	t.Run("Gas Validation", func(t *testing.T) {
		config := &transaction.ValidatorConfig{
			MinGasPrice: big.NewInt(1000000000), // 1 Gwei minimum
			MaxGasLimit: uint64(1000000),
		}

		validator := transaction.NewValidator(config, logger)
		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Test low gas price
		lowGasTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(500000000), // 0.5 Gwei - too low
			GasLimit:  uint64(21000),
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(lowGasTx)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "gas price")

		// Test high gas limit
		highGasLimitTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(2000000000),
			GasLimit:  uint64(2000000), // Exceeds max
			Timestamp: time.Now(),
		}

		result, err = validator.Validate(highGasLimitTx)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "gas limit")
	})

	t.Run("Address Validation", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Invalid from address
		invalidFromTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      "invalid-address",
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(invalidFromTx)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "from address")

		// Missing to address (contract creation)
		contractCreationTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        "", // Empty for contract creation
			Value:     big.NewInt(0),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(3000000),
			Data:      []byte{0x60, 0x60, 0x60}, // Contract bytecode
			Timestamp: time.Now(),
		}

		result, err = validator.Validate(contractCreationTx)
		assert.NoError(t, err)
		assert.True(t, result.Valid) // Contract creation is valid
		assert.True(t, result.IsContractCreation)
	})

	t.Run("Signature Verification", func(t *testing.T) {
		config := &transaction.ValidatorConfig{
			EnableSignatureVerification: true,
		}

		validator := transaction.NewValidator(config, logger)
		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Transaction with valid signature
		signedTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Nonce:     uint64(1),
			Timestamp: time.Now(),
		}

		// Sign transaction (mock signature)
		signedTx.V = big.NewInt(27)
		signedTx.R = generateBigInt()
		signedTx.S = generateBigInt()

		result, err := validator.Validate(signedTx)
		assert.NoError(t, err)
		// Note: In real implementation, signature verification would check against public key

		// Transaction without signature
		unsignedTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Timestamp: time.Now(),
		}

		result, err = validator.Validate(unsignedTx)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "signature")
	})

	t.Run("Replay Protection", func(t *testing.T) {
		config := &transaction.ValidatorConfig{
			EnableReplayProtection: true,
		}

		validator := transaction.NewValidator(config, logger)
		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// First transaction
		tx1 := &transaction.Transaction{
			ID:        "tx-123",
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Nonce:     uint64(1),
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(tx1)
		assert.NoError(t, err)
		assert.True(t, result.Valid)

		// Replay same transaction
		result, err = validator.Validate(tx1)
		assert.NoError(t, err)
		assert.False(t, result.Valid)
		assert.Contains(t, result.Errors[0], "replay")
	})

	t.Run("Batch Validation", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Create batch of transactions
		txs := make([]*transaction.Transaction, 10)
		for i := 0; i < 10; i++ {
			txs[i] = &transaction.Transaction{
				ID:        generateTxID(),
				From:      generateAddress(),
				To:        generateAddress(),
				Value:     big.NewInt(int64(1000 + i*100)),
				GasPrice:  big.NewInt(1000000000),
				GasLimit:  uint64(21000),
				Nonce:     uint64(i),
				Timestamp: time.Now(),
			}

			// Make some invalid
			if i%3 == 0 {
				txs[i].GasPrice = big.NewInt(100) // Too low
			}
		}

		results, err := validator.ValidateBatch(txs)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(results))

		validCount := 0
		for _, result := range results {
			if result.Valid {
				validCount++
			}
		}
		assert.Equal(t, 6, validCount) // 4 should be invalid (indices 0,3,6,9)
	})

	t.Run("Transaction Types", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Standard transfer
		transferTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(1000),
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(21000),
			Timestamp: time.Now(),
		}

		result, err := validator.Validate(transferTx)
		assert.NoError(t, err)
		assert.True(t, result.Valid)
		assert.Equal(t, transaction.TypeTransfer, result.TransactionType)

		// Contract call
		contractCallTx := &transaction.Transaction{
			ID:        generateTxID(),
			From:      generateAddress(),
			To:        generateAddress(),
			Value:     big.NewInt(0),
			Data:      []byte{0x12, 0x34, 0x56, 0x78}, // Function selector
			GasPrice:  big.NewInt(1000000000),
			GasLimit:  uint64(100000),
			Timestamp: time.Now(),
		}

		result, err = validator.Validate(contractCallTx)
		assert.NoError(t, err)
		assert.True(t, result.Valid)
		assert.Equal(t, transaction.TypeContractCall, result.TransactionType)
	})

	t.Run("Concurrent Validation", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Validate transactions concurrently
		var wg sync.WaitGroup
		validationCount := 100
		results := make([]*transaction.ValidationResult, validationCount)

		for i := 0; i < validationCount; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				tx := &transaction.Transaction{
					ID:        generateTxID(),
					From:      generateAddress(),
					To:        generateAddress(),
					Value:     big.NewInt(int64(1000 + idx)),
					GasPrice:  big.NewInt(1000000000),
					GasLimit:  uint64(21000),
					Nonce:     uint64(idx),
					Timestamp: time.Now(),
				}

				result, err := validator.Validate(tx)
				assert.NoError(t, err)
				results[idx] = result
			}(i)
		}

		wg.Wait()

		// Verify all validations completed
		for i, result := range results {
			assert.NotNil(t, result, "Result %d is nil", i)
		}
	})

	t.Run("Get Stats", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)
		defer validator.Stop()

		// Validate some transactions
		for i := 0; i < 10; i++ {
			tx := &transaction.Transaction{
				ID:        generateTxID(),
				From:      generateAddress(),
				To:        generateAddress(),
				Value:     big.NewInt(1000),
				GasPrice:  big.NewInt(1000000000),
				GasLimit:  uint64(21000),
				Timestamp: time.Now(),
			}

			if i%3 == 0 {
				tx.GasPrice = big.NewInt(100) // Make some invalid
			}

			validator.Validate(tx)
		}

		stats := validator.GetStats()
		assert.NotNil(t, stats)
		assert.Equal(t, uint64(10), stats.TotalValidations)
		assert.Equal(t, uint64(6), stats.ValidTransactions)
		assert.Equal(t, uint64(4), stats.InvalidTransactions)
	})

	t.Run("Stop", func(t *testing.T) {
		config := transaction.DefaultValidatorConfig()
		validator := transaction.NewValidator(config, logger)

		err := validator.Start()
		require.NoError(t, err)

		// Stop
		err = validator.Stop()
		assert.NoError(t, err)

		// Should not stop twice
		err = validator.Stop()
		assert.Error(t, err)

		// Operations should fail after stop
		tx := &transaction.Transaction{
			ID:    generateTxID(),
			From:  generateAddress(),
			To:    generateAddress(),
			Value: big.NewInt(1000),
		}

		_, err = validator.Validate(tx)
		assert.Error(t, err)
	})
}

func TestValidationResult(t *testing.T) {
	t.Run("Result Fields", func(t *testing.T) {
		result := &transaction.ValidationResult{
			Valid:              true,
			TransactionType:    transaction.TypeTransfer,
			IsContractCreation: false,
			EstimatedGas:       uint64(21000),
			Errors:             []string{},
			Warnings:           []string{"Low gas price"},
			ValidationDuration: 100 * time.Millisecond,
		}

		assert.True(t, result.Valid)
		assert.Equal(t, transaction.TypeTransfer, result.TransactionType)
		assert.False(t, result.IsContractCreation)
		assert.Equal(t, uint64(21000), result.EstimatedGas)
		assert.Empty(t, result.Errors)
		assert.NotEmpty(t, result.Warnings)
		assert.Equal(t, 100*time.Millisecond, result.ValidationDuration)
	})
}

func BenchmarkTransactionValidator(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := transaction.DefaultValidatorConfig()
	validator := transaction.NewValidator(config, logger)
	validator.Start()
	defer validator.Stop()

	// Create test transaction
	tx := &transaction.Transaction{
		ID:        generateTxID(),
		From:      generateAddress(),
		To:        generateAddress(),
		Value:     big.NewInt(1000),
		GasPrice:  big.NewInt(1000000000),
		GasLimit:  uint64(21000),
		Nonce:     uint64(1),
		Timestamp: time.Now(),
	}

	b.Run("SingleValidation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx.Nonce = uint64(i)
			validator.Validate(tx)
		}
	})

	b.Run("BatchValidation", func(b *testing.B) {
		// Prepare batch
		txs := make([]*transaction.Transaction, 100)
		for i := 0; i < 100; i++ {
			txs[i] = &transaction.Transaction{
				ID:        generateTxID(),
				From:      generateAddress(),
				To:        generateAddress(),
				Value:     big.NewInt(1000),
				GasPrice:  big.NewInt(1000000000),
				GasLimit:  uint64(21000),
				Nonce:     uint64(i),
				Timestamp: time.Now(),
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			validator.ValidateBatch(txs)
		}
	})

	b.Run("ParallelValidation", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				localTx := &transaction.Transaction{
					ID:        generateTxID(),
					From:      generateAddress(),
					To:        generateAddress(),
					Value:     big.NewInt(1000),
					GasPrice:  big.NewInt(1000000000),
					GasLimit:  uint64(21000),
					Nonce:     uint64(i),
					Timestamp: time.Now(),
				}
				validator.Validate(localTx)
				i++
			}
		})
	})
}

// Helper functions
func generateTxID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return "0x" + bytesToHex(b)
}

func generateAddress() string {
	b := make([]byte, 20)
	rand.Read(b)
	return "0x" + bytesToHex(b)
}

func generateBigInt() *big.Int {
	b := make([]byte, 32)
	rand.Read(b)
	return new(big.Int).SetBytes(b)
}

func bytesToHex(b []byte) string {
	hex := "0123456789abcdef"
	result := make([]byte, len(b)*2)
	for i, v := range b {
		result[i*2] = hex[v>>4]
		result[i*2+1] = hex[v&0x0f]
	}
	return string(result)
}

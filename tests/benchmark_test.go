package tests

import (
	"context"
	"diamante/common"
	"diamante/transaction"
	"fmt"
	"github.com/sirupsen/logrus"
	"runtime"
	"testing"
	"time"
)

func BenchmarkTransactionValidation(b *testing.B) {
	// Create parallel validator
	workers := runtime.NumCPU()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	validator := transaction.NewParallelValidator(workers, logger)

	// Create test transactions
	transactions := make([]common.Transaction, 1000)
	for i := range transactions {
		transactions[i] = common.Transaction{
			ID:        fmt.Sprintf("tx_%d_%d", time.Now().UnixNano(), i),
			Sender:    "0xa000000000000000000000000000000000000001",
			Receiver:  "0xa000000000000000000000000000000000000002",
			Amount:    1.0,
			Fee:       0.01,
			Nonce:     i,
			Timestamp: time.Now().Unix(),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark validation in batches
	batchSize := 100
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}
		batch := make([]*common.Transaction, 0, end-i)
		for j := i; j < end; j++ {
			tx := transactions[j%len(transactions)]
			batch = append(batch, &tx)
		}
		validator.ValidateBatch(
			context.Background(),
			batch,
			func(tx *common.Transaction) error {
				// Simple validation function
				if tx.Amount <= 0 {
					return fmt.Errorf("invalid amount")
				}
				return nil
			},
			func(tx common.Transaction) float64 {
				// Simple priority function based on fee
				return tx.Fee
			},
		)
	}

	// Report transactions per second
	tps := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(tps, "tx/sec")
}

func BenchmarkBatchOperations(b *testing.B) {
	b.ReportAllocs()

	// Simulate batch operations
	batchSize := 1000
	batches := b.N / batchSize
	if batches == 0 {
		batches = 1
	}

	start := time.Now()
	for i := 0; i < batches; i++ {
		// Simulate batch write
		time.Sleep(time.Microsecond * 100) // Simulate I/O
	}
	elapsed := time.Since(start)

	// Calculate throughput
	tps := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(tps, "tx/sec")
}

// tests/ledger/unit/evm_runtime_test.go

package unit

import (
	"math/big"
	"testing"

	"diamante/ledger/evm"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEVMRuntime(t *testing.T) {
	t.Run("CreateEVMRuntime", func(t *testing.T) {
		logger := logrus.New()
		logger.SetLevel(logrus.DebugLevel)

		runtime := evm.NewEVMRuntime()
		require.NotNil(t, runtime)
	})
}

func TestEvents(t *testing.T) {
	t.Run("EventManager creation", func(t *testing.T) {
		em := evm.NewEventManager()
		require.NotNil(t, em)
	})

	t.Run("AddLog and GetLogs", func(t *testing.T) {
		em := evm.NewEventManager()

		// Create test event
		event := evm.EventLog{
			Address:     ethcommon.HexToAddress("0x1234567890123456789012345678901234567890"),
			Topics:      []ethcommon.Hash{{1}, {2}},
			Data:        []byte("test event data"),
			BlockNumber: 100,
			TxHash:      ethcommon.Hash{3},
			TxIndex:     0,
			LogIndex:    0,
		}

		// Add event
		em.AddLog(event)

		// Get events using filter
		filter := evm.EventFilter{
			Addresses: []ethcommon.Address{event.Address},
		}
		events := em.GetLogs(filter)
		require.Len(t, events, 1)
		assert.Equal(t, event.Data, events[0].Data)

		// Get events by topic filter
		filter = evm.EventFilter{
			Topics: [][]ethcommon.Hash{{event.Topics[0]}},
		}
		events = em.GetLogs(filter)
		require.Len(t, events, 1)
		assert.Equal(t, event.Address, events[0].Address)

		// Get events by block
		filter = evm.EventFilter{
			FromBlock: big.NewInt(int64(event.BlockNumber)),
			ToBlock:   big.NewInt(int64(event.BlockNumber)),
		}
		events = em.GetLogs(filter)
		require.Len(t, events, 1)
		assert.Equal(t, event.TxHash, events[0].TxHash)
	})

	t.Run("Multiple events", func(t *testing.T) {
		em := evm.NewEventManager()

		addr1 := ethcommon.HexToAddress("0x1111111111111111111111111111111111111111")
		addr2 := ethcommon.HexToAddress("0x2222222222222222222222222222222222222222")
		topic1 := ethcommon.Hash{1}
		topic2 := ethcommon.Hash{2}

		// Add multiple events
		events := []evm.EventLog{
			{
				Address:     addr1,
				Topics:      []ethcommon.Hash{topic1},
				Data:        []byte("event 1"),
				BlockNumber: 100,
			},
			{
				Address:     addr1,
				Topics:      []ethcommon.Hash{topic2},
				Data:        []byte("event 2"),
				BlockNumber: 100,
			},
			{
				Address:     addr2,
				Topics:      []ethcommon.Hash{topic1},
				Data:        []byte("event 3"),
				BlockNumber: 101,
			},
		}

		for _, event := range events {
			em.AddLog(event)
		}

		// Test filters
		assert.Len(t, em.GetLogs(evm.EventFilter{Addresses: []ethcommon.Address{addr1}}), 2)
		assert.Len(t, em.GetLogs(evm.EventFilter{Addresses: []ethcommon.Address{addr2}}), 1)
		assert.Len(t, em.GetLogs(evm.EventFilter{Topics: [][]ethcommon.Hash{{topic1}}}), 2)
		assert.Len(t, em.GetLogs(evm.EventFilter{Topics: [][]ethcommon.Hash{{topic2}}}), 1)
		assert.Len(t, em.GetLogs(evm.EventFilter{
			FromBlock: big.NewInt(100),
			ToBlock:   big.NewInt(100),
		}), 2)
		assert.Len(t, em.GetLogs(evm.EventFilter{
			FromBlock: big.NewInt(101),
			ToBlock:   big.NewInt(101),
		}), 1)
	})

	t.Run("Clear events", func(t *testing.T) {
		em := evm.NewEventManager()

		// Add some events
		event := evm.EventLog{
			Address:     ethcommon.HexToAddress("0x1234567890123456789012345678901234567890"),
			Topics:      []ethcommon.Hash{{1}},
			Data:        []byte("test"),
			BlockNumber: 100,
		}
		em.AddLog(event)

		// Verify event exists
		assert.Len(t, em.GetLogs(evm.EventFilter{Addresses: []ethcommon.Address{event.Address}}), 1)

		// Clear events
		em.Clear()

		// Verify events cleared
		assert.Len(t, em.GetLogs(evm.EventFilter{Addresses: []ethcommon.Address{event.Address}}), 0)
	})
}

func TestContractVerifier(t *testing.T) {
	t.Run("VerifyContract with valid bytecode", func(t *testing.T) {
		// Create test state DB
		stateDB, _, _ := createTestStateDB(t)
		logger := logrus.New()
		verifier := evm.NewContractVerifier(stateDB, logger)

		// Simple valid bytecode (PUSH1 0x60 PUSH1 0x40 MSTORE)
		bytecode := []byte{0x60, 0x60, 0x60, 0x40, 0x52}
		codeHash := ethcommon.BytesToHash(bytecode).Bytes()
		contractID := "test-contract-1"

		// Verify should fail for non-existent contract
		err := verifier.VerifyContract(contractID, codeHash)
		assert.Error(t, err)

		// Check contract existence
		exists, err := verifier.VerifyContractExistence(contractID)
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestRuntimeTypes(t *testing.T) {
	t.Run("RuntimeConfig", func(t *testing.T) {
		config := &evm.RuntimeConfig{
			ChainID: "1", // ChainID is now a string
		}

		assert.Equal(t, "1", config.ChainID)
	})

	t.Run("ExecutionResult", func(t *testing.T) {
		result := &evm.ExecutionResult{
			ReturnData: []interface{}{"result"}, // ReturnData is []interface{}
			GasUsed:    21000,
		}

		assert.Equal(t, []interface{}{"result"}, result.ReturnData)
		assert.Equal(t, uint64(21000), result.GasUsed)
	})
}

func TestStateProof(t *testing.T) {
	t.Run("StateProof validation", func(t *testing.T) {
		// Create a proof
		proof := &evm.StateProof{
			// StateProof only has Proof field
			Proof: [][]byte{
				{1, 2, 3},
				{4, 5, 6},
			},
		}

		// Verification
		isValid := evm.VerifyStateProof(proof)
		assert.False(t, isValid) // Expected to fail without proper merkle proofs
	})
}

// Test concurrent event operations
func TestConcurrentEvents(t *testing.T) {
	em := evm.NewEventManager()

	// Add events concurrently
	done := make(chan bool, 3)

	// Writer 1
	go func() {
		for i := 0; i < 100; i++ {
			event := evm.EventLog{
				Address:     ethcommon.HexToAddress("0x1111111111111111111111111111111111111111"),
				Topics:      []ethcommon.Hash{{byte(i)}},
				Data:        []byte{byte(i)},
				BlockNumber: uint64(i),
			}
			em.AddLog(event)
		}
		done <- true
	}()

	// Writer 2
	go func() {
		for i := 0; i < 100; i++ {
			event := evm.EventLog{
				Address:     ethcommon.HexToAddress("0x2222222222222222222222222222222222222222"),
				Topics:      []ethcommon.Hash{{byte(i + 100)}},
				Data:        []byte{byte(i + 100)},
				BlockNumber: uint64(i),
			}
			em.AddLog(event)
		}
		done <- true
	}()

	// Reader
	go func() {
		for i := 0; i < 100; i++ {
			_ = em.GetLogs(evm.EventFilter{
				FromBlock: big.NewInt(int64(i)),
				ToBlock:   big.NewInt(int64(i)),
			})
		}
		done <- true
	}()

	// Wait for completion
	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify total events
	addr1Events := em.GetLogs(evm.EventFilter{
		Addresses: []ethcommon.Address{ethcommon.HexToAddress("0x1111111111111111111111111111111111111111")},
	})
	addr2Events := em.GetLogs(evm.EventFilter{
		Addresses: []ethcommon.Address{ethcommon.HexToAddress("0x2222222222222222222222222222222222222222")},
	})

	assert.Equal(t, 100, len(addr1Events))
	assert.Equal(t, 100, len(addr2Events))
}

// Benchmark tests
func BenchmarkEventManager(b *testing.B) {
	em := evm.NewEventManager()

	// Prepare events
	events := make([]evm.EventLog, 1000)
	for i := range events {
		events[i] = evm.EventLog{
			Address:     ethcommon.HexToAddress("0x1234567890123456789012345678901234567890"),
			Topics:      []ethcommon.Hash{{byte(i)}, {byte(i + 1)}},
			Data:        []byte{byte(i)},
			BlockNumber: uint64(i / 10),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.Clear()
		for _, event := range events {
			em.AddLog(event)
		}
		_ = em.GetLogs(evm.EventFilter{
			FromBlock: big.NewInt(50),
			ToBlock:   big.NewInt(50),
		})
	}
}

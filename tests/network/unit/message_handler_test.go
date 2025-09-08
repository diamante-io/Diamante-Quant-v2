package network_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"diamante/network"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	t.Run("Message Creation", func(t *testing.T) {
		payload := network.NewGenericPayload("test_data", map[string]interface{}{
			"key1": "value1",
			"key2": 123,
		})
		payloadBytes, _ := json.Marshal(payload)

		msg := network.Message{
			Type:      "test",
			Payload:   json.RawMessage(payloadBytes),
			ID:        "test-msg-1",
			Timestamp: time.Now().UnixNano(),
		}

		assert.Equal(t, "test", msg.Type)
		assert.Equal(t, "test-msg-1", msg.ID)
		assert.NotNil(t, msg.Payload)
	})

	t.Run("Message Types", func(t *testing.T) {
		// Test different message types
		messageTypes := []string{
			"transaction",
			"block",
			"consensus",
			"sync",
			"heartbeat",
		}

		for _, msgType := range messageTypes {
			payload := network.NewGenericPayload(msgType+"_data", map[string]interface{}{
				"data": "test",
			})
			payloadBytes, _ := json.Marshal(payload)

			msg := network.Message{
				Type:    msgType,
				Payload: json.RawMessage(payloadBytes),
			}
			assert.Equal(t, msgType, msg.Type)
		}
	})

	t.Run("Transaction Payload", func(t *testing.T) {
		txPayload := &network.TransactionPayload{
			TransactionID:   "tx-123",
			FromAddress:     "addr1",
			ToAddress:       "addr2",
			Amount:          1000,
			Fee:             10,
			Nonce:           1,
			Signature:       "sig123",
			TransactionType: "transfer",
		}

		assert.Equal(t, "transaction", txPayload.GetType())
		assert.NoError(t, txPayload.Validate())

		// Test invalid payload
		invalidPayload := &network.TransactionPayload{}
		assert.Error(t, invalidPayload.Validate())
	})

	t.Run("Block Payload", func(t *testing.T) {
		blockPayload := &network.BlockPayload{
			BlockNumber:     100,
			BlockHash:       "hash123",
			ParentHash:      "hash99",
			Timestamp:       time.Now().Unix(),
			TransactionIDs:  []string{"tx1", "tx2"},
			Proposer:        "node1",
			Signature:       "sig123",
			StateRoot:       "state123",
			TransactionRoot: "txroot123",
			Size:            1024,
		}

		assert.Equal(t, "block", blockPayload.GetType())
		assert.NoError(t, blockPayload.Validate())
	})

	t.Run("Vote Payload", func(t *testing.T) {
		votePayload := &network.VotePayload{
			BlockNumber:  100,
			Round:        1,
			BlockHash:    "hash123",
			Signature:    "sig123",
			VoterID:      "node1",
			VoteType:     "prevote",
			VoteID:       "vote123",
			Timestamp:    time.Now().Unix(),
			ValidatorSet: "validators123",
		}

		assert.Equal(t, "vote", votePayload.GetType())
		assert.NoError(t, votePayload.Validate())
	})

	t.Run("Generic Payload", func(t *testing.T) {
		payload := network.NewGenericPayload("custom_type", map[string]interface{}{
			"field1": "value1",
			"field2": 42,
			"field3": true,
			"field4": []string{"a", "b", "c"},
		})

		assert.Equal(t, "custom_type", payload.GetType())
		assert.NoError(t, payload.Validate())

		// Test empty data type
		emptyPayload := &network.GenericPayload{}
		assert.Error(t, emptyPayload.Validate())
	})
}

func TestMessageConcurrency(t *testing.T) {
	// Test concurrent message creation and access
	var wg sync.WaitGroup
	messageCount := 100
	messages := make([]*network.Message, messageCount)

	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			payload := network.NewGenericPayload("concurrent_test", map[string]interface{}{
				"index": idx,
				"data":  "test",
			})
			payloadBytes, _ := json.Marshal(payload)

			messages[idx] = &network.Message{
				Type:      "concurrent",
				ID:        string(rune('A' + (idx % 26))),
				Payload:   json.RawMessage(payloadBytes),
				Timestamp: time.Now().UnixNano(),
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were created
	for i, msg := range messages {
		require.NotNil(t, msg, "Message %d should not be nil", i)
		assert.Equal(t, "concurrent", msg.Type)
	}
}

func BenchmarkMessage(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	b.Run("MessageCreation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			payload := network.NewGenericPayload("benchmark", map[string]interface{}{
				"data": "test",
			})
			payloadBytes, _ := json.Marshal(payload)

			_ = network.Message{
				Type:      "benchmark",
				ID:        string(rune('A' + (i % 26))),
				Payload:   json.RawMessage(payloadBytes),
				Timestamp: time.Now().UnixNano(),
			}
		}
	})

	b.Run("PayloadValidation", func(b *testing.B) {
		payload := &network.TransactionPayload{
			TransactionID:   "tx-bench",
			FromAddress:     "addr1",
			ToAddress:       "addr2",
			Amount:          1000,
			Fee:             10,
			Nonce:           1,
			Signature:       "sig123",
			TransactionType: "transfer",
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = payload.Validate()
		}
	})

	b.Run("GenericPayloadCreation", func(b *testing.B) {
		data := map[string]interface{}{
			"field1": "value1",
			"field2": 42,
			"field3": true,
			"field4": []string{"a", "b", "c"},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = network.NewGenericPayload("benchmark", data)
		}
	})
}

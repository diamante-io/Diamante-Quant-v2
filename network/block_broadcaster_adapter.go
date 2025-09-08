package network

import (
	"encoding/json"
	"fmt"

	"diamante/common"
	"diamante/consensus"
	"github.com/sirupsen/logrus"
)

// BlockBroadcasterAdapter implements consensus.BlockBroadcaster interface
type BlockBroadcasterAdapter struct {
	netMgr *NetworkManager
	logger *logrus.Entry
}

// NewBlockBroadcasterAdapter creates a new adapter
func NewBlockBroadcasterAdapter(netMgr *NetworkManager) *BlockBroadcasterAdapter {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return &BlockBroadcasterAdapter{
		netMgr: netMgr,
		logger: logger.WithField("component", "BlockBroadcasterAdapter"),
	}
}

// BroadcastBlock implements the BlockBroadcaster interface
func (b *BlockBroadcasterAdapter) BroadcastBlock(block *common.Block) error {
	if b.netMgr == nil {
		return fmt.Errorf("network manager not set")
	}

	b.logger.WithField("blockNumber", block.Number).Info("Broadcasting block to peers")

	// Create a block payload message
	blockPayload := &BlockPayload{
		BlockHash:       block.Hash,
		BlockNumber:     uint64(block.Number),
		ParentHash:      block.PreviousHash,
		StateRoot:       block.StateRoot,
		TransactionRoot: block.TransactionRoot,
		Timestamp:       block.Timestamp,
		Proposer:        block.Validator,
		TransactionIDs:  make([]string, len(block.Transactions)),
		Transactions:    block.Transactions, // Include full transaction objects
		Signature:       fmt.Sprintf("%x", block.Signature),
		Size:            uint64(len(block.Data)), // Block size
		GasUsed:         block.GasUsed,
		GasLimit:        block.GasLimit,
	}

	// Extract transaction IDs
	for i, tx := range block.Transactions {
		blockPayload.TransactionIDs[i] = tx.ID
	}

	b.logger.WithFields(logrus.Fields{
		"blockNumber":      block.Number,
		"transactionCount": len(block.Transactions),
	}).Info("Block includes full transactions")

	// Create a block message
	msg := NewBlockMessage(b.netMgr.GetNodeID(), blockPayload)

	// Broadcast to all peers
	peers := b.netMgr.GetPeers()
	successCount := 0

	b.logger.WithFields(logrus.Fields{
		"blockNumber": block.Number,
		"blockHash":   block.Hash,
		"peerCount":   len(peers),
	}).Info("Starting block broadcast")

	for _, peer := range peers {
		b.logger.WithFields(logrus.Fields{
			"blockNumber": block.Number,
			"peer":        peer.Addr,
		}).Debug("Attempting to send block to peer")
		if err := peer.Send(*msg); err != nil {
			b.logger.WithFields(logrus.Fields{
				"blockNumber": block.Number,
				"peer":        peer.Addr,
				"error":       err,
			}).Error("Failed to send block to peer")
		} else {
			b.logger.WithFields(logrus.Fields{
				"blockNumber": block.Number,
				"peer":        peer.Addr,
			}).Debug("Successfully sent block to peer")
			successCount++
		}
	}

	b.logger.WithFields(logrus.Fields{
		"blockNumber":  block.Number,
		"successCount": successCount,
		"totalPeers":   len(peers),
	}).Info("Block broadcast complete")

	if successCount == 0 && len(peers) > 0 {
		return fmt.Errorf("failed to broadcast block to any peer")
	}

	return nil
}

// RequestSync implements the BlockBroadcaster interface
func (b *BlockBroadcasterAdapter) RequestSync(fromHeight, toHeight uint64) error {
	if b.netMgr == nil {
		return fmt.Errorf("network manager not set")
	}

	b.logger.WithFields(logrus.Fields{
		"fromHeight": fromHeight,
		"toHeight":   toHeight,
	}).Info("Requesting sync")

	// Create a sync request payload
	syncPayload := &SyncPayload{
		RequestType: "blocks",
		FromHeight:  fromHeight,
		ToHeight:    toHeight,
	}

	// Marshal the payload
	payloadBytes, err := json.Marshal(syncPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal sync payload: %w", err)
	}

	// Create sync request message
	msg := Message{
		Type:      MessageTypeSync,
		Payload:   payloadBytes,
		IsRequest: true,
		Timestamp: consensus.ConsensusUnixNano(),
	}

	// Broadcast sync request to all peers
	return b.netMgr.Broadcast(msg)
}

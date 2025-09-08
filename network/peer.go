package network

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"
	dtypes "diamante/types"
	"github.com/sirupsen/logrus"
)

// Peer represents a connected node in the network.
type Peer struct {
	Addr     string
	conn     net.Conn
	netMgr   *NetworkManager
	incoming chan Message
	quit     chan struct{}
	mu       sync.Mutex
	// Peer state information
	nodeInfo map[string]interface{}
	infoMu   sync.RWMutex
	logger   *logrus.Logger
}

// NewPeer creates a new Peer.
func NewPeer(addr string, conn net.Conn, nm *NetworkManager) *Peer {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	return &Peer{
		Addr:     addr,
		conn:     conn,
		netMgr:   nm,
		incoming: make(chan Message, 32),
		quit:     make(chan struct{}),
		nodeInfo: make(map[string]interface{}),
		logger:   logger,
	}
}

// Run starts read & write loops for the Peer.
func (p *Peer) Run() {
	go p.readLoop()
	go p.writeLoop()
}

// Close closes the peer’s connection.
func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.quit:
		// Already closed
		return nil
	default:
		close(p.quit)
		err := p.conn.Close()
		p.netMgr.RemovePeer(p.Addr)
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":  p.Addr,
				"error": err,
			}).Error("Failed to close peer connection")
			return fmt.Errorf("failed to close connection for peer %s: %w", p.Addr, err)
		}
		p.logger.WithFields(logrus.Fields{
			"peer": p.Addr,
		}).Info("Peer closed successfully")
		return nil
	}
}

// Send queues a message to be written to this peer.
func (p *Peer) Send(msg Message) error {
	// Check if peer is closed
	select {
	case <-p.quit:
		return fmt.Errorf("peer %s is closed", p.Addr)
	default:
	}

	// Set message defaults if not set
	if msg.Timestamp == 0 {
		msg.Timestamp = consensus.ConsensusUnixNano()
	}
	if msg.ID == "" && msg.IsRequest {
		msg.ID = generateMessageID()
	}

	// Try to send with a timeout
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case p.incoming <- msg:
		// message enqueued successfully
		return nil
	case <-timer.C:
		// Timeout - channel is likely full
		return fmt.Errorf("failed to send to peer %s: timeout (channel full)", p.Addr)
	case <-p.quit:
		return fmt.Errorf("peer %s closed while sending", p.Addr)
	}
}

// readLoop continuously reads messages from the peer’s socket.
func (p *Peer) readLoop() {
	defer p.Close()

	decoder := json.NewDecoder(p.conn)
	for {
		select {
		case <-p.quit:
			return
		default:
		}

		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				p.logger.WithFields(logrus.Fields{
					"peer": p.Addr,
				}).Info("Peer disconnected")
			} else {
				p.logger.WithFields(logrus.Fields{
					"peer":  p.Addr,
					"error": err,
				}).Error("Peer read error")
			}
			return
		}
		p.logger.WithFields(logrus.Fields{
			"peer":         p.Addr,
			"message_type": msg.Type,
			"message_id":   msg.ID,
		}).Debug("Received message from peer")

		// Handle the received message
		if err := p.handleMessage(&msg); err != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":         p.Addr,
				"error":        err,
				"message_type": msg.Type,
			}).Error("Error handling message")
		}
	}
}

// handleMessage processes incoming messages from the peer
func (p *Peer) handleMessage(msg *Message) error {
	// Validate message
	if msg == nil {
		return fmt.Errorf("received nil message")
	}

	// Set timestamp if not present
	if msg.Timestamp == 0 {
		msg.Timestamp = consensus.ConsensusUnixNano()
	}

	// Check if this is a response to a previous request
	if !msg.IsRequest && msg.RequestID != "" {
		// This is a response, route it to the request-response manager
		if p.netMgr != nil && p.netMgr.reqRespMgr != nil {
			return p.netMgr.reqRespMgr.HandleResponse(msg)
		}
		return fmt.Errorf("no request-response manager available to handle response")
	}

	// Handle different message types
	switch msg.Type {
	case "keepalive":
		// Keepalive messages don't need special handling
		return nil

	case MessageTypeHeartbeat, "Heartbeat":
		return p.handleHeartbeat(msg)

	case MessageTypeHeartbeatResponse, "HeartbeatResponse":
		return p.handleHeartbeat(msg)

	case MessageTypeStateRequest:
		return p.handleStateRequest(msg)

	case MessageTypeTransaction:
		return p.handleTransactionBroadcast(msg)

	case MessageTypeBlock:
		return p.handleBlockProposal(msg)

	case MessageTypeVote:
		return p.handleBlockVote(msg)

	case MessageTypeSync:
		return p.handleSyncRequest(msg)

	case "SyncResponse":
		return p.handleSyncResponse(msg)

	default:
		// Unknown message type, log for debugging
		p.logger.WithFields(logrus.Fields{
			"peer":         p.Addr,
			"message_type": msg.Type,
		}).Warn("Received unknown message type")
		// Optionally send error response for unknown request types
		if msg.IsRequest {
			response := NewResponseMessage(msg, NewGenericPayload("error", map[string]interface{}{
				"status": "error",
				"error":  fmt.Sprintf("unknown message type: %s", msg.Type),
			}))
			response.Sender = p.netMgr.GetNodeID()
			if err := p.Send(*response); err != nil {
				p.logger.WithFields(logrus.Fields{
					"peer":  p.Addr,
					"error": err,
				}).Error("Failed to send error response")
			}
		}
		return nil
	}
}

// handleHeartbeat processes heartbeat messages
func (p *Peer) handleHeartbeat(msg *Message) error {
	if msg.IsRequest {
		// Extract heartbeat payload from json.RawMessage
		var heartbeatPayload HeartbeatPayload
		if err := json.Unmarshal(msg.Payload, &heartbeatPayload); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat payload: %w", err)
		}

		// Get blockchain state for response
		blockHeight := uint64(0)
		latestHash := ""
		if p.netMgr.GetConsensusAdapter() != nil {
			if state, err := p.netMgr.GetConsensusAdapter().GetConsensusState(); err == nil {
				if height, ok := state["blockHeight"].(int); ok {
					blockHeight = uint64(height)
				} else if height, ok := state["blockHeight"].(uint64); ok {
					blockHeight = height
				}
				if hash, ok := state["latestBlockHash"].(string); ok {
					latestHash = hash
				}
			}
		}

		// Create response heartbeat
		responsePayload := &HeartbeatPayload{
			NodeID:        p.netMgr.GetNodeID(),
			Timestamp:     consensus.ConsensusUnix(),
			BlockHeight:   blockHeight,
			LatestHash:    latestHash,
			PeerCount:     len(p.netMgr.peers),
			NetworkStatus: "healthy",
			Version:       "1.0.0",
		}

		// Generate signature for heartbeat
		if p.netMgr != nil && p.netMgr.GetNodeID() != "" {
			payloadBytes, _ := json.Marshal(responsePayload)
			hashStr := common.HashData(payloadBytes)
			responsePayload.Signature = hashStr[:64] // Use first 32 bytes (64 hex chars)
		} else {
			responsePayload.Signature = ""
		}

		response := NewResponseMessage(msg, responsePayload)
		response.Sender = p.netMgr.GetNodeID()
		if err := p.Send(*response); err != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":  p.Addr,
				"error": err,
			}).Error("Failed to send heartbeat response")
			return err
		}

		// Update peer info
		p.updatePeerInfo(map[string]interface{}{
			"nodeID":          heartbeatPayload.NodeID,
			"blockHeight":     heartbeatPayload.BlockHeight,
			"latestBlockHash": heartbeatPayload.LatestHash,
			"peerCount":       heartbeatPayload.PeerCount,
			"version":         heartbeatPayload.Version,
		})

		// Log heartbeat received
		p.logger.WithFields(logrus.Fields{
			"event":            "heartbeatRecv",
			"peerID":           p.Addr,
			"blockHeight":      heartbeatPayload.BlockHeight,
			"latestBlockHash8": heartbeatPayload.LatestHash[:16],
		}).Info("Received heartbeat from peer")
	} else {
		// Handle heartbeat response
		// Extract heartbeat payload from json.RawMessage
		var heartbeatPayload HeartbeatPayload
		if err := json.Unmarshal(msg.Payload, &heartbeatPayload); err != nil {
			return fmt.Errorf("failed to unmarshal heartbeat response payload: %w", err)
		}

		// Update peer info from response
		p.updatePeerInfo(map[string]interface{}{
			"nodeID":          heartbeatPayload.NodeID,
			"blockHeight":     heartbeatPayload.BlockHeight,
			"latestBlockHash": heartbeatPayload.LatestHash,
			"peerCount":       heartbeatPayload.PeerCount,
			"version":         heartbeatPayload.Version,
		})

		// Log heartbeat response received
		p.logger.WithFields(logrus.Fields{
			"event":            "heartbeatRecv",
			"peerID":           p.Addr,
			"blockHeight":      heartbeatPayload.BlockHeight,
			"latestBlockHash8": heartbeatPayload.LatestHash[:16],
			"isResponse":       true,
		}).Info("Received heartbeat response from peer")
	}
	return nil
}

// handleStateRequest processes state request messages
func (p *Peer) handleStateRequest(msg *Message) error {
	if !msg.IsRequest {
		return nil
	}

	// Get the current blockchain state
	state, err := p.getBlockchainState()
	if err != nil {
		// Send error response
		response := NewResponseMessage(msg, NewGenericPayload("error", map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}))
		response.Sender = p.netMgr.GetNodeID()
		if sendErr := p.Send(*response); sendErr != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":  p.Addr,
				"error": sendErr,
			}).Error("Failed to send error response")
		}
		return err
	}

	// Create state payload
	statePayload := &StatePayload{
		StateHash:   state["latestBlockHash"].(string),
		BlockHeight: uint64(state["blockHeight"].(int)),
		StateData:   make(map[string]string),
		Timestamp:   consensus.ConsensusUnix(),
		NodeID:      p.netMgr.GetNodeID(),
	}

	// Generate deterministic signature for state payload
	payloadData, _ := json.Marshal(statePayload)
	hashStr := common.HashData(payloadData)
	statePayload.Signature = hashStr[:64] // Use first 32 bytes (64 hex chars)

	// Convert state data
	for k, v := range state {
		if k != "latestBlockHash" && k != "blockHeight" {
			statePayload.StateData[k] = fmt.Sprintf("%v", v)
		}
	}

	// Send state response
	response := NewResponseMessage(msg, statePayload)
	response.Sender = p.netMgr.GetNodeID()
	if err := p.Send(*response); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Failed to send state response")
		return err
	}
	return nil
}

// handleTransactionBroadcast processes transaction broadcast messages
func (p *Peer) handleTransactionBroadcast(msg *Message) error {
	p.logger.WithFields(logrus.Fields{
		"peer": p.Addr,
	}).Debug("Received transaction broadcast")

	// Extract transaction payload from json.RawMessage
	var txPayload TransactionPayload
	if err := json.Unmarshal(msg.Payload, &txPayload); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Failed to unmarshal transaction payload")
		return fmt.Errorf("failed to unmarshal transaction payload: %w", err)
	}

	p.logger.WithFields(logrus.Fields{
		"peer":  p.Addr,
		"tx_id": txPayload.TransactionID,
	}).Debug("Parsed transaction from peer")

	// Validate transaction payload structure
	if err := p.validateTransactionPayload(&txPayload); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Invalid transaction from peer")
		return err
	}

	// Convert payload to common.Transaction preserving all fields
	tx := &common.Transaction{
		ID:        txPayload.TransactionID,
		Sender:    txPayload.FromAddress,
		Receiver:  txPayload.ToAddress,
		Amount:    float64(txPayload.Amount) / 1e8, // Convert from smallest unit
		Fee:       float64(txPayload.Fee) / 1e8,
		Nonce:     int(txPayload.Nonce),
		Timestamp: txPayload.Timestamp,
		Data:      txPayload.Data,
	}

	// Decode hex signature
	if sig, err := hex.DecodeString(txPayload.Signature); err == nil {
		tx.Signature = sig
	} else {
		// Fallback - use raw string as bytes
		tx.Signature = []byte(txPayload.Signature)
	}

	// Verify transaction signature
	if err := common.VerifyTransactionSignature(tx); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Invalid transaction signature")
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Add to transaction pool via handler
	added := false
	reason := "no handler"
	if err := p.addTransactionToPool(&txPayload); err != nil {
		reason = err.Error()
		p.logger.WithFields(logrus.Fields{
			"tx_id":  tx.ID,
			"added":  false,
			"reason": reason,
		}).Info("Incoming transaction not added")
		return err
	} else {
		added = true
		reason = "success"
	}

	// Propagate to other peers (excluding sender)
	p.propagateTransaction(&txPayload, msg.Sender)

	p.logger.WithFields(logrus.Fields{
		"tx_id":  tx.ID,
		"added":  added,
		"reason": reason,
	}).Info("Processed incoming transaction")
	return nil
}

// handleBlockProposal processes block proposal messages
func (p *Peer) handleBlockProposal(msg *Message) error {
	// Extract block payload
	var blockPayload BlockPayload
	if err := json.Unmarshal(msg.Payload, &blockPayload); err != nil {
		return fmt.Errorf("invalid block proposal payload: %v", err)
	}

	// Validate block proposal
	if err := p.validateBlockPayload(&blockPayload); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Invalid block proposal")
		return err
	}

	// Calculate hash8 for logging
	hash8 := ""
	if len(blockPayload.BlockHash) >= 8 {
		hash8 = blockPayload.BlockHash[:8]
	} else {
		hash8 = blockPayload.BlockHash
	}

	// Get current blockchain state to check if we need to sync
	currentState, err := p.getBlockchainState()
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Failed to get blockchain state")
		// Continue anyway - let consensus handle it
	} else {
		currentHeight := uint64(0)
		if height, ok := currentState["blockHeight"].(int); ok {
			currentHeight = uint64(height)
		}

		p.logger.WithFields(logrus.Fields{
			"event":          "BlockProposalReceived",
			"height":         blockPayload.BlockNumber,
			"hash8":          hash8,
			"proposerID":     blockPayload.Proposer,
			"peer":           p.Addr,
			"current_height": currentHeight,
			"parent_hash":    blockPayload.ParentHash,
			"tx_count":       len(blockPayload.TransactionIDs),
		}).Info("Received block proposal from peer")

		// If this block is far ahead, trigger sync
		if blockPayload.BlockNumber > currentHeight+1 {
			gap := blockPayload.BlockNumber - currentHeight - 1
			p.logger.WithFields(logrus.Fields{
				"received_block": blockPayload.BlockNumber,
				"current_height": currentHeight,
				"gap":            gap,
			}).Warn("Block gap detected")

			// Request sync for missing blocks
			syncRequest := &SyncPayload{
				RequestType: "blocks",
				FromHeight:  currentHeight + 1,
				ToHeight:    blockPayload.BlockNumber - 1,
				MaxItems:    300,
				NodeID:      p.netMgr.GetNodeID(),
				Timestamp:   consensus.ConsensusUnix(),
			}

			// Generate deterministic sync signature
			syncData, _ := json.Marshal(syncRequest)
			hashStr := common.HashData(syncData)
			syncRequest.Signature = hashStr[:64] // Use first 32 bytes (64 hex chars)

			// Send sync request
			msg := NewSyncMessage(p.netMgr.GetNodeID(), syncRequest)
			if err := p.Send(*msg); err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to request missing blocks")
			} else {
				p.logger.WithFields(logrus.Fields{
					"from_height": currentHeight + 1,
					"to_height":   blockPayload.BlockNumber - 1,
					"peer":        p.Addr,
				}).Info("Requested missing blocks")
			}

			// Still try to forward the block - consensus will handle the gap
			// This ensures we don't drop future blocks
			p.logger.WithFields(logrus.Fields{
				"block_number": blockPayload.BlockNumber,
			}).Info("Forwarding future block to consensus")
		}
	}

	// Forward to consensus module
	if err := p.forwardBlockToConsensus(&blockPayload); err != nil {
		p.logger.WithFields(logrus.Fields{
			"event":  "ForwardBlockToConsensusFailed",
			"height": blockPayload.BlockNumber,
			"hash8":  hash8,
			"error":  err.Error(),
		}).Error("Failed to forward block proposal to consensus")
		return err
	}

	// Log successful forwarding
	p.logger.WithFields(logrus.Fields{
		"event":  "ForwardBlockToConsensusSuccess",
		"height": blockPayload.BlockNumber,
		"hash8":  hash8,
	}).Debug("Successfully queued block for consensus")

	return nil
}

// handleBlockVote processes block vote messages
func (p *Peer) handleBlockVote(msg *Message) error {
	// Extract vote payload
	var votePayload VotePayload
	if err := json.Unmarshal(msg.Payload, &votePayload); err != nil {
		return fmt.Errorf("invalid block vote payload: %v", err)
	}

	// Validate vote
	if err := votePayload.Validate(); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Invalid block vote")
		return err
	}

	// Forward to consensus module
	if err := p.forwardVoteToConsensus(&votePayload); err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Failed to forward block vote to consensus")
		return err
	}

	return nil
}

// handleSyncRequest processes sync request messages
func (p *Peer) handleSyncRequest(msg *Message) error {
	if !msg.IsRequest {
		return nil
	}

	// Extract sync payload
	var syncPayload SyncPayload
	if err := json.Unmarshal(msg.Payload, &syncPayload); err != nil {
		return fmt.Errorf("invalid sync payload: %v", err)
	}

	// Get sync data based on parameters
	syncData, err := p.getSyncDataFromPayload(&syncPayload)
	if err != nil {
		// Send error response
		response := NewResponseMessage(msg, NewGenericPayload("error", map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}))
		response.Sender = p.netMgr.GetNodeID()
		if sendErr := p.Send(*response); sendErr != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":  p.Addr,
				"error": sendErr,
			}).Error("Failed to send error response")
		}
		return err
	}

	// Send sync response
	response := NewResponseMessage(msg, syncData)
	response.Sender = p.netMgr.GetNodeID()
	if err := p.Send(*response); err != nil {
		p.logger.WithFields(logrus.Fields{
			"peer":  p.Addr,
			"error": err,
		}).Error("Failed to send sync response")
		return err
	}
	return nil
}

// handleSyncResponse processes sync response messages
func (p *Peer) handleSyncResponse(msg *Message) error {
	// Parse the sync response using typed envelope
	var envelope SyncBlocksEnvelope
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil {
		// Try parsing as GenericPayload for backward compatibility
		var genericPayload GenericPayload
		if err2 := json.Unmarshal(msg.Payload, &genericPayload); err2 != nil {
			p.logger.WithFields(logrus.Fields{
				"peer":  p.Addr,
				"error": err,
			}).Error("Failed to parse sync response")
			return fmt.Errorf("invalid sync response payload: %w", err)
		}

		// Convert generic payload to typed envelope
		if genericPayload.DataType == "sync_blocks" && genericPayload.Data != nil {
			// Extract blocks from generic payload fields
			blocksField, hasBlocks := genericPayload.Data.Fields["blocks"]
			if !hasBlocks || blocksField == nil {
				p.logger.WithFields(logrus.Fields{
					"peer": p.Addr,
				}).Error("Sync response missing blocks field")
				return fmt.Errorf("sync response missing blocks")
			}

			// Parse blocks array
			blocksData := string(blocksField.Data)
			var blocks []common.Block
			if err := json.Unmarshal([]byte(blocksData), &blocks); err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to parse blocks from sync response")
				return fmt.Errorf("invalid blocks data: %w", err)
			}

			// Build typed payload
			payload := SyncBlocksPayload{
				Status:     "ok",
				Blocks:     blocks,
				FromHeight: 0,
				ToHeight:   0,
			}

			// Extract heights if available
			if fromField, ok := genericPayload.Data.Fields["fromHeight"]; ok && fromField != nil {
				if val, err := strconv.ParseUint(string(fromField.Data), 10, 64); err == nil {
					payload.FromHeight = val
				}
			}
			if toField, ok := genericPayload.Data.Fields["toHeight"]; ok && toField != nil {
				if val, err := strconv.ParseUint(string(toField.Data), 10, 64); err == nil {
					payload.ToHeight = val
				}
			}

			envelope = SyncBlocksEnvelope{
				Type:    "sync_blocks",
				Payload: payload,
			}
		} else {
			return fmt.Errorf("unexpected sync response type: %s", genericPayload.DataType)
		}
	}

	// Validate envelope
	if envelope.Type != "sync_blocks" {
		return fmt.Errorf("unexpected envelope type: %s", envelope.Type)
	}

	if envelope.Payload.Status != "ok" {
		p.logger.WithFields(logrus.Fields{
			"status": envelope.Payload.Status,
		}).Error("Sync response status not ok")
		return fmt.Errorf("sync failed: %s", envelope.Payload.Status)
	}

	// Log sync response details
	p.logger.WithFields(logrus.Fields{
		"peer":        p.Addr,
		"block_count": len(envelope.Payload.Blocks),
		"from_height": envelope.Payload.FromHeight,
		"to_height":   envelope.Payload.ToHeight,
	}).Info("Received sync response")

	// Handle empty sync response - peer might be having issues
	if len(envelope.Payload.Blocks) == 0 {
		p.logger.WithFields(logrus.Fields{
			"peer":        p.Addr,
			"from_height": envelope.Payload.FromHeight,
			"to_height":   envelope.Payload.ToHeight,
		}).Warn("Received empty sync response")

		// If we expected blocks but got none, try a different peer or retry later
		// The periodicSyncCheck will retry with exponential backoff
		return nil
	}

	// Apply blocks in ascending order
	appliedCount := 0
	blockHandler := p.netMgr.GetBlockHandler()
	if blockHandler == nil {
		p.logger.Error("No block handler available to process sync blocks")
		return fmt.Errorf("block handler not configured")
	}

	// Get current blockchain state
	currentState, err := p.getBlockchainState()
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Failed to get blockchain state")
		return err
	}

	currentHeight := uint64(0)
	if height, ok := currentState["blockHeight"].(int); ok {
		currentHeight = uint64(height)
	}
	currentHash := ""
	if hash, ok := currentState["latestBlockHash"].(string); ok {
		currentHash = hash
	}

	// Sort blocks by height to ensure proper order
	sortedBlocks := make([]common.Block, len(envelope.Payload.Blocks))
	copy(sortedBlocks, envelope.Payload.Blocks)
	for i := 0; i < len(sortedBlocks); i++ {
		for j := i + 1; j < len(sortedBlocks); j++ {
			if sortedBlocks[i].Number > sortedBlocks[j].Number {
				sortedBlocks[i], sortedBlocks[j] = sortedBlocks[j], sortedBlocks[i]
			}
		}
	}

	// Process each block
	for _, block := range sortedBlocks {
		blockHeight := uint64(block.Number)

		// Skip if we already have this block
		if blockHeight <= currentHeight {
			p.logger.WithFields(logrus.Fields{
				"block_height":   blockHeight,
				"current_height": currentHeight,
			}).Debug("Skipping block (already have)")
			continue
		}

		// Check if this is the direct successor
		if blockHeight == currentHeight+1 {
			// Verify previous hash matches
			if currentHeight > 0 && block.PreviousHash != currentHash {
				p.logger.WithFields(logrus.Fields{
					"block_height":  blockHeight,
					"expected_hash": currentHash,
					"got_hash":      block.PreviousHash,
				}).Warn("Block has wrong previous hash")
				// This might indicate we're on a different chain
				continue
			}

			// Convert to BlockPayload and apply
			blockPayload := &BlockPayload{
				BlockHash:       block.Hash,
				BlockNumber:     blockHeight,
				ParentHash:      block.PreviousHash,
				StateRoot:       block.StateRoot,
				TransactionRoot: block.TransactionRoot,
				Timestamp:       block.Timestamp,
				Proposer:        block.Validator,
				TransactionIDs:  make([]string, len(block.Transactions)),
				Signature:       fmt.Sprintf("%x", block.Signature),
				Size:            uint64(len(block.Data)),
				GasUsed:         block.GasUsed,
				GasLimit:        block.GasLimit,
			}

			// Extract transaction IDs
			for i, tx := range block.Transactions {
				blockPayload.TransactionIDs[i] = tx.ID
			}

			// Apply the block
			prevOk := currentHeight == 0 || block.PreviousHash == currentHash
			p.logger.WithFields(logrus.Fields{
				"block_height": blockHeight,
				"block_hash":   block.Hash,
				"parent_hash":  block.PreviousHash,
				"peer":         p.Addr,
			}).Info("Applying sync block")

			// Apply block through block handler
			if err := blockHandler(blockPayload); err != nil {
				p.logger.WithFields(logrus.Fields{
					"event":          "ApplySyncBlock",
					"block_num":      blockHeight,
					"prev_ok":        prevOk,
					"applied":        false,
					"reason":         err.Error(),
					"current_height": currentHeight,
					"peer":           p.Addr,
				}).Info("ApplySyncBlock")
				// If it's a gap error, request the missing blocks
				if strings.HasPrefix(err.Error(), "BLOCK_GAP:") {
					// Parse gap from error message
					var current, received, gap uint64
					fmt.Sscanf(err.Error(), "BLOCK_GAP: current=%d, received=%d, gap=%d", &current, &received, &gap)
					p.logger.WithFields(logrus.Fields{
						"need_from": current + 1,
						"need_to":   received - 1,
					}).Warn("Gap detected during sync apply")

					// Request the missing blocks
					syncRequest := &SyncPayload{
						RequestType: "blocks",
						FromHeight:  current + 1,
						ToHeight:    received - 1,
						MaxItems:    300,
						NodeID:      p.netMgr.GetNodeID(),
						Timestamp:   consensus.ConsensusUnix(),
					}

					// Generate deterministic sync signature
					syncData, _ := json.Marshal(syncRequest)
					hashStr := common.HashData(syncData)
					syncRequest.Signature = hashStr[:64] // Use first 32 bytes (64 hex chars)
					msg := NewSyncMessage(p.netMgr.GetNodeID(), syncRequest)
					if sendErr := p.Send(*msg); sendErr != nil {
						p.logger.WithFields(logrus.Fields{
							"error": sendErr,
						}).Error("Failed to request gap blocks")
					}
					// Stop processing this batch
					break
				}
			} else {
				p.logger.WithFields(logrus.Fields{
					"event":          "ApplySyncBlock",
					"block_num":      blockHeight,
					"prev_ok":        prevOk,
					"applied":        true,
					"reason":         "success",
					"current_height": currentHeight,
					"peer":           p.Addr,
				}).Info("ApplySyncBlock")
				appliedCount++
				currentHeight = blockHeight
				currentHash = block.Hash
			}
		} else if blockHeight > currentHeight+1 {
			// We have a gap - need to request missing blocks
			gap := blockHeight - currentHeight - 1
			p.logger.WithFields(logrus.Fields{
				"gap":            gap,
				"current_height": currentHeight,
				"block_height":   blockHeight,
			}).Warn("Gap detected")

			// Request the missing range
			syncRequest := &SyncPayload{
				RequestType: "blocks",
				FromHeight:  currentHeight + 1,
				ToHeight:    blockHeight - 1,
				MaxItems:    300,
				NodeID:      p.netMgr.GetNodeID(),
				Timestamp:   consensus.ConsensusUnix(),
			}

			// Generate deterministic sync signature
			syncData, _ := json.Marshal(syncRequest)
			hashStr := common.HashData(syncData)
			syncRequest.Signature = hashStr[:64] // Use first 32 bytes (64 hex chars)

			// Send sync request
			msg := NewSyncMessage(p.netMgr.GetNodeID(), syncRequest)
			if err := p.Send(*msg); err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to request missing blocks")
			} else {
				p.logger.WithFields(logrus.Fields{
					"from_height": currentHeight + 1,
					"to_height":   blockHeight - 1,
					"peer":        p.Addr,
				}).Info("Requested missing blocks")
			}

			// Stop processing this batch since we have a gap
			break
		}
	}

	// Get final blockchain state
	finalState, _ := p.getBlockchainState()
	finalHeight := uint64(0)
	if height, ok := finalState["blockHeight"].(int); ok {
		finalHeight = uint64(height)
	}

	// Log results
	p.logger.WithFields(logrus.Fields{
		"applied_count": appliedCount,
		"from_height":   envelope.Payload.FromHeight,
		"to_height":     envelope.Payload.ToHeight,
		"final_height":  finalHeight,
		"final_hash": func() string {
			if finalState != nil {
				if hash, ok := finalState["latestBlockHash"].(string); ok {
					return hash
				}
			}
			return "unknown"
		}(),
	}).Info("Sync response processing complete")

	return nil
}

// Helper methods

// GetNodeState returns the current node state (public method)
func (p *Peer) GetNodeState() map[string]interface{} {
	return p.getNodeState()
}

// getNodeState returns the current node state for heartbeats
func (p *Peer) getNodeState() map[string]interface{} {
	p.infoMu.RLock()
	defer p.infoMu.RUnlock()

	// Return stored peer info
	state := make(map[string]interface{})
	for k, v := range p.nodeInfo {
		state[k] = v
	}

	// Add defaults if not present
	if _, ok := state["version"]; !ok {
		state["version"] = "1.0.0"
	}
	if _, ok := state["blockHeight"]; !ok {
		state["blockHeight"] = 0
	}

	return state
}

// updatePeerInfo updates stored information about the peer
func (p *Peer) updatePeerInfo(info map[string]interface{}) {
	p.infoMu.Lock()
	defer p.infoMu.Unlock()

	// Update stored peer info
	for k, v := range info {
		p.nodeInfo[k] = v
	}
	p.nodeInfo["lastUpdate"] = time.Now().Unix()

	p.logger.WithFields(logrus.Fields{
		"peer": p.Addr,
		"info": info,
	}).Debug("Updated peer info")
}

// getBlockchainState returns the current blockchain state
func (p *Peer) getBlockchainState() (map[string]interface{}, error) {
	// Always prefer consensus adapter data if available
	if p.netMgr.consensusAdapter != nil {
		consensusState, err := p.netMgr.consensusAdapter.GetConsensusState()
		if err == nil && consensusState != nil {
			// Ensure we have all required fields
			if _, hasHeight := consensusState["blockHeight"]; !hasHeight {
				consensusState["blockHeight"] = 0
			}
			if _, hasHash := consensusState["latestBlockHash"]; !hasHash {
				consensusState["latestBlockHash"] = ""
			}
			if _, hasStatus := consensusState["status"]; !hasStatus {
				consensusState["status"] = "ok"
			}
			if _, hasTimestamp := consensusState["timestamp"]; !hasTimestamp {
				consensusState["timestamp"] = consensus.ConsensusUnix()
			}
			p.logger.WithFields(logrus.Fields{
				"height": consensusState["blockHeight"],
				"hash":   consensusState["latestBlockHash"],
			}).Debug("Using consensus adapter data")
			return consensusState, nil
		}
		p.logger.WithFields(logrus.Fields{
			"error": err,
		}).Debug("Consensus adapter returned error or nil")
	}

	// Fallback state only if no consensus adapter
	p.logger.Debug("No consensus adapter, returning minimal state")
	state := map[string]interface{}{
		"status":          "ok",
		"blockHeight":     0,
		"latestBlockHash": "",
		"timestamp":       consensus.ConsensusUnix(),
	}
	return state, nil
}

// validateTransactionPayload validates incoming transaction payload
func (p *Peer) validateTransactionPayload(txPayload *TransactionPayload) error {
	return txPayload.Validate()
}

// addTransactionToPool adds validated transaction to the pool
func (p *Peer) addTransactionToPool(txPayload *TransactionPayload) error {
	// Get the transaction handler from the network manager
	handler := p.netMgr.GetTransactionHandler()
	if handler != nil {
		// Log transaction handler invocation
		p.logger.WithFields(logrus.Fields{
			"tx_id": txPayload.TransactionID,
		}).Debug("Invoking transaction handler")

		// Call the transaction handler
		if err := handler(txPayload); err != nil {
			p.logger.WithFields(logrus.Fields{
				"tx_id": txPayload.TransactionID,
				"error": err,
			}).Error("Transaction handler error")
			return fmt.Errorf("transaction handler error: %w", err)
		}
		p.logger.WithFields(logrus.Fields{
			"tx_id": txPayload.TransactionID,
		}).Debug("Transaction handler success")
	} else {
		// Fallback logging if no handler is set
		p.logger.WithFields(logrus.Fields{
			"tx_id": txPayload.TransactionID,
		}).Warn("No transaction handler registered")
	}
	return nil
}

// propagateTransaction propagates transaction to other peers
func (p *Peer) propagateTransaction(txPayload *TransactionPayload, excludePeer string) {
	// Get all peers except the sender
	peers, err := p.netMgr.GetPeerList()
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Failed to get peer list for transaction propagation")
		return
	}
	for _, peerID := range peers {
		if peerID != excludePeer && peerID != p.Addr {
			// Send transaction to peer
			peer := p.netMgr.GetPeerByID(peerID)
			if peer != nil {
				payloadBytes, err := json.Marshal(txPayload)
				if err != nil {
					p.logger.WithFields(logrus.Fields{
						"error": err,
					}).Error("Failed to marshal transaction payload")
					continue
				}
				if err := peer.Send(Message{
					Type:    MessageTypeTransaction,
					Payload: payloadBytes,
					Sender:  p.netMgr.GetNodeID(),
				}); err != nil {
					p.logger.WithFields(logrus.Fields{
						"peer_id": peerID,
						"error":   err,
					}).Error("Failed to propagate transaction")
					// Continue propagating to other peers even if one fails
				}
			}
		}
	}
}

// validateBlockPayload validates a block payload
func (p *Peer) validateBlockPayload(blockPayload *BlockPayload) error {
	return blockPayload.Validate()
}

// forwardBlockToConsensus forwards a block to the consensus module
func (p *Peer) forwardBlockToConsensus(blockPayload *BlockPayload) error {
	// Check if network manager has a block handler
	if p.netMgr != nil {
		if handler := p.netMgr.GetBlockHandler(); handler != nil {
			// Calculate hash8 for logging
			hash8 := ""
			if len(blockPayload.BlockHash) >= 8 {
				hash8 = blockPayload.BlockHash[:8]
			} else {
				hash8 = blockPayload.BlockHash
			}

			p.logger.WithFields(logrus.Fields{
				"event":       "ForwardBlockToConsensus",
				"height":      blockPayload.BlockNumber,
				"hash8":       hash8,
				"parent_hash": blockPayload.ParentHash,
			}).Debug("Forwarding block to consensus handler")

			err := handler(blockPayload)
			if err != nil {
				p.logger.WithFields(logrus.Fields{
					"event":  "ForwardBlockToConsensusFailed",
					"height": blockPayload.BlockNumber,
					"hash8":  hash8,
					"error":  err.Error(),
				}).Error("Failed to forward block to consensus")
			} else {
				p.logger.WithFields(logrus.Fields{
					"event":  "ForwardBlockToConsensusSuccess",
					"height": blockPayload.BlockNumber,
					"hash8":  hash8,
				}).Debug("Successfully forwarded block to consensus")
			}
			return err
		}
	}

	// No handler available, just log
	p.logger.WithFields(logrus.Fields{
		"block_hash": blockPayload.BlockHash,
	}).Warn("No block handler available")
	return nil
}

// forwardVoteToConsensus forwards a vote to the consensus module
func (p *Peer) forwardVoteToConsensus(votePayload *VotePayload) error {
	// This should interface with the actual consensus module
	// For now, just log
	p.logger.WithFields(logrus.Fields{
		"vote_id": votePayload.VoteID,
	}).Debug("Forwarding vote to consensus module")
	return nil
}

// getSyncDataFromPayload returns sync data based on sync payload
func (p *Peer) getSyncDataFromPayload(syncPayload *SyncPayload) (MessagePayload, error) {
	// Check if network manager has a sync handler
	if p.netMgr != nil {
		if handler := p.netMgr.GetSyncHandler(); handler != nil {
			return handler(syncPayload)
		}
	}

	// Fallback to default behavior if no handler is set
	switch syncPayload.RequestType {
	case "blocks":
		// Return block data (would normally query the ledger)
		return NewGenericPayload("sync_blocks", map[string]interface{}{
			"status":     "ok",
			"blocks":     []interface{}{},
			"fromHeight": syncPayload.FromHeight,
			"toHeight":   syncPayload.ToHeight,
		}), nil

	case "state":
		// Return state snapshot
		state, err := p.getBlockchainState()
		if err != nil {
			return nil, err
		}
		return NewGenericPayload("sync_state", state), nil

	default:
		// Return general sync info
		return NewGenericPayload("sync_info", map[string]interface{}{
			"status":      "ok",
			"blockHeight": 0, // Should come from ledger
			"syncTypes":   []string{"blocks", "state"},
		}), nil
	}
}

// writeLoop sends any queued messages to the peer.
func (p *Peer) writeLoop() {
	encoder := json.NewEncoder(p.conn)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.quit:
			return
		case msg := <-p.incoming:
			if err := encoder.Encode(msg); err != nil {
				p.logger.WithFields(logrus.Fields{
					"peer":  p.Addr,
					"error": err,
				}).Error("Peer write error")
				return
			}
		case <-ticker.C:
			// Periodic keepalive or ping, if desired
			genericPayload := &GenericPayload{
				Data: &dtypes.GenericPayloadData{
					Fields: map[string]*dtypes.Value{
						"time": dtypes.StringToValue(consensus.ConsensusNow().Format(time.RFC3339)),
					},
				},
				DataType: "keepalive",
			}
			payloadBytes, err := json.Marshal(genericPayload)
			if err != nil {
				p.logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("Failed to marshal keepalive payload")
				return
			}
			keepalive := Message{
				Type:    "keepalive",
				Payload: payloadBytes,
			}
			if err := encoder.Encode(keepalive); err != nil {
				p.logger.WithFields(logrus.Fields{
					"peer":  p.Addr,
					"error": err,
				}).Error("Keepalive write error")
				return
			}
		}
	}
}

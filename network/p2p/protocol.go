package p2p

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"diamante/apperrors"
	"diamante/common"
	"diamante/consensus"
	"diamante/crypto"

	"github.com/sirupsen/logrus"
)

// Protocol constants
const (
	// Magic bytes for Diamante protocol: "DIAM" in hex
	ProtocolMagic = uint32(0x4449414D)

	// Protocol version
	ProtocolVersion = uint8(1)

	// Maximum message size (10MB)
	MaxMessageSize = 10 * 1024 * 1024

	// Minimum message size (header only)
	MinMessageSize = 32

	// Header size in bytes
	HeaderSize = 32

	// Compression threshold (1KB)
	CompressionThreshold = 1024
)

// MessageType represents different types of P2P messages
type MessageType uint8

const (
	// Core protocol messages
	MessageTypeHandshake MessageType = iota
	MessageTypePing
	MessageTypePong
	MessageTypeDisconnect

	// Blockchain messages
	MessageTypeTransaction
	MessageTypeBlock
	MessageTypeBlockRequest
	MessageTypeBlockResponse

	// State synchronization
	MessageTypeStateRequest
	MessageTypeStateResponse
	MessageTypeStateSync

	// Peer discovery
	MessageTypePeerAnnounce
	MessageTypePeerRequest
	MessageTypePeerResponse

	// Gossip protocol
	MessageTypeGossip
	MessageTypeGossipAck

	// Custom/Extension messages
	MessageTypeCustom = 255
)

// MessageFlags represents message flags
type MessageFlags uint8

const (
	FlagCompressed MessageFlags = 1 << iota
	FlagEncrypted
	FlagPriority
	FlagReliable
)

// Message represents a P2P network message
type Message struct {
	// Header fields
	Magic     uint32       // Protocol magic bytes
	Version   uint8        // Protocol version
	Type      MessageType  // Message type
	Flags     MessageFlags // Message flags
	Reserved  uint8        // Reserved for future use
	Length    uint32       // Payload length
	ID        [16]byte     // Unique message ID
	Timestamp int64        // Unix timestamp in nanoseconds
	TTL       uint8        // Time-to-live in hops

	// Payload
	Payload []byte

	// Signature (not part of wire format, computed separately)
	Signature []byte
}

// MessageHandler defines the interface for handling messages
type MessageHandler interface {
	HandleMessage(peer *Peer, msg *Message) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(peer *Peer, msg *Message) error

// HandleMessage implements MessageHandler
func (f MessageHandlerFunc) HandleMessage(peer *Peer, msg *Message) error {
	return f(peer, msg)
}

// Protocol manages the wire protocol for P2P communication
type Protocol struct {
	version  uint8
	handlers map[MessageType]MessageHandler
	logger   *logrus.Logger

	// Crypto manager for signing/verification
	cryptoManager *crypto.CryptoManager
}

// NewProtocol creates a new protocol instance
func NewProtocol(logger *logrus.Logger, cryptoManager *crypto.CryptoManager) *Protocol {
	if logger == nil {
		logger = logrus.New()
	}

	return &Protocol{
		version:       ProtocolVersion,
		handlers:      make(map[MessageType]MessageHandler),
		logger:        logger,
		cryptoManager: cryptoManager,
	}
}

// RegisterHandler registers a message handler for a specific message type
func (p *Protocol) RegisterHandler(msgType MessageType, handler MessageHandler) {
	p.handlers[msgType] = handler
	p.logger.Debug("Registered message handler", "type", msgType)
}

// UnregisterHandler removes a message handler
func (p *Protocol) UnregisterHandler(msgType MessageType) {
	delete(p.handlers, msgType)
	p.logger.Debug("Unregistered message handler", "type", msgType)
}

// CreateMessage creates a new message with the specified type and payload
func (p *Protocol) CreateMessage(msgType MessageType, payload []byte) (*Message, error) {
	// Generate unique message ID
	var msgID [16]byte
	if _, err := rand.Read(msgID[:]); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to generate message ID")
	}

	msg := &Message{
		Magic:     ProtocolMagic,
		Version:   p.version,
		Type:      msgType,
		Flags:     0,
		Reserved:  0,
		Length:    uint32(len(payload)),
		ID:        msgID,
		Timestamp: common.ConsensusNow().UnixNano(),
		TTL:       3, // Default TTL
		Payload:   payload,
	}

	// Apply compression if payload is large enough
	if len(payload) > CompressionThreshold {
		compressed, err := compressPayload(payload)
		if err != nil {
			p.logger.Warn("Failed to compress payload", "error", err)
		} else if len(compressed) < len(payload) {
			msg.Payload = compressed
			msg.Length = uint32(len(compressed))
			msg.Flags |= FlagCompressed
			p.logger.Debug("Compressed message payload",
				"original", len(payload),
				"compressed", len(compressed))
		}
	}

	return msg, nil
}

// EncodeMessage encodes a message to wire format
func (p *Protocol) EncodeMessage(msg *Message) ([]byte, error) {
	if msg == nil {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message is nil")
	}

	// Validate message
	if err := p.validateMessage(msg); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message validation failed")
	}

	// Calculate total size
	totalSize := HeaderSize + len(msg.Payload)
	if totalSize > MaxMessageSize {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("message too large: %d bytes (max %d)", totalSize, MaxMessageSize))
	}

	// Create buffer
	buf := make([]byte, totalSize)

	// Encode header
	binary.BigEndian.PutUint32(buf[0:4], msg.Magic)
	buf[4] = msg.Version
	buf[5] = uint8(msg.Type)
	buf[6] = uint8(msg.Flags)
	buf[7] = msg.Reserved
	binary.BigEndian.PutUint32(buf[8:12], msg.Length)
	copy(buf[12:28], msg.ID[:])
	binary.BigEndian.PutUint64(buf[28:36], uint64(msg.Timestamp))
	buf[36] = msg.TTL

	// Reserved bytes (padding to 64-byte boundary for future use)
	// buf[37:64] are already zero-initialized

	// Copy payload
	if len(msg.Payload) > 0 {
		copy(buf[HeaderSize:], msg.Payload)
	}

	return buf, nil
}

// DecodeMessage decodes a message from wire format
func (p *Protocol) DecodeMessage(data []byte) (*Message, error) {
	if len(data) < HeaderSize {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("message too short: %d bytes (min %d)", len(data), HeaderSize))
	}

	// Decode header
	msg := &Message{
		Magic:     binary.BigEndian.Uint32(data[0:4]),
		Version:   data[4],
		Type:      MessageType(data[5]),
		Flags:     MessageFlags(data[6]),
		Reserved:  data[7],
		Length:    binary.BigEndian.Uint32(data[8:12]),
		Timestamp: int64(binary.BigEndian.Uint64(data[28:36])),
		TTL:       data[36],
	}

	// Copy message ID
	copy(msg.ID[:], data[12:28])

	// Validate header
	if msg.Magic != ProtocolMagic {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("invalid magic bytes: 0x%08X", msg.Magic))
	}

	if msg.Version != p.version {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("unsupported protocol version: %d", msg.Version))
	}

	// Check payload length
	expectedSize := HeaderSize + int(msg.Length)
	if len(data) != expectedSize {
		return nil, apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("message size mismatch: got %d, expected %d", len(data), expectedSize))
	}

	// Extract payload
	if msg.Length > 0 {
		msg.Payload = make([]byte, msg.Length)
		copy(msg.Payload, data[HeaderSize:])

		// Decompress if needed
		if msg.Flags&FlagCompressed != 0 {
			decompressed, err := decompressPayload(msg.Payload)
			if err != nil {
				return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
					"failed to decompress payload")
			}
			msg.Payload = decompressed
		}
	}

	// Validate complete message
	if err := p.validateMessage(msg); err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"decoded message validation failed")
	}

	return msg, nil
}

// SignMessage signs a message using the crypto manager
func (p *Protocol) SignMessage(msg *Message, privateKeyPair *crypto.DilithiumKeyPair) error {
	if msg == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message is nil")
	}

	if privateKeyPair == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"private key pair is nil")
	}

	if p.cryptoManager == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"crypto manager not available")
	}

	// Create signing data (header + payload)
	signingData, err := p.getSigningData(msg)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create signing data")
	}

	// Sign the data
	signature, err := p.cryptoManager.Sign(privateKeyPair, signingData)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to sign message")
	}

	msg.Signature = signature
	return nil
}

// VerifyMessage verifies a message signature
func (p *Protocol) VerifyMessage(msg *Message, publicKeyPair *crypto.DilithiumKeyPair) error {
	if msg == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message is nil")
	}

	if len(msg.Signature) == 0 {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message has no signature")
	}

	if publicKeyPair == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"public key pair is nil")
	}

	if p.cryptoManager == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInternal,
			"crypto manager not available")
	}

	// Create signing data
	signingData, err := p.getSigningData(msg)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to create signing data")
	}

	// Verify signature
	valid, err := p.cryptoManager.Verify(publicKeyPair, signingData, msg.Signature)
	if err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"signature verification failed")
	}

	if !valid {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"signature verification failed: invalid signature")
	}

	return nil
}

// HandleMessage processes an incoming message
func (p *Protocol) HandleMessage(peer *Peer, msg *Message) error {
	if msg == nil {
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			"message is nil")
	}

	// Check TTL
	if msg.TTL == 0 {
		p.logger.Debug("Message TTL expired", "id", fmt.Sprintf("%x", msg.ID))
		return nil // Silently drop expired messages
	}

	// Find handler
	handler, exists := p.handlers[msg.Type]
	if !exists {
		p.logger.Warn("No handler for message type", "type", msg.Type)
		return apperrors.New(apperrors.ModuleNetwork, apperrors.CodeInvalid,
			fmt.Sprintf("no handler for message type %d", msg.Type))
	}

	// Handle the message
	if err := handler.HandleMessage(peer, msg); err != nil {
		return apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"message handler failed")
	}

	return nil
}

// CreateHandshakeMessage creates a handshake message for peer connection
func (p *Protocol) CreateHandshakeMessage(nodeID string, userAgent string, blockHeight uint64, services PeerServices) (*Message, error) {
	handshake := HandshakePayload{
		NodeID:      nodeID,
		UserAgent:   userAgent,
		BlockHeight: blockHeight,
		Services:    uint64(services),
		Timestamp:   consensus.ConsensusUnix(),
		Nonce:       generateNonce(),
	}

	payload, err := encodeHandshakePayload(&handshake)
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to encode handshake payload")
	}

	return p.CreateMessage(MessageTypeHandshake, payload)
}

// CreatePingMessage creates a ping message
func (p *Protocol) CreatePingMessage() (*Message, error) {
	// Create ping payload with timestamp
	ping := PingPayload{
		Timestamp: consensus.ConsensusUnixNano(),
		Nonce:     generateNonce(),
	}

	payload, err := encodePingPayload(&ping)
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to encode ping payload")
	}

	return p.CreateMessage(MessageTypePing, payload)
}

// CreatePongMessage creates a pong message in response to a ping
func (p *Protocol) CreatePongMessage(pingID [16]byte, pingTimestamp int64) (*Message, error) {
	pong := PongPayload{
		PingID:        pingID,
		PingTimestamp: pingTimestamp,
		PongTimestamp: consensus.ConsensusUnixNano(),
	}

	payload, err := encodePongPayload(&pong)
	if err != nil {
		return nil, apperrors.Wrap(err, apperrors.ModuleNetwork, apperrors.CodeInternal,
			"failed to encode pong payload")
	}

	return p.CreateMessage(MessageTypePong, payload)
}

// Helper functions

// validateMessage validates a message structure
func (p *Protocol) validateMessage(msg *Message) error {
	if msg.Magic != ProtocolMagic {
		return fmt.Errorf("invalid magic bytes: 0x%08X", msg.Magic)
	}

	if msg.Version != p.version {
		return fmt.Errorf("unsupported protocol version: %d", msg.Version)
	}

	if msg.Length != uint32(len(msg.Payload)) {
		return fmt.Errorf("payload length mismatch: header=%d, actual=%d",
			msg.Length, len(msg.Payload))
	}

	if msg.Length > MaxMessageSize-HeaderSize {
		return fmt.Errorf("payload too large: %d bytes", msg.Length)
	}

	return nil
}

// getSigningData creates the data to be signed for a message
func (p *Protocol) getSigningData(msg *Message) ([]byte, error) {
	// Create a temporary message without signature for signing
	tempMsg := *msg
	tempMsg.Signature = nil

	// Encode the message
	encoded, err := p.EncodeMessage(&tempMsg)
	if err != nil {
		return nil, err
	}

	// Hash the encoded message
	hash := sha256.Sum256(encoded)
	return hash[:], nil
}

// generateNonce generates a random nonce
func generateNonce() uint64 {
	var nonce [8]byte
	rand.Read(nonce[:])
	return binary.BigEndian.Uint64(nonce[:])
}

// Payload structures

// HandshakePayload represents the handshake message payload
type HandshakePayload struct {
	Version     uint8  `json:"version"`
	NodeID      string `json:"nodeId"`
	Address     string `json:"address"`
	Port        uint16 `json:"port"`
	Services    uint64 `json:"services"`
	UserAgent   string `json:"userAgent"`
	BlockHeight uint64 `json:"blockHeight"`
	Timestamp   int64  `json:"timestamp"`
	Nonce       uint64 `json:"nonce"`
}

// PingPayload represents the ping message payload
type PingPayload struct {
	Timestamp int64  `json:"timestamp"`
	Nonce     uint64 `json:"nonce"`
}

// PongPayload represents the pong message payload
type PongPayload struct {
	PingID        [16]byte `json:"pingId"`
	PingTimestamp int64    `json:"pingTimestamp"`
	PongTimestamp int64    `json:"pongTimestamp"`
}

// PeerInfo represents peer information
type PeerInfo struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	Port        uint16 `json:"port"`
	Services    uint64 `json:"services"`
	UserAgent   string `json:"userAgent"`
	BlockHeight uint64 `json:"blockHeight"`
}

// Payload encoding/decoding functions (simplified JSON for now)
// In production, these would use more efficient binary encoding

func encodeHandshakePayload(h *HandshakePayload) ([]byte, error) {
	// Use a simple binary encoding for efficiency
	buf := bytes.NewBuffer(nil)

	// Write fields in order
	buf.WriteByte(h.Version)
	writeString(buf, h.NodeID)
	writeString(buf, h.Address)
	binary.Write(buf, binary.BigEndian, h.Port)
	binary.Write(buf, binary.BigEndian, h.Services)
	writeString(buf, h.UserAgent)
	binary.Write(buf, binary.BigEndian, h.BlockHeight)
	binary.Write(buf, binary.BigEndian, h.Timestamp)
	binary.Write(buf, binary.BigEndian, h.Nonce)

	return buf.Bytes(), nil
}

func encodePingPayload(p *PingPayload) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, p.Timestamp)
	binary.Write(buf, binary.BigEndian, p.Nonce)
	return buf.Bytes(), nil
}

func encodePongPayload(p *PongPayload) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.Write(p.PingID[:])
	binary.Write(buf, binary.BigEndian, p.PingTimestamp)
	binary.Write(buf, binary.BigEndian, p.PongTimestamp)
	return buf.Bytes(), nil
}

func writeString(buf *bytes.Buffer, s string) {
	data := []byte(s)
	binary.Write(buf, binary.BigEndian, uint16(len(data)))
	buf.Write(data)
}

func readString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}

	return string(data), nil
}

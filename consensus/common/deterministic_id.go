// consensus/common/deterministic_id.go

package common

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// DeterministicIDGenerator provides deterministic ID generation for consensus events
type DeterministicIDGenerator struct {
	// No state needed - all IDs are derived from input parameters
}

// NewDeterministicIDGenerator creates a new deterministic ID generator
func NewDeterministicIDGenerator() *DeterministicIDGenerator {
	return &DeterministicIDGenerator{}
}

// GenerateEventID generates a deterministic event ID based on event properties
func (dig *DeterministicIDGenerator) GenerateEventID(
	creator [32]byte,
	parentIDs [][32]byte,
	data []byte,
	height uint64,
	consensusTime Time,
) [32]byte {
	// Create a deterministic hash of all event properties
	h := sha256.New()

	// Add creator
	h.Write(creator[:])

	// Add parent IDs in sorted order for determinism
	sortedParents := sortParentIDs(parentIDs)
	for _, parentID := range sortedParents {
		h.Write(parentID[:])
	}

	// Add data hash (not the data itself to keep ID generation fast)
	dataHash := sha256.Sum256(data)
	h.Write(dataHash[:])

	// Add height
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, height)
	h.Write(heightBytes)

	// Add consensus time
	h.Write(TimeToBytes(consensusTime))

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GenerateBlockID generates a deterministic block ID
func (dig *DeterministicIDGenerator) GenerateBlockID(
	blockNumber uint64,
	producer [32]byte,
	eventIDs [][32]byte,
	pohHash [32]byte,
	consensusTime Time,
) [32]byte {
	h := sha256.New()

	// Add block number
	blockBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockBytes, blockNumber)
	h.Write(blockBytes)

	// Add producer
	h.Write(producer[:])

	// Add event IDs in sorted order
	sortedEvents := sortEventIDs(eventIDs)
	for _, eventID := range sortedEvents {
		h.Write(eventID[:])
	}

	// Add PoH hash
	h.Write(pohHash[:])

	// Add consensus time
	h.Write(TimeToBytes(consensusTime))

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GenerateValidatorID generates a deterministic validator ID from public key
func (dig *DeterministicIDGenerator) GenerateValidatorID(publicKey []byte) ([32]byte, error) {
	if len(publicKey) == 0 {
		return [32]byte{}, fmt.Errorf("empty public key")
	}

	// Hash the public key to generate ID
	return sha256.Sum256(publicKey), nil
}

// GenerateTransactionID generates a deterministic transaction ID
func (dig *DeterministicIDGenerator) GenerateTransactionID(
	sender [32]byte,
	nonce uint64,
	data []byte,
) [32]byte {
	h := sha256.New()

	// Add sender
	h.Write(sender[:])

	// Add nonce
	nonceBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, nonce)
	h.Write(nonceBytes)

	// Add data hash
	dataHash := sha256.Sum256(data)
	h.Write(dataHash[:])

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GenerateCheckpointID generates a deterministic checkpoint ID
func (dig *DeterministicIDGenerator) GenerateCheckpointID(
	blockNumber uint64,
	stateRoot [32]byte,
	validatorSetHash [32]byte,
) [32]byte {
	h := sha256.New()

	// Add block number
	blockBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockBytes, blockNumber)
	h.Write(blockBytes)

	// Add state root
	h.Write(stateRoot[:])

	// Add validator set hash
	h.Write(validatorSetHash[:])

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// sortParentIDs sorts parent IDs for deterministic ordering
func sortParentIDs(parentIDs [][32]byte) [][32]byte {
	// Create a copy to avoid modifying the original
	sorted := make([][32]byte, len(parentIDs))
	copy(sorted, parentIDs)

	// Sort using bubble sort for simplicity and determinism
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if compareBytes32(sorted[i], sorted[j]) > 0 {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// sortEventIDs sorts event IDs for deterministic ordering
func sortEventIDs(eventIDs [][32]byte) [][32]byte {
	// Create a copy to avoid modifying the original
	sorted := make([][32]byte, len(eventIDs))
	copy(sorted, eventIDs)

	// Sort using bubble sort for simplicity and determinism
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if compareBytes32(sorted[i], sorted[j]) > 0 {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// compareBytes32 compares two [32]byte arrays
func compareBytes32(a, b [32]byte) int {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// HashValidatorSet generates a deterministic hash of a validator set
func HashValidatorSet(validators []ValidatorInfo) [32]byte {
	h := sha256.New()

	// Sort validators by ID for determinism
	sorted := make([]ValidatorInfo, len(validators))
	copy(sorted, validators)

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if compareBytes32(sorted[i].ID, sorted[j].ID) > 0 {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// Hash each validator
	for _, val := range sorted {
		h.Write(val.ID[:])

		stakeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(stakeBytes, val.Stake)
		h.Write(stakeBytes)
	}

	var hash [32]byte
	copy(hash[:], h.Sum(nil))
	return hash
}

// ValidatorInfo represents basic validator information for hashing
type ValidatorInfo struct {
	ID    [32]byte
	Stake uint64
}

// GenerateProposalID generates a deterministic governance proposal ID
func (dig *DeterministicIDGenerator) GenerateProposalID(
	proposer [32]byte,
	proposalType string,
	data []byte,
	consensusTime Time,
) [32]byte {
	h := sha256.New()

	// Add proposer
	h.Write(proposer[:])

	// Add proposal type
	h.Write([]byte(proposalType))

	// Add data hash
	dataHash := sha256.Sum256(data)
	h.Write(dataHash[:])

	// Add consensus time
	h.Write(TimeToBytes(consensusTime))

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GenerateSlashEventID generates a deterministic slash event ID
func (dig *DeterministicIDGenerator) GenerateSlashEventID(
	validator [32]byte,
	slashType string,
	blockHeight uint64,
	evidence []byte,
) [32]byte {
	h := sha256.New()

	// Add validator
	h.Write(validator[:])

	// Add slash type
	h.Write([]byte(slashType))

	// Add block height
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, blockHeight)
	h.Write(heightBytes)

	// Add evidence hash
	evidenceHash := sha256.Sum256(evidence)
	h.Write(evidenceHash[:])

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

// GenerateNetworkMessageID generates a deterministic network message ID
func (dig *DeterministicIDGenerator) GenerateNetworkMessageID(
	sender [32]byte,
	messageType string,
	sequence uint64,
	payload []byte,
) [32]byte {
	h := sha256.New()

	// Add sender
	h.Write(sender[:])

	// Add message type
	h.Write([]byte(messageType))

	// Add sequence number
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, sequence)
	h.Write(seqBytes)

	// Add payload hash
	payloadHash := sha256.Sum256(payload)
	h.Write(payloadHash[:])

	// Generate final ID
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}

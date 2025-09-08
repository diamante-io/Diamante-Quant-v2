package shielded

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
)

// AssetID uniquely identifies an asset type
type AssetID [32]byte

// Hash represents a 256-bit hash
type Hash [32]byte

// Commitment represents a Pedersen commitment to a note
type Commitment Hash

// Nullifier uniquely identifies a spent note
type Nullifier Hash

// PublicKey represents a spending public key
type PublicKey [32]byte

// PrivateKey represents a spending private key
type PrivateKey [32]byte

// ViewingKey allows scanning for incoming transactions
type ViewingKey struct {
	Incoming [32]byte // For decrypting incoming notes
	Outgoing [32]byte // For decrypting outgoing notes (optional)
}

// ShieldedNote represents a private note in the shielded pool
type ShieldedNote struct {
	// Core fields
	Owner     PublicKey  `json:"owner"`
	Amount    *big.Int   `json:"amount"`
	AssetType AssetID    `json:"asset_type"`
	Blinding  fr.Element `json:"blinding"` // Random blinding factor

	// Derived fields
	Commitment Commitment `json:"commitment"`
	Nullifier  Nullifier  `json:"nullifier"`

	// Metadata
	Index       uint64 `json:"index"` // Position in Merkle tree
	BlockHeight uint64 `json:"block_height"`
}

// ShieldedTransaction represents a privacy-preserving transaction
type ShieldedTransaction struct {
	// Transaction ID
	ID string `json:"id"`

	// Public inputs
	Nullifiers  []Nullifier  `json:"nullifiers"`  // Spent notes
	Commitments []Commitment `json:"commitments"` // New notes
	MerkleRoot  Hash         `json:"merkle_root"` // Root at time of creation
	Fee         *big.Int     `json:"fee"`

	// Encrypted data
	EncryptedNotes []EncryptedNote `json:"encrypted_notes"`

	// Zero-knowledge proof
	Proof []byte `json:"proof"`

	// Metadata
	Timestamp   time.Time `json:"timestamp"`
	BlockHeight uint64    `json:"block_height"`
}

// EncryptedNote contains encrypted note data for recipients
type EncryptedNote struct {
	EphemeralPublicKey [32]byte `json:"ephemeral_key"`
	EncryptedData      []byte   `json:"encrypted_data"`
	AuthTag            [16]byte `json:"auth_tag"`
}

// TransactionType identifies the type of shielded operation
type TransactionType uint8

const (
	TxTypeMint     TransactionType = iota // Transparent to shielded
	TxTypeTransfer                        // Shielded to shielded
	TxTypeBurn                            // Shielded to transparent
	TxTypeSplit                           // Split one note into multiple
	TxTypeMerge                           // Merge multiple notes into one
)

// ShieldedOutput represents a new shielded note being created
type ShieldedOutput struct {
	Recipient PublicKey `json:"recipient"`
	Amount    *big.Int  `json:"amount"`
	AssetType AssetID   `json:"asset_type"`
	Memo      []byte    `json:"memo,omitempty"`
}

// ShieldedInput represents a shielded note being spent
type ShieldedInput struct {
	Note        *ShieldedNote `json:"note"`
	MerklePath  []Hash        `json:"merkle_path"`
	SpendingKey PrivateKey    `json:"-"` // Never serialized
}

// MintTransaction converts transparent funds to shielded
type MintTransaction struct {
	Source    string    `json:"source"` // Transparent address
	Amount    *big.Int  `json:"amount"`
	AssetType AssetID   `json:"asset_type"`
	Recipient PublicKey `json:"recipient"` // Shielded recipient
	Fee       *big.Int  `json:"fee"`
}

// BurnTransaction converts shielded funds to transparent
type BurnTransaction struct {
	Input       ShieldedInput `json:"-"`           // Private input
	Nullifier   Nullifier     `json:"nullifier"`   // Public nullifier
	Destination string        `json:"destination"` // Transparent address
	Amount      *big.Int      `json:"amount"`
	AssetType   AssetID       `json:"asset_type"`
	Proof       []byte        `json:"proof"`
}

// Generate creates a new random field element for blinding
func GenerateBlinding() (fr.Element, error) {
	var blinding fr.Element
	_, err := blinding.SetRandom()
	return blinding, err
}

// GenerateKeyPair generates a new spending key pair
func GenerateKeyPair() (PublicKey, PrivateKey, error) {
	var privKey PrivateKey
	var pubKey PublicKey

	// Generate random private key
	_, err := rand.Read(privKey[:])
	if err != nil {
		return pubKey, privKey, err
	}

	// Derive public key (simplified - in practice use proper curve operations)
	// This would use baby jubjub or similar curve
	copy(pubKey[:], privKey[:]) // Placeholder

	return pubKey, privKey, nil
}

// GenerateViewingKey derives a viewing key from a spending key
func GenerateViewingKey(spendingKey PrivateKey) ViewingKey {
	var vk ViewingKey
	// In practice, use proper key derivation
	copy(vk.Incoming[:], spendingKey[:])
	copy(vk.Outgoing[:], spendingKey[16:])
	return vk
}

// String returns hex representation of various types
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

func (c Commitment) String() string {
	return hex.EncodeToString(c[:])
}

func (n Nullifier) String() string {
	return hex.EncodeToString(n[:])
}

func (a AssetID) String() string {
	return hex.EncodeToString(a[:])
}

// IsZero checks if a hash is all zeros
func (h Hash) IsZero() bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}

// TransactionMetadata contains auxiliary transaction information
type TransactionMetadata struct {
	TxType       TransactionType `json:"tx_type"`
	NumInputs    int             `json:"num_inputs"`
	NumOutputs   int             `json:"num_outputs"`
	TotalFee     *big.Int        `json:"total_fee"`
	ProofGenTime time.Duration   `json:"proof_gen_time"`
}

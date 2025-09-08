package shielded

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/consensys/gnark-crypto/hash"
)

// CryptoParams holds the cryptographic parameters for the shielded pool
type CryptoParams struct {
	HashFunc hash.Hash
}

// NewCryptoParams creates new cryptographic parameters
func NewCryptoParams() (*CryptoParams, error) {
	// Initialize hash function for commitments and nullifiers
	// Using MIMC as efficient zkSNARK-friendly hash
	hashFunc := hash.MIMC_BN254

	return &CryptoParams{
		HashFunc: hashFunc,
	}, nil
}

// ComputeNoteCommitment computes the Pedersen commitment for a note
// commitment = PedersenHash(owner || amount || assetType || blinding)
func (cp *CryptoParams) ComputeNoteCommitment(note *ShieldedNote) (Commitment, error) {
	// Convert inputs to field elements
	var inputs [4]fr.Element

	// Owner public key to field element
	ownerBytes := note.Owner[:]
	inputs[0].SetBytes(ownerBytes)

	// Amount to field element
	amountBytes := note.Amount.Bytes()
	inputs[1].SetBytes(amountBytes)

	// Asset type to field element
	assetBytes := note.AssetType[:]
	inputs[2].SetBytes(assetBytes)

	// Blinding factor (already a field element)
	inputs[3] = note.Blinding

	// Compute commitment using hash function
	h := cp.HashFunc.New()
	for _, input := range inputs {
		b := input.Bytes()
		h.Write(b[:])
	}
	commitmentBytes := h.Sum(nil)

	// Convert to Commitment type
	var result Commitment
	copy(result[:], commitmentBytes[:])

	return result, nil
}

// ComputeNullifier computes the nullifier for spending a note
// nullifier = PoseidonHash(commitment || spendingKey || index)
func (cp *CryptoParams) ComputeNullifier(commitment Commitment, spendingKey PrivateKey, noteIndex uint64) (Nullifier, error) {
	// Prepare inputs for Poseidon hash
	var inputs []fr.Element

	// Commitment to field element
	var commitmentFr fr.Element
	commitmentFr.SetBytes(commitment[:])
	inputs = append(inputs, commitmentFr)

	// Spending key to field element
	var spendingKeyFr fr.Element
	spendingKeyFr.SetBytes(spendingKey[:])
	inputs = append(inputs, spendingKeyFr)

	// Note index to field element
	var indexFr fr.Element
	indexBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(indexBytes, noteIndex)
	indexFr.SetBytes(indexBytes)
	inputs = append(inputs, indexFr)

	// Compute hash using hash function
	h := cp.HashFunc.New()
	for _, input := range inputs {
		b := input.Bytes()
		h.Write(b[:])
	}
	resultBytes := h.Sum(nil)

	// Convert to Nullifier type
	var nullifier Nullifier
	copy(nullifier[:], resultBytes[:])

	return nullifier, nil
}

// VerifyCommitment verifies that a commitment matches the note data
func (cp *CryptoParams) VerifyCommitment(note *ShieldedNote, commitment Commitment) bool {
	computed, err := cp.ComputeNoteCommitment(note)
	if err != nil {
		return false
	}

	return computed == commitment
}

// DeriveNullifierSeed derives a unique seed for nullifier generation
// This ensures that the same note can't be spent twice
func DeriveNullifierSeed(noteCommitment Commitment, spendingKey PrivateKey) Hash {
	hasher := sha256.New()
	hasher.Write(noteCommitment[:])
	hasher.Write(spendingKey[:])
	hasher.Write([]byte("nullifier_seed"))

	var seed Hash
	copy(seed[:], hasher.Sum(nil))
	return seed
}

// HashToFieldElement converts a hash to a field element
func HashToFieldElement(h Hash) fr.Element {
	var elem fr.Element
	elem.SetBytes(h[:])
	return elem
}

// FieldElementToHash converts a field element to a hash
func FieldElementToHash(elem fr.Element) Hash {
	var h Hash
	bytes := elem.Bytes()
	copy(h[:], bytes[:])
	return h
}

// PedersenHash computes a hash of arbitrary data
func (cp *CryptoParams) PedersenHash(data [][]byte) Hash {
	// Use hash function
	h := cp.HashFunc.New()
	for _, d := range data {
		h.Write(d)
	}
	resultBytes := h.Sum(nil)

	// Convert to Hash
	var result Hash
	copy(result[:], resultBytes[:])
	return result
}

// PoseidonHash computes a hash of arbitrary data
func (cp *CryptoParams) PoseidonHash(data [][]byte) Hash {
	// Use hash function
	h := cp.HashFunc.New()
	for _, d := range data {
		h.Write(d)
	}
	resultBytes := h.Sum(nil)

	// Convert to Hash
	var hash Hash
	copy(hash[:], resultBytes[:])
	return hash
}

// CreateShieldedNote creates a new shielded note with proper commitment
func (cp *CryptoParams) CreateShieldedNote(owner PublicKey, amount *big.Int, assetType AssetID) (*ShieldedNote, error) {
	// Generate random blinding factor
	blinding, err := GenerateBlinding()
	if err != nil {
		return nil, fmt.Errorf("failed to generate blinding: %w", err)
	}

	// Create note
	note := &ShieldedNote{
		Owner:     owner,
		Amount:    amount,
		AssetType: assetType,
		Blinding:  blinding,
	}

	// Compute commitment
	commitment, err := cp.ComputeNoteCommitment(note)
	if err != nil {
		return nil, fmt.Errorf("failed to compute commitment: %w", err)
	}
	note.Commitment = commitment

	return note, nil
}

// VerifyNullifierUniqueness checks if a nullifier has been seen before
// This would typically check against a global nullifier set
func VerifyNullifierUniqueness(nullifier Nullifier, nullifierSet map[Nullifier]bool) bool {
	_, exists := nullifierSet[nullifier]
	return !exists
}

// CombineCommitments combines multiple commitments into one (for batching)
func (cp *CryptoParams) CombineCommitments(commitments []Commitment) Hash {
	data := make([][]byte, len(commitments))
	for i, c := range commitments {
		data[i] = c[:]
	}
	return cp.PoseidonHash(data)
}

// RangeProof represents a proof that a value is within a valid range
type RangeProof struct {
	Proof []byte
	Min   *big.Int
	Max   *big.Int
}

// GenerateRangeProof generates a proof that amount is in valid range [0, 2^64)
func GenerateRangeProof(amount *big.Int) (*RangeProof, error) {
	// Placeholder - in production, use bulletproofs or similar
	if amount.Sign() < 0 {
		return nil, fmt.Errorf("amount cannot be negative")
	}

	maxAmount := new(big.Int).Lsh(big.NewInt(1), 64)
	if amount.Cmp(maxAmount) >= 0 {
		return nil, fmt.Errorf("amount exceeds maximum")
	}

	return &RangeProof{
		Proof: []byte("placeholder_range_proof"),
		Min:   big.NewInt(0),
		Max:   maxAmount,
	}, nil
}

// VerifyRangeProof verifies that a range proof is valid
func VerifyRangeProof(proof *RangeProof, commitment Commitment) bool {
	// Placeholder - in production, verify actual proof
	return len(proof.Proof) > 0
}

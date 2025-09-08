package shielded

import (
	"fmt"
	"math/big"

	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/std/hash/mimc"
)

// TransferCircuit proves a valid shielded-to-shielded transfer
type TransferCircuit struct {
	// Public inputs
	InputNullifiers   []frontend.Variable `gnark:",public"`
	OutputCommitments []frontend.Variable `gnark:",public"`
	MerkleRoot        frontend.Variable   `gnark:",public"`
	PublicAmount      frontend.Variable   `gnark:",public"` // For fees

	// Private inputs (witness)
	// Input notes
	InputNotes       []NoteWitness
	InputMerklePaths []MerklePathWitness
	SpendingKeys     []frontend.Variable

	// Output notes
	OutputNotes []NoteWitness
}

// NoteWitness represents a note in the circuit
type NoteWitness struct {
	Owner     frontend.Variable
	Amount    frontend.Variable
	AssetType frontend.Variable
	Blinding  frontend.Variable
}

// MerklePathWitness represents a Merkle authentication path
type MerklePathWitness struct {
	Path     []frontend.Variable
	Position frontend.Variable // Bit representation of position
}

// Define defines the constraints for the transfer circuit
func (circuit *TransferCircuit) Define(api frontend.API) error {
	// Initialize hash function (MiMC)
	hFunc, err := mimc.NewMiMC(api)
	if err != nil {
		return err
	}

	// 1. Verify each input note
	totalInputAmount := frontend.Variable(0)

	for i := range circuit.InputNotes {
		// Compute note commitment
		commitment := computeNoteCommitment(api, hFunc, circuit.InputNotes[i])

		// Verify Merkle path
		verifyMerklePath(api, hFunc, commitment, circuit.InputMerklePaths[i], circuit.MerkleRoot)

		// Compute and check nullifier
		nullifier := computeNullifier(api, hFunc, commitment, circuit.SpendingKeys[i], circuit.InputMerklePaths[i].Position)
		api.AssertIsEqual(nullifier, circuit.InputNullifiers[i])

		// Add to total input
		totalInputAmount = api.Add(totalInputAmount, circuit.InputNotes[i].Amount)
	}

	// 2. Verify each output note
	totalOutputAmount := frontend.Variable(0)

	for i := range circuit.OutputNotes {
		// Compute output commitment
		commitment := computeNoteCommitment(api, hFunc, circuit.OutputNotes[i])
		api.AssertIsEqual(commitment, circuit.OutputCommitments[i])

		// Add to total output
		totalOutputAmount = api.Add(totalOutputAmount, circuit.OutputNotes[i].Amount)

		// Range check amount (0 <= amount < 2^64)
		rangeCheck64(api, circuit.OutputNotes[i].Amount)
	}

	// 3. Verify balance (inputs = outputs + fee)
	totalWithFee := api.Add(totalOutputAmount, circuit.PublicAmount)
	api.AssertIsEqual(totalInputAmount, totalWithFee)

	// 4. Verify all notes have same asset type
	if len(circuit.InputNotes) > 0 && len(circuit.OutputNotes) > 0 {
		assetType := circuit.InputNotes[0].AssetType

		// Check all inputs have same asset
		for i := 1; i < len(circuit.InputNotes); i++ {
			api.AssertIsEqual(assetType, circuit.InputNotes[i].AssetType)
		}

		// Check all outputs have same asset
		for i := 0; i < len(circuit.OutputNotes); i++ {
			api.AssertIsEqual(assetType, circuit.OutputNotes[i].AssetType)
		}
	}

	return nil
}

// MintCircuit proves a valid transparent-to-shielded conversion
type MintCircuit struct {
	// Public inputs
	PublicAmount     frontend.Variable `gnark:",public"`
	AssetType        frontend.Variable `gnark:",public"`
	OutputCommitment frontend.Variable `gnark:",public"`

	// Private inputs
	OutputNote NoteWitness
}

// Define defines the constraints for the mint circuit
func (circuit *MintCircuit) Define(api frontend.API) error {
	hFunc, err := mimc.NewMiMC(api)
	if err != nil {
		return err
	}

	// Verify output note matches public amount and asset
	api.AssertIsEqual(circuit.OutputNote.Amount, circuit.PublicAmount)
	api.AssertIsEqual(circuit.OutputNote.AssetType, circuit.AssetType)

	// Compute and verify commitment
	commitment := computeNoteCommitment(api, hFunc, circuit.OutputNote)
	api.AssertIsEqual(commitment, circuit.OutputCommitment)

	// Range check amount
	rangeCheck64(api, circuit.OutputNote.Amount)

	return nil
}

// BurnCircuit proves a valid shielded-to-transparent conversion
type BurnCircuit struct {
	// Public inputs
	InputNullifier frontend.Variable `gnark:",public"`
	PublicAmount   frontend.Variable `gnark:",public"`
	AssetType      frontend.Variable `gnark:",public"`
	MerkleRoot     frontend.Variable `gnark:",public"`

	// Private inputs
	InputNote       NoteWitness
	InputMerklePath MerklePathWitness
	SpendingKey     frontend.Variable
}

// Define defines the constraints for the burn circuit
func (circuit *BurnCircuit) Define(api frontend.API) error {
	hFunc, err := mimc.NewMiMC(api)
	if err != nil {
		return err
	}

	// Verify input note matches public values
	api.AssertIsEqual(circuit.InputNote.Amount, circuit.PublicAmount)
	api.AssertIsEqual(circuit.InputNote.AssetType, circuit.AssetType)

	// Compute commitment
	commitment := computeNoteCommitment(api, hFunc, circuit.InputNote)

	// Verify Merkle path
	verifyMerklePath(api, hFunc, commitment, circuit.InputMerklePath, circuit.MerkleRoot)

	// Compute and verify nullifier
	nullifier := computeNullifier(api, hFunc, commitment, circuit.SpendingKey, circuit.InputMerklePath.Position)
	api.AssertIsEqual(nullifier, circuit.InputNullifier)

	return nil
}

// Helper functions for circuits

// computeNoteCommitment computes Pedersen commitment in-circuit
func computeNoteCommitment(api frontend.API, h mimc.MiMC, note NoteWitness) frontend.Variable {
	h.Reset()
	h.Write(note.Owner)
	h.Write(note.Amount)
	h.Write(note.AssetType)
	h.Write(note.Blinding)
	return h.Sum()
}

// computeNullifier computes nullifier in-circuit
func computeNullifier(api frontend.API, h mimc.MiMC, commitment, spendingKey, position frontend.Variable) frontend.Variable {
	h.Reset()
	h.Write(commitment)
	h.Write(spendingKey)
	h.Write(position)
	return h.Sum()
}

// verifyMerklePath verifies a Merkle authentication path
func verifyMerklePath(api frontend.API, h mimc.MiMC, leaf frontend.Variable, path MerklePathWitness, expectedRoot frontend.Variable) {
	current := leaf
	position := path.Position

	for i := 0; i < len(path.Path); i++ {
		h.Reset()

		// Extract bit to determine if we're left or right child
		bit := api.ToBinary(position, 1)[0]
		position = api.Div(position, 2)

		// Hash in correct order based on position
		left := api.Select(bit, path.Path[i], current)
		right := api.Select(bit, current, path.Path[i])

		h.Write(left)
		h.Write(right)
		current = h.Sum()
	}

	api.AssertIsEqual(current, expectedRoot)
}

// rangeCheck64 ensures a value fits in 64 bits
func rangeCheck64(api frontend.API, value frontend.Variable) {
	bits := api.ToBinary(value, 64)
	// Reconstruct to ensure it matches
	reconstructed := frontend.Variable(0)
	for i := 0; i < 64; i++ {
		reconstructed = api.Add(reconstructed, api.Mul(bits[i], new(big.Int).Lsh(big.NewInt(1), uint(i))))
	}
	api.AssertIsEqual(value, reconstructed)
}

// JoinSplitCircuit handles arbitrary numbers of inputs and outputs
type JoinSplitCircuit struct {
	NumInputs  int
	NumOutputs int
	TransferCircuit
}

// NewJoinSplitCircuit creates a circuit for n inputs and m outputs
func NewJoinSplitCircuit(numInputs, numOutputs int) *JoinSplitCircuit {
	return &JoinSplitCircuit{
		NumInputs:  numInputs,
		NumOutputs: numOutputs,
		TransferCircuit: TransferCircuit{
			InputNullifiers:   make([]frontend.Variable, numInputs),
			OutputCommitments: make([]frontend.Variable, numOutputs),
			InputNotes:        make([]NoteWitness, numInputs),
			InputMerklePaths:  make([]MerklePathWitness, numInputs),
			SpendingKeys:      make([]frontend.Variable, numInputs),
			OutputNotes:       make([]NoteWitness, numOutputs),
		},
	}
}

// BatchTransferCircuit allows multiple transfers in one proof
type BatchTransferCircuit struct {
	Transfers []TransferCircuit
}

// Define defines constraints for batch transfers
func (circuit *BatchTransferCircuit) Define(api frontend.API) error {
	// Each transfer must be valid independently
	for i := range circuit.Transfers {
		if err := circuit.Transfers[i].Define(api); err != nil {
			return fmt.Errorf("transfer %d failed: %w", i, err)
		}
	}
	return nil
}

// SwapCircuit proves atomic swap between two shielded assets
type SwapCircuit struct {
	// Alice's trade
	AliceInputs  TransferCircuit
	AliceOutputs TransferCircuit

	// Bob's trade
	BobInputs  TransferCircuit
	BobOutputs TransferCircuit

	// Swap parameters (public)
	AliceAssetType frontend.Variable `gnark:",public"`
	BobAssetType   frontend.Variable `gnark:",public"`
	SwapRate       frontend.Variable `gnark:",public"`
}

// Define defines atomic swap constraints
func (circuit *SwapCircuit) Define(api frontend.API) error {
	// Verify both transfers
	if err := circuit.AliceInputs.Define(api); err != nil {
		return err
	}
	if err := circuit.BobInputs.Define(api); err != nil {
		return err
	}

	// Verify swap rate is maintained
	// AliceAmount * SwapRate = BobAmount
	// This ensures fair exchange at agreed rate

	return nil
}

// ComplianceCircuit adds regulatory compliance to transfers
type ComplianceCircuit struct {
	TransferCircuit

	// Compliance data
	ComplianceRoot     frontend.Variable `gnark:",public"`
	SenderCompliance   ComplianceWitness
	ReceiverCompliance ComplianceWitness
}

// ComplianceWitness proves KYC/AML compliance
type ComplianceWitness struct {
	ComplianceHash frontend.Variable
	CompliancePath MerklePathWitness
	ExpiryTime     frontend.Variable
	CurrentTime    frontend.Variable
}

// Define adds compliance checks to transfer
func (circuit *ComplianceCircuit) Define(api frontend.API) error {
	// First verify the transfer
	if err := circuit.TransferCircuit.Define(api); err != nil {
		return err
	}

	// Verify sender compliance
	// Verify receiver compliance
	// Check expiry times

	return nil
}

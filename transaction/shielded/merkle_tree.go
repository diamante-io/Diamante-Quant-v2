package shielded

import (
	"errors"
	"fmt"
	"sync"
)

// MerkleTree represents a commitment tree for shielded notes
type MerkleTree struct {
	depth        uint8
	root         Hash
	leaves       []Commitment
	nodes        map[string]Hash // level:index -> hash
	cryptoParams *CryptoParams
	mu           sync.RWMutex
}

// NewMerkleTree creates a new Merkle tree with specified depth
func NewMerkleTree(depth uint8, cryptoParams *CryptoParams) *MerkleTree {
	if depth > 32 {
		depth = 32 // Maximum reasonable depth
	}

	return &MerkleTree{
		depth:        depth,
		leaves:       make([]Commitment, 0),
		nodes:        make(map[string]Hash),
		cryptoParams: cryptoParams,
	}
}

// AddCommitment adds a new commitment to the tree
func (mt *MerkleTree) AddCommitment(commitment Commitment) (uint64, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Check if tree is full
	maxLeaves := uint64(1) << mt.depth
	if uint64(len(mt.leaves)) >= maxLeaves {
		return 0, errors.New("merkle tree is full")
	}

	// Add leaf
	index := uint64(len(mt.leaves))
	mt.leaves = append(mt.leaves, commitment)

	// Update tree
	mt.updateTree(index)

	return index, nil
}

// GetRoot returns the current Merkle root
func (mt *MerkleTree) GetRoot() Hash {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if len(mt.leaves) == 0 {
		return mt.emptyRoot()
	}

	return mt.root
}

// GetMerklePath returns the authentication path for a leaf
func (mt *MerkleTree) GetMerklePath(leafIndex uint64) ([]Hash, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if leafIndex >= uint64(len(mt.leaves)) {
		return nil, fmt.Errorf("leaf index %d out of bounds", leafIndex)
	}

	path := make([]Hash, mt.depth)

	for level := uint8(0); level < mt.depth; level++ {
		siblingIndex := leafIndex ^ 1 // Toggle the last bit to get sibling
		siblingKey := fmt.Sprintf("%d:%d", level, siblingIndex)

		if siblingHash, exists := mt.nodes[siblingKey]; exists {
			path[level] = siblingHash
		} else {
			// Use empty hash for non-existent siblings
			path[level] = mt.emptyHash(level)
		}

		leafIndex >>= 1 // Move to parent index
	}

	return path, nil
}

// VerifyMerklePath verifies that a commitment belongs to the tree
func (mt *MerkleTree) VerifyMerklePath(commitment Commitment, index uint64, path []Hash, root Hash) bool {
	if len(path) != int(mt.depth) {
		return false
	}

	currentHash := Hash(commitment)
	currentIndex := index

	for level := uint8(0); level < mt.depth; level++ {
		siblingHash := path[level]

		var parentHash Hash
		if currentIndex&1 == 0 {
			// Current node is left child
			parentHash = mt.hashPair(currentHash, siblingHash)
		} else {
			// Current node is right child
			parentHash = mt.hashPair(siblingHash, currentHash)
		}

		currentHash = parentHash
		currentIndex >>= 1
	}

	return currentHash == root
}

// updateTree updates the tree after adding a new leaf
func (mt *MerkleTree) updateTree(newLeafIndex uint64) {
	// Start with the leaf
	currentIndex := newLeafIndex
	currentHash := Hash(mt.leaves[newLeafIndex])

	// Store leaf node
	mt.nodes[fmt.Sprintf("0:%d", currentIndex)] = currentHash

	// Update each level up to the root
	for level := uint8(0); level < mt.depth; level++ {
		// Get sibling
		siblingIndex := currentIndex ^ 1
		siblingKey := fmt.Sprintf("%d:%d", level, siblingIndex)

		var siblingHash Hash
		if storedHash, exists := mt.nodes[siblingKey]; exists {
			siblingHash = storedHash
		} else {
			siblingHash = mt.emptyHash(level)
		}

		// Compute parent hash
		var parentHash Hash
		if currentIndex&1 == 0 {
			parentHash = mt.hashPair(currentHash, siblingHash)
		} else {
			parentHash = mt.hashPair(siblingHash, currentHash)
		}

		// Move to parent
		currentIndex >>= 1
		currentHash = parentHash

		// Store parent node
		if level < mt.depth-1 {
			mt.nodes[fmt.Sprintf("%d:%d", level+1, currentIndex)] = parentHash
		}
	}

	mt.root = currentHash
}

// hashPair hashes two child nodes to create parent
func (mt *MerkleTree) hashPair(left, right Hash) Hash {
	return mt.cryptoParams.PoseidonHash([][]byte{left[:], right[:]})
}

// emptyHash returns the empty hash for a given level
func (mt *MerkleTree) emptyHash(level uint8) Hash {
	// In a sparse Merkle tree, we use deterministic empty values
	data := []byte("empty")
	data = append(data, byte(level))
	return mt.cryptoParams.PoseidonHash([][]byte{data})
}

// emptyRoot returns the root of an empty tree
func (mt *MerkleTree) emptyRoot() Hash {
	currentHash := mt.emptyHash(0)

	for level := uint8(0); level < mt.depth; level++ {
		currentHash = mt.hashPair(currentHash, currentHash)
	}

	return currentHash
}

// GetLeafCount returns the number of leaves in the tree
func (mt *MerkleTree) GetLeafCount() uint64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return uint64(len(mt.leaves))
}

// GetCommitment returns the commitment at a given index
func (mt *MerkleTree) GetCommitment(index uint64) (Commitment, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if index >= uint64(len(mt.leaves)) {
		return Commitment{}, fmt.Errorf("index %d out of bounds", index)
	}

	return mt.leaves[index], nil
}

// Snapshot creates a snapshot of the current tree state
type MerkleSnapshot struct {
	Root      Hash   `json:"root"`
	LeafCount uint64 `json:"leaf_count"`
	Depth     uint8  `json:"depth"`
	Timestamp uint64 `json:"timestamp"`
}

// CreateSnapshot creates a snapshot of the current tree state
func (mt *MerkleTree) CreateSnapshot(timestamp uint64) MerkleSnapshot {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return MerkleSnapshot{
		Root:      mt.root,
		LeafCount: uint64(len(mt.leaves)),
		Depth:     mt.depth,
		Timestamp: timestamp,
	}
}

// BatchMerkleTree allows efficient batch insertions
type BatchMerkleTree struct {
	*MerkleTree
	pendingCommitments []Commitment
}

// NewBatchMerkleTree creates a new batch-enabled Merkle tree
func NewBatchMerkleTree(depth uint8, cryptoParams *CryptoParams) *BatchMerkleTree {
	return &BatchMerkleTree{
		MerkleTree:         NewMerkleTree(depth, cryptoParams),
		pendingCommitments: make([]Commitment, 0),
	}
}

// AddCommitmentBatch adds a commitment to the pending batch
func (bmt *BatchMerkleTree) AddCommitmentBatch(commitment Commitment) {
	bmt.pendingCommitments = append(bmt.pendingCommitments, commitment)
}

// CommitBatch commits all pending commitments to the tree
func (bmt *BatchMerkleTree) CommitBatch() ([]uint64, error) {
	indices := make([]uint64, len(bmt.pendingCommitments))

	for i, commitment := range bmt.pendingCommitments {
		index, err := bmt.AddCommitment(commitment)
		if err != nil {
			return nil, err
		}
		indices[i] = index
	}

	// Clear pending
	bmt.pendingCommitments = bmt.pendingCommitments[:0]

	return indices, nil
}

// MerkleProof represents a Merkle inclusion proof
type MerkleProof struct {
	Leaf      Commitment `json:"leaf"`
	LeafIndex uint64     `json:"leaf_index"`
	Path      []Hash     `json:"path"`
	Root      Hash       `json:"root"`
}

// GenerateProof generates a Merkle proof for a commitment
func (mt *MerkleTree) GenerateProof(leafIndex uint64) (*MerkleProof, error) {
	commitment, err := mt.GetCommitment(leafIndex)
	if err != nil {
		return nil, err
	}

	path, err := mt.GetMerklePath(leafIndex)
	if err != nil {
		return nil, err
	}

	return &MerkleProof{
		Leaf:      commitment,
		LeafIndex: leafIndex,
		Path:      path,
		Root:      mt.GetRoot(),
	}, nil
}

// Verify verifies a Merkle proof
func (mp *MerkleProof) Verify(depth uint8, cryptoParams *CryptoParams) bool {
	tree := NewMerkleTree(depth, cryptoParams)
	return tree.VerifyMerklePath(mp.Leaf, mp.LeafIndex, mp.Path, mp.Root)
}

// CompressedMerkleTree uses compressed storage for large trees
type CompressedMerkleTree struct {
	*MerkleTree
	compressedNodes map[uint64][]byte // Compressed node storage
}

// SerializeForCircuit serializes the Merkle path for use in zk-SNARK circuits
func SerializeMerklePathForCircuit(path []Hash) []byte {
	serialized := make([]byte, len(path)*32)
	for i, h := range path {
		copy(serialized[i*32:(i+1)*32], h[:])
	}
	return serialized
}

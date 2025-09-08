package shielded

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// NoteEncryption handles encryption and decryption of shielded notes
type NoteEncryption struct {
	algorithm string // "aes-gcm" or "chacha20-poly1305"
}

// NewNoteEncryption creates a new note encryption handler
func NewNoteEncryption(algorithm string) *NoteEncryption {
	if algorithm == "" {
		algorithm = "chacha20-poly1305" // Default to ChaCha20-Poly1305
	}
	return &NoteEncryption{algorithm: algorithm}
}

// EncryptNote encrypts a note for a recipient using their viewing key
func (ne *NoteEncryption) EncryptNote(note *ShieldedNote, recipientKey PublicKey, memo []byte) (*EncryptedNote, error) {
	// Generate ephemeral key pair for ECDH
	ephemeralPub, ephemeralPriv, err := GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ephemeral key: %w", err)
	}

	// Derive shared secret using ECDH
	sharedSecret := deriveSharedSecret(ephemeralPriv, recipientKey)

	// Derive encryption key and nonce
	encKey, nonce := deriveEncryptionKeys(sharedSecret, ephemeralPub)

	// Serialize note data
	noteData, err := serializeNote(note, memo)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize note: %w", err)
	}

	// Encrypt the note
	var ciphertext []byte
	var authTag [16]byte

	switch ne.algorithm {
	case "aes-gcm":
		ciphertext, authTag, err = ne.encryptAESGCM(noteData, encKey, nonce)
	case "chacha20-poly1305":
		ciphertext, authTag, err = ne.encryptChaCha20(noteData, encKey, nonce)
	default:
		return nil, fmt.Errorf("unsupported algorithm: %s", ne.algorithm)
	}

	if err != nil {
		return nil, err
	}

	return &EncryptedNote{
		EphemeralPublicKey: ephemeralPub,
		EncryptedData:      ciphertext,
		AuthTag:            authTag,
	}, nil
}

// DecryptNote decrypts a note using a viewing key
func (ne *NoteEncryption) DecryptNote(enc *EncryptedNote, viewingKey ViewingKey) (*ShieldedNote, []byte, error) {
	// Derive shared secret
	sharedSecret := deriveSharedSecretFromViewing(viewingKey, enc.EphemeralPublicKey)

	// Derive decryption key and nonce
	decKey, nonce := deriveEncryptionKeys(sharedSecret, enc.EphemeralPublicKey)

	// Decrypt the note
	var plaintext []byte
	var err error

	switch ne.algorithm {
	case "aes-gcm":
		plaintext, err = ne.decryptAESGCM(enc.EncryptedData, enc.AuthTag, decKey, nonce)
	case "chacha20-poly1305":
		plaintext, err = ne.decryptChaCha20(enc.EncryptedData, enc.AuthTag, decKey, nonce)
	default:
		return nil, nil, fmt.Errorf("unsupported algorithm: %s", ne.algorithm)
	}

	if err != nil {
		return nil, nil, err
	}

	// Deserialize note and memo
	note, memo, err := deserializeNote(plaintext)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize note: %w", err)
	}

	return note, memo, nil
}

// encryptAESGCM encrypts data using AES-GCM
func (ne *NoteEncryption) encryptAESGCM(plaintext, key, nonce []byte) ([]byte, [16]byte, error) {
	var authTag [16]byte

	block, err := aes.NewCipher(key[:32]) // Use 256-bit key
	if err != nil {
		return nil, authTag, err
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, authTag, err
	}

	// GCM appends auth tag to ciphertext
	combined := aead.Seal(nil, nonce[:12], plaintext, nil)

	// Split ciphertext and auth tag
	ciphertext := combined[:len(combined)-16]
	copy(authTag[:], combined[len(combined)-16:])

	return ciphertext, authTag, nil
}

// decryptAESGCM decrypts data using AES-GCM
func (ne *NoteEncryption) decryptAESGCM(ciphertext []byte, authTag [16]byte, key, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return nil, err
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Combine ciphertext and auth tag
	combined := append(ciphertext, authTag[:]...)

	return aead.Open(nil, nonce[:12], combined, nil)
}

// encryptChaCha20 encrypts data using ChaCha20-Poly1305
func (ne *NoteEncryption) encryptChaCha20(plaintext, key, nonce []byte) ([]byte, [16]byte, error) {
	var authTag [16]byte

	aead, err := chacha20poly1305.New(key[:32])
	if err != nil {
		return nil, authTag, err
	}

	// ChaCha20 uses 12-byte nonce
	combined := aead.Seal(nil, nonce[:12], plaintext, nil)

	// Split ciphertext and auth tag
	ciphertext := combined[:len(combined)-16]
	copy(authTag[:], combined[len(combined)-16:])

	return ciphertext, authTag, nil
}

// decryptChaCha20 decrypts data using ChaCha20-Poly1305
func (ne *NoteEncryption) decryptChaCha20(ciphertext []byte, authTag [16]byte, key, nonce []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:32])
	if err != nil {
		return nil, err
	}

	// Combine ciphertext and auth tag
	combined := append(ciphertext, authTag[:]...)

	return aead.Open(nil, nonce[:12], combined, nil)
}

// deriveSharedSecret derives a shared secret using ECDH
func deriveSharedSecret(privKey PrivateKey, pubKey PublicKey) []byte {
	// Simplified - in production use proper elliptic curve
	h := sha256.New()
	h.Write(privKey[:])
	h.Write(pubKey[:])
	return h.Sum(nil)
}

// deriveSharedSecretFromViewing derives shared secret using viewing key
func deriveSharedSecretFromViewing(vk ViewingKey, ephemeralPub PublicKey) []byte {
	// Use incoming viewing key for decryption
	h := sha256.New()
	h.Write(vk.Incoming[:])
	h.Write(ephemeralPub[:])
	return h.Sum(nil)
}

// deriveEncryptionKeys derives encryption key and nonce from shared secret
func deriveEncryptionKeys(sharedSecret []byte, ephemeralPub PublicKey) ([]byte, []byte) {
	// Use HKDF to derive keys
	hkdf := hkdf.New(sha256.New, sharedSecret, ephemeralPub[:], []byte("note_encryption"))

	key := make([]byte, 32)   // 256-bit key
	nonce := make([]byte, 12) // 96-bit nonce

	io.ReadFull(hkdf, key)
	io.ReadFull(hkdf, nonce)

	return key, nonce
}

// serializeNote serializes a note and memo for encryption
func serializeNote(note *ShieldedNote, memo []byte) ([]byte, error) {
	// Calculate size
	size := 32 + 32 + 32 + 32 + 8 + 8 + 4 + len(memo) // Fixed fields + memo length + memo

	data := make([]byte, size)
	offset := 0

	// Owner
	copy(data[offset:offset+32], note.Owner[:])
	offset += 32

	// Amount (as 32 bytes)
	amountBytes := note.Amount.Bytes()
	copy(data[offset+32-len(amountBytes):offset+32], amountBytes)
	offset += 32

	// Asset type
	copy(data[offset:offset+32], note.AssetType[:])
	offset += 32

	// Blinding factor
	blindingBytes := note.Blinding.Bytes()
	copy(data[offset:offset+32], blindingBytes[:])
	offset += 32

	// Index
	binary.BigEndian.PutUint64(data[offset:offset+8], note.Index)
	offset += 8

	// Block height
	binary.BigEndian.PutUint64(data[offset:offset+8], note.BlockHeight)
	offset += 8

	// Memo length and data
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(memo)))
	offset += 4
	copy(data[offset:], memo)

	return data, nil
}

// deserializeNote deserializes a note and memo from decrypted data
func deserializeNote(data []byte) (*ShieldedNote, []byte, error) {
	if len(data) < 32+32+32+32+8+8+4 {
		return nil, nil, errors.New("invalid note data length")
	}

	note := &ShieldedNote{}
	offset := 0

	// Owner
	copy(note.Owner[:], data[offset:offset+32])
	offset += 32

	// Amount
	note.Amount = new(big.Int).SetBytes(data[offset : offset+32])
	offset += 32

	// Asset type
	copy(note.AssetType[:], data[offset:offset+32])
	offset += 32

	// Blinding factor
	note.Blinding.SetBytes(data[offset : offset+32])
	offset += 32

	// Index
	note.Index = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Block height
	note.BlockHeight = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Memo
	memoLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(memoLen) > len(data) {
		return nil, nil, errors.New("invalid memo length")
	}

	memo := data[offset : offset+int(memoLen)]

	return note, memo, nil
}

// OutgoingViewingKey allows senders to decrypt their own sent notes
type OutgoingViewingKey struct {
	Key [32]byte
}

// EncryptForSender encrypts a note copy for the sender's records
func (ne *NoteEncryption) EncryptForSender(note *ShieldedNote, ovk OutgoingViewingKey) ([]byte, error) {
	// Simplified encryption for sender's copy
	key := sha256.Sum256(append(ovk.Key[:], []byte("outgoing")...))

	noteData, err := serializeNote(note, nil)
	if err != nil {
		return nil, err
	}

	// Generate random nonce
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	// Encrypt with key
	ciphertext, _, err := ne.encryptChaCha20(noteData, key[:], nonce)
	if err != nil {
		return nil, err
	}

	// Prepend nonce
	return append(nonce, ciphertext...), nil
}

// CompactNoteEncryption provides space-efficient note encryption
type CompactNoteEncryption struct {
	*NoteEncryption
}

// EncryptCompact encrypts only essential note data
func (cne *CompactNoteEncryption) EncryptCompact(amount *big.Int, assetType AssetID, recipient PublicKey) ([]byte, error) {
	// Encrypt only amount and asset type for compact notes
	data := make([]byte, 40) // 32 (amount) + 8 (asset)

	amountBytes := amount.Bytes()
	copy(data[32-len(amountBytes):32], amountBytes)
	copy(data[32:40], assetType[:8]) // First 8 bytes of asset ID

	// Encrypt with recipient's key
	key := sha256.Sum256(recipient[:])
	nonce := make([]byte, 12)
	rand.Read(nonce)

	ciphertext, _, err := cne.encryptChaCha20(data, key[:], nonce)
	if err != nil {
		return nil, err
	}

	return append(nonce, ciphertext...), nil
}

// BatchNoteEncryption encrypts multiple notes efficiently
type BatchNoteEncryption struct {
	*NoteEncryption
}

// EncryptBatch encrypts multiple notes with shared overhead
func (bne *BatchNoteEncryption) EncryptBatch(notes []*ShieldedNote, recipients []PublicKey) ([]*EncryptedNote, error) {
	if len(notes) != len(recipients) {
		return nil, errors.New("notes and recipients length mismatch")
	}

	encrypted := make([]*EncryptedNote, len(notes))

	for i, note := range notes {
		enc, err := bne.EncryptNote(note, recipients[i], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt note %d: %w", i, err)
		}
		encrypted[i] = enc
	}

	return encrypted, nil
}

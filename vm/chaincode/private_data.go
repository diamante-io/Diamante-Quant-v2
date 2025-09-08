package chaincode

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"diamante/storage"

	"github.com/sirupsen/logrus"
)

// PrivateCollection represents a private data collection configuration
type PrivateCollection struct {
	Name              string   `json:"name"`
	MemberOrgs        []string `json:"member_orgs"`
	BlockToLive       uint64   `json:"block_to_live"`
	MaxPeerCount      int      `json:"max_peer_count"`
	RequiredPeerCount int      `json:"required_peer_count"`
	Policy            string   `json:"policy"`
}

// PrivateDataEntry represents a private data entry
type PrivateDataEntry struct {
	Collection  string            `json:"collection"`
	Key         string            `json:"key"`
	Value       []byte            `json:"value"`
	TxID        string            `json:"tx_id"`
	BlockHeight uint64            `json:"block_height"`
	Timestamp   int64             `json:"timestamp"`
	Hash        string            `json:"hash"`
	Metadata    map[string]string `json:"metadata"`
}

// PrivateDataManager manages private data collections and distribution
type PrivateDataManager struct {
	collections     map[string]*PrivateCollection
	privateData     map[string]map[string]*PrivateDataEntry // collection -> key -> entry
	authorized      map[string][]string                     // collection -> authorized orgs
	encryption      *EncryptionManager
	storage         storage.LedgerStore
	logger          *logrus.Logger
	mu              sync.RWMutex
	config          *PrivateDataConfig
	distributionMgr *PrivateDataDistribution
}

// PrivateDataConfig contains configuration for private data management
type PrivateDataConfig struct {
	EncryptionEnabled    bool   `json:"encryption_enabled"`
	MaxCollections       int    `json:"max_collections"`
	DefaultBlockToLive   uint64 `json:"default_block_to_live"`
	DistributionTimeout  int    `json:"distribution_timeout_ms"`
	PurgeInterval        int    `json:"purge_interval_minutes"`
	CompressionEnabled   bool   `json:"compression_enabled"`
	AccessControlEnabled bool   `json:"access_control_enabled"`
}

// EncryptionManager handles encryption/decryption of private data
type EncryptionManager struct {
	masterKey []byte
	logger    *logrus.Logger
}

// PrivateDataDistribution handles distribution of private data to authorized peers
type PrivateDataDistribution struct {
	peers           map[string][]string // collection -> peer list
	distributionLog map[string]time.Time
	logger          *logrus.Logger
	timeout         time.Duration
	mu              sync.RWMutex
}

// NewPrivateDataManager creates a new private data manager
func NewPrivateDataManager(config *PrivateDataConfig, storage storage.LedgerStore, logger *logrus.Logger) (*PrivateDataManager, error) {
	if config == nil {
		config = DefaultPrivateDataConfig()
	}

	encMgr, err := NewEncryptionManager(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryption manager: %v", err)
	}

	distMgr := &PrivateDataDistribution{
		peers:           make(map[string][]string),
		distributionLog: make(map[string]time.Time),
		logger:          logger,
		timeout:         time.Duration(config.DistributionTimeout) * time.Millisecond,
	}

	return &PrivateDataManager{
		collections:     make(map[string]*PrivateCollection),
		privateData:     make(map[string]map[string]*PrivateDataEntry),
		authorized:      make(map[string][]string),
		encryption:      encMgr,
		storage:         storage,
		logger:          logger,
		config:          config,
		distributionMgr: distMgr,
	}, nil
}

// DefaultPrivateDataConfig returns default configuration
func DefaultPrivateDataConfig() *PrivateDataConfig {
	return &PrivateDataConfig{
		EncryptionEnabled:    true,
		MaxCollections:       100,
		DefaultBlockToLive:   1000,
		DistributionTimeout:  100, // 100ms
		PurgeInterval:        60,  // 60 minutes
		CompressionEnabled:   true,
		AccessControlEnabled: true,
	}
}

// CreateCollection creates a new private data collection
func (pdm *PrivateDataManager) CreateCollection(collection *PrivateCollection) error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	if len(pdm.collections) >= pdm.config.MaxCollections {
		return fmt.Errorf("maximum number of collections reached: %d", pdm.config.MaxCollections)
	}

	if _, exists := pdm.collections[collection.Name]; exists {
		return fmt.Errorf("collection '%s' already exists", collection.Name)
	}

	if collection.BlockToLive == 0 {
		collection.BlockToLive = pdm.config.DefaultBlockToLive
	}

	// Validate collection configuration
	if err := pdm.validateCollection(collection); err != nil {
		return fmt.Errorf("invalid collection configuration: %v", err)
	}

	pdm.collections[collection.Name] = collection
	pdm.privateData[collection.Name] = make(map[string]*PrivateDataEntry)
	pdm.authorized[collection.Name] = collection.MemberOrgs

	pdm.logger.WithFields(logrus.Fields{
		"collection":    collection.Name,
		"member_orgs":   collection.MemberOrgs,
		"block_to_live": collection.BlockToLive,
	}).Info("Created private data collection")

	return nil
}

// PutPrivateData stores private data in a collection
func (pdm *PrivateDataManager) PutPrivateData(collection, key string, value []byte, txID string, blockHeight uint64) error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	coll, exists := pdm.collections[collection]
	if !exists {
		return fmt.Errorf("collection '%s' does not exist", collection)
	}

	// Encrypt data if encryption is enabled
	var encryptedValue []byte
	var err error
	if pdm.config.EncryptionEnabled {
		encryptedValue, err = pdm.encryption.Encrypt(value)
		if err != nil {
			return fmt.Errorf("failed to encrypt private data: %v", err)
		}
	} else {
		encryptedValue = value
	}

	// Create data entry
	entry := &PrivateDataEntry{
		Collection:  collection,
		Key:         key,
		Value:       encryptedValue,
		TxID:        txID,
		BlockHeight: blockHeight,
		Timestamp:   time.Now().Unix(),
		Hash:        pdm.computeHash(collection, key, value),
		Metadata:    make(map[string]string),
	}

	// Store in memory
	pdm.privateData[collection][key] = entry

	// Persist to storage with namespace
	storageKey := fmt.Sprintf("private_data:%s:%s", collection, key)
	entryData, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal private data entry: %v", err)
	}

	if err := pdm.storage.SaveState([]byte(storageKey), entryData); err != nil {
		return fmt.Errorf("failed to persist private data: %v", err)
	}

	pdm.logger.WithFields(logrus.Fields{
		"collection":   collection,
		"key":          key,
		"tx_id":        txID,
		"block_height": blockHeight,
		"encrypted":    pdm.config.EncryptionEnabled,
	}).Debug("Stored private data")

	// Distribute to authorized peers
	go pdm.distributePrivateData(coll, entry)

	return nil
}

// GetPrivateData retrieves private data from a collection
func (pdm *PrivateDataManager) GetPrivateData(collection, key string, orgID string) ([]byte, error) {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	// Check authorization
	if pdm.config.AccessControlEnabled {
		if !pdm.isAuthorized(collection, orgID) {
			return nil, fmt.Errorf("organization '%s' not authorized for collection '%s'", orgID, collection)
		}
	}

	// Try memory first
	if collData, exists := pdm.privateData[collection]; exists {
		if entry, exists := collData[key]; exists {
			return pdm.decryptValue(entry.Value)
		}
	}

	// Try storage
	storageKey := fmt.Sprintf("private_data:%s:%s", collection, key)
	entryData, err := pdm.storage.GetState([]byte(storageKey))
	if err != nil {
		return nil, fmt.Errorf("private data not found: %v", err)
	}

	var entry PrivateDataEntry
	if err := json.Unmarshal(entryData, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal private data entry: %v", err)
	}

	return pdm.decryptValue(entry.Value)
}

// DeletePrivateData removes private data from a collection
func (pdm *PrivateDataManager) DeletePrivateData(collection, key string, orgID string) error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	// Check authorization
	if pdm.config.AccessControlEnabled {
		if !pdm.isAuthorized(collection, orgID) {
			return fmt.Errorf("organization '%s' not authorized for collection '%s'", orgID, collection)
		}
	}

	// Remove from memory
	if collData, exists := pdm.privateData[collection]; exists {
		delete(collData, key)
	}

	// Remove from storage
	storageKey := fmt.Sprintf("private_data:%s:%s", collection, key)
	// Note: LedgerStore doesn't have DeleteState, so we save empty value
	if err := pdm.storage.SaveState([]byte(storageKey), []byte{}); err != nil {
		return fmt.Errorf("failed to delete private data from storage: %v", err)
	}

	pdm.logger.WithFields(logrus.Fields{
		"collection": collection,
		"key":        key,
		"org_id":     orgID,
	}).Debug("Deleted private data")

	return nil
}

// GetPrivateDataHash returns the hash of private data without revealing the data
func (pdm *PrivateDataManager) GetPrivateDataHash(collection, key string) (string, error) {
	pdm.mu.RLock()
	defer pdm.mu.RUnlock()

	if collData, exists := pdm.privateData[collection]; exists {
		if entry, exists := collData[key]; exists {
			return entry.Hash, nil
		}
	}

	// Try storage
	storageKey := fmt.Sprintf("private_data:%s:%s", collection, key)
	entryData, err := pdm.storage.GetState([]byte(storageKey))
	if err != nil {
		return "", fmt.Errorf("private data not found: %v", err)
	}

	var entry PrivateDataEntry
	if err := json.Unmarshal(entryData, &entry); err != nil {
		return "", fmt.Errorf("failed to unmarshal private data entry: %v", err)
	}

	return entry.Hash, nil
}

// PurgeExpiredData removes expired private data based on block-to-live policy
func (pdm *PrivateDataManager) PurgeExpiredData(currentBlockHeight uint64) error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	purgedCount := 0
	for collName, collData := range pdm.privateData {
		collection := pdm.collections[collName]
		if collection == nil {
			continue
		}

		for key, entry := range collData {
			// Check if data has expired
			if currentBlockHeight > entry.BlockHeight+collection.BlockToLive {
				delete(collData, key)

				// Remove from storage
				storageKey := fmt.Sprintf("private_data:%s:%s", collName, key)
				pdm.storage.SaveState([]byte(storageKey), []byte{})

				purgedCount++
			}
		}
	}

	if purgedCount > 0 {
		pdm.logger.WithFields(logrus.Fields{
			"purged_count":   purgedCount,
			"current_height": currentBlockHeight,
		}).Info("Purged expired private data")
	}

	return nil
}

// Helper methods

func (pdm *PrivateDataManager) validateCollection(collection *PrivateCollection) error {
	if collection.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if len(collection.MemberOrgs) == 0 {
		return fmt.Errorf("collection must have at least one member organization")
	}
	if collection.RequiredPeerCount > collection.MaxPeerCount {
		return fmt.Errorf("required peer count cannot exceed max peer count")
	}
	return nil
}

func (pdm *PrivateDataManager) isAuthorized(collection, orgID string) bool {
	if orgs, exists := pdm.authorized[collection]; exists {
		for _, authorizedOrg := range orgs {
			if authorizedOrg == orgID {
				return true
			}
		}
	}
	return false
}

func (pdm *PrivateDataManager) computeHash(collection, key string, value []byte) string {
	hasher := sha256.New()
	hasher.Write([]byte(collection))
	hasher.Write([]byte(key))
	hasher.Write(value)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (pdm *PrivateDataManager) decryptValue(encryptedValue []byte) ([]byte, error) {
	if pdm.config.EncryptionEnabled {
		return pdm.encryption.Decrypt(encryptedValue)
	}
	return encryptedValue, nil
}

func (pdm *PrivateDataManager) distributePrivateData(collection *PrivateCollection, entry *PrivateDataEntry) {
	// Simulate private data distribution to authorized peers
	// In a real implementation, this would use the network layer to distribute
	pdm.distributionMgr.mu.Lock()
	defer pdm.distributionMgr.mu.Unlock()

	key := fmt.Sprintf("%s:%s", collection.Name, entry.Key)
	pdm.distributionMgr.distributionLog[key] = time.Now()

	pdm.logger.WithFields(logrus.Fields{
		"collection":   collection.Name,
		"key":          entry.Key,
		"member_orgs":  collection.MemberOrgs,
		"distribution": "simulated",
	}).Debug("Distributed private data")
}

// Encryption Manager Implementation

func NewEncryptionManager(logger *logrus.Logger) (*EncryptionManager, error) {
	// Generate a random master key for AES-256
	masterKey := make([]byte, 32)
	if _, err := rand.Read(masterKey); err != nil {
		return nil, fmt.Errorf("failed to generate master key: %v", err)
	}

	return &EncryptionManager{
		masterKey: masterKey,
		logger:    logger,
	}, nil
}

func (em *EncryptionManager) Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(em.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %v", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %v", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

func (em *EncryptionManager) Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(em.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %v", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %v", err)
	}

	return plaintext, nil
}

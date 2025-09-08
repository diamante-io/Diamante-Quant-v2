package contracts

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"diamante/common"
	"diamante/consensus"
)

// ContractStore defines the interface for contract persistence
type ContractStore interface {
	SaveContract(contract *ManagedContract) error
	GetContract(id string) (*ManagedContract, error)
	DeleteContract(id string) error
	ListContracts() ([]*ManagedContract, error)
	ArchiveContract(contract *ManagedContract) error
	GetArchivedContract(id string) (*ManagedContract, error)
}

// InMemoryContractStore provides an in-memory implementation of ContractStore
type InMemoryContractStore struct {
	mu                sync.RWMutex
	contracts         map[string]*ManagedContract
	archivedContracts map[string]*ManagedContract
	logger            *logrus.Logger
}

// NewInMemoryContractStore creates a new in-memory contract store
func NewInMemoryContractStore(logger *logrus.Logger) *InMemoryContractStore {
	if logger == nil {
		logger = logrus.New()
	}
	return &InMemoryContractStore{
		contracts:         make(map[string]*ManagedContract),
		archivedContracts: make(map[string]*ManagedContract),
		logger:            logger,
	}
}

// SaveContract saves a contract to the store
func (s *InMemoryContractStore) SaveContract(contract *ManagedContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}
	if contract.ID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a deep copy to avoid external modifications
	contractCopy := s.deepCopyContract(contract)
	s.contracts[contract.ID] = contractCopy

	s.logger.WithFields(logrus.Fields{
		"contractID": contract.ID,
		"state":      contract.State,
		"version":    contract.CurrentVersion,
	}).Debug("Contract saved")

	return nil
}

// GetContract retrieves a contract from the store
func (s *InMemoryContractStore) GetContract(id string) (*ManagedContract, error) {
	if id == "" {
		return nil, fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	contract, exists := s.contracts[id]
	if !exists {
		return nil, fmt.Errorf("contract %s not found", id)
	}

	// Return a deep copy to prevent external modifications
	return s.deepCopyContract(contract), nil
}

// DeleteContract removes a contract from the store
func (s *InMemoryContractStore) DeleteContract(id string) error {
	if id == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.contracts[id]; !exists {
		return fmt.Errorf("contract %s not found", id)
	}

	delete(s.contracts, id)
	s.logger.WithField("contractID", id).Debug("Contract deleted")

	return nil
}

// ListContracts returns all contracts in the store
func (s *InMemoryContractStore) ListContracts() ([]*ManagedContract, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	contracts := make([]*ManagedContract, 0, len(s.contracts))
	for _, contract := range s.contracts {
		contracts = append(contracts, s.deepCopyContract(contract))
	}

	return contracts, nil
}

// ArchiveContract moves a contract to the archive
func (s *InMemoryContractStore) ArchiveContract(contract *ManagedContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}
	if contract.ID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a deep copy for archiving
	archivedCopy := s.deepCopyContract(contract)
	archivedCopy.Metadata["archivedAt"] = consensus.ConsensusUnix()
	s.archivedContracts[contract.ID] = archivedCopy

	s.logger.WithFields(logrus.Fields{
		"contractID": contract.ID,
		"state":      contract.State,
		"version":    contract.CurrentVersion,
	}).Info("Contract archived")

	return nil
}

// GetArchivedContract retrieves an archived contract
func (s *InMemoryContractStore) GetArchivedContract(id string) (*ManagedContract, error) {
	if id == "" {
		return nil, fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	contract, exists := s.archivedContracts[id]
	if !exists {
		return nil, fmt.Errorf("archived contract %s not found", id)
	}

	return s.deepCopyContract(contract), nil
}

// deepCopyContract creates a deep copy of a ManagedContract
func (s *InMemoryContractStore) deepCopyContract(c *ManagedContract) *ManagedContract {
	if c == nil {
		return nil
	}

	// Deep copy versions
	versions := make([]ContractVersion, len(c.Versions))
	for i, v := range c.Versions {
		versions[i] = ContractVersion{
			Version:     v.Version,
			Code:        append([]byte(nil), v.Code...),
			ABI:         append([]byte(nil), v.ABI...),
			DeployedAt:  v.DeployedAt,
			DeployedBy:  v.DeployedBy,
			BlockNumber: v.BlockNumber,
		}
	}

	// Deep copy access control
	accessControl := AccessControl{
		Admins:    append([]string(nil), c.AccessControl.Admins...),
		Operators: append([]string(nil), c.AccessControl.Operators...),
		Pausers:   append([]string(nil), c.AccessControl.Pausers...),
	}

	// Deep copy upgrade policy
	upgradePolicy := UpgradePolicy{
		RequiresGovernance: c.UpgradePolicy.RequiresGovernance,
		TimeLock:           c.UpgradePolicy.TimeLock,
		MultiSigRequired:   c.UpgradePolicy.MultiSigRequired,
		Signers:            append([]string(nil), c.UpgradePolicy.Signers...),
	}

	// Deep copy metadata
	metadata := make(map[string]interface{})
	for k, v := range c.Metadata {
		metadata[k] = v
	}

	return &ManagedContract{
		ID:             c.ID,
		Owner:          c.Owner,
		State:          c.State,
		CurrentVersion: c.CurrentVersion,
		Versions:       versions,
		UpgradePolicy:  upgradePolicy,
		AccessControl:  accessControl,
		Metadata:       metadata,
	}
}

// DatabaseContractStore provides a database-backed implementation of ContractStore
type DatabaseContractStore struct {
	db     *mongo.Database // MongoDB database connection
	logger *logrus.Logger
	mu     sync.RWMutex // Add mutex for thread safety
}

// NewDatabaseContractStore creates a new database-backed contract store
func NewDatabaseContractStore(db *mongo.Database, logger *logrus.Logger) *DatabaseContractStore {
	if logger == nil {
		logger = logrus.New()
	}
	return &DatabaseContractStore{
		db:     db,
		logger: logger,
	}
}

// SaveContract saves a contract to the database
func (s *DatabaseContractStore) SaveContract(contract *ManagedContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}
	if contract.ID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Serialize contract to JSON
	data, err := json.Marshal(contract)
	if err != nil {
		return fmt.Errorf("failed to serialize contract: %w", err)
	}

	if s.db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Execute MongoDB upsert operation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = s.db.Collection("contracts").ReplaceOne(
		ctx,
		bson.M{"_id": contract.ID},
		contract,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("failed to save contract to database: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"contractID": contract.ID,
		"dataSize":   len(data),
		"state":      contract.State,
		"version":    contract.CurrentVersion,
	}).Info("Contract saved to database")

	return nil
}

// GetContract retrieves a contract from the database
func (s *DatabaseContractStore) GetContract(id string) (*ManagedContract, error) {
	if id == "" {
		return nil, fmt.Errorf("contract ID cannot be empty")
	}

	// Create a MongoDB-style query (assuming db is a MongoDB connection)
	// In production, you would adapt this to your specific database
	s.logger.WithField("contractID", id).Debug("Retrieving contract from database")

	// Simulate database query with proper error handling
	var contract ManagedContract

	// Check database connection
	if s.db == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// Execute MongoDB query with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.db.Collection("contracts").FindOne(ctx, bson.M{"_id": id}).Decode(&contract)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("contract with ID %s not found", id)
		}
		return nil, fmt.Errorf("failed to retrieve contract from database: %w", err)
	}

	s.logger.WithField("contractID", id).Info("Contract retrieved from database")
	return &contract, nil
}

// DeleteContract removes a contract from the database
func (s *DatabaseContractStore) DeleteContract(id string) error {
	if id == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Execute MongoDB delete operation with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := s.db.Collection("contracts").DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to delete contract: %w", err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf("contract %s not found", id)
	}

	s.logger.WithField("contractID", id).Info("Contract deleted from database")
	return nil
}

// ListContracts returns all contracts from the database
func (s *DatabaseContractStore) ListContracts() ([]*ManagedContract, error) {
	s.logger.Debug("Listing all contracts from database")

	if s.db == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// Execute MongoDB query to list all contracts
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cursor, err := s.db.Collection("contracts").Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list contracts: %w", err)
	}
	defer cursor.Close(ctx)

	var contracts []*ManagedContract
	for cursor.Next(ctx) {
		var contract ManagedContract
		if err := cursor.Decode(&contract); err != nil {
			s.logger.WithError(err).Warn("Failed to decode contract, skipping")
			continue
		}
		contracts = append(contracts, &contract)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error while listing contracts: %w", err)
	}

	s.logger.WithField("count", len(contracts)).Info("Contracts listed from database")
	return contracts, nil
}

// ArchiveContract archives a contract in the database
func (s *DatabaseContractStore) ArchiveContract(contract *ManagedContract) error {
	if contract == nil {
		return fmt.Errorf("contract cannot be nil")
	}
	if contract.ID == "" {
		return fmt.Errorf("contract ID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Add archive metadata
	archivedContract := *contract
	if archivedContract.Metadata == nil {
		archivedContract.Metadata = make(map[string]interface{})
	}
	archivedContract.Metadata["archivedAt"] = common.ConsensusUnix()
	archivedContract.State = ContractStateArchived

	// Use MongoDB transaction to archive contract atomically
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := s.db.Client().StartSession()
	if err != nil {
		return fmt.Errorf("failed to start MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		// Insert into archive collection
		if _, err := s.db.Collection("archived_contracts").InsertOne(sc, archivedContract); err != nil {
			return fmt.Errorf("failed to insert archived contract: %w", err)
		}
		// Delete from active contracts collection
		result, err := s.db.Collection("contracts").DeleteOne(sc, bson.M{"_id": contract.ID})
		if err != nil {
			return fmt.Errorf("failed to delete active contract: %w", err)
		}
		if result.DeletedCount == 0 {
			return fmt.Errorf("contract %s not found in active collection", contract.ID)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to archive contract: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"contractID": contract.ID,
		"state":      contract.State,
		"version":    contract.CurrentVersion,
	}).Info("Contract archived in database")

	return nil
}

// GetArchivedContract retrieves an archived contract from the database
func (s *DatabaseContractStore) GetArchivedContract(id string) (*ManagedContract, error) {
	if id == "" {
		return nil, fmt.Errorf("contract ID cannot be empty")
	}

	s.logger.WithField("contractID", id).Debug("Retrieving archived contract from database")

	if s.db == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// Query the archived contracts collection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var contract ManagedContract
	err := s.db.Collection("archived_contracts").FindOne(ctx, bson.M{"_id": id}).Decode(&contract)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("archived contract %s not found", id)
		}
		return nil, fmt.Errorf("failed to retrieve archived contract: %w", err)
	}

	s.logger.WithField("contractID", id).Info("Archived contract retrieved from database")
	return &contract, nil
}

// DatabaseContractVersion represents a contract version in the database
// This is a placeholder for potential future implementation
type DatabaseContractVersion struct {
	ID       string                 `bson:"_id"`
	Status   string                 `bson:"status"`
	Metadata map[string]interface{} `bson:"metadata"`
}

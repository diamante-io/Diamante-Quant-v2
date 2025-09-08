package chaincode

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// EndorsementPolicy defines the policy for transaction endorsements
type EndorsementPolicy struct {
	Type      string   `json:"type"`      // "AND", "OR", "OutOf"
	Orgs      []string `json:"orgs"`      // Organization IDs
	Threshold int      `json:"threshold"` // For "OutOf" policies
	Policy    string   `json:"policy"`    // Human readable policy description
}

// Endorsement represents a single endorsement
type Endorsement struct {
	OrgID        string               `json:"org_id"`
	PeerID       string               `json:"peer_id"`
	Signature    string               `json:"signature"`
	Timestamp    int64                `json:"timestamp"`
	ProposalHash string               `json:"proposal_hash"`
	Response     *EndorsementResponse `json:"response"`
	Metadata     map[string]string    `json:"metadata"`
}

// EndorsementResponse contains the response from an endorsing peer
type EndorsementResponse struct {
	Status   int       `json:"status"`    // 200 for success, error codes for failure
	Message  string    `json:"message"`   // Response message or error description
	Payload  []byte    `json:"payload"`   // Response payload
	ReadSet  []KVRead  `json:"read_set"`  // Keys read during execution
	WriteSet []KVWrite `json:"write_set"` // Keys written during execution
}

// KVRead represents a key-value read during chaincode execution
type KVRead struct {
	Namespace string   `json:"namespace"`
	Key       string   `json:"key"`
	Version   *Version `json:"version"`
}

// KVWrite represents a key-value write during chaincode execution
type KVWrite struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	IsDelete  bool   `json:"is_delete"`
}

// Version represents the version of a key
type Version struct {
	BlockNum uint64 `json:"block_num"`
	TxNum    uint64 `json:"tx_num"`
}

// EndorsementRequest represents a request for endorsement
type EndorsementRequest struct {
	ChaincodeID   string            `json:"chaincode_id"`
	Function      string            `json:"function"`
	Args          []string          `json:"args"`
	TransientData map[string][]byte `json:"transient_data"`
	TxID          string            `json:"tx_id"`
	Timestamp     int64             `json:"timestamp"`
	ChannelID     string            `json:"channel_id"`
	Metadata      map[string]string `json:"metadata"`
}

// TransactionProposal represents a complete transaction proposal
type TransactionProposal struct {
	Request      *EndorsementRequest `json:"request"`
	Policy       *EndorsementPolicy  `json:"policy"`
	Endorsements []*Endorsement      `json:"endorsements"`
	ProposalID   string              `json:"proposal_id"`
	Status       string              `json:"status"` // "pending", "endorsed", "rejected"
	CreatedAt    int64               `json:"created_at"`
	ExpiresAt    int64               `json:"expires_at"`
}

// EndorsementManager manages the endorsement process
type EndorsementManager struct {
	policies  map[string]*EndorsementPolicy   // chaincode_id -> policy
	proposals map[string]*TransactionProposal // proposal_id -> proposal
	endorsers map[string]*EndorsingPeer       // org_id -> endorsing peer
	runtime   *ChaincodeRuntime               // Reference to chaincode runtime
	logger    *logrus.Logger
	mu        sync.RWMutex
	config    *EndorsementConfig
}

// EndorsingPeer represents a peer that can provide endorsements
type EndorsingPeer struct {
	OrgID    string `json:"org_id"`
	PeerID   string `json:"peer_id"`
	Endpoint string `json:"endpoint"`
	Active   bool   `json:"active"`
}

// EndorsementConfig contains configuration for the endorsement system
type EndorsementConfig struct {
	ProposalTimeout        int  `json:"proposal_timeout_seconds"`
	MaxConcurrentProposals int  `json:"max_concurrent_proposals"`
	RequireAllEndorsers    bool `json:"require_all_endorsers"`
	SimulationEnabled      bool `json:"simulation_enabled"`
}

// NewEndorsementManager creates a new endorsement manager
func NewEndorsementManager(runtime *ChaincodeRuntime, logger *logrus.Logger) *EndorsementManager {
	return &EndorsementManager{
		policies:  make(map[string]*EndorsementPolicy),
		proposals: make(map[string]*TransactionProposal),
		endorsers: make(map[string]*EndorsingPeer),
		runtime:   runtime,
		logger:    logger,
		config: &EndorsementConfig{
			ProposalTimeout:        300, // 5 minutes
			MaxConcurrentProposals: 1000,
			RequireAllEndorsers:    false,
			SimulationEnabled:      true,
		},
	}
}

// SetEndorsementPolicy sets the endorsement policy for a chaincode
func (em *EndorsementManager) SetEndorsementPolicy(chaincodeID string, policy *EndorsementPolicy) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	if err := em.validatePolicy(policy); err != nil {
		return fmt.Errorf("invalid endorsement policy: %v", err)
	}

	em.policies[chaincodeID] = policy

	em.logger.WithFields(logrus.Fields{
		"chaincode_id": chaincodeID,
		"policy_type":  policy.Type,
		"orgs":         policy.Orgs,
		"threshold":    policy.Threshold,
	}).Info("Set endorsement policy")

	return nil
}

// RegisterEndorser registers an endorsing peer
func (em *EndorsementManager) RegisterEndorser(endorser *EndorsingPeer) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.endorsers[endorser.OrgID] = endorser

	em.logger.WithFields(logrus.Fields{
		"org_id":   endorser.OrgID,
		"peer_id":  endorser.PeerID,
		"endpoint": endorser.Endpoint,
	}).Info("Registered endorsing peer")

	return nil
}

// SubmitProposal submits a transaction proposal for endorsement
func (em *EndorsementManager) SubmitProposal(request *EndorsementRequest) (*TransactionProposal, error) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if len(em.proposals) >= em.config.MaxConcurrentProposals {
		return nil, fmt.Errorf("maximum concurrent proposals reached: %d", em.config.MaxConcurrentProposals)
	}

	policy, exists := em.policies[request.ChaincodeID]
	if !exists {
		return nil, fmt.Errorf("no endorsement policy found for chaincode: %s", request.ChaincodeID)
	}

	proposalID := em.generateProposalID(request)
	proposal := &TransactionProposal{
		Request:      request,
		Policy:       policy,
		Endorsements: make([]*Endorsement, 0),
		ProposalID:   proposalID,
		Status:       "pending",
		CreatedAt:    time.Now().Unix(),
		ExpiresAt:    time.Now().Add(time.Duration(em.config.ProposalTimeout) * time.Second).Unix(),
	}

	em.proposals[proposalID] = proposal

	em.logger.WithFields(logrus.Fields{
		"proposal_id":  proposalID,
		"chaincode_id": request.ChaincodeID,
		"function":     request.Function,
		"tx_id":        request.TxID,
	}).Info("Submitted proposal for endorsement")

	// Start endorsement process
	go em.processEndorsement(proposal)

	return proposal, nil
}

// GetProposal retrieves a transaction proposal by ID
func (em *EndorsementManager) GetProposal(proposalID string) (*TransactionProposal, error) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	proposal, exists := em.proposals[proposalID]
	if !exists {
		return nil, fmt.Errorf("proposal not found: %s", proposalID)
	}

	return proposal, nil
}

// processEndorsement processes a transaction proposal through the endorsement workflow
func (em *EndorsementManager) processEndorsement(proposal *TransactionProposal) {
	em.logger.WithField("proposal_id", proposal.ProposalID).Debug("Starting endorsement process")

	// Simulate transaction execution to get read/write sets
	readSet, writeSet, err := em.simulateTransaction(proposal.Request)
	if err != nil {
		em.updateProposalStatus(proposal.ProposalID, "rejected")
		em.logger.WithError(err).Error("Transaction simulation failed")
		return
	}

	// Collect endorsements from required peers
	requiredOrgs := proposal.Policy.Orgs
	endorsements := make([]*Endorsement, 0, len(requiredOrgs))

	for _, orgID := range requiredOrgs {
		endorser, exists := em.endorsers[orgID]
		if !exists {
			em.logger.WithField("org_id", orgID).Warn("Endorser not found for organization")
			continue
		}

		if !endorser.Active {
			em.logger.WithField("org_id", orgID).Warn("Endorser is inactive")
			continue
		}

		endorsement, err := em.requestEndorsement(endorser, proposal, readSet, writeSet)
		if err != nil {
			em.logger.WithError(err).WithField("org_id", orgID).Error("Failed to get endorsement")
			continue
		}

		endorsements = append(endorsements, endorsement)
	}

	// Update proposal with endorsements
	em.mu.Lock()
	proposal.Endorsements = endorsements
	em.mu.Unlock()

	// Evaluate endorsement policy
	if em.evaluatePolicy(proposal.Policy, endorsements) {
		em.updateProposalStatus(proposal.ProposalID, "endorsed")
		em.logger.WithField("proposal_id", proposal.ProposalID).Info("Proposal successfully endorsed")
	} else {
		em.updateProposalStatus(proposal.ProposalID, "rejected")
		em.logger.WithField("proposal_id", proposal.ProposalID).Warn("Proposal rejected - insufficient endorsements")
	}
}

// simulateTransaction simulates transaction execution to determine read/write sets
func (em *EndorsementManager) simulateTransaction(request *EndorsementRequest) ([]KVRead, []KVWrite, error) {
	if !em.config.SimulationEnabled {
		return nil, nil, nil
	}

	// Create a simulated execution environment
	// This is a simplified simulation - in a real implementation,
	// this would use a snapshot of the current state
	readSet := []KVRead{
		{
			Namespace: request.ChaincodeID,
			Key:       "simulated_key",
			Version:   &Version{BlockNum: 100, TxNum: 1},
		},
	}

	writeSet := []KVWrite{
		{
			Namespace: request.ChaincodeID,
			Key:       "simulated_key",
			Value:     []byte("simulated_value"),
			IsDelete:  false,
		},
	}

	em.logger.WithFields(logrus.Fields{
		"chaincode_id": request.ChaincodeID,
		"function":     request.Function,
		"read_count":   len(readSet),
		"write_count":  len(writeSet),
	}).Debug("Transaction simulation completed")

	return readSet, writeSet, nil
}

// requestEndorsement requests an endorsement from a specific peer
func (em *EndorsementManager) requestEndorsement(endorser *EndorsingPeer, proposal *TransactionProposal, readSet []KVRead, writeSet []KVWrite) (*Endorsement, error) {
	// In a real implementation, this would make a network call to the endorsing peer
	// For now, we simulate the endorsement process

	proposalHash := em.computeProposalHash(proposal.Request)

	response := &EndorsementResponse{
		Status:   200,
		Message:  "Success",
		Payload:  []byte("endorsement_response"),
		ReadSet:  readSet,
		WriteSet: writeSet,
	}

	endorsement := &Endorsement{
		OrgID:        endorser.OrgID,
		PeerID:       endorser.PeerID,
		Signature:    em.generateSignature(proposalHash, endorser.OrgID),
		Timestamp:    time.Now().Unix(),
		ProposalHash: proposalHash,
		Response:     response,
		Metadata:     make(map[string]string),
	}

	em.logger.WithFields(logrus.Fields{
		"org_id":        endorser.OrgID,
		"peer_id":       endorser.PeerID,
		"proposal_hash": proposalHash,
	}).Debug("Generated endorsement")

	return endorsement, nil
}

// evaluatePolicy evaluates whether the collected endorsements satisfy the policy
func (em *EndorsementManager) evaluatePolicy(policy *EndorsementPolicy, endorsements []*Endorsement) bool {
	switch policy.Type {
	case "AND":
		return em.evaluateANDPolicy(policy, endorsements)
	case "OR":
		return em.evaluateORPolicy(policy, endorsements)
	case "OutOf":
		return em.evaluateOutOfPolicy(policy, endorsements)
	default:
		em.logger.WithField("policy_type", policy.Type).Error("Unknown policy type")
		return false
	}
}

// evaluateANDPolicy evaluates AND policy (all organizations must endorse)
func (em *EndorsementManager) evaluateANDPolicy(policy *EndorsementPolicy, endorsements []*Endorsement) bool {
	requiredOrgs := make(map[string]bool)
	for _, org := range policy.Orgs {
		requiredOrgs[org] = false
	}

	for _, endorsement := range endorsements {
		if _, required := requiredOrgs[endorsement.OrgID]; required {
			requiredOrgs[endorsement.OrgID] = true
		}
	}

	for _, endorsed := range requiredOrgs {
		if !endorsed {
			return false
		}
	}

	return true
}

// evaluateORPolicy evaluates OR policy (at least one organization must endorse)
func (em *EndorsementManager) evaluateORPolicy(policy *EndorsementPolicy, endorsements []*Endorsement) bool {
	for _, endorsement := range endorsements {
		for _, org := range policy.Orgs {
			if endorsement.OrgID == org {
				return true
			}
		}
	}

	return false
}

// evaluateOutOfPolicy evaluates OutOf policy (threshold number of organizations must endorse)
func (em *EndorsementManager) evaluateOutOfPolicy(policy *EndorsementPolicy, endorsements []*Endorsement) bool {
	endorsedOrgs := make(map[string]bool)

	for _, endorsement := range endorsements {
		for _, org := range policy.Orgs {
			if endorsement.OrgID == org {
				endorsedOrgs[org] = true
			}
		}
	}

	return len(endorsedOrgs) >= policy.Threshold
}

// Helper methods

func (em *EndorsementManager) validatePolicy(policy *EndorsementPolicy) error {
	if policy.Type == "" {
		return fmt.Errorf("policy type cannot be empty")
	}

	if policy.Type != "AND" && policy.Type != "OR" && policy.Type != "OutOf" {
		return fmt.Errorf("invalid policy type: %s", policy.Type)
	}

	if len(policy.Orgs) == 0 {
		return fmt.Errorf("policy must specify at least one organization")
	}

	if policy.Type == "OutOf" && policy.Threshold <= 0 {
		return fmt.Errorf("OutOf policy must specify a positive threshold")
	}

	if policy.Type == "OutOf" && policy.Threshold > len(policy.Orgs) {
		return fmt.Errorf("OutOf policy threshold cannot exceed number of organizations")
	}

	return nil
}

func (em *EndorsementManager) generateProposalID(request *EndorsementRequest) string {
	hasher := sha256.New()
	hasher.Write([]byte(request.ChaincodeID))
	hasher.Write([]byte(request.Function))
	hasher.Write([]byte(request.TxID))
	hasher.Write([]byte(fmt.Sprintf("%d", request.Timestamp)))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (em *EndorsementManager) computeProposalHash(request *EndorsementRequest) string {
	data, _ := json.Marshal(request)
	hasher := sha256.New()
	hasher.Write(data)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (em *EndorsementManager) generateSignature(proposalHash, orgID string) string {
	// In a real implementation, this would use cryptographic signatures
	hasher := sha256.New()
	hasher.Write([]byte(proposalHash))
	hasher.Write([]byte(orgID))
	hasher.Write([]byte("signature_secret"))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (em *EndorsementManager) updateProposalStatus(proposalID, status string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if proposal, exists := em.proposals[proposalID]; exists {
		proposal.Status = status
	}
}

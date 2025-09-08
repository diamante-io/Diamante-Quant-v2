package contracts

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"

	"diamante/common"
)

// ApprovalStatus represents the status of a multi-sig approval
type ApprovalStatus int

const (
	// ApprovalPending is waiting for signatures
	ApprovalPending ApprovalStatus = iota
	// ApprovalApproved has enough signatures
	ApprovalApproved
	// ApprovalRejected was rejected by signers
	ApprovalRejected
	// ApprovalExpired passed the deadline
	ApprovalExpired
	// ApprovalExecuted was executed
	ApprovalExecuted
)

// MultiSigApproval represents a multi-signature approval request
type MultiSigApproval struct {
	ID               string
	Type             string // "upgrade", "pause", "destroy", etc.
	TargetContract   string
	Proposer         string
	RequiredSigners  []string
	CollectedSigners map[string]*Signature
	Threshold        int
	Status           ApprovalStatus
	Data             map[string]interface{}
	ProposedAt       time.Time
	Deadline         time.Time
	ExecutedAt       *time.Time
}

// Signature represents a cryptographic signature
type Signature struct {
	Signer    string
	Signature []byte
	SignedAt  time.Time
	Message   []byte
}

// MultiSigManager manages multi-signature approvals
type MultiSigManager struct {
	mu        sync.RWMutex
	approvals map[string]*MultiSigApproval
	store     ContractStore
	logger    *logrus.Logger
}

// NewMultiSigManager creates a new multi-signature manager
func NewMultiSigManager(store ContractStore, logger *logrus.Logger) *MultiSigManager {
	if logger == nil {
		logger = logrus.New()
	}
	return &MultiSigManager{
		approvals: make(map[string]*MultiSigApproval),
		store:     store,
		logger:    logger,
	}
}

// CreateApproval creates a new multi-signature approval request
func (m *MultiSigManager) CreateApproval(
	approvalType string,
	targetContract string,
	proposer string,
	requiredSigners []string,
	threshold int,
	data map[string]interface{},
	deadline time.Duration,
) (*MultiSigApproval, error) {
	if approvalType == "" || targetContract == "" || proposer == "" {
		return nil, fmt.Errorf("approval type, target contract, and proposer are required")
	}

	if len(requiredSigners) == 0 {
		return nil, fmt.Errorf("at least one signer is required")
	}

	if threshold <= 0 || threshold > len(requiredSigners) {
		return nil, fmt.Errorf("invalid threshold: must be between 1 and %d", len(requiredSigners))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	approval := &MultiSigApproval{
		ID:               fmt.Sprintf("approval_%s_%d", targetContract, common.ConsensusUnixNano()),
		Type:             approvalType,
		TargetContract:   targetContract,
		Proposer:         proposer,
		RequiredSigners:  requiredSigners,
		CollectedSigners: make(map[string]*Signature),
		Threshold:        threshold,
		Status:           ApprovalPending,
		Data:             data,
		ProposedAt:       common.ConsensusNow(),
		Deadline:         common.ConsensusNow().Add(deadline),
	}

	m.approvals[approval.ID] = approval

	m.logger.WithFields(logrus.Fields{
		"approvalID":     approval.ID,
		"type":           approvalType,
		"targetContract": targetContract,
		"proposer":       proposer,
		"threshold":      threshold,
		"signers":        len(requiredSigners),
		"deadline":       approval.Deadline,
	}).Info("Multi-sig approval created")

	return approval, nil
}

// AddSignature adds a signature to an approval
func (m *MultiSigManager) AddSignature(approvalID string, signer string, signature []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	approval, exists := m.approvals[approvalID]
	if !exists {
		return fmt.Errorf("approval %s not found", approvalID)
	}

	// Check if approval is still pending
	if approval.Status != ApprovalPending {
		return fmt.Errorf("approval is no longer pending")
	}

	// Check if deadline has passed
	if common.ConsensusNow().After(approval.Deadline) {
		approval.Status = ApprovalExpired
		return fmt.Errorf("approval deadline has passed")
	}

	// Check if signer is authorized
	authorized := false
	for _, s := range approval.RequiredSigners {
		if s == signer {
			authorized = true
			break
		}
	}
	if !authorized {
		return fmt.Errorf("signer %s is not authorized for this approval", signer)
	}

	// Check if signer has already signed
	if _, alreadySigned := approval.CollectedSigners[signer]; alreadySigned {
		return fmt.Errorf("signer %s has already signed", signer)
	}

	// Verify signature
	message := m.getApprovalMessage(approval)
	if err := m.verifySignature(signer, message, signature); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Add signature
	approval.CollectedSigners[signer] = &Signature{
		Signer:    signer,
		Signature: signature,
		SignedAt:  common.ConsensusNow(),
		Message:   message,
	}

	m.logger.WithFields(logrus.Fields{
		"approvalID": approvalID,
		"signer":     signer,
		"collected":  len(approval.CollectedSigners),
		"required":   approval.Threshold,
	}).Info("Signature added to approval")

	// Check if threshold is met
	if len(approval.CollectedSigners) >= approval.Threshold {
		approval.Status = ApprovalApproved
		m.logger.WithField("approvalID", approvalID).Info("Approval threshold met")
	}

	return nil
}

// CheckApproval checks if an approval has enough signatures
func (m *MultiSigManager) CheckApproval(approvalID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	approval, exists := m.approvals[approvalID]
	if !exists {
		return false, fmt.Errorf("approval %s not found", approvalID)
	}

	// Check deadline
	if common.ConsensusNow().After(approval.Deadline) && approval.Status == ApprovalPending {
		approval.Status = ApprovalExpired
		return false, fmt.Errorf("approval has expired")
	}

	return approval.Status == ApprovalApproved, nil
}

// ExecuteApproval marks an approval as executed
func (m *MultiSigManager) ExecuteApproval(approvalID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	approval, exists := m.approvals[approvalID]
	if !exists {
		return fmt.Errorf("approval %s not found", approvalID)
	}

	if approval.Status != ApprovalApproved {
		return fmt.Errorf("approval is not approved")
	}

	now := common.ConsensusNow()
	approval.Status = ApprovalExecuted
	approval.ExecutedAt = &now

	m.logger.WithField("approvalID", approvalID).Info("Approval executed")

	return nil
}

// GetApproval retrieves an approval by ID
func (m *MultiSigManager) GetApproval(approvalID string) (*MultiSigApproval, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	approval, exists := m.approvals[approvalID]
	if !exists {
		return nil, fmt.Errorf("approval %s not found", approvalID)
	}

	// Return a copy to prevent external modification
	approvalCopy := *approval
	approvalCopy.CollectedSigners = make(map[string]*Signature)
	for k, v := range approval.CollectedSigners {
		sigCopy := *v
		approvalCopy.CollectedSigners[k] = &sigCopy
	}

	return &approvalCopy, nil
}

// GetPendingApprovals returns all pending approvals
func (m *MultiSigManager) GetPendingApprovals() ([]*MultiSigApproval, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pending []*MultiSigApproval
	for _, approval := range m.approvals {
		if approval.Status == ApprovalPending {
			// Check and update expired approvals
			if common.ConsensusNow().After(approval.Deadline) {
				approval.Status = ApprovalExpired
			} else {
				approvalCopy := *approval
				pending = append(pending, &approvalCopy)
			}
		}
	}

	return pending, nil
}

// getApprovalMessage creates a message to be signed for an approval
func (m *MultiSigManager) getApprovalMessage(approval *MultiSigApproval) []byte {
	message := fmt.Sprintf(
		"MultiSigApproval:%s:%s:%s:%s:%d:%d",
		approval.ID,
		approval.Type,
		approval.TargetContract,
		approval.Proposer,
		approval.Threshold,
		approval.ProposedAt.Unix(),
	)
	return []byte(message)
}

// verifySignature verifies a signature against a message and signer
func (m *MultiSigManager) verifySignature(signer string, message []byte, signature []byte) error {
	// Hash the message
	messageHash := crypto.Keccak256(message)

	// Recover the public key from the signature
	pubKey, err := crypto.SigToPub(messageHash, signature)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	// Get the address from the public key
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)

	// Compare with the expected signer
	expectedAddr := ethcommon.HexToAddress(signer)
	if recoveredAddr != expectedAddr {
		return fmt.Errorf("signature does not match signer")
	}

	return nil
}

// SignMessage signs a message with a private key
func SignMessage(privateKey *ecdsa.PrivateKey, message []byte) ([]byte, error) {
	if privateKey == nil {
		return nil, errors.New("private key is required")
	}

	// Hash the message
	messageHash := crypto.Keccak256(message)

	// Sign the hash
	signature, err := crypto.Sign(messageHash, privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign message: %w", err)
	}

	return signature, nil
}

// VerifyMultiSigPolicy verifies that a multi-sig policy is satisfied
func VerifyMultiSigPolicy(policy UpgradePolicy, approvalID string, manager *MultiSigManager) error {
	if !policy.MultiSigRequired {
		return nil
	}

	approval, err := manager.GetApproval(approvalID)
	if err != nil {
		return fmt.Errorf("failed to get approval: %w", err)
	}

	if approval.Status != ApprovalApproved && approval.Status != ApprovalExecuted {
		return fmt.Errorf("multi-sig approval not satisfied")
	}

	// Verify all required signers have signed if specified
	if len(policy.Signers) > 0 {
		for _, requiredSigner := range policy.Signers {
			if _, signed := approval.CollectedSigners[requiredSigner]; !signed {
				return fmt.Errorf("required signer %s has not signed", requiredSigner)
			}
		}
	}

	return nil
}

// MultiSigConfig represents configuration for multi-signature requirements
type MultiSigConfig struct {
	DefaultThreshold   int
	DefaultTimeout     time.Duration
	RequireAllSigners  bool
	AllowSelfApproval  bool
	MinSigners         int
	MaxSigners         int
	AutoCleanupPeriod  time.Duration
	NotificationConfig NotificationConfig
}

// NotificationConfig represents configuration for notifications
type NotificationConfig struct {
	EmailEnabled    bool
	WebhookEnabled  bool
	WebhookURL      string
	EmailRecipients []string
}

// DefaultMultiSigConfig returns default multi-sig configuration
func DefaultMultiSigConfig() *MultiSigConfig {
	return &MultiSigConfig{
		DefaultThreshold:  2,
		DefaultTimeout:    24 * time.Hour,
		RequireAllSigners: false,
		AllowSelfApproval: false,
		MinSigners:        2,
		MaxSigners:        10,
		AutoCleanupPeriod: 7 * 24 * time.Hour,
	}
}

// CleanupExpiredApprovals removes expired approvals
func (m *MultiSigManager) CleanupExpiredApprovals() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for id, approval := range m.approvals {
		if approval.Status == ApprovalExpired || approval.Status == ApprovalExecuted {
			if approval.ExecutedAt != nil && time.Since(*approval.ExecutedAt) > 30*24*time.Hour {
				delete(m.approvals, id)
				count++
			} else if approval.Status == ApprovalExpired && time.Since(approval.Deadline) > 7*24*time.Hour {
				delete(m.approvals, id)
				count++
			}
		}
	}

	if count > 0 {
		m.logger.WithField("count", count).Info("Cleaned up expired approvals")
	}

	return count
}

// GenerateApprovalReport generates a report of approval activity
func (m *MultiSigManager) GenerateApprovalReport(since time.Time) map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	report := map[string]interface{}{
		"total":    0,
		"pending":  0,
		"approved": 0,
		"rejected": 0,
		"expired":  0,
		"executed": 0,
		"byType":   make(map[string]int),
		"avgTime":  0,
	}

	var totalTime time.Duration
	var executedCount int

	for _, approval := range m.approvals {
		if approval.ProposedAt.Before(since) {
			continue
		}

		report["total"] = report["total"].(int) + 1

		// Count by status
		switch approval.Status {
		case ApprovalPending:
			report["pending"] = report["pending"].(int) + 1
		case ApprovalApproved:
			report["approved"] = report["approved"].(int) + 1
		case ApprovalRejected:
			report["rejected"] = report["rejected"].(int) + 1
		case ApprovalExpired:
			report["expired"] = report["expired"].(int) + 1
		case ApprovalExecuted:
			report["executed"] = report["executed"].(int) + 1
			if approval.ExecutedAt != nil {
				totalTime += approval.ExecutedAt.Sub(approval.ProposedAt)
				executedCount++
			}
		}

		// Count by type
		byType := report["byType"].(map[string]int)
		byType[approval.Type]++
	}

	// Calculate average time to execution
	if executedCount > 0 {
		report["avgTime"] = totalTime / time.Duration(executedCount)
	}

	return report
}

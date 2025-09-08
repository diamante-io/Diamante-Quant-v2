package contracts

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"diamante/common"
	"diamante/consensus"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
)

// ContractState represents the lifecycle state of a contract.
type ContractState int

const (
	ContractStateDraft ContractState = iota
	ContractStateDeployed
	ContractStatePaused
	ContractStateUpgrading
	ContractStateDeprecated
	ContractStateDestroyed
	ContractStateArchived // Contract has been archived
	ContractStateUnknown  // Contract state is unknown
)

// ContractVersion holds metadata about a deployed contract version.
type ContractVersion struct {
	Version     string
	Code        []byte
	ABI         []byte
	DeployedAt  time.Time
	DeployedBy  string
	BlockNumber uint64
}

// ManagedContract tracks metadata for a deployed contract.
type ManagedContract struct {
	ID             string
	Owner          string
	State          ContractState
	CurrentVersion string
	Versions       []ContractVersion
	UpgradePolicy  UpgradePolicy
	AccessControl  AccessControl
	Metadata       map[string]interface{}
}

// UpgradePolicy specifies upgrade requirements for a contract.
type UpgradePolicy struct {
	RequiresGovernance bool
	TimeLock           time.Duration
	MultiSigRequired   bool
	Signers            []string
}

// AccessControl defines which accounts can manage a contract.
type AccessControl struct {
	Admins    []string
	Operators []string
	Pausers   []string
}

// EVMExecutor abstracts contract deployment.
type EVMExecutor interface {
	DeployContract(caller ethcommon.Address, code []byte, value *big.Int, gasLimit uint64) (ethcommon.Address, []byte, uint64, error)
	GetCode(addr ethcommon.Address) ([]byte, error)
}

// ContractLifecycleManager manages smart contract lifecycles.
type ContractLifecycleManager struct {
	mu              sync.RWMutex
	contracts       map[string]*ManagedContract
	evmExecutor     EVMExecutor
	ledger          common.LedgerAPI
	governanceAddr  string
	logger          *logrus.Logger
	store           ContractStore
	stateMigrator   *StateMigrator
	onChainControl  *OnChainController
	multiSigManager *MultiSigManager
}

// NewContractLifecycleManager creates a new manager instance.
func NewContractLifecycleManager(exec EVMExecutor, ledger common.LedgerAPI) *ContractLifecycleManager {
	logger := logrus.New()
	store := NewInMemoryContractStore(logger)

	clm := &ContractLifecycleManager{
		contracts:       make(map[string]*ManagedContract),
		evmExecutor:     exec,
		ledger:          ledger,
		logger:          logger,
		store:           store,
		stateMigrator:   NewStateMigrator(exec, logger),
		onChainControl:  NewOnChainController(exec, logger),
		multiSigManager: NewMultiSigManager(store, logger),
	}

	return clm
}

// SetGovernanceAddress sets the governance address for the manager
func (clm *ContractLifecycleManager) SetGovernanceAddress(addr string) {
	clm.mu.Lock()
	defer clm.mu.Unlock()
	clm.governanceAddr = addr
	clm.logger.WithField("address", addr).Info("Governance address set")
}

// SetStore sets a custom contract store
func (clm *ContractLifecycleManager) SetStore(store ContractStore) {
	clm.mu.Lock()
	defer clm.mu.Unlock()
	clm.store = store
}

// DeployContract deploys a new contract via the EVM executor.
func (clm *ContractLifecycleManager) DeployContract(owner string, code, abi []byte, policy UpgradePolicy, metadata map[string]interface{}) (string, error) {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	addr, _, _, err := clm.evmExecutor.DeployContract(ethcommon.HexToAddress(owner), code, big.NewInt(0), 0)
	if err != nil {
		return "", fmt.Errorf("EVM deployment failed: %w", err)
	}

	id := addr.Hex()
	mc := &ManagedContract{
		ID:             id,
		Owner:          owner,
		State:          ContractStateDeployed,
		CurrentVersion: "1.0.0",
		Versions: []ContractVersion{{
			Version:     "1.0.0",
			Code:        code,
			ABI:         abi,
			DeployedAt:  consensus.ConsensusNow(),
			DeployedBy:  owner,
			BlockNumber: clm.getCurrentBlockNumber(),
		}},
		UpgradePolicy: policy,
		AccessControl: AccessControl{
			Admins:    []string{owner},
			Operators: []string{owner},
			Pausers:   []string{owner},
		},
		Metadata: metadata,
	}
	clm.contracts[id] = mc

	return id, clm.persistContract(mc)
}

// UpgradeContract deploys a new implementation and updates metadata.
func (clm *ContractLifecycleManager) UpgradeContract(id string, newCode, newABI []byte, newVersion, upgrader string) error {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}
	if !clm.hasUpgradePermission(mc, upgrader) {
		return fmt.Errorf("upgrader %s lacks permission", upgrader)
	}
	if err := clm.validateUpgradePolicy(mc, upgrader); err != nil {
		return err
	}

	mc.State = ContractStateUpgrading
	addr, _, _, err := clm.evmExecutor.DeployContract(ethcommon.HexToAddress(upgrader), newCode, big.NewInt(0), 0)
	if err != nil {
		mc.State = ContractStateDeployed
		return fmt.Errorf("new implementation deployment failed: %w", err)
	}
	if err := clm.migrateContractState(id, addr.Hex()); err != nil {
		mc.State = ContractStateDeployed
		return fmt.Errorf("state migration failed: %w", err)
	}

	mc.Versions = append(mc.Versions, ContractVersion{
		Version:     newVersion,
		Code:        newCode,
		ABI:         newABI,
		DeployedAt:  consensus.ConsensusNow(),
		DeployedBy:  upgrader,
		BlockNumber: clm.getCurrentBlockNumber(),
	})
	mc.CurrentVersion = newVersion
	mc.State = ContractStateDeployed

	return clm.persistContract(mc)
}

// PauseContract pauses a deployed contract.
func (clm *ContractLifecycleManager) PauseContract(id, pauser string) error {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}
	if !clm.hasPausePermission(mc, pauser) {
		return fmt.Errorf("pauser %s lacks permission", pauser)
	}
	if mc.State != ContractStateDeployed {
		return fmt.Errorf("can only pause deployed contracts")
	}

	mc.State = ContractStatePaused
	if err := clm.callContractPause(id); err != nil {
		clm.logger.Warnf("Contract pause function failed: %v", err)
	}
	return clm.persistContract(mc)
}

// ResumeContract resumes a paused contract.
func (clm *ContractLifecycleManager) ResumeContract(id, resumer string) error {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}
	if !clm.hasResumePermission(mc, resumer) {
		return fmt.Errorf("resumer %s lacks permission", resumer)
	}
	if mc.State != ContractStatePaused {
		return fmt.Errorf("can only resume paused contracts")
	}

	mc.State = ContractStateDeployed
	if err := clm.callContractResume(id); err != nil {
		clm.logger.Warnf("Contract resume function failed: %v", err)
	}
	return clm.persistContract(mc)
}

// DestroyContract removes a contract from active use.
func (clm *ContractLifecycleManager) DestroyContract(id, destroyer string) error {
	clm.mu.Lock()
	defer clm.mu.Unlock()

	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}
	if destroyer != mc.Owner && destroyer != clm.governanceAddr {
		return fmt.Errorf("only owner or governance can destroy contract")
	}

	mc.State = ContractStateDestroyed
	if err := clm.callContractDestroy(id); err != nil {
		return fmt.Errorf("contract destruction failed: %w", err)
	}
	if err := clm.archiveContract(mc); err != nil {
		clm.logger.Errorf("Failed to archive destroyed contract: %v", err)
	}
	delete(clm.contracts, id)
	if clm.ledger != nil {
		if err := clm.ledger.RemoveSmartContract(id); err != nil {
			clm.logger.WithError(err).WithField("contractID", id).Warn("Failed to remove contract from ledger during removal")
			// Continue with removal from local state even if ledger removal fails
		}
	}
	return nil
}

func (clm *ContractLifecycleManager) hasUpgradePermission(c *ManagedContract, user string) bool {
	for _, a := range c.AccessControl.Admins {
		if a == user {
			return true
		}
	}
	return c.UpgradePolicy.RequiresGovernance && user == clm.governanceAddr
}

func (clm *ContractLifecycleManager) hasPausePermission(c *ManagedContract, user string) bool {
	for _, p := range c.AccessControl.Pausers {
		if p == user {
			return true
		}
	}
	for _, a := range c.AccessControl.Admins {
		if a == user {
			return true
		}
	}
	return false
}

func (clm *ContractLifecycleManager) hasResumePermission(c *ManagedContract, user string) bool {
	return clm.hasPausePermission(c, user)
}

func (clm *ContractLifecycleManager) validateUpgradePolicy(c *ManagedContract, upgrader string) error {
	policy := c.UpgradePolicy
	if policy.TimeLock > 0 {
		last := c.Versions[len(c.Versions)-1]
		elapsed := time.Since(last.DeployedAt)
		if elapsed < policy.TimeLock {
			return fmt.Errorf("timelock not expired: %v remaining", policy.TimeLock-elapsed)
		}
	}
	if policy.MultiSigRequired {
		if clm.multiSigManager == nil {
			return fmt.Errorf("multi-sig manager not configured")
		}

		// Check if there's an approved multi-sig approval for this upgrade
		approvals, err := clm.multiSigManager.GetPendingApprovals()
		if err != nil {
			return fmt.Errorf("failed to get pending approvals: %w", err)
		}

		// Look for an approved upgrade approval
		approved := false
		for _, approval := range approvals {
			if approval.Type == "upgrade" && approval.TargetContract == c.ID && approval.Status == ApprovalApproved {
				approved = true
				break
			}
		}

		if !approved {
			// Create a new approval request
			deadline := 24 * time.Hour
			if policy.TimeLock > 0 {
				deadline = policy.TimeLock
			}

			_, err := clm.multiSigManager.CreateApproval(
				"upgrade",
				c.ID,
				upgrader,
				policy.Signers,
				len(policy.Signers)/2+1, // Majority threshold
				map[string]interface{}{
					"newVersion": "pending",
					"upgrader":   upgrader,
				},
				deadline,
			)
			if err != nil {
				return fmt.Errorf("failed to create multi-sig approval: %w", err)
			}

			return fmt.Errorf("multi-sig approval required - approval request created")
		}
	}
	return nil
}

func (clm *ContractLifecycleManager) migrateContractState(oldAddr, newAddr string) error {
	if clm.stateMigrator == nil {
		clm.logger.Warn("State migrator not configured, skipping migration")
		return nil
	}

	return clm.stateMigrator.MigrateContractState(oldAddr, newAddr)
}

func (clm *ContractLifecycleManager) persistContract(c *ManagedContract) error {
	if clm.store == nil {
		clm.logger.Warn("Contract store not configured, skipping persistence")
		return nil
	}

	return clm.store.SaveContract(c)
}

func (clm *ContractLifecycleManager) archiveContract(c *ManagedContract) error {
	if clm.store == nil {
		clm.logger.Warn("Contract store not configured, skipping archival")
		return nil
	}

	return clm.store.ArchiveContract(c)
}

func (clm *ContractLifecycleManager) callContractPause(id string) error {
	if clm.onChainControl == nil {
		clm.logger.Warn("On-chain controller not configured, skipping on-chain pause")
		return nil
	}

	// Get the contract to find the pauser
	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}

	// Use the first admin as the pauser
	pauser := mc.Owner
	if len(mc.AccessControl.Admins) > 0 {
		pauser = mc.AccessControl.Admins[0]
	}

	return clm.onChainControl.PauseContract(id, pauser)
}

func (clm *ContractLifecycleManager) callContractResume(id string) error {
	if clm.onChainControl == nil {
		clm.logger.Warn("On-chain controller not configured, skipping on-chain resume")
		return nil
	}

	// Get the contract to find the resumer
	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}

	// Use the first admin as the resumer
	resumer := mc.Owner
	if len(mc.AccessControl.Admins) > 0 {
		resumer = mc.AccessControl.Admins[0]
	}

	return clm.onChainControl.ResumeContract(id, resumer)
}

func (clm *ContractLifecycleManager) callContractDestroy(id string) error {
	if clm.onChainControl == nil {
		clm.logger.Warn("On-chain controller not configured, skipping on-chain destroy")
		return nil
	}

	// Get the contract to find the destroyer
	mc, ok := clm.contracts[id]
	if !ok {
		return fmt.Errorf("contract %s not found", id)
	}

	// Use the owner as the destroyer
	destroyer := mc.Owner

	return clm.onChainControl.DestroyContract(id, destroyer)
}

func (clm *ContractLifecycleManager) getCurrentBlockNumber() uint64 {
	if clm.ledger == nil {
		return 0
	}
	h, err := clm.ledger.GetBlockHeight()
	if err != nil {
		return 0
	}
	return uint64(h)
}

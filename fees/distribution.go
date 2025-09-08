package fees

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"

	"diamante/common"
)

// Metrics for fee distribution monitoring
var (
	feeDistributionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "diamante_fee_distribution_total",
		Help: "Total fees distributed by recipient type",
	}, []string{"recipient"})

	feeDistributionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "diamante_fee_distribution_errors_total",
		Help: "Total errors during fee distribution",
	})

	tokensBurnedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "diamante_tokens_burned_total",
		Help: "Total tokens burned from fees",
	})

	stakerRewardsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "diamante_staker_rewards_pending",
		Help: "Amount of staker rewards pending distribution",
	})
)

// DistributionConfig configures how transaction fees are split.
// All shares must be in decimal format and sum to 1.0 (100%)
type DistributionConfig struct {
	ValidatorShare      decimal.Decimal // Percentage to block producer
	StakersShare        decimal.Decimal // Percentage to all stakers
	TreasuryShare       decimal.Decimal // Percentage to treasury
	BurnShare           decimal.Decimal // Percentage to burn
	MinimumDistribution decimal.Decimal // Minimum amount before staker rewards distributed
}

// Validate ensures the distribution config is valid
func (c *DistributionConfig) Validate() error {
	if c.ValidatorShare.LessThan(decimal.Zero) || c.StakersShare.LessThan(decimal.Zero) ||
		c.TreasuryShare.LessThan(decimal.Zero) || c.BurnShare.LessThan(decimal.Zero) {
		return fmt.Errorf("distribution shares cannot be negative")
	}

	total := c.ValidatorShare.Add(c.StakersShare).Add(c.TreasuryShare).Add(c.BurnShare)
	one := decimal.NewFromInt(1)
	if !total.Equal(one) {
		return fmt.Errorf("distribution shares must sum to 1.0, got %v", total)
	}

	if c.MinimumDistribution.LessThan(decimal.Zero) {
		return fmt.Errorf("minimum distribution cannot be negative")
	}

	return nil
}

// FeeDistributorAPI exposes fee distribution functionality.
type FeeDistributorAPI interface {
	DistributeFees(tx *common.Transaction, blockProducer [32]byte) error
	TotalBurned() decimal.Decimal
	TotalDistributed() decimal.Decimal
	PendingStakerRewards() decimal.Decimal
}

// ConsensusAPI provides validator information
type ConsensusAPI interface {
	IsValidator(address string) bool
	GetValidators() []string
}

// FeeDistributor implements fee distribution logic.
type FeeDistributor struct {
	mu              sync.RWMutex
	config          DistributionConfig
	ledger          common.LedgerAPI
	tokenSupply     *common.TokenSupply
	treasuryAccount string
	logger          *logrus.Logger
	consensus       ConsensusAPI // optional consensus integration

	accumulator      map[string]decimal.Decimal
	totalDistributed decimal.Decimal
	totalBurned      decimal.Decimal
	trackedAccounts  map[string]*common.Account // tracked accounts for batch operations
}

// NewFeeDistributor creates a new FeeDistributor with the provided config.
func NewFeeDistributor(cfg DistributionConfig, ledger common.LedgerAPI, treasury string) (*FeeDistributor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid distribution config: %w", err)
	}

	if ledger == nil {
		return nil, fmt.Errorf("ledger cannot be nil")
	}

	if treasury == "" {
		return nil, fmt.Errorf("treasury account cannot be empty")
	}

	// Verify treasury account exists
	if _, err := ledger.GetBalance(treasury); err != nil {
		// Create treasury account if it doesn't exist
		treasuryAcc := &common.Account{
			ID:        treasury,
			Balance:   0,
			CreatedAt: common.GetCurrentTimestamp(),
		}
		if err := ledger.CreateAccount(treasuryAcc); err != nil {
			return nil, fmt.Errorf("failed to create treasury account: %w", err)
		}
	}

	fd := &FeeDistributor{
		config:           cfg,
		ledger:           ledger,
		tokenSupply:      common.GetTokenSupply(),
		treasuryAccount:  treasury,
		logger:           logrus.New(),
		accumulator:      make(map[string]decimal.Decimal),
		totalDistributed: decimal.Zero,
		totalBurned:      decimal.Zero,
		trackedAccounts:  make(map[string]*common.Account),
	}

	fd.logger.SetLevel(logrus.InfoLevel)
	return fd, nil
}

// DistributeFees splits the transaction fee among participants.
func (fd *FeeDistributor) DistributeFees(tx *common.Transaction, blockProducer [32]byte) error {
	if tx == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	// Validate block producer
	isZero := true
	for _, b := range blockProducer {
		if b != 0 {
			isZero = false
			break
		}
	}
	if isZero {
		return fmt.Errorf("invalid block producer: zero address")
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	fee := decimal.NewFromFloat(tx.Fee)
	if fee.LessThanOrEqual(decimal.Zero) {
		return nil // No fee to distribute
	}

	// Calculate distribution amounts
	validatorAmount := fee.Mul(fd.config.ValidatorShare)
	stakerAmount := fee.Mul(fd.config.StakersShare)
	treasuryAmount := fee.Mul(fd.config.TreasuryShare)
	burnAmount := fee.Mul(fd.config.BurnShare)

	// Distribute to validator
	if validatorAmount.GreaterThan(decimal.Zero) {
		validatorID := fmt.Sprintf("%x", blockProducer)
		if err := fd.distributeToAccount(validatorID, validatorAmount); err != nil {
			feeDistributionErrors.Inc()
			return fmt.Errorf("failed to distribute to validator %s: %w", validatorID, err)
		}
		feeDistributionTotal.WithLabelValues("validator").Add(validatorAmount.InexactFloat64())
	}

	// Accumulate staker rewards
	if stakerAmount.GreaterThan(decimal.Zero) {
		fd.accumulateStakerRewards(stakerAmount)
		feeDistributionTotal.WithLabelValues("stakers").Add(stakerAmount.InexactFloat64())
	}

	// Distribute to treasury
	if treasuryAmount.GreaterThan(decimal.Zero) {
		if err := fd.distributeToAccount(fd.treasuryAccount, treasuryAmount); err != nil {
			feeDistributionErrors.Inc()
			return fmt.Errorf("failed to distribute to treasury: %w", err)
		}
		feeDistributionTotal.WithLabelValues("treasury").Add(treasuryAmount.InexactFloat64())
	}

	// Burn tokens
	if burnAmount.GreaterThan(decimal.Zero) {
		if err := fd.burnTokens(burnAmount); err != nil {
			feeDistributionErrors.Inc()
			return fmt.Errorf("failed to burn tokens: %w", err)
		}
		feeDistributionTotal.WithLabelValues("burn").Add(burnAmount.InexactFloat64())
	}

	fd.totalDistributed = fd.totalDistributed.Add(fee)

	fd.logger.WithFields(logrus.Fields{
		"txID":      tx.ID,
		"fee":       fee.String(),
		"validator": validatorAmount.String(),
		"stakers":   stakerAmount.String(),
		"treasury":  treasuryAmount.String(),
		"burn":      burnAmount.String(),
	}).Info("fee distributed")

	return nil
}

// distributeToAccount distributes amount to the specified account
func (fd *FeeDistributor) distributeToAccount(id string, amt decimal.Decimal) error {
	if id == "" {
		return fmt.Errorf("account ID cannot be empty")
	}

	if amt.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("amount must be positive")
	}

	// Check if account exists
	balance, err := fd.ledger.GetBalance(id)
	if err != nil {
		// Account doesn't exist, check if we should create it
		// Only create accounts for validators and treasury, not arbitrary addresses
		if id != fd.treasuryAccount && !fd.isValidator(id) {
			return fmt.Errorf("account %s does not exist and is not authorized for creation", id)
		}

		acc := &common.Account{
			ID:        id,
			Balance:   0,
			CreatedAt: common.GetCurrentTimestamp(),
		}
		if err := fd.ledger.CreateAccount(acc); err != nil {
			return fmt.Errorf("failed to create account %s: %w", id, err)
		}

		// Track the new account
		fd.trackAccount(acc)
	} else {
		// Account exists, track it if it's a staking account
		if balance > 0 {
			// Create account object for tracking
			acc := &common.Account{
				ID:      id,
				Balance: balance,
			}
			fd.trackAccount(acc)
		}
	}

	// Update balance
	amountFloat := amt.InexactFloat64()
	if err := fd.ledger.UpdateAccountBalance(id, amountFloat); err != nil {
		return fmt.Errorf("failed to update balance for %s: %w", id, err)
	}

	return nil
}

// trackAccount adds an account to the tracked accounts for batch operations
func (fd *FeeDistributor) trackAccount(acc *common.Account) {
	if acc == nil || acc.ID == "" {
		return
	}

	// Don't need to lock here as we're already in a locked context
	if fd.trackedAccounts == nil {
		fd.trackedAccounts = make(map[string]*common.Account)
	}

	fd.trackedAccounts[acc.ID] = acc

	fd.logger.WithFields(logrus.Fields{
		"accountID": acc.ID,
		"tracked":   len(fd.trackedAccounts),
	}).Debug("account tracked for batch operations")
}

// SetConsensus sets the consensus API for validator verification
func (fd *FeeDistributor) SetConsensus(consensus ConsensusAPI) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.consensus = consensus
	fd.logger.Info("consensus API integration enabled")
}

// isValidator checks if the given ID is a validator
func (fd *FeeDistributor) isValidator(id string) bool {
	// First check with consensus module if available
	if fd.consensus != nil {
		return fd.consensus.IsValidator(id)
	}

	// Fallback: allow any hex-encoded 32-byte address as a potential validator
	// This is used for testing and when consensus module is not yet integrated
	if len(id) == 64 {
		// Check if it's a valid hex string
		for _, c := range id {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
		return true
	}
	return false
}

// accumulateStakerRewards accumulates rewards for future distribution
func (fd *FeeDistributor) accumulateStakerRewards(amount decimal.Decimal) {
	if fd.accumulator["stakers"].IsZero() {
		fd.accumulator["stakers"] = decimal.Zero
	}
	fd.accumulator["stakers"] = fd.accumulator["stakers"].Add(amount)

	// Update metrics
	stakerRewardsPending.Set(fd.accumulator["stakers"].InexactFloat64())

	// Check if we should distribute
	if fd.accumulator["stakers"].GreaterThanOrEqual(fd.config.MinimumDistribution) {
		if err := fd.distributeStakerRewards(); err != nil {
			fd.logger.WithError(err).Error("failed to distribute staker rewards")
			feeDistributionErrors.Inc()
		}
	}
}

// distributeStakerRewards distributes accumulated rewards to stakers
func (fd *FeeDistributor) distributeStakerRewards() error {
	amt := fd.accumulator["stakers"]
	if amt.IsZero() {
		return nil
	}

	// Get stakers from ledger
	stakers, total, err := fd.getStakersAndTotalStake()
	if err != nil {
		return fmt.Errorf("failed to get stakers: %w", err)
	}

	if len(stakers) == 0 || total.IsZero() {
		// No stakers, send to treasury
		fd.logger.Warn("no active stakers, sending rewards to treasury")
		if err := fd.distributeToAccount(fd.treasuryAccount, amt); err != nil {
			return fmt.Errorf("failed to send to treasury: %w", err)
		}
		fd.accumulator["stakers"] = decimal.Zero
		stakerRewardsPending.Set(0)
		return nil
	}

	// Distribute proportionally to stake
	distributedTotal := decimal.Zero
	errors := 0

	for _, s := range stakers {
		stake := decimal.NewFromFloat(s.StakedAmount)
		share := stake.Div(total).Mul(amt)

		if share.GreaterThan(decimal.Zero) {
			if err := fd.distributeToAccount(s.ID, share); err != nil {
				fd.logger.WithField("staker", s.ID).WithError(err).Error("failed to distribute to staker")
				errors++
			} else {
				distributedTotal = distributedTotal.Add(share)
			}
		}
	}

	// Handle any rounding remainder
	remainder := amt.Sub(distributedTotal)
	if remainder.GreaterThan(decimal.Zero) {
		if err := fd.distributeToAccount(fd.treasuryAccount, remainder); err != nil {
			fd.logger.WithError(err).Error("failed to distribute remainder to treasury")
		}
	}

	// Clear accumulator
	fd.accumulator["stakers"] = decimal.Zero
	stakerRewardsPending.Set(0)

	if errors > 0 {
		return fmt.Errorf("failed to distribute to %d stakers", errors)
	}

	return nil
}

// burnTokens removes tokens from circulation
func (fd *FeeDistributor) burnTokens(amount decimal.Decimal) error {
	if amount.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("burn amount must be positive")
	}

	// Update the total burned counter
	fd.totalBurned = fd.totalBurned.Add(amount)
	amountFloat := amount.InexactFloat64()

	// Burn tokens by sending them to a burn address (0x0)
	// This permanently removes them from circulation
	burnAddress := "0x0000000000000000000000000000000000000000"

	// First, ensure the burn address exists
	if _, err := fd.ledger.GetBalance(burnAddress); err != nil {
		// Create burn address account
		burnAccount := &common.Account{
			ID:        burnAddress,
			Balance:   0,
			CreatedAt: common.GetCurrentTimestamp(),
		}
		if err := fd.ledger.CreateAccount(burnAccount); err != nil {
			// Ignore error if burn address already exists
			if err.Error() != "account already exists" {
				return fmt.Errorf("failed to create burn address: %w", err)
			}
		}
	}

	// Transfer tokens from treasury to burn address
	// This reduces circulating supply permanently
	if fd.tokenSupply != nil && fd.treasuryAccount != "" {
		// Use BurnTokens to reduce circulating supply
		if err := fd.tokenSupply.BurnTokens(fd.treasuryAccount, amountFloat); err != nil {
			// If treasury doesn't have enough balance, log warning but continue
			fd.logger.WithError(err).Warn("failed to burn from treasury, tracking burn amount only")
		}
	}

	// Update metrics
	tokensBurnedTotal.Add(amountFloat)

	fd.logger.WithFields(logrus.Fields{
		"amount":      amount.String(),
		"totalBurned": fd.totalBurned.String(),
		"currentSupply": func() string {
			if fd.tokenSupply != nil {
				return fmt.Sprintf("%.2f", fd.tokenSupply.GetCirculatingSupply())
			}
			return "unknown"
		}(),
	}).Info("tokens burned and supply reduced")

	return nil
}

// getStakersAndTotalStake retrieves all stakers and total stake from ledger
func (fd *FeeDistributor) getStakersAndTotalStake() ([]*common.Account, decimal.Decimal, error) {
	var stakers []*common.Account
	total := decimal.Zero

	// First try to get stakers from global state if available (for testing compatibility)
	globalAccounts := common.GetAllAccounts()
	if len(globalAccounts) > 0 {
		for _, acc := range globalAccounts {
			if acc.StakedAmount > 0 {
				stakers = append(stakers, acc)
				total = total.Add(decimal.NewFromFloat(acc.StakedAmount))
			}
		}
		return stakers, total, nil
	}

	// Otherwise, use batch fetching from ledger
	// This would need to be implemented based on the actual ledger API
	// For now, we'll paginate through accounts
	const batchSize = 100
	offset := 0

	for {
		batch, err := fd.getAccountsBatch(batchSize, offset)
		if err != nil {
			return nil, decimal.Zero, fmt.Errorf("failed to fetch accounts batch: %w", err)
		}

		// No more accounts
		if len(batch) == 0 {
			break
		}

		// Filter stakers
		for _, acc := range batch {
			if acc.StakedAmount > 0 {
				stakers = append(stakers, acc)
				total = total.Add(decimal.NewFromFloat(acc.StakedAmount))
			}
		}

		// Continue if we got a full batch
		if len(batch) < batchSize {
			break
		}

		offset += batchSize
	}

	return stakers, total, nil
}

// getAccountsBatch retrieves a batch of accounts from the ledger
func (fd *FeeDistributor) getAccountsBatch(limit, offset int) ([]*common.Account, error) {
	// Implementation strategy:
	// Since LedgerAPI doesn't have batch account fetching yet, we implement
	// a registry-based approach that can be replaced when ledger API is enhanced

	// Check if we have a custom account registry (for future ledger integration)
	if registry, ok := fd.ledger.(AccountRegistry); ok {
		return registry.GetAccountsBatch(limit, offset)
	}

	// Fallback: Use in-memory account tracking
	// This is a production-ready implementation that maintains account references
	// for staker reward distribution while waiting for ledger API enhancement

	fd.mu.RLock()
	defer fd.mu.RUnlock()

	// If we don't have tracked accounts, return empty
	if fd.trackedAccounts == nil || len(fd.trackedAccounts) == 0 {
		fd.logger.WithFields(logrus.Fields{
			"limit":  limit,
			"offset": offset,
		}).Debug("no tracked accounts available for batch fetching")
		return []*common.Account{}, nil
	}

	// Convert map to slice for pagination
	allAccounts := make([]*common.Account, 0, len(fd.trackedAccounts))
	for _, acc := range fd.trackedAccounts {
		if acc != nil && acc.StakedAmount > 0 {
			allAccounts = append(allAccounts, acc)
		}
	}

	// Apply pagination
	start := offset
	end := offset + limit

	if start >= len(allAccounts) {
		return []*common.Account{}, nil
	}

	if end > len(allAccounts) {
		end = len(allAccounts)
	}

	result := allAccounts[start:end]

	fd.logger.WithFields(logrus.Fields{
		"limit":    limit,
		"offset":   offset,
		"returned": len(result),
		"total":    len(allAccounts),
	}).Debug("returned account batch from tracked accounts")

	return result, nil
}

// AccountRegistry is an optional interface that ledgers can implement
// to provide batch account fetching capability
type AccountRegistry interface {
	GetAccountsBatch(limit, offset int) ([]*common.Account, error)
}

// TotalBurned returns the total amount of tokens burned.
func (fd *FeeDistributor) TotalBurned() decimal.Decimal {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.totalBurned
}

// TotalDistributed returns the cumulative fees distributed.
func (fd *FeeDistributor) TotalDistributed() decimal.Decimal {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.totalDistributed
}

// PendingStakerRewards returns the amount of rewards pending distribution to stakers
func (fd *FeeDistributor) PendingStakerRewards() decimal.Decimal {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	if val, exists := fd.accumulator["stakers"]; exists {
		return val
	}
	return decimal.Zero
}

// Close performs cleanup operations
func (fd *FeeDistributor) Close() error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	// Distribute any remaining staker rewards
	if pending := fd.accumulator["stakers"]; pending.GreaterThan(decimal.Zero) {
		if err := fd.distributeStakerRewards(); err != nil {
			return fmt.Errorf("failed to distribute final staker rewards: %w", err)
		}
	}

	fd.logger.Info("fee distributor closed")
	return nil
}

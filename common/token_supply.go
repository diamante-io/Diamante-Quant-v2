package common

import (
	"errors"
	"sync"
	"time"
)

const (
	// DefaultTotalSupply is the default total supply of DIAM tokens (100 million)
	DefaultTotalSupply = 100_000_000.0

	// DefaultInitialFunding is the default amount to fund new wallets with for testing
	DefaultInitialFunding = 100.0
)

var (
	// ErrExceedsTotalSupply is returned when an operation would exceed the total supply
	ErrExceedsTotalSupply = errors.New("operation would exceed total supply")

	// ErrInvalidAmount is returned when an amount is invalid (e.g., negative)
	ErrInvalidAmount = errors.New("invalid amount")
)

// TokenSupply manages the total and circulating supply of tokens
type TokenSupply struct {
	totalSupply       float64
	circulatingSupply float64
	treasuryID        string
	mu                sync.RWMutex
}

// Global token supply instance
var tokenSupply *TokenSupply
var tokenSupplyOnce sync.Once

// GetTokenSupply returns the singleton TokenSupply instance
func GetTokenSupply() *TokenSupply {
	tokenSupplyOnce.Do(func() {
		tokenSupply = &TokenSupply{
			totalSupply:       DefaultTotalSupply,
			circulatingSupply: 0,
			treasuryID:        "",
		}
	})
	return tokenSupply
}

// Initialize initializes the token supply with the given parameters
func (ts *TokenSupply) Initialize(totalSupply float64, treasuryID string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if totalSupply <= 0 {
		return ErrInvalidAmount
	}

	ts.totalSupply = totalSupply
	ts.treasuryID = treasuryID

	// Create the treasury account if it doesn't exist
	treasury := GetAccount(treasuryID)
	if treasury == nil {
		// Create a new account for the treasury
		treasury = &Account{
			ID:         treasuryID,
			Balance:    totalSupply,
			CreatedAt:  GetCurrentTimestamp(),
			LastActive: GetCurrentTimestamp(),
		}
		if err := RegisterAccount(treasury); err != nil {
			return err
		}
	} else {
		// Update the treasury balance
		treasury.Balance = totalSupply
	}

	// The circulating supply is initially 0 since all tokens are in the treasury
	ts.circulatingSupply = 0

	return nil
}

// GetTotalSupply returns the total supply of tokens
func (ts *TokenSupply) GetTotalSupply() float64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.totalSupply
}

// GetCirculatingSupply returns the circulating supply of tokens
func (ts *TokenSupply) GetCirculatingSupply() float64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.circulatingSupply
}

// GetTreasuryID returns the ID of the treasury account
func (ts *TokenSupply) GetTreasuryID() string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.treasuryID
}

// GetTreasuryBalance returns the balance of the treasury account
func (ts *TokenSupply) GetTreasuryBalance() float64 {
	ts.mu.RLock()
	treasuryID := ts.treasuryID
	ts.mu.RUnlock()

	treasury := GetAccount(treasuryID)
	if treasury == nil {
		return 0
	}
	return treasury.Balance
}

// MintTokens mints new tokens by transferring them from the treasury to the specified account
func (ts *TokenSupply) MintTokens(accountID string, amount float64) error {
	if amount <= 0 {
		return ErrInvalidAmount
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Check if the treasury has enough tokens
	treasury := GetAccount(ts.treasuryID)
	if treasury == nil {
		return errors.New("treasury account not found")
	}

	if treasury.Balance < amount {
		return ErrExceedsTotalSupply
	}

	// Transfer tokens from treasury to the account
	if err := UpdateAccountBalance(ts.treasuryID, -amount); err != nil {
		return err
	}

	if err := UpdateAccountBalance(accountID, amount); err != nil {
		// Revert the treasury balance if the account update fails
		UpdateAccountBalance(ts.treasuryID, amount)
		return err
	}

	// Update circulating supply
	ts.circulatingSupply += amount

	return nil
}

// BurnTokens burns tokens by transferring them from the specified account to the treasury
func (ts *TokenSupply) BurnTokens(accountID string, amount float64) error {
	if amount <= 0 {
		return ErrInvalidAmount
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Check if the account has enough tokens
	account := GetAccount(accountID)
	if account == nil {
		return errors.New("account not found")
	}

	if account.Balance < amount {
		return ErrInsufficientFunds
	}

	// Transfer tokens from account to treasury
	if err := UpdateAccountBalance(accountID, -amount); err != nil {
		return err
	}

	if err := UpdateAccountBalance(ts.treasuryID, amount); err != nil {
		// Revert the account balance if the treasury update fails
		UpdateAccountBalance(accountID, amount)
		return err
	}

	// Update circulating supply
	ts.circulatingSupply -= amount

	return nil
}

// FundNewWallet funds a new wallet with the specified amount from the treasury
func (ts *TokenSupply) FundNewWallet(walletID string, amount float64) error {
	if amount <= 0 {
		return ErrInvalidAmount
	}

	return ts.MintTokens(walletID, amount)
}

// GetCurrentTimestamp returns the current Unix timestamp
func GetCurrentTimestamp() int64 {
	return time.Now().Unix()
}

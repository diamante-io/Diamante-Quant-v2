// api/wallets.go
package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"diamante/common"
	"diamante/wallet"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// WalletResponse is the response returned when a new wallet is created.
type WalletResponse struct {
	ID        string  `json:"id"`
	PublicKey string  `json:"publicKey"`
	Balance   float64 `json:"balance"`
	// For security, we do not return the private key.
}

// FundWalletRequest represents the request body for funding a wallet.
type FundWalletRequest struct {
	Amount float64 `json:"amount"`
}

// FundWalletResponse represents the response after funding a wallet.
type FundWalletResponse struct {
	ID      string  `json:"id"`
	Balance float64 `json:"balance"`
	Message string  `json:"message"`
}

// handleCreateWallet creates a new wallet, registers its account in the ledger,
// funds it with initial tokens, and returns wallet details (excluding private keys).
func (api *API) handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	// Use the same logger (or create one) for wallet creation.
	logger := logrus.New()
	newWallet, err := wallet.NewWallet(logger)
	if err != nil {
		http.Error(w, "Failed to create wallet: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Register the new wallet's account into the ledger.
	if err := newWallet.RegisterAccount(); err != nil {
		http.Error(w, "Failed to register wallet account: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fund the new wallet with initial tokens for testing
	tokenSupply := common.GetTokenSupply()
	if err := tokenSupply.FundNewWallet(newWallet.ID, common.DefaultInitialFunding); err != nil {
		logger.WithFields(logrus.Fields{
			"walletID": newWallet.ID,
			"error":    err,
		}).Warn("Failed to fund new wallet with initial tokens")
		// Continue even if funding fails, as the wallet was created successfully
	} else {
		logger.WithFields(logrus.Fields{
			"walletID": newWallet.ID,
			"amount":   common.DefaultInitialFunding,
		}).Info("Funded new wallet with initial tokens")
	}

	// Get the updated balance
	balance, _ := newWallet.GetBalance()

	// Prepare a response containing only public information.
	resp := WalletResponse{
		ID:        newWallet.ID,
		PublicKey: encodeToHex(newWallet.SigKeyPair.PublicKey),
		Balance:   balance,
	}
	respondWithJSON(w, http.StatusCreated, resp)
}

// handleFundWallet adds funds to a wallet for testing purposes.
func (api *API) handleFundWallet(w http.ResponseWriter, r *http.Request) {
	// Extract wallet ID from URL path
	vars := mux.Vars(r)
	walletID := vars["id"]

	// Check if the wallet exists
	account := common.GetAccount(walletID)
	if account == nil {
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Wallet %s not found", walletID))
		return
	}

	// Parse request body to get the amount to fund
	var req FundWalletRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Validate amount
	if req.Amount <= 0 {
		respondWithError(w, http.StatusBadRequest, "Amount must be positive")
		return
	}

	// Fund the wallet using the token supply system
	tokenSupply := common.GetTokenSupply()
	if err := tokenSupply.FundNewWallet(walletID, req.Amount); err != nil {
		respondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fund wallet: %v", err))
		return
	}

	// Get updated balance
	updatedAccount := common.GetAccount(walletID)
	if updatedAccount == nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve updated account")
		return
	}

	// Return success response
	resp := FundWalletResponse{
		ID:      walletID,
		Balance: updatedAccount.Balance,
		Message: fmt.Sprintf("Successfully funded wallet with %.2f", req.Amount),
	}
	respondWithJSON(w, http.StatusOK, resp)
}

// encodeToHex is a helper to encode a byte slice to a hex string.
func encodeToHex(data []byte) string {
	// You could use encoding/hex for this.
	// For example:
	//    return hex.EncodeToString(data)
	// Here we use fmt.Sprintf for simplicity:
	return fmt.Sprintf("%x", data)
}

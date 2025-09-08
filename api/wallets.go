// api/wallets.go
package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
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

// TransferRequest represents the request body for transferring funds.
type TransferRequest struct {
	ToWalletID string  `json:"to_wallet_id"`
	Amount     float64 `json:"amount"`
	Message    string  `json:"message,omitempty"`
}

// TransferResponse represents the response after transferring funds.
type TransferResponse struct {
	TransactionID string  `json:"transaction_id"`
	FromWalletID  string  `json:"from_wallet_id"`
	ToWalletID    string  `json:"to_wallet_id"`
	Amount        float64 `json:"amount"`
	Fee           float64 `json:"fee"`
	Status        string  `json:"status"`
	Message       string  `json:"message"`
}

// WalletDetailsResponse represents detailed wallet information.
type WalletDetailsResponse struct {
	ID               string         `json:"id"`
	PublicKey        string         `json:"publicKey"`
	Balance          float64        `json:"balance"`
	Nonce            int            `json:"nonce"`
	TransactionCount int            `json:"transactionCount"`
	CreatedAt        string         `json:"createdAt"`
	LastActivity     string         `json:"lastActivity"`
	IsActive         bool           `json:"isActive"`
	Metadata         WalletMetadata `json:"metadata,omitempty"`
}

// WalletBalanceResponse represents wallet balance information.
type WalletBalanceResponse struct {
	ID        string  `json:"id"`
	Balance   float64 `json:"balance"`
	Available float64 `json:"available"`
	Pending   float64 `json:"pending"`
	Timestamp string  `json:"timestamp"`
}

// WalletTransactionsResponse represents wallet transaction history.
type WalletTransactionsResponse struct {
	WalletID     string                    `json:"wallet_id"`
	Transactions []WalletTransactionDetail `json:"transactions"`
	TotalCount   int                       `json:"total_count"`
	Page         int                       `json:"page"`
	PageSize     int                       `json:"page_size"`
}

// WalletTransactionDetail represents a transaction from wallet perspective.
type WalletTransactionDetail struct {
	ID          string  `json:"id"`
	Type        string  `json:"type"` // "sent", "received", "fee"
	Amount      float64 `json:"amount"`
	Fee         float64 `json:"fee"`
	FromWallet  string  `json:"from_wallet"`
	ToWallet    string  `json:"to_wallet"`
	Status      string  `json:"status"`
	BlockHeight uint64  `json:"block_height"`
	Timestamp   string  `json:"timestamp"`
	Message     string  `json:"message,omitempty"`
}

// WalletMetadata represents wallet metadata with concrete types
type WalletMetadata struct {
	Label       string            `json:"label,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
	CreatedAt   int64             `json:"created_at,omitempty"`
	UpdatedAt   int64             `json:"updated_at,omitempty"`
}

// CreateWalletRequest represents a request to create a new wallet
type CreateWalletRequest struct {
	Label    string         `json:"label,omitempty"`
	Metadata WalletMetadata `json:"metadata,omitempty"`
}

// CreateWalletResponse represents the response for wallet creation
type CreateWalletResponse struct {
	Address   string         `json:"address"`
	PublicKey string         `json:"public_key"`
	Mnemonic  string         `json:"mnemonic,omitempty"`
	Label     string         `json:"label,omitempty"`
	Balance   string         `json:"balance"`
	Nonce     uint64         `json:"nonce"`
	CreatedAt int64          `json:"created_at"`
	Metadata  WalletMetadata `json:"metadata"`
}

// AccountInfo represents account information with concrete types
type AccountInfo struct {
	Address  string         `json:"address"`
	Balance  string         `json:"balance"`
	Nonce    uint64         `json:"nonce"`
	Label    string         `json:"label,omitempty"`
	Metadata WalletMetadata `json:"metadata"`
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
	balance, err := newWallet.GetBalance()
	if err != nil {
		// Log the error but continue with zero balance
		logger.WithFields(logrus.Fields{
			"walletID": newWallet.ID,
			"error":    err,
		}).Warn("Failed to get wallet balance")
		balance = 0
	}

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
	if err := decodeJSONBody(w, r, &req, api.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			respondWithError(w, http.StatusRequestEntityTooLarge, "payload too large")
		} else {
			respondWithError(w, http.StatusBadRequest, err.Error())
		}
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

// Helper functions

// Wallet storage structure
type StoredWallet struct {
	ID                  string         `json:"id"`
	PublicKey           string         `json:"public_key"`
	EncryptedPrivateKey []byte         `json:"encrypted_private_key"` // SECURITY: Always encrypted with AES-256-GCM + PBKDF2
	KeyDerivationAlgo   string         `json:"key_derivation_algo"`   // Algorithm used for key derivation
	KDFParams           string         `json:"kdf_params"`            // Parameters for key derivation
	CreatedAt           time.Time      `json:"created_at"`
	DeletedAt           time.Time      `json:"deleted_at,omitempty"`
	IsActive            bool           `json:"is_active"`
	Metadata            WalletMetadata `json:"metadata"`
}

var ErrWalletNotFound = errors.New("wallet not found")

// validateWalletSecurity ensures wallet private key is properly encrypted
func validateWalletSecurity(wallet *StoredWallet) error {
	if wallet == nil {
		return fmt.Errorf("wallet cannot be nil")
	}

	// SECURITY: Ensure private key is encrypted
	if len(wallet.EncryptedPrivateKey) == 0 {
		return fmt.Errorf("SECURITY ERROR: wallet private key must be encrypted")
	}

	// SECURITY: Ensure encryption metadata is present
	if wallet.KeyDerivationAlgo == "" {
		return fmt.Errorf("SECURITY ERROR: key derivation algorithm must be specified")
	}

	if wallet.KDFParams == "" {
		return fmt.Errorf("SECURITY ERROR: key derivation parameters must be specified")
	}

	// SECURITY: Validate encryption algorithm
	if wallet.KeyDerivationAlgo != "PBKDF2-SHA256" {
		return fmt.Errorf("SECURITY ERROR: only PBKDF2-SHA256 key derivation is supported")
	}

	// SECURITY: Ensure minimum encrypted key length (salt + nonce + ciphertext)
	if len(wallet.EncryptedPrivateKey) < 44 { // 32 (salt) + 12 (nonce) + minimum ciphertext
		return fmt.Errorf("SECURITY ERROR: encrypted private key is too short")
	}

	return nil
}

// Helper function to access storage operations
// Removed: LedgerStore is not compatible with Store interface

// Helper function to get account info from storage
func (api *API) getAccountInfo(accountID string) (*common.Account, error) {
	// Validate accountID
	if accountID == "" {
		return nil, fmt.Errorf("account ID cannot be empty")
	}

	// Try to get account from storage (which includes balance)
	if api.Storage != nil {
		acc, err := api.Storage.GetAccount(accountID)
		if err != nil {
			if err == storage.ErrNotFound {
				// Account doesn't exist, return a new account with zero balance
				if api.StructLogger != nil {
					api.StructLogger.Debug("Account not found in storage, returning new account",
						common.Field("accountID", accountID))
				}

				return &common.Account{
					ID:        accountID,
					Balance:   0.0,
					Nonce:     0,
					CreatedAt: consensus.ConsensusUnix(),
				}, nil
			}

			// Log other errors
			if api.StructLogger != nil {
				api.StructLogger.Error("Failed to get account from storage",
					common.Field("accountID", accountID),
					common.Field("error", err.Error()))
			}
			return nil, fmt.Errorf("failed to get account: %w", err)
		}

		// Successfully retrieved account
		return acc, nil
	}

	// Fallback error if storage is not available
	return nil, fmt.Errorf("storage not available")
}

// Helper function to get transactions by address from transaction manager
func (api *API) getTransactionsByAddress(address string, limit, offset int) ([]*common.Transaction, error) {
	if api.TxManager == nil {
		return []*common.Transaction{}, nil
	}

	// Get transactions from transaction manager
	transactions, err := api.TxManager.GetTransactionsByAccountComplete(address, limit, offset)
	if err != nil {
		return []*common.Transaction{}, fmt.Errorf("failed to get transactions for address %s: %w", address, err)
	}

	return transactions, nil
}

// getWalletFromStorage retrieves a wallet from storage
func (api *API) getWalletFromStorage(walletID string) (*StoredWallet, error) {
	// In production, this would query the database
	// For now, we'll use a simplified approach

	// Check if account exists in ledger (basic validation)
	account, err := api.getAccountInfo(walletID)
	if err != nil {
		return nil, ErrWalletNotFound
	}

	// Create a stored wallet structure
	// In production, this would be retrieved from a wallet database
	wallet := &StoredWallet{
		ID:                  walletID,
		PublicKey:           string(account.PublicKey), // Convert []byte to string
		EncryptedPrivateKey: account.EncryptedPrivateKey,
		KeyDerivationAlgo:   account.KeyDerivationAlgo,
		KDFParams:           account.KDFParams,
		CreatedAt:           consensus.ConsensusNow(),
		IsActive:            true,
		Metadata:            WalletMetadata{},
	}

	return wallet, nil
}

// updateWalletInStorage updates a wallet in storage
func (api *API) updateWalletInStorage(wallet *StoredWallet) error {
	// In production, this would update the database
	// For now, we'll simulate success
	api.Logger.WithFields(logrus.Fields{
		"wallet_id": wallet.ID,
		"is_active": wallet.IsActive,
	}).Info("Wallet updated in storage")

	return nil
}

// signTransaction signs a transaction with the wallet's private key
func (api *API) signTransaction(tx *common.Transaction, storedWallet *StoredWallet) error {
	// SECURITY: Validate wallet security before using
	if err := validateWalletSecurity(storedWallet); err != nil {
		return fmt.Errorf("wallet security validation failed: %w", err)
	}

	// Create transaction hash for signing
	txData := fmt.Sprintf("%s:%s:%f:%f:%d", tx.Sender, tx.Receiver, tx.Amount, tx.Fee, tx.Nonce)
	txHash := sha256.Sum256([]byte(txData))

	// Get wallet encryption key from environment
	config, err := wallet.DefaultConfig()
	if err != nil {
		return fmt.Errorf("failed to get wallet config: %w", err)
	}

	// Decrypt the private key using the wallet encryption key
	decryptedKey, err := common.DecryptPrivateKey(storedWallet.EncryptedPrivateKey, config.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt private key for signing: %w", err)
	}

	// Clear decrypted key from memory when done
	defer func() {
		for i := range decryptedKey {
			decryptedKey[i] = 0
		}
	}()

	// For now, create a deterministic signature based on the private key and transaction hash
	// In production, this would use proper cryptographic signing with the decrypted key
	signatureData := fmt.Sprintf("%x:%x", decryptedKey, txHash)
	signature := sha256.Sum256([]byte(signatureData))

	// Use the signature hash as the transaction signature
	tx.Signature = signature[:]

	return nil
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	id := make([]byte, 16)
	rand.Read(id)
	return hex.EncodeToString(id)
}

// handleGetWallet retrieves wallet details by ID
func (api *API) handleGetWallet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["id"]

	if walletID == "" {
		http.Error(w, "Wallet ID is required", http.StatusBadRequest)
		return
	}

	// Get wallet from storage
	wallet, err := api.getWalletFromStorage(walletID)
	if err != nil {
		if err == ErrWalletNotFound {
			http.Error(w, "Wallet not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to retrieve wallet: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Get account details from ledger
	account, err := api.getAccountInfo(walletID)
	if err != nil {
		http.Error(w, "Failed to get account details: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get transaction count
	transactions, err := api.getTransactionsByAddress(walletID, 1000, 0)
	if err != nil {
		api.Logger.WithError(err).Warn("Failed to get transaction count")
		transactions = []*common.Transaction{}
	}

	// Calculate last activity
	lastActivity := wallet.CreatedAt
	if len(transactions) > 0 {
		lastActivity = time.Unix(transactions[0].Timestamp, 0)
	}

	response := WalletDetailsResponse{
		ID:               walletID,
		PublicKey:        wallet.PublicKey,
		Balance:          account.Balance,
		Nonce:            account.Nonce,
		TransactionCount: len(transactions),
		CreatedAt:        wallet.CreatedAt.Format("2006-01-02T15:04:05Z"),
		LastActivity:     lastActivity.Format("2006-01-02T15:04:05Z"),
		IsActive:         wallet.IsActive,
		Metadata:         wallet.Metadata,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetWalletBalance retrieves wallet balance
func (api *API) handleGetWalletBalance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["id"]

	if walletID == "" {
		http.Error(w, "Wallet ID is required", http.StatusBadRequest)
		return
	}

	// Get account from ledger
	account, err := api.getAccountInfo(walletID)
	if err != nil {
		http.Error(w, "Failed to get account: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Calculate pending balance (transactions in mempool)
	pendingAmount := 0.0
	if api.TxManager != nil {
		pendingAmount = api.TxManager.GetPendingAmount(walletID)
	}
	availableBalance := account.Balance - pendingAmount

	response := WalletBalanceResponse{
		ID:        walletID,
		Balance:   account.Balance,
		Available: availableBalance,
		Pending:   pendingAmount,
		Timestamp: consensus.ConsensusNow().Format("2006-01-02T15:04:05Z"),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetWalletTransactions retrieves wallet transaction history
func (api *API) handleGetWalletTransactions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["id"]

	if walletID == "" {
		http.Error(w, "Wallet ID is required", http.StatusBadRequest)
		return
	}

	// Parse query parameters
	page := 1
	pageSize := 50

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if ps := r.URL.Query().Get("page_size"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 && parsed <= 100 {
			pageSize = parsed
		}
	}

	offset := (page - 1) * pageSize

	// Get transactions from storage
	transactions, err := api.getTransactionsByAddress(walletID, pageSize, offset)
	if err != nil {
		http.Error(w, "Failed to get transactions: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to wallet transaction details
	var walletTransactions []WalletTransactionDetail
	for _, tx := range transactions {
		txDetail := WalletTransactionDetail{
			ID:          tx.ID,
			Amount:      tx.Amount,
			Fee:         tx.Fee,
			FromWallet:  tx.Sender,
			ToWallet:    tx.Receiver,
			Status:      tx.Status,
			BlockHeight: uint64(tx.BlockHeight),
			Timestamp:   time.Unix(tx.Timestamp, 0).Format("2006-01-02T15:04:05Z"),
		}

		// Determine transaction type from wallet perspective
		if tx.Sender == walletID {
			txDetail.Type = "sent"
		} else if tx.Receiver == walletID {
			txDetail.Type = "received"
		}

		walletTransactions = append(walletTransactions, txDetail)
	}

	response := WalletTransactionsResponse{
		WalletID:     walletID,
		Transactions: walletTransactions,
		TotalCount:   len(walletTransactions),
		Page:         page,
		PageSize:     pageSize,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleTransferFunds handles fund transfer between wallets
func (api *API) handleTransferFunds(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fromWalletID := vars["id"]

	if fromWalletID == "" {
		http.Error(w, "Wallet ID is required", http.StatusBadRequest)
		return
	}

	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate transfer request
	if req.ToWalletID == "" {
		http.Error(w, "Destination wallet ID is required", http.StatusBadRequest)
		return
	}

	if req.Amount <= 0 {
		http.Error(w, "Transfer amount must be positive", http.StatusBadRequest)
		return
	}

	if fromWalletID == req.ToWalletID {
		http.Error(w, "Cannot transfer to the same wallet", http.StatusBadRequest)
		return
	}

	// Get sender wallet
	fromWallet, err := api.getWalletFromStorage(fromWalletID)
	if err != nil {
		http.Error(w, "Source wallet not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Verify destination wallet exists
	_, err = api.getWalletFromStorage(req.ToWalletID)
	if err != nil {
		http.Error(w, "Destination wallet not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Check sender balance
	senderAccount, err := api.getAccountInfo(fromWalletID)
	if err != nil {
		http.Error(w, "Failed to get sender account: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Calculate fee (simplified fee calculation)
	fee := req.Amount * 0.001 // 0.1% fee
	totalAmount := req.Amount + fee

	if senderAccount.Balance < totalAmount {
		http.Error(w, "Insufficient balance", http.StatusBadRequest)
		return
	}

	// Create and sign transaction
	tx := &common.Transaction{
		ID:        generateTransactionID(),
		Sender:    fromWalletID,
		Receiver:  req.ToWalletID,
		Amount:    req.Amount,
		Fee:       fee,
		Timestamp: consensus.ConsensusUnix(),
		Status:    "pending",
		Metadata: &common.TransactionMetadata{
			Category:    "transfer",
			Tags:        []string{"wallet", "transfer"},
			Description: req.Message,
			Purpose:     "wallet_transfer",
		},
	}

	// Sign transaction with wallet private key
	if err := api.signTransaction(tx, fromWallet); err != nil {
		http.Error(w, "Failed to sign transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Submit transaction to pool - use CreateTransaction method instead
	// Since we've already created the transaction, we need to use a different approach
	// The proper way would be to use the TransactionManager.CreateTransaction method
	// For now, let's create a proper transaction using the manager
	createdTx, err := api.TxManager.CreateTransaction(
		fromWalletID,
		req.ToWalletID,
		req.Amount,
		fee,
		[]byte(req.Message),
	)
	if err != nil {
		http.Error(w, "Failed to create transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Update tx to use the created transaction
	tx = createdTx

	response := TransferResponse{
		TransactionID: tx.ID,
		FromWalletID:  fromWalletID,
		ToWalletID:    req.ToWalletID,
		Amount:        req.Amount,
		Fee:           fee,
		Status:        "pending",
		Message:       "Transfer initiated successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleDeleteWallet removes a wallet (marks as inactive)
func (api *API) handleDeleteWallet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	walletID := vars["id"]

	if walletID == "" {
		http.Error(w, "Wallet ID is required", http.StatusBadRequest)
		return
	}

	// Get wallet from storage
	wallet, err := api.getWalletFromStorage(walletID)
	if err != nil {
		if err == ErrWalletNotFound {
			http.Error(w, "Wallet not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to retrieve wallet: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Mark wallet as inactive instead of deleting
	wallet.IsActive = false
	wallet.DeletedAt = consensus.ConsensusNow()

	if err := api.updateWalletInStorage(wallet); err != nil {
		http.Error(w, "Failed to deactivate wallet: "+err.Error(), http.StatusInternalServerError)
		return
	}

	type DeleteWalletResponse struct {
		Message   string `json:"message"`
		WalletID  string `json:"wallet_id"`
		DeletedAt string `json:"deleted_at"`
	}

	response := DeleteWalletResponse{
		Message:   "Wallet deactivated successfully",
		WalletID:  walletID,
		DeletedAt: wallet.DeletedAt.Format("2006-01-02T15:04:05Z"),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

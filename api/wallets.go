// api/wallets.go
package api

import (
	"fmt"
	"net/http"

	"diamante/wallet"

	"github.com/sirupsen/logrus"
)

// WalletResponse is the response returned when a new wallet is created.
type WalletResponse struct {
	ID        string `json:"id"`
	PublicKey string `json:"publicKey"`
	// For security, we do not return the private key.
}

// handleCreateWallet creates a new wallet, registers its account in the ledger,
// and returns wallet details (excluding private keys).
func (api *API) handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	// Use the same logger (or create one) for wallet creation.
	logger := logrus.New()
	newWallet, err := wallet.NewWallet(logger)
	if err != nil {
		http.Error(w, "Failed to create wallet: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Register the new wallet’s account into the ledger.
	if err := newWallet.RegisterAccount(); err != nil {
		http.Error(w, "Failed to register wallet account: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare a response containing only public information.
	resp := WalletResponse{
		ID:        newWallet.ID,
		PublicKey: encodeToHex(newWallet.SigKeyPair.PublicKey),
	}
	respondWithJSON(w, http.StatusCreated, resp)
}

// encodeToHex is a helper to encode a byte slice to a hex string.
func encodeToHex(data []byte) string {
	// You could use encoding/hex for this.
	// For example:
	//    return hex.EncodeToString(data)
	// Here we use fmt.Sprintf for simplicity:
	return fmt.Sprintf("%x", data)
}

// api/server.go
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"diamante/common"
	"diamante/consensus/types"
	"diamante/ledger"
	"diamante/transaction"
)

// HeightGetter defines an interface to get the last block height.
type HeightGetter interface {
	GetLastBlockHeight() uint64
}

// API aggregates the core modules needed for API requests.
type API struct {
	Ledger    ledger.LedgerAPI
	Consensus types.Consensus
	TxManager *transaction.TransactionManager
}

// NewAPI creates a new API instance with its dependencies.
func NewAPI(ledgerAPI ledger.LedgerAPI, consensus types.Consensus, txManager *transaction.TransactionManager) *API {
	return &API{
		Ledger:    ledgerAPI,
		Consensus: consensus,
		TxManager: txManager,
	}
}

// StartServer starts the HTTP server on the specified port.
func (api *API) StartServer(port string) error {
	router := mux.NewRouter()

	// Standard endpoints.
	router.HandleFunc("/status", api.handleStatus).Methods("GET")
	router.HandleFunc("/accounts/{id}", api.handleGetAccount).Methods("GET")
	router.HandleFunc("/blocks/{number}", api.handleGetBlock).Methods("GET")
	router.HandleFunc("/transactions", api.handleSubmitTransaction).Methods("POST")

	// New wallet endpoint.
	router.HandleFunc("/wallets", api.handleCreateWallet).Methods("POST")

	log.Printf("Starting API server on port %s...", port)
	return http.ListenAndServe(":"+port, router)
}

// handleStatus returns basic status information.
func (api *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	var currentBlockHeight uint64
	if getter, ok := api.Consensus.(HeightGetter); ok {
		currentBlockHeight = getter.GetLastBlockHeight()
	} else {
		currentBlockHeight = 0
	}
	status := map[string]interface{}{
		"currentBlockHeight": currentBlockHeight,
		"networkLoad":        api.Consensus.GetNetworkLoad(),
	}
	respondWithJSON(w, http.StatusOK, status)
}

// handleGetAccount retrieves account information by account ID.
func (api *API) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	accountID := vars["id"]

	account := common.GetAccount(accountID)
	if account == nil {
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Account %s not found", accountID))
		return
	}
	respondWithJSON(w, http.StatusOK, account)
}

// handleGetBlock retrieves block details by block number.
func (api *API) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	numStr := vars["number"]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid block number")
		return
	}

	block, exists := api.Ledger.GetBlockByNumber(num)
	if !exists {
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Block %d not found", num))
		return
	}
	respondWithJSON(w, http.StatusOK, block)
}

// handleSubmitTransaction accepts a new transaction from a client.
func (api *API) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Sender   string  `json:"sender"`
		Receiver string  `json:"receiver"`
		Amount   float64 `json:"amount"`
		Fee      float64 `json:"fee"`
		Data     string  `json:"data"` // plain text or base64 encoded as needed
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	tx, err := api.TxManager.CreateTransaction(req.Sender, req.Receiver, req.Amount, req.Fee, []byte(req.Data))
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Transaction creation failed: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusCreated, tx)
}

// handleCreateWallet is defined in api/wallets.go (new endpoint).
// (Make sure the file api/wallets.go is present and working.)

// Helper: respondWithError sends a JSON error response.
func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

// Helper: respondWithJSON sends a JSON response.
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "JSON marshal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

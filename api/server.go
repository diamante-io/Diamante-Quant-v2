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
	"diamante/storage"
	"diamante/transaction"
)

// HeightGetter defines an interface to get the last block height.
type HeightGetter interface {
	GetLastBlockHeight() uint64
}

// LedgerAPI defines the interface for ledger operations
type LedgerAPI interface {
	GetBlockByNumber(num int) (common.Block, bool)
	CreateSnapshot(height int) error
	RestoreSnapshot(height int) error
}

// API aggregates the core modules needed for API requests.
type API struct {
	Ledger    LedgerAPI
	Consensus types.Consensus
	TxManager *transaction.TransactionManager
	Storage   storage.Store
}

// NewAPI creates a new API instance with its dependencies.
func NewAPI(
	ledgerInst LedgerAPI,
	consensus types.Consensus,
	txManager *transaction.TransactionManager,
	store storage.Store,
) *API {
	return &API{
		Ledger:    ledgerInst,
		Consensus: consensus,
		TxManager: txManager,
		Storage:   store,
	}
}

// StartServer starts the HTTP server on the specified port.
func (api *API) StartServer(port string) error {
	router := mux.NewRouter()

	// Standard endpoints.
	router.HandleFunc("/status", api.handleStatus).Methods("GET")
	router.HandleFunc("/accounts/{id}", api.handleGetAccount).Methods("GET")
	router.HandleFunc("/blocks/{number}", api.handleGetBlock).Methods("GET")

	// Register transaction routes
	api.RegisterTransactionRoutes(router)

	// New wallet endpoint.
	router.HandleFunc("/wallets", api.handleCreateWallet).Methods("POST")

	// Fund wallet endpoint (for testing only)
	router.HandleFunc("/wallets/{id}/fund", api.handleFundWallet).Methods("POST")

	// Token supply endpoints
	router.HandleFunc("/token-supply", api.handleGetTokenSupply).Methods("GET")

	// Ledger snapshot endpoints.
	router.HandleFunc("/ledger/snapshot/{height}", api.handleCreateSnapshot).Methods("GET")
	router.HandleFunc("/ledger/restore/{height}", api.handleRestoreSnapshot).Methods("POST")

	// Storage endpoint: get a block from persistent storage.
	router.HandleFunc("/storage/block/{number}", api.handleStorageGetBlock).Methods("GET")

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

// handleGetBlock retrieves block details by block number from the ledger.
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

// Note: handleSubmitTransaction is now defined in api/transactions.go

// handleCreateWallet is defined in api/wallets.go (new endpoint).
// (Ensure that file exists and works correctly.)

// handleCreateSnapshot creates a snapshot of the ledger state at the given height.
func (api *API) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid snapshot height")
		return
	}
	err = api.Ledger.CreateSnapshot(height)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Snapshot creation failed: "+err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]string{"message": fmt.Sprintf("Snapshot created at height %d", height)})
}

// handleRestoreSnapshot restores the ledger state to the snapshot at the given height.
func (api *API) handleRestoreSnapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	heightStr := vars["height"]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid snapshot height")
		return
	}
	err = api.Ledger.RestoreSnapshot(height)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Snapshot restore failed: "+err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]string{"message": fmt.Sprintf("Ledger restored to snapshot at height %d", height)})
}

// handleStorageGetBlock retrieves a block from the persistent JSON store.
func (api *API) handleStorageGetBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	numStr := vars["number"]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid block number")
		return
	}

	block, err := api.Storage.GetBlock(uint64(num))
	if err != nil {
		respondWithError(w, http.StatusNotFound, fmt.Sprintf("Block %d not found in storage", num))
		return
	}
	respondWithJSON(w, http.StatusOK, block)
}

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

package shielded

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// PrivacyAPI provides HTTP/RPC endpoints for privacy features
type PrivacyAPI struct {
	pool          *ShieldedPool
	vmIntegration *VMIntegration
	logger        *logrus.Logger
}

// NewPrivacyAPI creates a new privacy API instance
func NewPrivacyAPI(pool *ShieldedPool, logger *logrus.Logger) *PrivacyAPI {
	return &PrivacyAPI{
		pool:          pool,
		vmIntegration: NewVMIntegration(pool, logger),
		logger:        logger,
	}
}

// RegisterRoutes registers privacy API routes
func (api *PrivacyAPI) RegisterRoutes(router *mux.Router) {
	// Privacy endpoints
	router.HandleFunc("/api/v1/privacy/shield", api.HandleShield).Methods("POST")
	router.HandleFunc("/api/v1/privacy/unshield", api.HandleUnshield).Methods("POST")
	router.HandleFunc("/api/v1/privacy/transfer", api.HandleTransfer).Methods("POST")
	router.HandleFunc("/api/v1/privacy/balance", api.HandleGetBalance).Methods("POST")
	router.HandleFunc("/api/v1/privacy/transaction/{id}", api.HandleGetTransaction).Methods("GET")
	router.HandleFunc("/api/v1/privacy/proof/{commitment}", api.HandleGetProof).Methods("GET")
	router.HandleFunc("/api/v1/privacy/nullifier/{nullifier}", api.HandleCheckNullifier).Methods("GET")
	router.HandleFunc("/api/v1/privacy/stats", api.HandleGetStats).Methods("GET")

	// Key management
	router.HandleFunc("/api/v1/privacy/keys/generate", api.HandleGenerateKeys).Methods("POST")
	router.HandleFunc("/api/v1/privacy/keys/viewing", api.HandleDeriveViewingKey).Methods("POST")
}

// === API Handlers ===

// HandleShield handles requests to shield transparent assets
func (api *PrivacyAPI) HandleShield(w http.ResponseWriter, r *http.Request) {
	var req ShieldRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Validate request
	if req.Amount == "" || req.Recipient == "" {
		api.writeError(w, http.StatusBadRequest, "Missing required fields")
		return
	}

	// Parse parameters
	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok {
		api.writeError(w, http.StatusBadRequest, "Invalid amount")
		return
	}

	var assetType AssetID
	if req.AssetType != "" {
		assetBytes, err := hex.DecodeString(req.AssetType)
		if err != nil {
			api.writeError(w, http.StatusBadRequest, "Invalid asset type")
			return
		}
		copy(assetType[:], assetBytes)
	}

	var recipient PublicKey
	recipientBytes, err := hex.DecodeString(req.Recipient)
	if err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid recipient")
		return
	}
	copy(recipient[:], recipientBytes)

	// Create mint transaction
	mintTx := &MintTransaction{
		Source:    req.Source,
		Amount:    amount,
		AssetType: assetType,
		Recipient: recipient,
		Fee:       big.NewInt(0),
	}

	// Execute shield operation
	ctx := context.Background()
	shieldedTx, err := api.pool.Mint(ctx, mintTx)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, "Shield failed: "+err.Error())
		return
	}

	// Return response
	resp := ShieldResponse{
		TransactionID: shieldedTx.ID,
		Commitment:    hex.EncodeToString(shieldedTx.Commitments[0][:]),
		MerkleRoot:    hex.EncodeToString(shieldedTx.MerkleRoot[:]),
		Proof:         hex.EncodeToString(shieldedTx.Proof),
	}

	api.writeJSON(w, http.StatusOK, resp)
}

// HandleUnshield handles requests to unshield assets
func (api *PrivacyAPI) HandleUnshield(w http.ResponseWriter, r *http.Request) {
	var req UnshieldRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// This would parse the burn proof and execute unshield
	api.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "unshield_pending",
		"message": "Unshield operation requires full implementation",
	})
}

// HandleTransfer handles shielded transfer requests
func (api *PrivacyAPI) HandleTransfer(w http.ResponseWriter, r *http.Request) {
	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// This would parse inputs/outputs and execute transfer
	api.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "transfer_pending",
		"message": "Transfer operation requires full implementation",
	})
}

// HandleGetBalance handles balance queries using viewing key
func (api *PrivacyAPI) HandleGetBalance(w http.ResponseWriter, r *http.Request) {
	var req BalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	// Parse viewing key
	var viewingKey ViewingKey
	vkBytes, err := hex.DecodeString(req.ViewingKey)
	if err != nil || len(vkBytes) != 64 {
		api.writeError(w, http.StatusBadRequest, "Invalid viewing key")
		return
	}
	copy(viewingKey.Incoming[:], vkBytes[:32])
	copy(viewingKey.Outgoing[:], vkBytes[32:])

	// Get balance (placeholder implementation)
	balances := make(map[string]string)
	balances["native"] = "0"

	api.writeJSON(w, http.StatusOK, BalanceResponse{
		Balances: balances,
	})
}

// HandleGetTransaction retrieves a shielded transaction
func (api *PrivacyAPI) HandleGetTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txID := vars["id"]

	// This would retrieve transaction from storage
	api.writeJSON(w, http.StatusOK, map[string]string{
		"id":     txID,
		"status": "not_implemented",
	})
}

// HandleGetProof returns a Merkle proof for a commitment
func (api *PrivacyAPI) HandleGetProof(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	commitmentHex := vars["commitment"]

	commitmentBytes, err := hex.DecodeString(commitmentHex)
	if err != nil || len(commitmentBytes) != 32 {
		api.writeError(w, http.StatusBadRequest, "Invalid commitment")
		return
	}

	var commitment Commitment
	copy(commitment[:], commitmentBytes)

	proof, err := api.pool.GetMerkleProof(commitment)
	if err != nil {
		api.writeError(w, http.StatusNotFound, "Commitment not found")
		return
	}

	api.writeJSON(w, http.StatusOK, proof)
}

// HandleCheckNullifier checks if a nullifier has been spent
func (api *PrivacyAPI) HandleCheckNullifier(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nullifierHex := vars["nullifier"]

	nullifierBytes, err := hex.DecodeString(nullifierHex)
	if err != nil || len(nullifierBytes) != 32 {
		api.writeError(w, http.StatusBadRequest, "Invalid nullifier")
		return
	}

	var nullifier Nullifier
	copy(nullifier[:], nullifierBytes)

	api.pool.mu.RLock()
	spent := api.pool.nullifierSet[nullifier]
	api.pool.mu.RUnlock()

	api.writeJSON(w, http.StatusOK, map[string]bool{
		"spent": spent,
	})
}

// HandleGetStats returns shielded pool statistics
func (api *PrivacyAPI) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	stats := api.pool.GetPoolStats()
	api.writeJSON(w, http.StatusOK, stats)
}

// HandleGenerateKeys generates new shielded keys
func (api *PrivacyAPI) HandleGenerateKeys(w http.ResponseWriter, r *http.Request) {
	pubKey, privKey, err := GenerateKeyPair()
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, "Key generation failed")
		return
	}

	viewingKey := GenerateViewingKey(privKey)

	resp := KeyGenerationResponse{
		PublicKey:  hex.EncodeToString(pubKey[:]),
		ViewingKey: hex.EncodeToString(append(viewingKey.Incoming[:], viewingKey.Outgoing[:]...)),
	}

	api.writeJSON(w, http.StatusOK, resp)
}

// HandleDeriveViewingKey derives viewing key from spending key
func (api *PrivacyAPI) HandleDeriveViewingKey(w http.ResponseWriter, r *http.Request) {
	var req ViewingKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request")
		return
	}

	// This would derive viewing key from spending key
	api.writeJSON(w, http.StatusOK, map[string]string{
		"viewing_key": "placeholder",
	})
}

// === Request/Response Types ===

type ShieldRequest struct {
	Source    string `json:"source"`
	Amount    string `json:"amount"`
	AssetType string `json:"asset_type,omitempty"`
	Recipient string `json:"recipient"`
}

type ShieldResponse struct {
	TransactionID string `json:"transaction_id"`
	Commitment    string `json:"commitment"`
	MerkleRoot    string `json:"merkle_root"`
	Proof         string `json:"proof"`
}

type UnshieldRequest struct {
	Proof       string `json:"proof"`
	Nullifier   string `json:"nullifier"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"`
	AssetType   string `json:"asset_type"`
}

type TransferRequest struct {
	Inputs  []string `json:"inputs"`  // Serialized inputs with proofs
	Outputs []string `json:"outputs"` // Serialized outputs
	Fee     string   `json:"fee,omitempty"`
}

type BalanceRequest struct {
	ViewingKey string `json:"viewing_key"`
}

type BalanceResponse struct {
	Balances map[string]string `json:"balances"` // asset_type -> amount
}

type KeyGenerationResponse struct {
	PublicKey  string `json:"public_key"`
	ViewingKey string `json:"viewing_key"`
}

type ViewingKeyRequest struct {
	SpendingKey string `json:"spending_key"`
}

// === Helper Methods ===

func (api *PrivacyAPI) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (api *PrivacyAPI) writeError(w http.ResponseWriter, status int, message string) {
	api.writeJSON(w, status, map[string]string{
		"error": message,
	})
}

// === gRPC Interface ===

// PrivacyService implements gRPC service for privacy features
type PrivacyService struct {
	api *PrivacyAPI
}

// Shield implements the gRPC Shield method
func (s *PrivacyService) Shield(ctx context.Context, req *ShieldRequest) (*ShieldResponse, error) {
	// Validate request
	if req.Amount == "" || req.Recipient == "" {
		return nil, fmt.Errorf("missing required fields")
	}

	// Parse parameters
	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount")
	}

	var assetType AssetID
	if req.AssetType != "" {
		assetBytes, err := hex.DecodeString(req.AssetType)
		if err != nil {
			return nil, fmt.Errorf("invalid asset type: %v", err)
		}
		copy(assetType[:], assetBytes)
	}

	// Parse recipient
	recipientBytes, err := hex.DecodeString(req.Recipient)
	if err != nil || len(recipientBytes) != 32 {
		return nil, fmt.Errorf("invalid recipient address")
	}
	var recipient PublicKey
	copy(recipient[:], recipientBytes)

	// Create and execute mint transaction
	mintTx := &MintTransaction{
		Source:    req.Source,
		Amount:    amount,
		AssetType: assetType,
		Recipient: recipient,
		Fee:       big.NewInt(0),
	}

	shieldedTx, err := s.api.pool.Mint(ctx, mintTx)
	if err != nil {
		return nil, fmt.Errorf("shield failed: %v", err)
	}

	return &ShieldResponse{
		TransactionID: shieldedTx.ID,
		Commitment:    hex.EncodeToString(shieldedTx.Commitments[0][:]),
		MerkleRoot:    hex.EncodeToString(shieldedTx.MerkleRoot[:]),
		Proof:         hex.EncodeToString(shieldedTx.Proof),
	}, nil
}

// Additional gRPC methods would follow...

// === WebSocket Support ===

// PrivacyWebSocket provides real-time updates for shielded transactions
type PrivacyWebSocket struct {
	api *PrivacyAPI
}

// HandleWebSocket handles WebSocket connections for privacy updates
func (ws *PrivacyWebSocket) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Would implement WebSocket handler for real-time note discovery
	// Clients with viewing keys can subscribe to new notes
}

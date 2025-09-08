// api/governance.go
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"diamante/consensus/governance"
)

// GovernanceHandler handles governance-related API requests
type GovernanceHandler struct {
	governance *governance.Governance
	logger     *logrus.Logger
	bodyLimit  int64
}

// NewGovernanceHandler creates a new governance handler
func NewGovernanceHandler(gov *governance.Governance, logger *logrus.Logger) *GovernanceHandler {
	return &GovernanceHandler{
		governance: gov,
		logger:     logger,
		bodyLimit:  1 << 20,
	}
}

// RegisterGovernanceRoutes registers governance-related routes
func (api *API) RegisterGovernanceRoutes(router *mux.Router) {
	// Create a governance handler
	govHandler := NewGovernanceHandler(api.Governance, api.Logger)
	govHandler.bodyLimit = api.bodyLimit

	// Register routes
	router.HandleFunc("/governance/proposals", govHandler.handleGetProposals).Methods("GET")
	router.HandleFunc("/governance/proposals", govHandler.handleCreateProposal).Methods("POST")
	router.HandleFunc("/governance/proposals/{id}", govHandler.handleGetProposal).Methods("GET")
	router.HandleFunc("/governance/proposals/{id}/vote", govHandler.handleVote).Methods("POST")
	router.HandleFunc("/governance/proposals/{id}/cancel", govHandler.handleCancelProposal).Methods("POST")
	router.HandleFunc("/governance/proposals/{id}/execute", govHandler.handleExecuteProposal).Methods("POST")
	router.HandleFunc("/governance/proposals/{id}/votes", govHandler.handleGetVotes).Methods("GET")
	router.HandleFunc("/governance/stats", govHandler.handleGetStats).Methods("GET")
}

// handleGetProposals handles requests to get all proposals
func (h *GovernanceHandler) handleGetProposals(w http.ResponseWriter, r *http.Request) {
	// Get query parameters
	status := r.URL.Query().Get("status")

	var proposals []*governance.Proposal
	switch strings.ToLower(status) {
	case "", "all":
		proposals = h.governance.GetAllProposals()
	case "pending":
		proposals = h.governance.GetProposalsByStatus(governance.Pending)
	case "active":
		proposals = h.governance.GetProposalsByStatus(governance.Active)
	case "passed":
		proposals = h.governance.GetProposalsByStatus(governance.Passed)
	case "rejected":
		proposals = h.governance.GetProposalsByStatus(governance.Rejected)
	case "executed":
		proposals = h.governance.GetProposalsByStatus(governance.Executed)
	default:
		respondWithError(w, http.StatusBadRequest, "Invalid status filter")
		return
	}

	// Convert proposals to a response format
	type proposalResponse struct {
		ID          string    `json:"id"`
		Type        string    `json:"type"`
		Description string    `json:"description"`
		StartTime   time.Time `json:"startTime"`
		EndTime     time.Time `json:"endTime"`
		Status      string    `json:"status"`
		Creator     string    `json:"creator"`
		VoteCount   int       `json:"voteCount"`
	}

	var response []proposalResponse
	for _, prop := range proposals {
		// Get vote count
		voteCount, _ := h.governance.GetVoterCount(prop.ID)

		// Convert proposal type to string
		var typeStr string
		switch prop.Type {
		case governance.ConsensusChange:
			typeStr = "ConsensusChange"
		case governance.ParameterChange:
			typeStr = "ParameterChange"
		case governance.UpgradeProposal:
			typeStr = "UpgradeProposal"
		default:
			typeStr = "Unknown"
		}

		response = append(response, proposalResponse{
			ID:          hex.EncodeToString(prop.ID[:]),
			Type:        typeStr,
			Description: prop.Description,
			StartTime:   prop.StartTime,
			EndTime:     prop.EndTime,
			Status:      prop.Status.String(),
			Creator:     hex.EncodeToString(prop.Creator[:]),
			VoteCount:   voteCount,
		})
	}

	respondWithJSON(w, http.StatusOK, response)
}

// handleCreateProposal handles requests to create a new proposal
func (h *GovernanceHandler) handleCreateProposal(w http.ResponseWriter, r *http.Request) {
	// Read request body
	var req struct {
		Type        string          `json:"type"`
		Description string          `json:"description"`
		Data        json.RawMessage `json:"data"`
		Creator     string          `json:"creator"`
	}
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			respondWithError(w, http.StatusRequestEntityTooLarge, "payload too large")
		} else {
			respondWithError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	// Validate request
	if req.Type == "" {
		respondWithError(w, http.StatusBadRequest, "Proposal type is required")
		return
	}
	if req.Description == "" {
		respondWithError(w, http.StatusBadRequest, "Description is required")
		return
	}
	if req.Creator == "" {
		respondWithError(w, http.StatusBadRequest, "Creator is required")
		return
	}

	// Convert proposal type from string to ProposalType
	var propType governance.ProposalType
	switch req.Type {
	case "ConsensusChange":
		propType = governance.ConsensusChange
	case "ParameterChange":
		propType = governance.ParameterChange
	case "UpgradeProposal":
		propType = governance.UpgradeProposal
	default:
		respondWithError(w, http.StatusBadRequest, "Invalid proposal type")
		return
	}

	// Decode creator ID
	creatorBytes, err := hex.DecodeString(req.Creator)
	if err != nil || len(creatorBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid creator ID")
		return
	}
	var creatorID [32]byte
	copy(creatorID[:], creatorBytes)

	// Create proposal
	propID, err := h.governance.CreateProposal(propType, req.Description, req.Data, creatorID)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to create proposal: "+err.Error())
		return
	}

	// Return proposal ID
	respondWithJSON(w, http.StatusCreated, map[string]string{
		"id": hex.EncodeToString(propID[:]),
	})
}

// handleGetProposal handles requests to get a specific proposal
func (h *GovernanceHandler) handleGetProposal(w http.ResponseWriter, r *http.Request) {
	// Get proposal ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	// Decode proposal ID
	idBytes, err := hex.DecodeString(idStr)
	if err != nil || len(idBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid proposal ID")
		return
	}
	var propID [32]byte
	copy(propID[:], idBytes)

	// Get proposal
	prop, err := h.governance.GetProposal(propID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Proposal not found")
		return
	}

	// Get vote count
	voteCount, _ := h.governance.GetVoterCount(propID)

	// Convert proposal type to string
	var typeStr string
	switch prop.Type {
	case governance.ConsensusChange:
		typeStr = "ConsensusChange"
	case governance.ParameterChange:
		typeStr = "ParameterChange"
	case governance.UpgradeProposal:
		typeStr = "UpgradeProposal"
	default:
		typeStr = "Unknown"
	}

	// Convert proposal to response format
	response := struct {
		ID          string          `json:"id"`
		Type        string          `json:"type"`
		Description string          `json:"description"`
		StartTime   time.Time       `json:"startTime"`
		EndTime     time.Time       `json:"endTime"`
		Status      string          `json:"status"`
		Creator     string          `json:"creator"`
		VoteCount   int             `json:"voteCount"`
		Data        json.RawMessage `json:"data,omitempty"`
	}{
		ID:          hex.EncodeToString(prop.ID[:]),
		Type:        typeStr,
		Description: prop.Description,
		StartTime:   prop.StartTime,
		EndTime:     prop.EndTime,
		Status:      prop.Status.String(),
		Creator:     hex.EncodeToString(prop.Creator[:]),
		VoteCount:   voteCount,
		Data:        prop.Data,
	}

	respondWithJSON(w, http.StatusOK, response)
}

// handleVote handles requests to vote on a proposal
func (h *GovernanceHandler) handleVote(w http.ResponseWriter, r *http.Request) {
	// Get proposal ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	// Decode proposal ID
	idBytes, err := hex.DecodeString(idStr)
	if err != nil || len(idBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid proposal ID")
		return
	}
	var propID [32]byte
	copy(propID[:], idBytes)

	// Read request body
	var req struct {
		Voter string `json:"voter"`
		Vote  bool   `json:"vote"`
	}
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			respondWithError(w, http.StatusRequestEntityTooLarge, "payload too large")
		} else {
			respondWithError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	// Validate request
	if req.Voter == "" {
		respondWithError(w, http.StatusBadRequest, "Voter is required")
		return
	}

	// Decode voter ID
	voterBytes, err := hex.DecodeString(req.Voter)
	if err != nil || len(voterBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid voter ID")
		return
	}
	var voterID [32]byte
	copy(voterID[:], voterBytes)

	// Vote on proposal
	if err := h.governance.Vote(propID, voterID, req.Vote); err != nil {
		respondWithError(w, http.StatusBadRequest, "Failed to vote: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{
		"message": "Vote recorded successfully",
	})
}

// handleCancelProposal handles requests to cancel a proposal
func (h *GovernanceHandler) handleCancelProposal(w http.ResponseWriter, r *http.Request) {
	// Get proposal ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	// Decode proposal ID
	idBytes, err := hex.DecodeString(idStr)
	if err != nil || len(idBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid proposal ID")
		return
	}
	var propID [32]byte
	copy(propID[:], idBytes)

	// Read request body
	var req struct {
		Canceler string `json:"canceler"`
	}
	if err := decodeJSONBody(w, r, &req, h.bodyLimit); err != nil {
		if errors.Is(err, ErrBodyTooLarge) {
			respondWithError(w, http.StatusRequestEntityTooLarge, "payload too large")
		} else {
			respondWithError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	// Validate request
	if req.Canceler == "" {
		respondWithError(w, http.StatusBadRequest, "Canceler is required")
		return
	}

	// Decode canceler ID
	cancelerBytes, err := hex.DecodeString(req.Canceler)
	if err != nil || len(cancelerBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid canceler ID")
		return
	}
	var cancelerID [32]byte
	copy(cancelerID[:], cancelerBytes)

	// Cancel proposal
	if err := h.governance.CancelProposal(propID, cancelerID); err != nil {
		respondWithError(w, http.StatusBadRequest, "Failed to cancel proposal: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{
		"message": "Proposal cancelled successfully",
	})
}

// handleExecuteProposal handles requests to execute a proposal
func (h *GovernanceHandler) handleExecuteProposal(w http.ResponseWriter, r *http.Request) {
	// Get proposal ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	// Decode proposal ID
	idBytes, err := hex.DecodeString(idStr)
	if err != nil || len(idBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid proposal ID")
		return
	}
	var propID [32]byte
	copy(propID[:], idBytes)

	// Execute proposal
	if err := h.governance.ExecuteProposal(propID); err != nil {
		respondWithError(w, http.StatusBadRequest, "Failed to execute proposal: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, map[string]string{
		"message": "Proposal executed successfully",
	})
}

// handleGetVotes handles requests to get votes for a proposal
func (h *GovernanceHandler) handleGetVotes(w http.ResponseWriter, r *http.Request) {
	// Get proposal ID from URL
	vars := mux.Vars(r)
	idStr := vars["id"]

	// Decode proposal ID
	idBytes, err := hex.DecodeString(idStr)
	if err != nil || len(idBytes) != 32 {
		respondWithError(w, http.StatusBadRequest, "Invalid proposal ID")
		return
	}
	var propID [32]byte
	copy(propID[:], idBytes)

	// Get voting results
	results, err := h.governance.GetVotingResults(propID)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Failed to get voting results: "+err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, results)
}

// handleGetStats handles requests to get governance statistics
func (h *GovernanceHandler) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats := h.governance.GetProposalStats()
	respondWithJSON(w, http.StatusOK, stats)
}

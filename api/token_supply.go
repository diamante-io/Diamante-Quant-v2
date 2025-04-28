// api/token_supply.go
package api

import (
	"net/http"

	"diamante/common"
)

// TokenSupplyResponse represents the response for token supply information
type TokenSupplyResponse struct {
	TotalSupply       float64 `json:"totalSupply"`
	CirculatingSupply float64 `json:"circulatingSupply"`
	TreasuryBalance   float64 `json:"treasuryBalance"`
	TreasuryID        string  `json:"treasuryId"`
}

// handleGetTokenSupply returns information about the token supply
func (api *API) handleGetTokenSupply(w http.ResponseWriter, r *http.Request) {
	tokenSupply := common.GetTokenSupply()

	resp := TokenSupplyResponse{
		TotalSupply:       tokenSupply.GetTotalSupply(),
		CirculatingSupply: tokenSupply.GetCirculatingSupply(),
		TreasuryBalance:   tokenSupply.GetTreasuryBalance(),
		TreasuryID:        tokenSupply.GetTreasuryID(),
	}

	respondWithJSON(w, http.StatusOK, resp)
}

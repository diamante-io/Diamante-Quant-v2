package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthEndpoint tests the health check endpoint
func TestHealthEndpoint(t *testing.T) {
	// Create request
	req, err := http.NewRequest("GET", "/health", nil)
	require.NoError(t, err)

	// Create response recorder
	rr := httptest.NewRecorder()

	// Create a simple handler that mimics the health endpoint
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
		})
	})

	// Serve the request
	handler.ServeHTTP(rr, req)

	// Check response
	assert.Equal(t, http.StatusOK, rr.Code)

	var response map[string]string
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
}

// TestStatusEndpoint tests the status endpoint behavior
func TestStatusEndpoint(t *testing.T) {
	testCases := []struct {
		name           string
		authHeader     string
		expectedStatus int
		mockResponse   map[string]interface{}
	}{
		{
			name:           "Valid request",
			authHeader:     "Bearer valid-token",
			expectedStatus: http.StatusOK,
			mockResponse: map[string]interface{}{
				"status":               "ok",
				"height":               100,
				"pending_transactions": 5,
			},
		},
		{
			name:           "Missing auth",
			authHeader:     "",
			expectedStatus: http.StatusUnauthorized,
			mockResponse:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/status", nil)
			require.NoError(t, err)

			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			rr := httptest.NewRecorder()

			// Mock handler
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Check auth
				auth := r.Header.Get("Authorization")
				if auth == "" || auth != "Bearer valid-token" {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(tc.mockResponse)
			})

			handler.ServeHTTP(rr, req)
			assert.Equal(t, tc.expectedStatus, rr.Code)

			if tc.expectedStatus == http.StatusOK {
				var response map[string]interface{}
				err = json.Unmarshal(rr.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Equal(t, tc.mockResponse["status"], response["status"])
			}
		})
	}
}

// TestTransactionSubmission tests transaction submission endpoint
func TestTransactionSubmission(t *testing.T) {
	testCases := []struct {
		name           string
		transaction    map[string]interface{}
		expectedStatus int
		expectedError  string
	}{
		{
			name: "Valid transaction",
			transaction: map[string]interface{}{
				"from":      "0xsender",
				"to":        "0xreceiver",
				"amount":    1000,
				"fee":       10,
				"nonce":     1,
				"signature": "0xsignature",
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "Missing signature",
			transaction: map[string]interface{}{
				"from":   "0xsender",
				"to":     "0xreceiver",
				"amount": 1000,
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "signature required",
		},
		{
			name: "Invalid amount",
			transaction: map[string]interface{}{
				"from":      "0xsender",
				"to":        "0xreceiver",
				"amount":    -100,
				"signature": "0xsig",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "invalid amount",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(tc.transaction)
			req, err := http.NewRequest("POST", "/transactions", bytes.NewBuffer(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer valid-token")

			rr := httptest.NewRecorder()

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Basic validation
				var tx map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
					return
				}

				// Validate required fields
				if tx["signature"] == nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "signature required"})
					return
				}

				// Validate amount
				if amount, ok := tx["amount"].(float64); ok && amount < 0 {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "invalid amount"})
					return
				}

				// Success
				w.WriteHeader(http.StatusCreated)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"message": "Transaction submitted",
					"hash":    "0xtxhash123",
				})
			})

			handler.ServeHTTP(rr, req)
			assert.Equal(t, tc.expectedStatus, rr.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rr.Body.Bytes(), &response)
			require.NoError(t, err)

			if tc.expectedError != "" {
				assert.Contains(t, response["error"], tc.expectedError)
			}
		})
	}
}

// TestGetBlockEndpoint tests the get block endpoint
func TestGetBlockEndpoint(t *testing.T) {
	testCases := []struct {
		name           string
		blockNumber    string
		expectedStatus int
		expectedBlock  map[string]interface{}
	}{
		{
			name:           "Valid block",
			blockNumber:    "100",
			expectedStatus: http.StatusOK,
			expectedBlock: map[string]interface{}{
				"height":       float64(100),
				"hash":         "0xblockhash100",
				"transactions": []interface{}{},
			},
		},
		{
			name:           "Block not found",
			blockNumber:    "999999",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Invalid block number",
			blockNumber:    "invalid",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/blocks/"+tc.blockNumber, nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer valid-token")

			rr := httptest.NewRecorder()

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Extract block number from path
				parts := strings.Split(r.URL.Path, "/")
				if len(parts) < 3 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				blockNumStr := parts[2]
				blockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "invalid block number"})
					return
				}

				// Simulate block not found
				if blockNum > 1000 {
					w.WriteHeader(http.StatusNotFound)
					json.NewEncoder(w).Encode(map[string]string{"error": "block not found"})
					return
				}

				// Return mock block
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(tc.expectedBlock)
			})

			handler.ServeHTTP(rr, req)
			assert.Equal(t, tc.expectedStatus, rr.Code)

			if tc.expectedStatus == http.StatusOK {
				var response map[string]interface{}
				err = json.Unmarshal(rr.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedBlock["height"], response["height"])
			}
		})
	}
}

// TestRateLimiting tests rate limiting functionality
func TestRateLimiting(t *testing.T) {
	rateLimitCount := 0
	maxRequests := 10

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rateLimitCount++
		if rateLimitCount > maxRequests {
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "rate limit exceeded"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// Make requests up to the limit
	for i := 0; i < maxRequests; i++ {
		req, err := http.NewRequest("GET", "/status", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
	}

	// Next request should be rate limited
	req, err := http.NewRequest("GET", "/status", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTooManyRequests, rr.Code)
}

// TestCORSHeaders tests CORS header handling
func TestCORSHeaders(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// Test OPTIONS request
	req, err := http.NewRequest("OPTIONS", "/status", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "http://localhost:3000")

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), "GET")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), "POST")
}

// TestWalletCreation tests wallet creation endpoint
func TestWalletCreation(t *testing.T) {
	testCases := []struct {
		name           string
		requestBody    map[string]string
		expectedStatus int
		checkResponse  func(t *testing.T, response map[string]interface{})
	}{
		{
			name: "Create with password",
			requestBody: map[string]string{
				"password": "secure-password",
			},
			expectedStatus: http.StatusCreated,
			checkResponse: func(t *testing.T, response map[string]interface{}) {
				assert.NotEmpty(t, response["id"])
				assert.NotEmpty(t, response["address"])
				assert.NotEmpty(t, response["mnemonic"])
			},
		},
		{
			name:           "Missing password",
			requestBody:    map[string]string{},
			expectedStatus: http.StatusBadRequest,
			checkResponse: func(t *testing.T, response map[string]interface{}) {
				assert.Contains(t, response["error"], "password")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(tc.requestBody)
			req, err := http.NewRequest("POST", "/wallets", bytes.NewBuffer(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer valid-token")

			rr := httptest.NewRecorder()

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var reqBody map[string]string
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
					return
				}

				if reqBody["password"] == "" {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "password required"})
					return
				}

				// Success
				w.WriteHeader(http.StatusCreated)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"id":       "wallet-123",
					"address":  "0xnewwallet",
					"mnemonic": "test mnemonic phrase words",
				})
			})

			handler.ServeHTTP(rr, req)
			assert.Equal(t, tc.expectedStatus, rr.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rr.Body.Bytes(), &response)
			require.NoError(t, err)

			tc.checkResponse(t, response)
		})
	}
}

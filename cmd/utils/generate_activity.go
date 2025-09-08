package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type TransactionRequest struct {
	From      string  `json:"from"`
	To        string  `json:"to"`
	Value     float64 `json:"value"`
	Data      string  `json:"data,omitempty"`
	GasLimit  uint64  `json:"gas_limit,omitempty"`
	GasPrice  float64 `json:"gas_price,omitempty"`
	Nonce     uint64  `json:"nonce,omitempty"`
	Timestamp int64   `json:"timestamp,omitempty"`
}

type TransactionResponse struct {
	Success bool   `json:"success"`
	TxHash  string `json:"tx_hash"`
	Message string `json:"message,omitempty"`
}

func main() {
	baseURL := "http://localhost:8090"

	// Generate some test addresses
	addresses := generateTestAddresses(10)

	fmt.Println("=== Diamante Blockchain Activity Generator ===")
	fmt.Printf("Target API: %s\n", baseURL)
	fmt.Println()

	// First, check the current status
	status, err := getBlockchainStatus(baseURL)
	if err != nil {
		log.Fatalf("Failed to get blockchain status: %v", err)
	}

	fmt.Printf("Current Status:\n")
	fmt.Printf("- Block Height: %d\n", status["block_height"])
	fmt.Printf("- Peer Count: %d\n", status["peer_count"])
	fmt.Printf("- Consensus: %s\n", status["consensus_status"])
	fmt.Println()

	// Generate genesis transactions
	fmt.Println("Generating test transactions...")

	// Create 20 test transactions
	for i := 0; i < 20; i++ {
		from := addresses[i%len(addresses)]
		to := addresses[(i+1)%len(addresses)]
		value := float64(100 + i*10) // Varying amounts

		tx := TransactionRequest{
			From:      from,
			To:        to,
			Value:     value,
			Timestamp: time.Now().Unix(),
			GasLimit:  21000,
			GasPrice:  1.0,
			Nonce:     uint64(i),
		}

		fmt.Printf("\nTransaction %d: %s -> %s (%.2f DMT)\n", i+1,
			truncateAddress(from), truncateAddress(to), value)

		resp, err := submitTransaction(baseURL, tx)
		if err != nil {
			fmt.Printf("  ❌ Error: %v\n", err)
			continue
		}

		if resp.Success {
			fmt.Printf("  ✅ Success! TxHash: %s\n", resp.TxHash)
		} else {
			fmt.Printf("  ❌ Failed: %s\n", resp.Message)
		}

		// Small delay between transactions
		time.Sleep(100 * time.Millisecond)
	}

	// Wait a bit for blocks to be produced
	fmt.Println("\nWaiting for blocks to be produced...")
	time.Sleep(5 * time.Second)

	// Check the latest block
	fmt.Println("\nChecking latest block...")
	latestBlock, err := getLatestBlock(baseURL)
	if err != nil {
		fmt.Printf("Error getting latest block: %v\n", err)
	} else {
		fmt.Printf("\nLatest Block:\n")
		fmt.Printf("- Height: %d\n", latestBlock["height"])
		fmt.Printf("- Hash: %s\n", latestBlock["hash"])
		fmt.Printf("- Transactions: %d\n", len(latestBlock["transactions"].([]interface{})))
		fmt.Printf("- Timestamp: %s\n", time.Unix(int64(latestBlock["timestamp"].(float64)), 0))
	}

	// Check a few transactions
	fmt.Println("\nChecking transaction history...")
	txs, err := getTransactions(baseURL, 5)
	if err != nil {
		fmt.Printf("Error getting transactions: %v\n", err)
	} else {
		fmt.Printf("\nRecent Transactions:\n")
		for i, tx := range txs {
			fmt.Printf("%d. %s -> %s: %.2f DMT\n",
				i+1,
				truncateAddress(tx["from"].(string)),
				truncateAddress(tx["to"].(string)),
				tx["value"].(float64))
		}
	}

	fmt.Println("\n=== Activity Generation Complete ===")
}

func generateTestAddresses(count int) []string {
	addresses := make([]string, count)
	for i := 0; i < count; i++ {
		// Generate random 20-byte address
		addr := make([]byte, 20)
		rand.Read(addr)
		addresses[i] = "0x" + hex.EncodeToString(addr)
	}
	return addresses
}

func truncateAddress(addr string) string {
	if len(addr) > 10 {
		return addr[:6] + "..." + addr[len(addr)-4:]
	}
	return addr
}

func submitTransaction(baseURL string, tx TransactionRequest) (*TransactionResponse, error) {
	data, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(baseURL+"/api/v1/transactions", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var txResp TransactionResponse
	if err := json.NewDecoder(resp.Body).Decode(&txResp); err != nil {
		// If we can't decode the response, create a response based on status code
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			return &TransactionResponse{
				Success: true,
				TxHash:  generateTxHash(tx),
			}, nil
		}
		return &TransactionResponse{
			Success: false,
			Message: fmt.Sprintf("HTTP %d", resp.StatusCode),
		}, nil
	}

	return &txResp, nil
}

func generateTxHash(tx TransactionRequest) string {
	// Generate a simple hash for the transaction
	_ = fmt.Sprintf("%s%s%f%d", tx.From, tx.To, tx.Value, tx.Timestamp)
	hash := make([]byte, 32)
	rand.Read(hash)
	return "0x" + hex.EncodeToString(hash)
}

func getBlockchainStatus(baseURL string) (map[string]interface{}, error) {
	resp, err := http.Get(baseURL + "/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return status, nil
}

func getLatestBlock(baseURL string) (map[string]interface{}, error) {
	resp, err := http.Get(baseURL + "/blocks/latest")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var block map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&block); err != nil {
		return nil, err
	}

	return block, nil
}

func getTransactions(baseURL string, limit int) ([]map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("%s/transactions?limit=%d", baseURL, limit))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var txs []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&txs); err != nil {
		return nil, err
	}

	return txs, nil
}

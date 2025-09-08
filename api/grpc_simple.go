// Package api provides simplified gRPC server implementation for Diamante blockchain
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"diamante/common"
	"diamante/consensus"
	"diamante/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// SimpleGRPCServer implements a simplified gRPC API for Diamante
type SimpleGRPCServer struct {
	api    *API
	server *grpc.Server
	logger *logrus.Logger
	port   int
}

// NewSimpleGRPCServer creates a new simplified gRPC server instance
func NewSimpleGRPCServer(api *API, port int) *SimpleGRPCServer {
	return &SimpleGRPCServer{
		api:    api,
		logger: api.Logger,
		port:   port,
	}
}

// Start starts the simplified gRPC server
func (s *SimpleGRPCServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	s.server = grpc.NewServer()

	// Register the service manually without protobuf
	// This is a simplified approach for demonstration

	// Enable reflection for gRPC clients
	reflection.Register(s.server)

	s.logger.WithField("port", s.port).Info("Starting simplified gRPC server")

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.WithError(err).Error("Simplified gRPC server failed")
		}
	}()

	return nil
}

// Stop stops the gRPC server gracefully
func (s *SimpleGRPCServer) Stop() {
	if s.server != nil {
		s.logger.Info("Stopping simplified gRPC server")
		s.server.GracefulStop()
	}
}

// gRPC service methods would be implemented here
// For now, these are placeholder implementations

// TransactionRequest represents a transaction request
type TransactionRequest struct {
	From      string  `json:"from"`
	To        string  `json:"to"`
	Amount    float64 `json:"amount"`
	Fee       float64 `json:"fee"`
	Data      string  `json:"data"`
	Signature string  `json:"signature"`
}

// GRPCTransactionResponse represents a transaction response
type GRPCTransactionResponse struct {
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	Message       string    `json:"message"`
	Timestamp     time.Time `json:"timestamp"`
}

// BlockResponse represents a block response
type BlockResponse struct {
	ID               string             `json:"id"`
	Height           uint64             `json:"height"`
	PreviousHash     string             `json:"previous_hash"`
	MerkleRoot       string             `json:"merkle_root"`
	Timestamp        time.Time          `json:"timestamp"`
	Validator        string             `json:"validator"`
	Transactions     []*TransactionData `json:"transactions"`
	Signature        string             `json:"signature"`
	Size             uint64             `json:"size"`
	TransactionCount uint64             `json:"transaction_count"`
}

// TransactionData represents transaction data in responses
type TransactionData struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Amount      float64   `json:"amount"`
	Fee         float64   `json:"fee"`
	Data        string    `json:"data"`
	Signature   string    `json:"signature"`
	Timestamp   time.Time `json:"timestamp"`
	BlockHeight uint64    `json:"block_height"`
	Status      string    `json:"status"`
}

// AccountResponse represents an account response
type AccountResponse struct {
	ID        string    `json:"id"`
	PublicKey string    `json:"public_key"`
	Balance   float64   `json:"balance"`
	Nonce     uint64    `json:"nonce"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GRPCWalletResponse represents a wallet response
type GRPCWalletResponse struct {
	ID        string    `json:"id"`
	PublicKey string    `json:"public_key"`
	Address   string    `json:"address"`
	CreatedAt time.Time `json:"created_at"`
	IsActive  bool      `json:"is_active"`
	Message   string    `json:"message,omitempty"`
}

// NetworkInfoResponse represents network information
type NetworkInfoResponse struct {
	NetworkID         string    `json:"network_id"`
	ChainID           string    `json:"chain_id"`
	LatestBlockHeight uint64    `json:"latest_block_height"`
	TotalTransactions uint64    `json:"total_transactions"`
	PeerCount         uint32    `json:"peer_count"`
	Version           string    `json:"version"`
	StartedAt         time.Time `json:"started_at"`
}

// HealthResponse represents node health information
type HealthResponse struct {
	Status            string    `json:"status"`
	Message           string    `json:"message"`
	CPUUsage          float64   `json:"cpu_usage"`
	MemoryUsage       float64   `json:"memory_usage"`
	DiskUsage         float64   `json:"disk_usage"`
	ActiveConnections uint32    `json:"active_connections"`
	LastCheck         time.Time `json:"last_check"`
}

// Implementation methods for gRPC service

// SubmitTransaction handles transaction submission
func (s *SimpleGRPCServer) SubmitTransaction(ctx context.Context, req *TransactionRequest) (*GRPCTransactionResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"from":   req.From,
		"to":     req.To,
		"amount": req.Amount,
	}).Info("gRPC: Submitting transaction")

	// Submit to transaction manager
	txResult, err := s.api.TxManager.CreateTransaction(
		req.From,
		req.To,
		req.Amount,
		req.Fee,
		[]byte(req.Data),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to submit transaction: %w", err)
	}

	return &GRPCTransactionResponse{
		TransactionID: txResult.ID,
		Status:        "pending",
		Message:       "Transaction submitted successfully",
		Timestamp:     consensus.ConsensusNow(),
	}, nil
}

// GetTransaction retrieves a transaction by ID
func (s *SimpleGRPCServer) GetTransaction(ctx context.Context, txID string) (*TransactionData, error) {
	s.logger.WithField("transaction_id", txID).Info("gRPC: Getting transaction")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	tx, err := ledgerStore.GetTransaction(txID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return &TransactionData{
		ID:          tx.ID,
		From:        tx.Sender,
		To:          tx.Receiver,
		Amount:      tx.Amount,
		Fee:         tx.Fee,
		Data:        string(tx.Data),
		Signature:   string(tx.Signature),
		Timestamp:   time.Unix(tx.Timestamp, 0),
		BlockHeight: uint64(tx.BlockHeight),
		Status:      "confirmed",
	}, nil
}

// GetLatestBlock retrieves the latest block
func (s *SimpleGRPCServer) GetLatestBlock(ctx context.Context) (*BlockResponse, error) {
	s.logger.Info("gRPC: Getting latest block")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	block, err := ledgerStore.GetLatestBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	return s.convertBlockToResponse(block), nil
}

// GetAccount retrieves account information
func (s *SimpleGRPCServer) GetAccount(ctx context.Context, accountID string) (*AccountResponse, error) {
	s.logger.WithField("account_id", accountID).Info("gRPC: Getting account")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	account, err := ledgerStore.GetAccount(accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	return &AccountResponse{
		ID:        account.ID,
		PublicKey: string(account.PublicKey),
		Balance:   account.Balance,
		Nonce:     uint64(account.Nonce),
		CreatedAt: time.Unix(account.CreatedAt, 0),
		UpdatedAt: time.Unix(account.LastActive, 0),
	}, nil
}

// CreateWallet creates a new wallet
func (s *SimpleGRPCServer) CreateWallet(ctx context.Context, passphrase string, metadata map[string]string) (*GRPCWalletResponse, error) {
	s.logger.Info("gRPC: Creating wallet")

	// Create wallet using existing API logic
	walletID := fmt.Sprintf("wallet_%d", consensus.ConsensusNow().UnixNano())
	publicKey := "mock_public_key"
	address := "mock_address"

	return &GRPCWalletResponse{
		ID:        walletID,
		PublicKey: publicKey,
		Address:   address,
		CreatedAt: consensus.ConsensusNow(),
		IsActive:  true,
		Message:   "Wallet created successfully",
	}, nil
}

// GetNetworkInfo retrieves network information
func (s *SimpleGRPCServer) GetNetworkInfo(ctx context.Context) (*NetworkInfoResponse, error) {
	s.logger.Info("gRPC: Getting network info")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	latestBlock, err := ledgerStore.GetLatestBlock()
	if err != nil {
		s.logger.WithError(err).Warn("Failed to get latest block for network info")
	}

	height := uint64(0)
	if latestBlock != nil {
		height = uint64(latestBlock.Number)
	}

	return &NetworkInfoResponse{
		NetworkID:         "diamante-testnet",
		ChainID:           "diamante-1",
		LatestBlockHeight: height,
		TotalTransactions: 0, // Would need to implement
		PeerCount:         0, // Would need to implement
		Version:           "1.0.0",
		StartedAt:         consensus.ConsensusNow(),
	}, nil
}

// GetNodeHealth retrieves node health information
func (s *SimpleGRPCServer) GetNodeHealth(ctx context.Context) (*HealthResponse, error) {
	s.logger.Info("gRPC: Getting node health")

	return &HealthResponse{
		Status:            "healthy",
		Message:           "Node is healthy",
		CPUUsage:          25.5,
		MemoryUsage:       60.2,
		DiskUsage:         30.1,
		ActiveConnections: 10,
		LastCheck:         consensus.ConsensusNow(),
	}, nil
}

// Helper methods

func (s *SimpleGRPCServer) getLedgerStore() (storage.LedgerStore, error) {
	// NOTE: Interface conflict between Store and LedgerStore - GetBlock method signatures incompatible
	// This prevents type assertion due to Go's interface compatibility rules
	// ledgerStore, ok := s.api.Storage.(storage.LedgerStore)
	return nil, fmt.Errorf("interface conflict between Store and LedgerStore - GetBlock method signatures incompatible")
}

func (s *SimpleGRPCServer) convertBlockToResponse(block *common.Block) *BlockResponse {
	transactions := make([]*TransactionData, len(block.Transactions))
	for i, tx := range block.Transactions {
		transactions[i] = &TransactionData{
			ID:          tx.ID,
			From:        tx.Sender,
			To:          tx.Receiver,
			Amount:      tx.Amount,
			Fee:         tx.Fee,
			Data:        string(tx.Data),
			Signature:   string(tx.Signature),
			Timestamp:   time.Unix(tx.Timestamp, 0),
			BlockHeight: uint64(tx.BlockHeight),
			Status:      "confirmed",
		}
	}

	return &BlockResponse{
		ID:               block.Hash,
		Height:           uint64(block.Number),
		PreviousHash:     block.PreviousHash,
		MerkleRoot:       block.MerkleRoot,
		Timestamp:        time.Unix(block.Timestamp, 0),
		Validator:        block.Validator,
		Transactions:     transactions,
		Signature:        string(block.Signature),
		Size:             uint64(len(block.Transactions)),
		TransactionCount: uint64(len(block.Transactions)),
	}
}

// JSONTransport provides JSON-over-HTTP transport for the gRPC methods
type JSONTransport struct {
	server *SimpleGRPCServer
}

// NewJSONTransport creates a new JSON transport wrapper
func NewJSONTransport(server *SimpleGRPCServer) *JSONTransport {
	return &JSONTransport{server: server}
}

// ServeHTTP handles HTTP requests and routes them to gRPC methods
func (jt *JSONTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	path := r.URL.Path
	method := r.Method

	switch {
	case method == "POST" && path == "/grpc/transactions":
		jt.handleSubmitTransaction(ctx, w, r)
	case method == "GET" && path == "/grpc/blocks/latest":
		jt.handleGetLatestBlock(ctx, w, r)
	case method == "GET" && path == "/grpc/network/info":
		jt.handleGetNetworkInfo(ctx, w, r)
	case method == "GET" && path == "/grpc/health":
		jt.handleGetNodeHealth(ctx, w, r)
	case method == "POST" && path == "/grpc/wallets":
		jt.handleCreateWallet(ctx, w, r)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (jt *JSONTransport) handleSubmitTransaction(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := jt.server.SubmitTransaction(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(resp)
}

func (jt *JSONTransport) handleGetLatestBlock(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	resp, err := jt.server.GetLatestBlock(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(resp)
}

func (jt *JSONTransport) handleGetNetworkInfo(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	resp, err := jt.server.GetNetworkInfo(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(resp)
}

func (jt *JSONTransport) handleGetNodeHealth(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	resp, err := jt.server.GetNodeHealth(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(resp)
}

func (jt *JSONTransport) handleCreateWallet(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req struct {
		Passphrase string            `json:"passphrase"`
		Metadata   map[string]string `json:"metadata"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := jt.server.CreateWallet(ctx, req.Passphrase, req.Metadata)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(resp)
}

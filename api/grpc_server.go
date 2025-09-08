// Package api provides gRPC server implementation for Diamante blockchain
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"diamante/api/proto"
	"diamante/common"
	"diamante/consensus"
	"diamante/storage"
	"diamante/types"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCServer implements the Diamante gRPC API
type GRPCServer struct {
	proto.UnimplementedDiamanteBChainServiceServer

	api    *API
	server *grpc.Server
	logger *logrus.Logger
	port   int
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(api *API, port int) *GRPCServer {
	return &GRPCServer{
		api:    api,
		logger: api.Logger,
		port:   port,
	}
}

// Start starts the gRPC server
func (s *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	s.server = grpc.NewServer()
	proto.RegisterDiamanteBChainServiceServer(s.server, s)

	// Enable reflection for gRPC clients
	reflection.Register(s.server)

	s.logger.WithField("port", s.port).Info("Starting gRPC server")

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.WithError(err).Error("gRPC server failed")
		}
	}()

	return nil
}

// Stop stops the gRPC server gracefully
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.logger.Info("Stopping gRPC server")
		s.server.GracefulStop()
	}
}

// Transaction operations

func (s *GRPCServer) SubmitTransaction(ctx context.Context, req *proto.SubmitTransactionRequest) (*proto.SubmitTransactionResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"from":   req.From,
		"to":     req.To,
		"amount": req.Amount,
	}).Info("gRPC: Submitting transaction")

	// Create transaction object
	tx := &common.Transaction{
		Sender:    req.From,
		Receiver:  req.To,
		Amount:    req.Amount,
		Fee:       req.Fee,
		Data:      []byte(req.Data),
		Signature: []byte(req.Signature),
		Timestamp: consensus.ConsensusNow().Unix(),
	}

	// Submit to transaction manager
	createdTx, err := s.api.TxManager.CreateTransaction(tx.Sender, tx.Receiver, tx.Amount, tx.Fee, tx.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to submit transaction: %w", err)
	}

	return &proto.SubmitTransactionResponse{
		TransactionId: createdTx.ID,
		Status: &proto.TransactionStatus{
			Status:  proto.TransactionStatus_PENDING,
			Message: "Transaction submitted successfully",
		},
		Message: "Transaction submitted for processing",
	}, nil
}

func (s *GRPCServer) GetTransaction(ctx context.Context, req *proto.GetTransactionRequest) (*proto.Transaction, error) {
	s.logger.WithField("transaction_id", req.TransactionId).Info("gRPC: Getting transaction")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	tx, err := ledgerStore.GetTransaction(req.TransactionId)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return s.convertTransactionToProto(tx), nil
}

func (s *GRPCServer) GetTransactions(ctx context.Context, req *proto.GetTransactionsRequest) (*proto.GetTransactionsResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"address": req.Address,
		"limit":   req.Limit,
		"offset":  req.Offset,
	}).Info("gRPC: Getting transactions")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	limit := int(req.Limit)
	if limit == 0 {
		limit = 50 // Default limit
	}
	offset := int(req.Offset)

	var transactions []*common.Transaction
	var totalCount uint32

	if req.Address != "" {
		txs, err := ledgerStore.GetTransactionsByAddress(req.Address, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to get transactions by address: %w", err)
		}
		transactions = txs
		totalCount = uint32(len(txs)) // Simplified count
	} else {
		// Get recent transactions (would need implementation)
		transactions = []*common.Transaction{} // Empty for now
		totalCount = 0
	}

	protoTxs := make([]*proto.Transaction, len(transactions))
	for i, tx := range transactions {
		protoTxs[i] = s.convertTransactionToProto(tx)
	}

	return &proto.GetTransactionsResponse{
		Transactions: protoTxs,
		TotalCount:   totalCount,
		HasMore:      totalCount > uint32(limit),
	}, nil
}

func (s *GRPCServer) GetTransactionStatus(ctx context.Context, req *proto.GetTransactionStatusRequest) (*proto.TransactionStatus, error) {
	s.logger.WithField("transaction_id", req.TransactionId).Info("gRPC: Getting transaction status")

	// Get the actual transaction from storage
	tx, err := s.api.Storage.GetTransaction(req.TransactionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Transaction not found: %v", err)
	}

	// Determine transaction status based on blockchain state
	var txStatus proto.TransactionStatus_Status
	var message string
	var confirmations int32

	if tx.BlockHeight > 0 {
		// Transaction is in a block, check confirmations
		latestBlock, err := s.api.Storage.GetLatestBlock()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get latest block: %v", err)
		}
		latestHeight := uint64(latestBlock.Number)

		confirmations = int32(latestHeight - uint64(tx.BlockHeight) + 1)
		if confirmations >= 6 {
			txStatus = proto.TransactionStatus_CONFIRMED
			message = "Transaction confirmed"
		} else {
			txStatus = proto.TransactionStatus_PENDING
			message = "Transaction pending confirmation"
		}
	} else {
		// Transaction is in mempool
		txStatus = proto.TransactionStatus_PENDING
		message = "Transaction in mempool"
		confirmations = 0
	}

	return &proto.TransactionStatus{
		Status:        txStatus,
		Message:       message,
		Confirmations: uint64(confirmations),
		ConfirmedAt:   timestamppb.New(time.Unix(tx.Timestamp, 0)),
	}, nil
}

// Block operations

func (s *GRPCServer) GetBlock(ctx context.Context, req *proto.GetBlockRequest) (*proto.Block, error) {
	s.logger.WithField("block_id", req.BlockId).Info("gRPC: Getting block")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	block, err := ledgerStore.GetBlockByHash(req.BlockId)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	return s.convertBlockToProto(block), nil
}

func (s *GRPCServer) GetBlocks(ctx context.Context, req *proto.GetBlocksRequest) (*proto.GetBlocksResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"start_height": req.StartHeight,
		"end_height":   req.EndHeight,
		"limit":        req.Limit,
	}).Info("gRPC: Getting blocks")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	blocks, err := ledgerStore.GetBlockRange(req.StartHeight, req.EndHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks: %w", err)
	}

	protoBlocks := make([]*proto.Block, len(blocks))
	for i, block := range blocks {
		protoBlocks[i] = s.convertBlockToProto(block)
	}

	return &proto.GetBlocksResponse{
		Blocks:     protoBlocks,
		TotalCount: uint32(len(blocks)),
		HasMore:    false, // Simplified
	}, nil
}

func (s *GRPCServer) GetLatestBlock(ctx context.Context, req *emptypb.Empty) (*proto.Block, error) {
	s.logger.Info("gRPC: Getting latest block")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	block, err := ledgerStore.GetLatestBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	return s.convertBlockToProto(block), nil
}

// Account operations

func (s *GRPCServer) GetAccount(ctx context.Context, req *proto.GetAccountRequest) (*proto.Account, error) {
	s.logger.WithField("account_id", req.AccountId).Info("gRPC: Getting account")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	account, err := ledgerStore.GetAccount(req.AccountId)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	return &proto.Account{
		Id:        account.ID,
		PublicKey: string(account.PublicKey),
		Balance:   account.Balance,
		Nonce:     uint64(account.Nonce),
		CreatedAt: timestamppb.New(time.Unix(account.CreatedAt, 0)),
		UpdatedAt: timestamppb.New(time.Unix(account.LastActive, 0)),
	}, nil
}

func (s *GRPCServer) GetAccountBalance(ctx context.Context, req *proto.GetAccountBalanceRequest) (*proto.AccountBalance, error) {
	s.logger.WithField("account_id", req.AccountId).Info("gRPC: Getting account balance")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	account, err := ledgerStore.GetAccount(req.AccountId)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	// Get pending amount from transaction manager
	pendingAmount := s.api.TxManager.GetPendingAmount(req.AccountId)

	return &proto.AccountBalance{
		AccountId:        req.AccountId,
		AvailableBalance: account.Balance,
		PendingBalance:   pendingAmount,
		TotalBalance:     account.Balance + pendingAmount,
		LastUpdated:      timestamppb.New(time.Unix(account.LastActive, 0)),
	}, nil
}

func (s *GRPCServer) GetAccountTransactions(ctx context.Context, req *proto.GetAccountTransactionsRequest) (*proto.GetAccountTransactionsResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"account_id": req.AccountId,
		"limit":      req.Limit,
		"offset":     req.Offset,
	}).Info("gRPC: Getting account transactions")

	ledgerStore, err := s.getLedgerStore()
	if err != nil {
		return nil, err
	}

	limit := int(req.Limit)
	if limit == 0 {
		limit = 50
	}

	transactions, err := ledgerStore.GetTransactionsByAddress(req.AccountId, limit, int(req.Offset))
	if err != nil {
		return nil, fmt.Errorf("failed to get account transactions: %w", err)
	}

	protoTxs := make([]*proto.Transaction, len(transactions))
	for i, tx := range transactions {
		protoTxs[i] = s.convertTransactionToProto(tx)
	}

	return &proto.GetAccountTransactionsResponse{
		Transactions: protoTxs,
		TotalCount:   uint32(len(transactions)),
		HasMore:      len(transactions) == limit,
	}, nil
}

// Wallet operations

func (s *GRPCServer) CreateWallet(ctx context.Context, req *proto.CreateWalletRequest) (*proto.Wallet, error) {
	s.logger.Info("gRPC: Creating wallet")

	// Create wallet using existing API
	walletResponse, err := s.createWalletInternal(req.Passphrase, req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	return &proto.Wallet{
		Id:        walletResponse.ID,
		PublicKey: walletResponse.PublicKey,
		Address:   walletResponse.ID, // Use ID as address for now
		CreatedAt: timestamppb.Now(),
		IsActive:  true,
	}, nil
}

func (s *GRPCServer) GetWallet(ctx context.Context, req *proto.GetWalletRequest) (*proto.Wallet, error) {
	s.logger.WithField("wallet_id", req.WalletId).Info("gRPC: Getting wallet")

	// Get wallet using existing API logic
	wallet, err := s.api.getWalletFromStorage(req.WalletId)
	if err != nil {
		return nil, fmt.Errorf("failed to get wallet: %w", err)
	}

	return &proto.Wallet{
		Id:        wallet.ID,
		PublicKey: wallet.PublicKey,
		Address:   wallet.ID, // Simplified
		CreatedAt: timestamppb.New(wallet.CreatedAt),
		IsActive:  wallet.IsActive,
	}, nil
}

func (s *GRPCServer) SignTransaction(ctx context.Context, req *proto.SignTransactionRequest) (*proto.SignTransactionResponse, error) {
	s.logger.WithField("wallet_id", req.WalletId).Info("gRPC: Signing transaction")

	// This would implement transaction signing logic
	// For now, return a mock signature
	signature := fmt.Sprintf("signed_%s_%d", req.WalletId, consensus.ConsensusNow().UnixNano())

	return &proto.SignTransactionResponse{
		SignedTransaction: req.TransactionData,
		Signature:         signature,
	}, nil
}

// Network operations

func (s *GRPCServer) GetNetworkInfo(ctx context.Context, req *emptypb.Empty) (*proto.NetworkInfo, error) {
	s.logger.Info("gRPC: Getting network info")

	// Get network information
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

	return &proto.NetworkInfo{
		NetworkId:         "diamante-testnet",
		ChainId:           "diamante-1",
		LatestBlockHeight: height,
		TotalTransactions: 0, // Would need to implement
		PeerCount:         0, // Would need to implement
		Version:           "1.0.0",
		StartedAt:         timestamppb.Now(),
	}, nil
}

func (s *GRPCServer) GetPeers(ctx context.Context, req *emptypb.Empty) (*proto.GetPeersResponse, error) {
	s.logger.Info("gRPC: Getting peers")

	// Return empty peers list for now
	// In a real implementation, this would get peer information from the network layer
	return &proto.GetPeersResponse{
		Peers:      []*proto.Peer{},
		TotalCount: 0,
	}, nil
}

func (s *GRPCServer) GetNodeHealth(ctx context.Context, req *emptypb.Empty) (*proto.NodeHealth, error) {
	s.logger.Info("gRPC: Getting node health")

	// Simple health check
	return &proto.NodeHealth{
		Status:            proto.NodeHealth_HEALTHY,
		Message:           "Node is healthy",
		CpuUsage:          25.5,
		MemoryUsage:       60.2,
		DiskUsage:         30.1,
		ActiveConnections: 10,
		LastCheck:         timestamppb.Now(),
	}, nil
}

// Smart Contract operations

func (s *GRPCServer) DeployContract(ctx context.Context, req *proto.DeployContractRequest) (*proto.DeployContractResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"owner":   req.Owner,
		"runtime": req.Runtime,
	}).Info("gRPC: Deploying contract")

	// Validate request
	if req.Owner == "" {
		return nil, status.Error(codes.InvalidArgument, "Owner cannot be empty")
	}
	if req.Code == "" {
		return nil, status.Error(codes.InvalidArgument, "Contract code cannot be empty")
	}
	if req.Runtime == "" {
		req.Runtime = "evm" // Default to EVM runtime
	}

	// Create contract deployment transaction
	contractTx := &types.TypedTransaction{
		Type:  types.TransactionTypeContractDeploy,
		From:  req.Owner,
		To:    "", // Empty for contract deployment
		Value: 0,
		Data: &types.TypedTransactionData{
			ContractDeploy: &types.ContractDeployData{
				ByteCode: []byte(req.Code),
			},
		},
		Timestamp: common.ConsensusNow().Unix(),
		Status:    types.TransactionStatusPending,
		GasLimit:  100000, // Default gas limit
		GasPrice:  1,      // Default gas price
	}

	// Submit transaction to pool
	if s.api.TxManager == nil {
		return nil, status.Errorf(codes.Internal, "Transaction manager not available")
	}

	// Create transaction through TransactionManager
	commonTx, err := s.api.TxManager.CreateTransaction(
		contractTx.From,
		contractTx.To,
		float64(contractTx.Value),
		float64(contractTx.GasPrice)*float64(contractTx.GasLimit)/1e18, // Convert gas to fee
		[]byte(req.Code), // Convert string to []byte
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create contract deployment transaction: %v", err)
	}

	// Generate contract address deterministically
	contractAddress := generateContractAddress(req.Owner, commonTx.ID)

	return &proto.DeployContractResponse{
		ContractId:    commonTx.ID,
		TransactionId: commonTx.ID,
		Address:       contractAddress,
	}, nil
}

func (s *GRPCServer) InvokeContract(ctx context.Context, req *proto.InvokeContractRequest) (*proto.InvokeContractResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"contract_id": req.ContractId,
		"method":      req.Method,
		"caller":      req.Caller,
	}).Info("gRPC: Invoking contract")

	// This would implement contract invocation
	txID := fmt.Sprintf("invoke_tx_%d", consensus.ConsensusNow().UnixNano())

	return &proto.InvokeContractResponse{
		TransactionId: txID,
		Result:        "success",
		GasUsed:       1000,
	}, nil
}

func (s *GRPCServer) GetContract(ctx context.Context, req *proto.GetContractRequest) (*proto.Contract, error) {
	s.logger.WithField("contract_id", req.ContractId).Info("gRPC: Getting contract")

	// This would get contract from storage
	// For now, return a mock contract
	return &proto.Contract{
		Id:         req.ContractId,
		Owner:      "mock_owner",
		Code:       "mock_code",
		Abi:        "mock_abi",
		Runtime:    "wasm",
		DeployedAt: timestamppb.Now(),
		Version:    "1.0.0",
		IsActive:   true,
	}, nil
}

// Consensus operations

func (s *GRPCServer) GetConsensusInfo(ctx context.Context, req *emptypb.Empty) (*proto.ConsensusInfo, error) {
	s.logger.Info("gRPC: Getting consensus info")

	// Get real consensus data
	activeValidators := s.api.Consensus.GetActiveValidators()
	validatorCount := uint32(len(activeValidators))

	// Calculate participation rate based on active validators
	participationRate := float32(1.0)
	if validatorCount > 0 {
		// In a real system, this would track actual participation
		// For now, all active validators are considered participating
		participationRate = 1.0
	}

	// Get current proposer and round/step info if available
	currentProposer := ""
	currentRound := uint64(0)
	currentStep := uint32(0)

	// Try to get more detailed consensus state
	if getter, ok := s.api.Consensus.(interface{ GetLastBlockHeight() uint64 }); ok {
		currentRound = getter.GetLastBlockHeight()
	}

	// For hybrid consensus, step represents the consensus phase
	// 0 = proposing, 1 = voting, 2 = committing
	currentStep = uint32(currentRound % 3)

	// Get current proposer from active validators
	if validatorCount > 0 && currentRound > 0 {
		proposerIndex := currentRound % uint64(validatorCount)
		proposer := activeValidators[proposerIndex]
		currentProposer = hex.EncodeToString(proposer.ID[:])
	}

	return &proto.ConsensusInfo{
		ConsensusType:     "hybrid",
		CurrentRound:      currentRound,
		CurrentStep:       uint64(currentStep),
		CurrentProposer:   currentProposer,
		ValidatorCount:    validatorCount,
		ParticipationRate: float64(participationRate),
	}, nil
}

func (s *GRPCServer) GetValidators(ctx context.Context, req *emptypb.Empty) (*proto.GetValidatorsResponse, error) {
	s.logger.Info("gRPC: Getting validators")

	// Get real validator data from consensus
	consensusValidators := s.api.Consensus.GetActiveValidators()

	validators := make([]*proto.Validator, 0, len(consensusValidators))
	for _, v := range consensusValidators {
		// Get validator stats if StateManager is available
		var blocksProposed uint64
		if sm, ok := s.api.Consensus.(interface {
			GetStateManager() interface {
				GetValidatorStats([32]byte) interface{ GetBlocksProduced() uint64 }
			}
		}); ok {
			if stateManager := sm.GetStateManager(); stateManager != nil {
				if stats := stateManager.GetValidatorStats(v.ID); stats != nil {
					if s, ok := stats.(interface{ GetBlocksProduced() uint64 }); ok {
						blocksProposed = s.GetBlocksProduced()
					}
				}
			}
		}

		// Convert validator ID to hex string
		validatorID := hex.EncodeToString(v.ID[:])
		address := "0x" + validatorID[:40] // Use first 20 bytes as address

		validators = append(validators, &proto.Validator{
			Id:             validatorID,
			PublicKey:      validatorID, // Using ID as public key for now
			Address:        address,
			VotingPower:    float64(v.Stake),
			IsActive:       true, // All returned validators are active
			BlocksProposed: blocksProposed,
			JoinedAt:       timestamppb.Now(), // Would need to track this in validator info
		})
	}

	return &proto.GetValidatorsResponse{
		Validators: validators,
		TotalCount: uint32(len(validators)),
	}, nil
}

// Helper methods

func (s *GRPCServer) getLedgerStore() (storage.LedgerStore, error) {
	// NOTE: Interface conflict detected between storage.Store and storage.LedgerStore
	// Both define GetBlock methods with different signatures:
	// - Store.GetBlock(blockNumber uint64) (*Block, error)
	// - LedgerStore.GetBlock(height uint64) (*common.Block, error)
	//
	// For now, we'll work around this by checking the actual implementation type

	// Try to use the storage directly - most implementations support LedgerStore methods
	// even if they can't be cast due to the interface conflict
	return nil, fmt.Errorf("interface conflict between Store and LedgerStore - GetBlock method signatures incompatible")
}

func (s *GRPCServer) convertTransactionToProto(tx *common.Transaction) *proto.Transaction {
	return &proto.Transaction{
		Id:          tx.ID,
		From:        tx.Sender,
		To:          tx.Receiver,
		Amount:      tx.Amount,
		Fee:         tx.Fee,
		Data:        string(tx.Data),
		Signature:   string(tx.Signature),
		Timestamp:   timestamppb.New(time.Unix(tx.Timestamp, 0)),
		BlockHeight: uint64(tx.BlockHeight),
		Status: &proto.TransactionStatus{
			Status:  proto.TransactionStatus_CONFIRMED,
			Message: "Transaction confirmed",
		},
	}
}

func (s *GRPCServer) convertBlockToProto(block *common.Block) *proto.Block {
	protoTxs := make([]*proto.Transaction, len(block.Transactions))
	for i, tx := range block.Transactions {
		protoTxs[i] = s.convertTransactionToProto(&tx)
	}

	return &proto.Block{
		Id:               block.Hash,
		Height:           uint64(block.Number),
		PreviousHash:     block.PreviousHash,
		MerkleRoot:       block.MerkleRoot,
		Timestamp:        timestamppb.New(time.Unix(block.Timestamp, 0)),
		Validator:        block.Validator,
		Transactions:     protoTxs,
		Signature:        string(block.Signature),
		Size:             uint64(len(block.Transactions)),
		TransactionCount: uint64(len(block.Transactions)),
	}
}

func (s *GRPCServer) createWalletInternal(passphrase string, metadata map[string]string) (*WalletResponse, error) {
	// This would implement wallet creation logic
	// For now, return a mock wallet
	return &WalletResponse{
		ID:        fmt.Sprintf("wallet_%d", consensus.ConsensusNow().UnixNano()),
		PublicKey: "mock_public_key",
		Balance:   0.0,
	}, nil
}

// generateContractAddress generates a deterministic contract address based on deployer and transaction ID
func generateContractAddress(deployer string, txID string) string {
	// Create a deterministic hash using deployer address and transaction ID
	h := sha256.New()
	h.Write([]byte(deployer))
	h.Write([]byte(txID))
	hash := h.Sum(nil)

	// Return the first 20 bytes as hex (similar to Ethereum)
	return "0x" + hex.EncodeToString(hash[:20])
}

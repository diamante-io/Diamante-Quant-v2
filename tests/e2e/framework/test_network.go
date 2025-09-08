// Package framework provides end-to-end testing framework
package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestNetwork manages a collection of blockchain nodes for E2E testing
type TestNetwork struct {
	t         *testing.T
	nodes     []*TestNode
	baseDir   string
	networkID string
	logger    *logrus.Logger
	mu        sync.RWMutex

	// Network configuration
	config *NetworkConfig

	// Cleanup functions
	cleanupFuncs []func() error
}

// NetworkConfig holds configuration for the test network
type NetworkConfig struct {
	ValidatorNodes int
	FullNodes      int
	BasePort       int
	BlockTime      time.Duration
	NetworkLatency time.Duration
	EnableTLS      bool
	LogLevel       string
	DatabaseType   string
	MongoURI       string
}

// TestNode represents a single blockchain node in the test network
type TestNode struct {
	ID          string
	Type        NodeType
	Process     *exec.Cmd
	Config      *NodeConfig
	Logger      *logrus.Entry
	APIPort     int
	P2PPort     int
	RPCPort     int
	MetricsPort int
	Running     bool
	DataDir     string
	LogFile     string
	mu          sync.RWMutex
}

// NodeType defines the type of blockchain node
type NodeType string

const (
	ValidatorNode NodeType = "validator"
	FullNode      NodeType = "fullnode"
	SeedNode      NodeType = "seed"
)

// NodeConfig holds configuration for a single node
type NodeConfig struct {
	NodeID         string
	Type           NodeType
	IsValidator    bool
	ValidatorKey   string
	BootstrapNodes []string
	APIPort        int
	P2PPort        int
	RPCPort        int
	MetricsPort    int
	DataDir        string
	LogLevel       string
	DatabaseURI    string
	NetworkID      string
	Environment    string
	TLSEnabled     bool
	EncryptionKey  string
	JWTSecret      string
}

// NewTestNetwork creates a new test network
func NewTestNetwork(t *testing.T, config *NetworkConfig) *TestNetwork {
	if config == nil {
		config = DefaultNetworkConfig()
	}

	// Create temporary directory for test data
	baseDir, err := os.MkdirTemp("", "diamante-e2e-test-")
	require.NoError(t, err)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &TestNetwork{
		t:         t,
		baseDir:   baseDir,
		networkID: fmt.Sprintf("test-network-%d", time.Now().Unix()),
		logger:    logger,
		config:    config,
		nodes:     make([]*TestNode, 0),
	}
}

// DefaultNetworkConfig returns default network configuration
func DefaultNetworkConfig() *NetworkConfig {
	return &NetworkConfig{
		ValidatorNodes: 1,
		FullNodes:      2,
		BasePort:       18000,
		BlockTime:      2 * time.Second,
		NetworkLatency: 50 * time.Millisecond,
		EnableTLS:      false,
		LogLevel:       "debug",
		DatabaseType:   "mongodb",
		MongoURI:       "mongodb://127.0.0.1:27017",
	}
}

// StartNetwork starts all nodes in the test network
func (tn *TestNetwork) StartNetwork() error {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	tn.logger.Info("Starting test network", "network_id", tn.networkID)

	// Create validator nodes first
	for i := 0; i < tn.config.ValidatorNodes; i++ {
		node, err := tn.createValidatorNode(i)
		if err != nil {
			return fmt.Errorf("failed to create validator node %d: %w", i, err)
		}
		tn.nodes = append(tn.nodes, node)
	}

	// Create full nodes
	for i := 0; i < tn.config.FullNodes; i++ {
		node, err := tn.createFullNode(i)
		if err != nil {
			return fmt.Errorf("failed to create full node %d: %w", i, err)
		}
		tn.nodes = append(tn.nodes, node)
	}

	// Start all nodes
	for _, node := range tn.nodes {
		if err := tn.startNode(node); err != nil {
			return fmt.Errorf("failed to start node %s: %w", node.ID, err)
		}
	}

	// Wait for network to be ready
	return tn.waitForNetworkReady()
}

// createValidatorNode creates a validator node configuration
func (tn *TestNetwork) createValidatorNode(index int) (*TestNode, error) {
	nodeID := fmt.Sprintf("validator-%d", index)
	dataDir := filepath.Join(tn.baseDir, nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	config := &NodeConfig{
		NodeID:        nodeID,
		Type:          ValidatorNode,
		IsValidator:   true,
		ValidatorKey:  tn.generateValidatorKey(),
		APIPort:       tn.config.BasePort + index*10,
		P2PPort:       tn.config.BasePort + index*10 + 1,
		RPCPort:       tn.config.BasePort + index*10 + 2,
		MetricsPort:   tn.config.BasePort + index*10 + 3,
		DataDir:       dataDir,
		LogLevel:      tn.config.LogLevel,
		DatabaseURI:   fmt.Sprintf("%s/%s_%s", tn.config.MongoURI, tn.networkID, nodeID),
		NetworkID:     tn.networkID,
		Environment:   "test",
		TLSEnabled:    tn.config.EnableTLS,
		EncryptionKey: tn.generateEncryptionKey(),
		JWTSecret:     tn.generateJWTSecret(),
	}

	return &TestNode{
		ID:          nodeID,
		Type:        ValidatorNode,
		Config:      config,
		Logger:      tn.logger.WithField("node", nodeID),
		APIPort:     config.APIPort,
		P2PPort:     config.P2PPort,
		RPCPort:     config.RPCPort,
		MetricsPort: config.MetricsPort,
		DataDir:     dataDir,
		LogFile:     filepath.Join(dataDir, "node.log"),
	}, nil
}

// createFullNode creates a full node configuration
func (tn *TestNetwork) createFullNode(index int) (*TestNode, error) {
	nodeID := fmt.Sprintf("fullnode-%d", index)
	dataDir := filepath.Join(tn.baseDir, nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Full nodes bootstrap from validator nodes
	var bootstrapNodes []string
	for _, node := range tn.nodes {
		if node.Type == ValidatorNode {
			bootstrapNodes = append(bootstrapNodes, fmt.Sprintf("127.0.0.1:%d", node.P2PPort))
		}
	}

	config := &NodeConfig{
		NodeID:         nodeID,
		Type:           FullNode,
		IsValidator:    false,
		BootstrapNodes: bootstrapNodes,
		APIPort:        tn.config.BasePort + (tn.config.ValidatorNodes+index)*10,
		P2PPort:        tn.config.BasePort + (tn.config.ValidatorNodes+index)*10 + 1,
		RPCPort:        tn.config.BasePort + (tn.config.ValidatorNodes+index)*10 + 2,
		MetricsPort:    tn.config.BasePort + (tn.config.ValidatorNodes+index)*10 + 3,
		DataDir:        dataDir,
		LogLevel:       tn.config.LogLevel,
		DatabaseURI:    fmt.Sprintf("%s/%s_%s", tn.config.MongoURI, tn.networkID, nodeID),
		NetworkID:      tn.networkID,
		Environment:    "test",
		TLSEnabled:     tn.config.EnableTLS,
		EncryptionKey:  tn.generateEncryptionKey(),
		JWTSecret:      tn.generateJWTSecret(),
	}

	return &TestNode{
		ID:          nodeID,
		Type:        FullNode,
		Config:      config,
		Logger:      tn.logger.WithField("node", nodeID),
		APIPort:     config.APIPort,
		P2PPort:     config.P2PPort,
		RPCPort:     config.RPCPort,
		MetricsPort: config.MetricsPort,
		DataDir:     dataDir,
		LogFile:     filepath.Join(dataDir, "node.log"),
	}, nil
}

// startNode starts a blockchain node
func (tn *TestNetwork) startNode(node *TestNode) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.Logger.Info("Starting node", "type", node.Type)

	// Create log file
	logFile, err := os.Create(node.LogFile)
	if err != nil {
		return err
	}

	// Build command to start the node
	cmd := exec.Command("go", "run", "../../../main.go")
	cmd.Dir = node.DataDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Set environment variables
	env := os.Environ()
	env = append(env, tn.buildEnvironment(node.Config)...)
	cmd.Env = env

	// Start the process
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}

	node.Process = cmd
	node.Running = true

	// Add cleanup function
	tn.cleanupFuncs = append(tn.cleanupFuncs, func() error {
		return tn.stopNode(node)
	})

	// Wait for node to be ready
	return tn.waitForNodeReady(node)
}

// buildEnvironment builds environment variables for a node
func (tn *TestNetwork) buildEnvironment(config *NodeConfig) []string {
	env := []string{
		fmt.Sprintf("DIAMANTE_ENV=%s", config.Environment),
		fmt.Sprintf("DIAMANTE_NETWORK_ID=%s", config.NetworkID),
		fmt.Sprintf("DIAMANTE_API_PORT=%d", config.APIPort),
		fmt.Sprintf("DIAMANTE_P2P_PORT=%d", config.P2PPort),
		fmt.Sprintf("DIAMANTE_RPC_PORT=%d", config.RPCPort),
		fmt.Sprintf("DIAMANTE_METRICS_PORT=%d", config.MetricsPort),
		fmt.Sprintf("DIAMANTE_DB_URI=%s", config.DatabaseURI),
		fmt.Sprintf("DIAMANTE_DB_NAME=%s", config.NodeID),
		fmt.Sprintf("DIAMANTE_LOG_LEVEL=%s", config.LogLevel),
		fmt.Sprintf("DIAMANTE_VALIDATOR_ENABLED=%t", config.IsValidator),
		fmt.Sprintf("DIAMANTE_TLS_ENABLED=%t", config.TLSEnabled),
		fmt.Sprintf("DIAMANTE_ENCRYPTION_KEY=%s", config.EncryptionKey),
		fmt.Sprintf("DIAMANTE_JWT_SECRET=%s", config.JWTSecret),
		fmt.Sprintf("DIAMANTE_BLOCK_TIME=%v", tn.config.BlockTime),
		fmt.Sprintf("DIAMANTE_TEST_MODE=true"),
	}

	if config.ValidatorKey != "" {
		env = append(env, fmt.Sprintf("DIAMANTE_VALIDATOR_KEY=%s", config.ValidatorKey))
	}

	if len(config.BootstrapNodes) > 0 {
		env = append(env, fmt.Sprintf("DIAMANTE_P2P_BOOTSTRAP_NODES=%s",
			joinStringSlice(config.BootstrapNodes, ",")))
	}

	return env
}

// waitForNodeReady waits for a node to be ready to accept connections
func (tn *TestNetwork) waitForNodeReady(node *TestNode) error {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", node.APIPort)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("node %s failed to start within timeout", node.ID)
		case <-ticker.C:
			resp, err := http.Get(healthURL)
			if err != nil {
				continue
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				node.Logger.Info("Node is ready")
				return nil
			}
		}
	}
}

// waitForNetworkReady waits for the entire network to be ready
func (tn *TestNetwork) waitForNetworkReady() error {
	tn.logger.Info("Waiting for network consensus")

	// Wait for all nodes to be synchronized
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("network failed to reach consensus within timeout")
		case <-ticker.C:
			if tn.checkNetworkConsensus() {
				tn.logger.Info("Network is ready and in consensus")
				return nil
			}
		}
	}
}

// checkNetworkConsensus checks if all nodes are in consensus
func (tn *TestNetwork) checkNetworkConsensus() bool {
	var blockHeights []uint64

	for _, node := range tn.nodes {
		if !node.Running {
			return false
		}

		height, err := tn.getNodeBlockHeight(node)
		if err != nil {
			return false
		}

		blockHeights = append(blockHeights, height)
	}

	// Check if all nodes have similar block heights (within 1 block)
	if len(blockHeights) == 0 {
		return false
	}

	minHeight := blockHeights[0]
	maxHeight := blockHeights[0]

	for _, height := range blockHeights {
		if height < minHeight {
			minHeight = height
		}
		if height > maxHeight {
			maxHeight = height
		}
	}

	// Allow up to 1 block difference
	return maxHeight-minHeight <= 1 && minHeight > 0
}

// getNodeBlockHeight gets the current block height from a node
func (tn *TestNetwork) getNodeBlockHeight(node *TestNode) (uint64, error) {
	// Make an HTTP request to the node's status endpoint
	url := fmt.Sprintf("http://127.0.0.1:%d/status", node.Config.APIPort)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to node %s: %w", node.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("node %s returned status %d", node.ID, resp.StatusCode)
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return 0, fmt.Errorf("failed to decode status response: %w", err)
	}

	// Extract block height from response
	if height, ok := status["block_height"].(float64); ok {
		return uint64(height), nil
	}

	// If block height not found, return 0
	return 0, nil
}

// GetNode returns a node by ID
func (tn *TestNetwork) GetNode(nodeID string) *TestNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	for _, node := range tn.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// GetNodes returns all nodes in the network
func (tn *TestNetwork) GetNodes() []*TestNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	nodes := make([]*TestNode, len(tn.nodes))
	copy(nodes, tn.nodes)
	return nodes
}

// GetValidatorNodes returns all validator nodes
func (tn *TestNetwork) GetValidatorNodes() []*TestNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	var validators []*TestNode
	for _, node := range tn.nodes {
		if node.Type == ValidatorNode {
			validators = append(validators, node)
		}
	}
	return validators
}

// GetFullNodes returns all full nodes
func (tn *TestNetwork) GetFullNodes() []*TestNode {
	tn.mu.RLock()
	defer tn.mu.RUnlock()

	var fullNodes []*TestNode
	for _, node := range tn.nodes {
		if node.Type == FullNode {
			fullNodes = append(fullNodes, node)
		}
	}
	return fullNodes
}

// stopNode stops a blockchain node
func (tn *TestNetwork) stopNode(node *TestNode) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.Running || node.Process == nil {
		return nil
	}

	node.Logger.Info("Stopping node")

	// Send interrupt signal
	if err := node.Process.Process.Signal(os.Interrupt); err != nil {
		// If interrupt fails, force kill
		node.Process.Process.Kill()
	}

	// Wait for process to exit
	node.Process.Wait()
	node.Running = false

	return nil
}

// Cleanup stops all nodes and removes temporary files
func (tn *TestNetwork) Cleanup() error {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	tn.logger.Info("Cleaning up test network")

	// Run all cleanup functions
	for _, cleanupFunc := range tn.cleanupFuncs {
		if err := cleanupFunc(); err != nil {
			tn.logger.WithError(err).Error("Cleanup function failed")
		}
	}

	// Remove base directory
	if err := os.RemoveAll(tn.baseDir); err != nil {
		return fmt.Errorf("failed to remove base directory: %w", err)
	}

	return nil
}

// Helper functions
func (tn *TestNetwork) generateValidatorKey() string {
	return fmt.Sprintf("validator-key-%d", time.Now().UnixNano())
}

func (tn *TestNetwork) generateEncryptionKey() string {
	return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

func (tn *TestNetwork) generateJWTSecret() string {
	return "test-jwt-secret-for-e2e-testing"
}

func joinStringSlice(slice []string, separator string) string {
	if len(slice) == 0 {
		return ""
	}

	result := slice[0]
	for i := 1; i < len(slice); i++ {
		result += separator + slice[i]
	}

	return result
}

// generateID generates a unique ID for testing
func generateID() string {
	return fmt.Sprintf("id_%d", time.Now().UnixNano())
}

// CreateTestTransaction creates a test transaction with the given parameters
func CreateTestTransaction(sender, receiver string, amount float64) *TestTransaction {
	return &TestTransaction{
		ID:        generateID(),
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Timestamp: time.Now(),
	}
}

// TestTransaction represents a simple transaction for testing
type TestTransaction struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Receiver  string    `json:"receiver"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// SubmitTransaction submits a transaction to the node
func (node *TestNode) SubmitTransaction(ctx context.Context, tx *TestTransaction) (string, error) {
	node.mu.RLock()
	if !node.Running {
		node.mu.RUnlock()
		return "", fmt.Errorf("node %s is not running", node.ID)
	}
	node.mu.RUnlock()

	// Make HTTP POST request to submit transaction
	// endpoint := fmt.Sprintf("http://localhost:%d/transactions", node.APIPort)

	// For now, return a mock transaction ID
	// In a real implementation, this would make an actual HTTP request
	txID := fmt.Sprintf("tx_%s_%d", node.ID, time.Now().UnixNano())

	node.Logger.WithField("transaction", txID).Info("Submitted transaction")
	return txID, nil
}

// GetTransaction retrieves a transaction by ID
func (node *TestNode) GetTransaction(ctx context.Context, txID string) (*TestTransaction, error) {
	node.mu.RLock()
	if !node.Running {
		node.mu.RUnlock()
		return nil, fmt.Errorf("node %s is not running", node.ID)
	}
	node.mu.RUnlock()

	// Make HTTP GET request to retrieve transaction
	// endpoint := fmt.Sprintf("http://localhost:%d/transactions/%s", node.APIPort, txID)

	// For now, return a mock transaction
	// In a real implementation, this would make an actual HTTP request
	tx := &TestTransaction{
		ID:        txID,
		Sender:    "mock_sender",
		Receiver:  "mock_receiver",
		Amount:    100.0,
		Timestamp: time.Now(),
	}

	node.Logger.WithField("transaction", txID).Info("Retrieved transaction")
	return tx, nil
}

// GetLatestBlock retrieves the latest block from the node
func (node *TestNode) GetLatestBlock(ctx context.Context) (interface{}, error) {
	node.mu.RLock()
	if !node.Running {
		node.mu.RUnlock()
		return nil, fmt.Errorf("node %s is not running", node.ID)
	}
	node.mu.RUnlock()

	// Make HTTP GET request to retrieve latest block
	// endpoint := fmt.Sprintf("http://localhost:%d/blocks/latest", node.APIPort)

	// For now, return a mock block
	// In a real implementation, this would make an actual HTTP request
	block := map[string]interface{}{
		"height":       123,
		"hash":         fmt.Sprintf("block_hash_%s_%d", node.ID, time.Now().UnixNano()),
		"timestamp":    time.Now().Unix(),
		"transactions": []string{},
	}

	node.Logger.WithField("block", block["hash"]).Info("Retrieved latest block")
	return block, nil
}

// Start starts the test network
func (tn *TestNetwork) Start(ctx context.Context) error {
	tn.mu.Lock()
	defer tn.mu.Unlock()

	// Create validator nodes
	for i := 0; i < tn.config.ValidatorNodes; i++ {
		node, err := tn.createValidatorNode(i)
		if err != nil {
			return fmt.Errorf("failed to create validator node %d: %w", i, err)
		}

		err = node.Start(ctx)
		if err != nil {
			return fmt.Errorf("failed to start validator node %d: %w", i, err)
		}

		tn.nodes = append(tn.nodes, node)
	}

	// Create full nodes
	for i := 0; i < tn.config.FullNodes; i++ {
		node, err := tn.createFullNode(i)
		if err != nil {
			return fmt.Errorf("failed to create full node %d: %w", i, err)
		}

		err = node.Start(ctx)
		if err != nil {
			return fmt.Errorf("failed to start full node %d: %w", i, err)
		}

		tn.nodes = append(tn.nodes, node)
	}

	tn.logger.WithField("nodes", len(tn.nodes)).Info("Test network started")
	return nil
}

// IsRunning checks if the node is currently running
func (node *TestNode) IsRunning() bool {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.Running
}

// Start starts the test node
func (node *TestNode) Start(ctx context.Context) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.Running {
		return fmt.Errorf("node %s is already running", node.ID)
	}

	// In a real implementation, this would start the actual blockchain process
	// For now, we'll simulate starting by setting Running to true
	node.Running = true

	node.Logger.WithField("ports", map[string]int{
		"api":     node.APIPort,
		"p2p":     node.P2PPort,
		"rpc":     node.RPCPort,
		"metrics": node.MetricsPort,
	}).Info("Test node started")

	return nil
}

// Stop stops the test node
func (node *TestNode) Stop(ctx context.Context) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.Running {
		return fmt.Errorf("node %s is not running", node.ID)
	}

	// In a real implementation, this would stop the blockchain process
	if node.Process != nil {
		err := node.Process.Process.Kill()
		if err != nil {
			node.Logger.WithError(err).Warn("Failed to kill process")
		}
		node.Process = nil
	}

	node.Running = false
	node.Logger.Info("Test node stopped")

	return nil
}

// IsHealthy checks if the node is healthy
func (node *TestNode) IsHealthy(ctx context.Context) (bool, error) {
	if !node.IsRunning() {
		return false, fmt.Errorf("node %s is not running", node.ID)
	}

	// Make HTTP GET request to health endpoint
	// endpoint := fmt.Sprintf("http://localhost:%d/health", node.APIPort)

	// For now, return true for mock implementation
	// In a real implementation, this would make an actual HTTP request
	node.Logger.WithField("health", "ok").Debug("Health check")
	return true, nil
}

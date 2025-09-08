// network/network_manager_test.go
package network_test

import (
	"sync"
	"testing"

	"diamante/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockDiscovery implements Discovery interface for testing
type MockDiscovery struct {
	peers     []string
	isRunning bool
	mu        sync.RWMutex
	nm        *network.NetworkManager
}

func NewMockDiscovery(peers []string) *MockDiscovery {
	return &MockDiscovery{
		peers: peers,
	}
}

func (md *MockDiscovery) Start() {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.isRunning = true
}

func (md *MockDiscovery) Stop() {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.isRunning = false
}

func (md *MockDiscovery) GetPeers() []string {
	md.mu.RLock()
	defer md.mu.RUnlock()
	return md.peers
}

func (md *MockDiscovery) GetKnownPeers() []string {
	md.mu.RLock()
	defer md.mu.RUnlock()
	return md.peers
}

func (md *MockDiscovery) SetNetworkManager(nm *network.NetworkManager) {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.nm = nm
}

func (md *MockDiscovery) AddPeer(addr string) {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.peers = append(md.peers, addr)
}

// MockConsensusAdapter implements ConsensusAdapter for testing
type MockConsensusAdapter struct {
	paused bool
	mu     sync.RWMutex
}

func NewMockConsensusAdapter() *MockConsensusAdapter {
	return &MockConsensusAdapter{}
}

func (mca *MockConsensusAdapter) PauseConsensus() {
	mca.mu.Lock()
	defer mca.mu.Unlock()
	mca.paused = true
}

func (mca *MockConsensusAdapter) ResumeConsensus() {
	mca.mu.Lock()
	defer mca.mu.Unlock()
	mca.paused = false
}

func (mca *MockConsensusAdapter) GetConsensusState() interface{} {
	mca.mu.RLock()
	defer mca.mu.RUnlock()
	return map[string]interface{}{
		"paused": mca.paused,
		"status": "running",
	}
}

func TestNetworkManager_Creation(t *testing.T) {
	discovery := NewMockDiscovery([]string{})
	nm := network.NewNetworkManager("localhost:0", discovery, nil, nil)

	require.NotNil(t, nm)
	// Cannot access unexported field localAddr from different package
}

func TestNetworkManager_Start(t *testing.T) {
	discovery := NewMockDiscovery([]string{})
	nm := network.NewNetworkManager("localhost:0", discovery, nil, nil)

	err := nm.Start()
	require.NoError(t, err)

	// Clean up
	nm.Stop()
}

func TestNetworkManager_PeerList(t *testing.T) {
	discovery := NewMockDiscovery([]string{"peer1", "peer2"})
	nm := network.NewNetworkManager("localhost:0", discovery, nil, nil)

	peers, err := nm.GetPeerList()
	require.NoError(t, err)
	assert.Len(t, peers, 2)
}

func TestMockDiscovery_Basic(t *testing.T) {
	discovery := NewMockDiscovery([]string{"localhost:8081", "localhost:8082"})

	peers := discovery.GetKnownPeers()
	assert.Len(t, peers, 2)
	assert.Contains(t, peers, "localhost:8081")
	assert.Contains(t, peers, "localhost:8082")

	discovery.Start()
	discovery.Stop()
}

func TestMockConsensusAdapter_Basic(t *testing.T) {
	adapter := NewMockConsensusAdapter()

	state := adapter.GetConsensusState()
	assert.NotNil(t, state)

	adapter.PauseConsensus()
	adapter.ResumeConsensus()
}

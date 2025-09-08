// Package network provides tests for network components
package network

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"diamante/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestPeerPair(t *testing.T) (*network.Peer, *network.Peer, func()) {
	// Create connected socket pair
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	// Client connects
	clientConn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	// Server accepts
	serverConn, err := listener.Accept()
	require.NoError(t, err)

	// Create peers
	// Since Peer requires NetworkManager, we create mock ones
	mockNM := &network.NetworkManager{}
	clientPeer := network.NewPeer("client-addr", clientConn, mockNM)
	serverPeer := network.NewPeer("server-addr", serverConn, mockNM)

	cleanup := func() {
		clientPeer.Close()
		serverPeer.Close()
		listener.Close()
	}

	return clientPeer, serverPeer, cleanup
}

func TestPeer(t *testing.T) {
	t.Run("BasicCommunication", func(t *testing.T) {
		client, server, cleanup := createTestPeerPair(t)
		defer cleanup()

		// Start the peers
		client.Run()
		server.Run()

		// Send from client to server
		payload, _ := json.Marshal(map[string]interface{}{
			"message": "hello from client",
		})
		testMsg := network.Message{
			Type:    "test",
			Payload: json.RawMessage(payload),
		}
		err := client.Send(testMsg)
		assert.NoError(t, err)

		// Give time for message to be sent
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("PeerClose", func(t *testing.T) {
		client, _, cleanup := createTestPeerPair(t)
		defer cleanup()

		// Close peer
		err := client.Close()
		assert.NoError(t, err)

		// Try to send after close
		testMsg := network.Message{
			Type: "test",
		}
		err = client.Send(testMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closed")
	})

	t.Run("PeerTimeout", func(t *testing.T) {
		client, _, cleanup := createTestPeerPair(t)
		defer cleanup()

		// Don't start the peer, so channel will be full
		// Fill up the channel
		for i := 0; i < 32; i++ {
			msg := network.Message{
				Type: fmt.Sprintf("test-%d", i),
			}
			client.Send(msg)
		}

		// Next send should timeout
		msg := network.Message{
			Type: "overflow",
		}
		err := client.Send(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})
}

// mockDiscovery implements network.Discovery interface for testing
type mockDiscovery struct{}

func (m *mockDiscovery) Start() {}
func (m *mockDiscovery) Stop()  {}
func (m *mockDiscovery) GetKnownPeers() []string {
	return []string{}
}
func (m *mockDiscovery) SetNetworkManager(nm *network.NetworkManager) {}

func TestNetworkManager(t *testing.T) {
	t.Run("CreateNetworkManager", func(t *testing.T) {
		listenAddr := "127.0.0.1:0"

		// Create minimal discovery
		discovery := &mockDiscovery{}

		nm := network.NewNetworkManager(listenAddr, discovery, nil, nil)
		require.NotNil(t, nm)

		// Clean up
		nm.Close()
	})

	t.Run("PeerConnection", func(t *testing.T) {
		discovery := &mockDiscovery{}
		nm := network.NewNetworkManager("127.0.0.1:0", discovery, nil, nil)
		defer nm.Close()

		// The NetworkManager handles peers internally
		// We can test by checking the Peers() method if it exists
		// For now, just test that manager was created successfully
		assert.NotNil(t, nm)
	})
}

func BenchmarkPeerCommunication(b *testing.B) {
	client, server, cleanup := createTestPeerPair(&testing.T{})
	defer cleanup()

	client.Run()
	server.Run()

	payload, _ := json.Marshal(map[string]interface{}{
		"data": "benchmark message data",
	})
	msg := network.Message{
		Type:    "benchmark",
		Payload: json.RawMessage(payload),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Send(msg); err != nil {
			b.Fatal(err)
		}
	}
}

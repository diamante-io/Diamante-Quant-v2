package network

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"
)

func TestNetworkManager(t *testing.T) {
	// We'll pick random localhost ports for each test run
	localAddr1 := getFreePort()
	localAddr2 := getFreePort()

	// Create minimal discovery stubs
	discovery1 := NewBasicDiscovery([]string{}, 3*time.Second, nil)
	discovery2 := NewBasicDiscovery([]string{localAddr1}, 3*time.Second, nil)

	manager1 := NewNetworkManager(localAddr1, discovery1)
	manager2 := NewNetworkManager(localAddr2, discovery2)
	discovery1.nm = manager1
	discovery2.nm = manager2

	// Start both managers
	if err := manager1.Start(); err != nil {
		t.Fatalf("manager1 start error: %v", err)
	}
	defer manager1.Stop()

	if err := manager2.Start(); err != nil {
		t.Fatalf("manager2 start error: %v", err)
	}
	defer manager2.Stop()

	// Wait a bit for discovery to do its job
	time.Sleep(2 * time.Second)

	// manager2 tries to dial manager1 directly
	if err := manager2.DialPeer(localAddr1); err != nil {
		t.Errorf("manager2 dial manager1 error: %v", err)
	}

	// Broadcast a message from manager1
	msg := Message{
		Type:    "test",
		Payload: "Hello from manager1",
	}
	manager1.Broadcast(msg)

	// Let them exchange a bit
	time.Sleep(2 * time.Second)

	health1 := manager1.GetNetworkHealth()
	health2 := manager2.GetNetworkHealth()
	log.Printf("Manager1 health: %d, Manager2 health: %d\n", health1, health2)
}

// getFreePort is a test helper to allocate a random free port on localhost.
func getFreePort() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("cannot get free port: %v", err))
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

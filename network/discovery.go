package network

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Discovery interface defines peer discovery logic.
type Discovery interface {
	Start()
	Stop()
	GetKnownPeers() []string
	// New method: allow setting (or updating) the network manager reference.
	SetNetworkManager(nm *NetworkManager)
}

type BasicDiscovery struct {
	mu           sync.RWMutex
	knownPeers   []string
	stopChan     chan struct{}
	pollInterval time.Duration
	nm           *NetworkManager
	logger       *logrus.Entry
}

// NewBasicDiscovery creates a new BasicDiscovery instance.
func NewBasicDiscovery(initialSeeds []string, poll time.Duration, nm *NetworkManager) *BasicDiscovery {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	return &BasicDiscovery{
		knownPeers:   initialSeeds,
		stopChan:     make(chan struct{}),
		pollInterval: poll,
		nm:           nm,
		logger:       logger.WithField("component", "BasicDiscovery"),
	}
}

func (bd *BasicDiscovery) Start() {
	go bd.runDiscovery()
}

func (bd *BasicDiscovery) Stop() {
	close(bd.stopChan)
}

func (bd *BasicDiscovery) GetKnownPeers() []string {
	bd.mu.RLock()
	defer bd.mu.RUnlock()
	return append([]string{}, bd.knownPeers...)
}

// SetNetworkManager sets (or updates) the network manager reference.
func (bd *BasicDiscovery) SetNetworkManager(nm *NetworkManager) {
	bd.mu.Lock()
	defer bd.mu.Unlock()
	bd.nm = nm
}

func (bd *BasicDiscovery) runDiscovery() {
	ticker := time.NewTicker(bd.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bd.stopChan:
			bd.logger.Info("BasicDiscovery stopping")
			return
		case <-ticker.C:
			bd.discoverPeers()
		}
	}
}

func (bd *BasicDiscovery) discoverPeers() {
	bd.mu.RLock()
	seeds := append([]string{}, bd.knownPeers...)
	bd.mu.RUnlock()

	for _, seed := range seeds {
		// Only attempt to dial if we have a network manager reference.
		bd.mu.RLock()
		nm := bd.nm
		bd.mu.RUnlock()
		if nm == nil || seed == nm.localAddr {
			continue
		}
		bd.logger.WithField("seed", seed).Debug("Discovery: Attempting to dial seed")
		if err := nm.DialPeer(seed); err != nil {
			bd.logger.WithFields(logrus.Fields{
				"seed":  seed,
				"error": err,
			}).Error("Discovery: Failed to dial seed")
			// Continue with other seeds instead of failing completely
		}
	}
}

package p2p

import (
	"math"
	"sort"
	"sync"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// PeerScorer manages peer reputation scores
// Score factors:
// - Message validity rate (40% weight)
// - Latency (20% weight)
// - Uptime (20% weight)
// - Bandwidth contribution (20% weight)
type PeerScorer struct {
	scores   map[string]*PeerScore
	mu       sync.RWMutex
	config   *ScoringConfig
	decay    float64 // Score decay factor
	minScore float64 // Minimum score before ban
	maxScore float64 // Maximum possible score
	logger   *logrus.Entry

	// Decay timer
	decayTicker *time.Ticker
	stopCh      chan struct{}
}

// PeerScore represents the reputation score and statistics for a peer
type PeerScore struct {
	// Core score
	Score float64

	// Message statistics
	MessagesSent     uint64
	MessagesReceived uint64
	InvalidMessages  uint64
	ValidMessages    uint64

	// Bandwidth statistics
	BytesSent     uint64
	BytesReceived uint64

	// Connection statistics
	ConnectionTime  time.Time
	LastActivity    time.Time
	LatencyEWMA     float64 // Exponentially weighted moving average
	DisconnectCount int
	ReconnectCount  int

	// Behavior tracking
	BehaviorFlags   uint32 // Bit flags for various behaviors
	LastScoreUpdate time.Time

	// Component scores (for debugging/analysis)
	ValidityScore  float64
	LatencyScore   float64
	UptimeScore    float64
	BandwidthScore float64

	mu sync.RWMutex
}

// ScoringConfig holds configuration for peer scoring
type ScoringConfig struct {
	InitialScore  float64       // Initial score for new peers
	MaxScore      float64       // Maximum possible score
	MinScore      float64       // Minimum score (below this = ban)
	BanThreshold  float64       // Score below which peer is banned
	DecayInterval time.Duration // How often to decay scores
	DecayFactor   float64       // Factor by which to decay scores

	// Scoring weights (must sum to 1.0)
	LatencyWeight   float64 // Weight for latency component
	ValidityWeight  float64 // Weight for message validity component
	UptimeWeight    float64 // Weight for uptime component
	BandwidthWeight float64 // Weight for bandwidth contribution component

	// Scoring parameters
	MaxLatency            time.Duration // Latency above this gets 0 score
	OptimalLatency        time.Duration // Latency below this gets max score
	InvalidMessagePenalty float64       // Penalty per invalid message
	ValidMessageReward    float64       // Reward per valid message
	DisconnectPenalty     float64       // Penalty per disconnect
	ReconnectPenalty      float64       // Penalty per reconnect

	// Bandwidth scoring
	MinBandwidthForReward uint64 // Minimum bandwidth for positive score
	OptimalBandwidth      uint64 // Bandwidth for maximum score
}

// Behavior flags for tracking peer behavior
const (
	BehaviorSpamming   uint32 = 1 << iota // Peer is sending too many messages
	BehaviorSlow                          // Peer is consistently slow
	BehaviorUnreliable                    // Peer disconnects frequently
	BehaviorMalicious                     // Peer sends invalid messages
	BehaviorFlood                         // Peer floods with large messages
	BehaviorStale                         // Peer hasn't been active recently
)

// DefaultScoringConfig returns a default scoring configuration
func DefaultScoringConfig() *ScoringConfig {
	return &ScoringConfig{
		InitialScore:  50.0,
		MaxScore:      100.0,
		MinScore:      0.0,
		BanThreshold:  10.0,
		DecayInterval: 1 * time.Hour,
		DecayFactor:   0.95, // 5% decay per hour

		// Weights (sum to 1.0)
		ValidityWeight:  0.4,
		LatencyWeight:   0.2,
		UptimeWeight:    0.2,
		BandwidthWeight: 0.2,

		// Scoring parameters
		MaxLatency:            5 * time.Second,
		OptimalLatency:        100 * time.Millisecond,
		InvalidMessagePenalty: 5.0,
		ValidMessageReward:    0.1,
		DisconnectPenalty:     2.0,
		ReconnectPenalty:      1.0,

		// Bandwidth thresholds
		MinBandwidthForReward: 1024 * 1024,      // 1MB
		OptimalBandwidth:      10 * 1024 * 1024, // 10MB
	}
}

// NewPeerScorer creates a new peer scorer with the given configuration
func NewPeerScorer(config *ScoringConfig, logger *logrus.Logger) *PeerScorer {
	if config == nil {
		config = DefaultScoringConfig()
	}

	if logger == nil {
		logger = logrus.New()
	}

	// Validate configuration
	totalWeight := config.ValidityWeight + config.LatencyWeight +
		config.UptimeWeight + config.BandwidthWeight
	if math.Abs(totalWeight-1.0) > 0.001 {
		logger.Warn("Scoring weights do not sum to 1.0, normalizing",
			"total", totalWeight)

		// Normalize weights
		config.ValidityWeight /= totalWeight
		config.LatencyWeight /= totalWeight
		config.UptimeWeight /= totalWeight
		config.BandwidthWeight /= totalWeight
	}

	ps := &PeerScorer{
		scores:   make(map[string]*PeerScore),
		config:   config,
		decay:    config.DecayFactor,
		minScore: config.MinScore,
		maxScore: config.MaxScore,
		logger:   logger.WithField("component", "peer_scorer"),
		stopCh:   make(chan struct{}),
	}

	// Start decay timer
	ps.decayTicker = time.NewTicker(config.DecayInterval)
	go ps.decayLoop()

	return ps
}

// UpdateScore updates a peer's score based on an event
func (ps *PeerScorer) UpdateScore(peerID string, event ScoreEvent, value ...interface{}) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	score, exists := ps.scores[peerID]
	if !exists {
		score = &PeerScore{
			Score:          ps.config.InitialScore,
			ConnectionTime: consensus.ConsensusNow(),
			LastActivity:   consensus.ConsensusNow(),
			LatencyEWMA:    float64(ps.config.OptimalLatency.Milliseconds()),
		}
		ps.scores[peerID] = score
	}

	score.mu.Lock()
	defer score.mu.Unlock()

	score.LastActivity = consensus.ConsensusNow()
	score.LastScoreUpdate = consensus.ConsensusNow()

	switch event {
	case ScoreEventValidMessage:
		score.ValidMessages++
		score.MessagesSent++
		ps.applyValidMessageReward(score)

	case ScoreEventInvalidMessage:
		score.InvalidMessages++
		score.MessagesSent++
		ps.applyInvalidMessagePenalty(score)
		score.BehaviorFlags |= BehaviorMalicious

	case ScoreEventTimeout:
		ps.applyTimeoutPenalty(score)
		score.BehaviorFlags |= BehaviorSlow

	case ScoreEventDisconnect:
		score.DisconnectCount++
		ps.applyDisconnectPenalty(score)
		score.BehaviorFlags |= BehaviorUnreliable

	case ScoreEventReconnect:
		score.ReconnectCount++
		ps.applyReconnectPenalty(score)

	case ScoreEventBandwidthContribution:
		if len(value) >= 2 {
			if sent, ok := value[0].(uint64); ok {
				score.BytesSent += sent
			}
			if received, ok := value[1].(uint64); ok {
				score.BytesReceived += received
			}
		}

	case ScoreEventLatencyUpdate:
		if len(value) >= 1 {
			if latency, ok := value[0].(time.Duration); ok {
				ps.updateLatency(score, latency)
			}
		}

	case ScoreEventHandshakeSuccess:
		ps.applyHandshakeSuccess(score)

	case ScoreEventHandshakeFailure:
		ps.applyHandshakeFailure(score)
	}

	// Recalculate overall score
	ps.calculateOverallScore(score)

	// Apply bounds
	if score.Score > ps.maxScore {
		score.Score = ps.maxScore
	}
	if score.Score < ps.minScore {
		score.Score = ps.minScore
	}

	ps.logger.WithFields(logrus.Fields{
		"peer_id": peerID,
		"event":   event,
		"score":   score.Score,
	}).Debug("Updated peer score")
}

// GetScore returns the current score for a peer
func (ps *PeerScorer) GetScore(peerID string) float64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if score, exists := ps.scores[peerID]; exists {
		score.mu.RLock()
		defer score.mu.RUnlock()
		return score.Score
	}

	return ps.config.InitialScore
}

// IsBanned returns true if a peer is banned (score below threshold)
func (ps *PeerScorer) IsBanned(peerID string) bool {
	return ps.GetScore(peerID) < ps.config.BanThreshold
}

// GetTopPeers returns the top N peers by score
func (ps *PeerScorer) GetTopPeers(n int) []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	type peerWithScore struct {
		id    string
		score float64
	}

	peers := make([]peerWithScore, 0, len(ps.scores))
	for id, score := range ps.scores {
		score.mu.RLock()
		peers = append(peers, peerWithScore{id: id, score: score.Score})
		score.mu.RUnlock()
	}

	// Sort by score descending
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].score > peers[j].score
	})

	// Return top N
	if n > len(peers) {
		n = len(peers)
	}

	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = peers[i].id
	}

	return result
}

// DecayScores applies time-based decay to all peer scores
func (ps *PeerScorer) DecayScores() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	now := consensus.ConsensusNow()
	decayed := 0

	for peerID, score := range ps.scores {
		score.mu.Lock()

		// Apply time-based decay
		timeSinceUpdate := now.Sub(score.LastScoreUpdate)
		if timeSinceUpdate > ps.config.DecayInterval {
			decayPeriods := int(timeSinceUpdate / ps.config.DecayInterval)
			decayFactor := math.Pow(ps.config.DecayFactor, float64(decayPeriods))

			oldScore := score.Score
			score.Score *= decayFactor

			// Update component scores proportionally
			score.ValidityScore *= decayFactor
			score.LatencyScore *= decayFactor
			score.UptimeScore *= decayFactor
			score.BandwidthScore *= decayFactor

			score.LastScoreUpdate = now
			decayed++

			ps.logger.WithFields(logrus.Fields{
				"peer_id":      peerID,
				"old_score":    oldScore,
				"new_score":    score.Score,
				"decay_factor": decayFactor,
			}).Debug("Applied score decay")
		}

		// Check for stale peers
		if now.Sub(score.LastActivity) > 24*time.Hour {
			score.BehaviorFlags |= BehaviorStale
		}

		score.mu.Unlock()
	}

	if decayed > 0 {
		ps.logger.WithField("decayed_peers", decayed).Debug("Applied score decay")
	}
}

// ExportScores returns a map of peer scores for persistence
func (ps *PeerScorer) ExportScores() map[string]float64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	scores := make(map[string]float64, len(ps.scores))
	for id, score := range ps.scores {
		score.mu.RLock()
		scores[id] = score.Score
		score.mu.RUnlock()
	}

	return scores
}

// ImportScores loads peer scores from persistence
func (ps *PeerScorer) ImportScores(scores map[string]float64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	imported := 0
	for peerID, scoreValue := range scores {
		// Create or update peer score
		score, exists := ps.scores[peerID]
		if !exists {
			score = &PeerScore{
				Score:          scoreValue,
				ConnectionTime: consensus.ConsensusNow(),
				LastActivity:   consensus.ConsensusNow(),
				LatencyEWMA:    float64(ps.config.OptimalLatency.Milliseconds()),
			}
			ps.scores[peerID] = score
		} else {
			score.mu.Lock()
			score.Score = scoreValue
			score.mu.Unlock()
		}
		imported++
	}

	ps.logger.WithField("imported_scores", imported).Info("Imported peer scores")
}

// GetPeerScore returns detailed score information for a peer
func (ps *PeerScorer) GetPeerScore(peerID string) (*PeerScore, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if score, exists := ps.scores[peerID]; exists {
		score.mu.RLock()
		defer score.mu.RUnlock()

		// Return a copy to avoid race conditions
		return &PeerScore{
			Score:            score.Score,
			MessagesSent:     score.MessagesSent,
			MessagesReceived: score.MessagesReceived,
			InvalidMessages:  score.InvalidMessages,
			ValidMessages:    score.ValidMessages,
			BytesSent:        score.BytesSent,
			BytesReceived:    score.BytesReceived,
			ConnectionTime:   score.ConnectionTime,
			LastActivity:     score.LastActivity,
			LatencyEWMA:      score.LatencyEWMA,
			DisconnectCount:  score.DisconnectCount,
			ReconnectCount:   score.ReconnectCount,
			BehaviorFlags:    score.BehaviorFlags,
			LastScoreUpdate:  score.LastScoreUpdate,
			ValidityScore:    score.ValidityScore,
			LatencyScore:     score.LatencyScore,
			UptimeScore:      score.UptimeScore,
			BandwidthScore:   score.BandwidthScore,
		}, true
	}

	return nil, false
}

// RemovePeer removes a peer from scoring
func (ps *PeerScorer) RemovePeer(peerID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.scores, peerID)
	ps.logger.WithField("peer_id", peerID).Debug("Removed peer from scoring")
}

// GetStats returns scoring statistics
func (ps *PeerScorer) GetStats() map[string]interface{} {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	totalPeers := len(ps.scores)
	bannedPeers := 0
	avgScore := 0.0
	maxScore := 0.0
	minScore := ps.maxScore

	for _, score := range ps.scores {
		score.mu.RLock()
		if score.Score < ps.config.BanThreshold {
			bannedPeers++
		}
		avgScore += score.Score
		if score.Score > maxScore {
			maxScore = score.Score
		}
		if score.Score < minScore {
			minScore = score.Score
		}
		score.mu.RUnlock()
	}

	if totalPeers > 0 {
		avgScore /= float64(totalPeers)
	}

	return map[string]interface{}{
		"total_peers":    totalPeers,
		"banned_peers":   bannedPeers,
		"average_score":  avgScore,
		"max_score":      maxScore,
		"min_score":      minScore,
		"ban_threshold":  ps.config.BanThreshold,
		"decay_interval": ps.config.DecayInterval.String(),
	}
}

// Stop stops the peer scorer and cleans up resources
func (ps *PeerScorer) Stop() {
	close(ps.stopCh)
	if ps.decayTicker != nil {
		ps.decayTicker.Stop()
	}
}

// Private methods for score calculations

func (ps *PeerScorer) decayLoop() {
	for {
		select {
		case <-ps.decayTicker.C:
			ps.DecayScores()
		case <-ps.stopCh:
			return
		}
	}
}

func (ps *PeerScorer) applyValidMessageReward(score *PeerScore) {
	score.Score += ps.config.ValidMessageReward
}

func (ps *PeerScorer) applyInvalidMessagePenalty(score *PeerScore) {
	score.Score -= ps.config.InvalidMessagePenalty
}

func (ps *PeerScorer) applyTimeoutPenalty(score *PeerScore) {
	score.Score -= 1.0 // Fixed timeout penalty
}

func (ps *PeerScorer) applyDisconnectPenalty(score *PeerScore) {
	score.Score -= ps.config.DisconnectPenalty
}

func (ps *PeerScorer) applyReconnectPenalty(score *PeerScore) {
	score.Score -= ps.config.ReconnectPenalty
}

func (ps *PeerScorer) applyHandshakeSuccess(score *PeerScore) {
	score.Score += 1.0 // Fixed handshake success reward
}

func (ps *PeerScorer) applyHandshakeFailure(score *PeerScore) {
	score.Score -= 2.0 // Fixed handshake failure penalty
}

func (ps *PeerScorer) updateLatency(score *PeerScore, latency time.Duration) {
	// Update exponentially weighted moving average
	alpha := 0.1 // Smoothing factor
	newLatency := float64(latency.Milliseconds())

	if score.LatencyEWMA == 0 {
		score.LatencyEWMA = newLatency
	} else {
		score.LatencyEWMA = alpha*newLatency + (1-alpha)*score.LatencyEWMA
	}
}

func (ps *PeerScorer) calculateOverallScore(score *PeerScore) {
	// Calculate validity component
	if score.MessagesSent > 0 {
		validityRatio := float64(score.ValidMessages) / float64(score.MessagesSent)
		score.ValidityScore = validityRatio * ps.maxScore
	} else {
		score.ValidityScore = ps.config.InitialScore
	}

	// Calculate latency component
	avgLatency := time.Duration(score.LatencyEWMA) * time.Millisecond
	if avgLatency <= ps.config.OptimalLatency {
		score.LatencyScore = ps.maxScore
	} else if avgLatency >= ps.config.MaxLatency {
		score.LatencyScore = 0.0
	} else {
		// Linear interpolation between optimal and max latency
		ratio := float64(ps.config.MaxLatency-avgLatency) / float64(ps.config.MaxLatency-ps.config.OptimalLatency)
		score.LatencyScore = ratio * ps.maxScore
	}

	// Calculate uptime component
	uptime := time.Since(score.ConnectionTime)
	uptimeHours := uptime.Hours()

	// Reward longer uptime, penalize frequent disconnects
	baseUptimeScore := math.Min(uptimeHours/24.0, 1.0) * ps.maxScore // Max score after 24 hours
	disconnectPenalty := float64(score.DisconnectCount) * 5.0
	score.UptimeScore = math.Max(0, baseUptimeScore-disconnectPenalty)

	// Calculate bandwidth component
	totalBandwidth := score.BytesSent + score.BytesReceived
	if totalBandwidth < ps.config.MinBandwidthForReward {
		score.BandwidthScore = 0.0
	} else if totalBandwidth >= ps.config.OptimalBandwidth {
		score.BandwidthScore = ps.maxScore
	} else {
		// Linear interpolation between min and optimal bandwidth
		ratio := float64(totalBandwidth-ps.config.MinBandwidthForReward) /
			float64(ps.config.OptimalBandwidth-ps.config.MinBandwidthForReward)
		score.BandwidthScore = ratio * ps.maxScore
	}

	// Combine component scores using weights
	score.Score = score.ValidityScore*ps.config.ValidityWeight +
		score.LatencyScore*ps.config.LatencyWeight +
		score.UptimeScore*ps.config.UptimeWeight +
		score.BandwidthScore*ps.config.BandwidthWeight
}

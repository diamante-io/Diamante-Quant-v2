package monitoring

import "time"

// CalculateHealthScore returns a health score from 0-100 based on peer count,
// transaction pool utilization, and block processing lag. Higher scores indicate
// better health. Score penalties are applied for low peer count, high pool
// utilization, and high block processing lag.
func CalculateHealthScore(peerCount int, poolUtil float64, blockLag time.Duration) int {
	score := 100

	// Peer count penalties
	if peerCount < 3 {
		score -= 30
	} else if peerCount < 10 {
		score -= 10
	}

	// Pool utilization penalties
	if poolUtil > 0.80 {
		score -= 20
	} else if poolUtil > 0.50 {
		score -= 10
	}

	// Block lag penalties
	if blockLag > time.Minute {
		score -= 30
	} else if blockLag > 30*time.Second {
		score -= 10
	}

	if score < 0 {
		score = 0
	}
	return score
}

// crypto_time_adapter.go
package consensus

import (
	"time"
)

// CryptoTimeProvider implements the crypto.TimeProvider interface
// using consensus time functions
type CryptoTimeProvider struct{}

// NewCryptoTimeProvider creates a new crypto time provider
func NewCryptoTimeProvider() *CryptoTimeProvider {
	return &CryptoTimeProvider{}
}

// Now returns the current consensus time
func (c *CryptoTimeProvider) Now() time.Time {
	return ConsensusNow()
}

// Unix returns the current consensus Unix timestamp
func (c *CryptoTimeProvider) Unix() int64 {
	return ConsensusUnix()
}

// Since returns the duration since t using consensus time
func (c *CryptoTimeProvider) Since(t time.Time) time.Duration {
	return ConsensusSince(t)
}

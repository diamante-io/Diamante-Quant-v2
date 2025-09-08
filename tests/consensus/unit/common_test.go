// Package consensus_test provides common test utilities
package consensus_test

// generateValidatorID generates a deterministic validator ID for testing
func generateValidatorID(index int) [32]byte {
	var id [32]byte
	// Create a proper 32-byte ID with index encoded
	// Fill with zeros first
	for i := range id {
		id[i] = 0
	}
	// Set the index in the first few bytes
	id[0] = byte(index >> 24)
	id[1] = byte(index >> 16)
	id[2] = byte(index >> 8)
	id[3] = byte(index)
	// Add some pattern to make it recognizable
	id[4] = 0x34                  // '4' in hex
	id[5] = 0x31 + byte(index%10) // '1' + index
	return id
}

// generateEventID generates a deterministic event ID for testing
func generateEventID() [32]byte {
	var id [32]byte
	// Simple deterministic ID for testing
	copy(id[:], []byte("test-event-id"))
	return id
}

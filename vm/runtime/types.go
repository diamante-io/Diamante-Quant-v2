// Package runtime provides additional types for the hybrid VM architecture
package runtime

// CallResult represents the result of a contract call
type CallResult struct {
	// Whether the call was successful
	Success bool

	// Return data from the call
	ReturnData []byte

	// Gas used during the call
	GasUsed uint64

	// Error message if the call failed
	Error string
}

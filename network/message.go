package network

// Message is a minimal struct representing a network message.
type Message struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

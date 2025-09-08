package tests

import (
	"fmt"
	"time"
)

func TestTimestamp() {
	now := time.Now().Unix()
	fmt.Printf("Current Unix timestamp: %d\n", now)
	fmt.Printf("Current time: %s\n", time.Now().Format(time.RFC3339))
}

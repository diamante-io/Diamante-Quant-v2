// Package types provides contract-related type definitions
package types

import (
	"encoding/json"
	"fmt"
)

// ContractArgument represents a typed argument for contract calls
type ContractArgument struct {
	Name  string    `json:"name"`
	Type  ValueType `json:"type"`
	Value *Value    `json:"value"`
}

// ContractArguments represents a collection of contract arguments
type ContractArguments struct {
	Args []ContractArgument `json:"args"`
}

// NewContractArguments creates a new empty argument collection
func NewContractArguments() *ContractArguments {
	return &ContractArguments{
		Args: make([]ContractArgument, 0),
	}
}

// Add adds a new argument to the collection
func (ca *ContractArguments) Add(name string, vtype ValueType, data []byte) {
	ca.Args = append(ca.Args, ContractArgument{
		Name:  name,
		Type:  vtype,
		Value: NewValue(vtype, data),
	})
}

// AddString adds a string argument
func (ca *ContractArguments) AddString(name string, value string) {
	ca.Add(name, ValueTypeString, []byte(value))
}

// AddInt64 adds an int64 argument
func (ca *ContractArguments) AddInt64(name string, value int64) {
	ca.Add(name, ValueTypeInt64, Uint64ToBytes(uint64(value)))
}

// AddUint64 adds a uint64 argument
func (ca *ContractArguments) AddUint64(name string, value uint64) {
	ca.Add(name, ValueTypeUint64, Uint64ToBytes(value))
}

// AddBool adds a bool argument
func (ca *ContractArguments) AddBool(name string, value bool) {
	var b byte
	if value {
		b = 1
	}
	ca.Add(name, ValueTypeBool, []byte{b})
}

// AddBytes adds a bytes argument
func (ca *ContractArguments) AddBytes(name string, value []byte) {
	ca.Add(name, ValueTypeBytes, value)
}

// AddAddress adds an address argument
func (ca *ContractArguments) AddAddress(name string, value string) {
	ca.Add(name, ValueTypeAddress, []byte(value))
}

// Get retrieves an argument by name
func (ca *ContractArguments) Get(name string) (*ContractArgument, error) {
	for _, arg := range ca.Args {
		if arg.Name == name {
			return &arg, nil
		}
	}
	return nil, fmt.Errorf("argument %s not found", name)
}

// GetByIndex retrieves an argument by index
func (ca *ContractArguments) GetByIndex(index int) (*ContractArgument, error) {
	if index < 0 || index >= len(ca.Args) {
		return nil, fmt.Errorf("index %d out of range", index)
	}
	return &ca.Args[index], nil
}

// Count returns the number of arguments
func (ca *ContractArguments) Count() int {
	return len(ca.Args)
}

// ToJSON converts arguments to JSON
func (ca *ContractArguments) ToJSON() ([]byte, error) {
	return json.Marshal(ca)
}

// FromJSON loads arguments from JSON
func (ca *ContractArguments) FromJSON(data []byte) error {
	return json.Unmarshal(data, ca)
}

// ContractResult represents a typed result from contract execution
type ContractResult struct {
	Success bool               `json:"success"`
	Values  []ContractArgument `json:"values,omitempty"`
	Error   string             `json:"error,omitempty"`
	GasUsed uint64             `json:"gas_used"`
	Logs    []ContractLog      `json:"logs,omitempty"`
}

// ContractLog represents a log entry from contract execution
type ContractLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    []byte   `json:"data"`
	Index   uint     `json:"index"`
}

// ContractMetadata represents metadata about a contract
type ContractMetadata struct {
	ContractID   string            `json:"contract_id"`
	Language     string            `json:"language"`
	Version      string            `json:"version"`
	Author       string            `json:"author"`
	Description  string            `json:"description"`
	Dependencies []string          `json:"dependencies,omitempty"`
	Interfaces   []string          `json:"interfaces,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
}

// ContractState represents the state of a contract
type ContractState struct {
	ContractID string            `json:"contract_id"`
	Variables  map[string]*Value `json:"variables"`
	Version    uint64            `json:"version"`
	Hash       string            `json:"hash"`
}

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	ContractID  string             `json:"contract_id"`
	EventName   string             `json:"event_name"`
	Arguments   *ContractArguments `json:"arguments"`
	BlockHeight uint64             `json:"block_height"`
	TxHash      string             `json:"tx_hash"`
	Index       uint               `json:"index"`
	Timestamp   int64              `json:"timestamp"`
}

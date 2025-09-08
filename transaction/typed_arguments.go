package transaction

import (
	"encoding/json"
	"fmt"
)

// ArgumentType represents the type of a contract argument
type ArgumentType int

const (
	ArgumentTypeString ArgumentType = iota
	ArgumentTypeInt
	ArgumentTypeUint
	ArgumentTypeBool
	ArgumentTypeBytes
	ArgumentTypeAddress
	ArgumentTypeArray
	ArgumentTypeMap
)

// TypedArgument represents a typed contract argument
type TypedArgument struct {
	Name  string       `json:"name"`
	Type  ArgumentType `json:"type"`
	Value TypedValue   `json:"value"`
}

// TypedValue holds the actual typed value
type TypedValue struct {
	StringValue  string                `json:"string_value,omitempty"`
	IntValue     int64                 `json:"int_value,omitempty"`
	UintValue    uint64                `json:"uint_value,omitempty"`
	BoolValue    bool                  `json:"bool_value,omitempty"`
	BytesValue   []byte                `json:"bytes_value,omitempty"`
	AddressValue string                `json:"address_value,omitempty"`
	ArrayValue   []TypedValue          `json:"array_value,omitempty"`
	MapValue     map[string]TypedValue `json:"map_value,omitempty"`
}

// NewTypedArgument creates a typed argument from raw JSON
func NewTypedArgument(name string, rawValue json.RawMessage) (*TypedArgument, error) {
	// Try to determine type and unmarshal
	var value TypedValue
	var argType ArgumentType

	// Try string first
	var strVal string
	if err := json.Unmarshal(rawValue, &strVal); err == nil {
		value.StringValue = strVal
		argType = ArgumentTypeString
		return &TypedArgument{Name: name, Type: argType, Value: value}, nil
	}

	// Try bool
	var boolVal bool
	if err := json.Unmarshal(rawValue, &boolVal); err == nil {
		value.BoolValue = boolVal
		argType = ArgumentTypeBool
		return &TypedArgument{Name: name, Type: argType, Value: value}, nil
	}

	// Try number (could be int or uint)
	var numVal json.Number
	if err := json.Unmarshal(rawValue, &numVal); err == nil {
		// Try int64 first
		if intVal, err := numVal.Int64(); err == nil {
			value.IntValue = intVal
			argType = ArgumentTypeInt
			return &TypedArgument{Name: name, Type: argType, Value: value}, nil
		}
		// Try uint64
		if floatVal, err := numVal.Float64(); err == nil && floatVal >= 0 {
			value.UintValue = uint64(floatVal)
			argType = ArgumentTypeUint
			return &TypedArgument{Name: name, Type: argType, Value: value}, nil
		}
	}

	// Try array
	var arrVal []json.RawMessage
	if err := json.Unmarshal(rawValue, &arrVal); err == nil {
		value.ArrayValue = make([]TypedValue, len(arrVal))
		for i, elem := range arrVal {
			subArg, err := NewTypedArgument("", elem)
			if err != nil {
				return nil, fmt.Errorf("failed to parse array element %d: %w", i, err)
			}
			value.ArrayValue[i] = subArg.Value
		}
		argType = ArgumentTypeArray
		return &TypedArgument{Name: name, Type: argType, Value: value}, nil
	}

	// Try map
	var mapVal map[string]json.RawMessage
	if err := json.Unmarshal(rawValue, &mapVal); err == nil {
		value.MapValue = make(map[string]TypedValue)
		for k, v := range mapVal {
			subArg, err := NewTypedArgument("", v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse map value for key %s: %w", k, err)
			}
			value.MapValue[k] = subArg.Value
		}
		argType = ArgumentTypeMap
		return &TypedArgument{Name: name, Type: argType, Value: value}, nil
	}

	// Default to bytes
	value.BytesValue = []byte(rawValue)
	argType = ArgumentTypeBytes
	return &TypedArgument{Name: name, Type: argType, Value: value}, nil
}

// ToInterface converts TypedValue back to interface{} for compatibility
func (tv *TypedValue) ToInterface(argType ArgumentType) interface{} {
	switch argType {
	case ArgumentTypeString:
		return tv.StringValue
	case ArgumentTypeInt:
		return tv.IntValue
	case ArgumentTypeUint:
		return tv.UintValue
	case ArgumentTypeBool:
		return tv.BoolValue
	case ArgumentTypeBytes:
		return tv.BytesValue
	case ArgumentTypeAddress:
		return tv.AddressValue
	case ArgumentTypeArray:
		result := make([]interface{}, len(tv.ArrayValue))
		for i, v := range tv.ArrayValue {
			result[i] = v.ToInterfaceAuto()
		}
		return result
	case ArgumentTypeMap:
		result := make(map[string]interface{})
		for k, v := range tv.MapValue {
			result[k] = v.ToInterfaceAuto()
		}
		return result
	default:
		return nil
	}
}

// ToInterfaceAuto converts TypedValue to interface{} by detecting type
func (tv *TypedValue) ToInterfaceAuto() interface{} {
	// Check which field is set
	if tv.StringValue != "" {
		return tv.StringValue
	}
	if tv.IntValue != 0 {
		return tv.IntValue
	}
	if tv.UintValue != 0 {
		return tv.UintValue
	}
	if tv.BoolValue {
		return tv.BoolValue
	}
	if len(tv.BytesValue) > 0 {
		return tv.BytesValue
	}
	if tv.AddressValue != "" {
		return tv.AddressValue
	}
	if len(tv.ArrayValue) > 0 {
		result := make([]interface{}, len(tv.ArrayValue))
		for i, v := range tv.ArrayValue {
			result[i] = v.ToInterfaceAuto()
		}
		return result
	}
	if len(tv.MapValue) > 0 {
		result := make(map[string]interface{})
		for k, v := range tv.MapValue {
			result[k] = v.ToInterfaceAuto()
		}
		return result
	}
	return nil
}

// TypedDeploymentArgs represents typed deployment arguments
type TypedDeploymentArgs struct {
	Runtime  string            `json:"runtime"`
	Code     []byte            `json:"code"`
	Args     []*TypedArgument  `json:"args"`
	Metadata map[string]string `json:"metadata"`
}

// TypedExecutionArgs represents typed execution arguments
type TypedExecutionArgs struct {
	ContractID string            `json:"contract_id"`
	Method     string            `json:"method"`
	Args       []*TypedArgument  `json:"args"`
	Metadata   map[string]string `json:"metadata"`
}

// ParseDeploymentData parses deployment data into typed arguments
func ParseDeploymentData(data []byte) (*TypedDeploymentArgs, error) {
	// First parse into raw structure
	var raw struct {
		Runtime  string            `json:"runtime"`
		Code     []byte            `json:"code"`
		Args     []json.RawMessage `json:"args"`
		Metadata map[string]string `json:"metadata"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal deployment data: %w", err)
	}

	// Convert raw args to typed args
	typedArgs := make([]*TypedArgument, len(raw.Args))
	for i, rawArg := range raw.Args {
		arg, err := NewTypedArgument(fmt.Sprintf("arg%d", i), rawArg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse argument %d: %w", i, err)
		}
		typedArgs[i] = arg
	}

	return &TypedDeploymentArgs{
		Runtime:  raw.Runtime,
		Code:     raw.Code,
		Args:     typedArgs,
		Metadata: raw.Metadata,
	}, nil
}

// ParseExecutionData parses execution data into typed arguments
func ParseExecutionData(data []byte) (*TypedExecutionArgs, error) {
	// First parse into raw structure
	var raw struct {
		ContractID string            `json:"contract_id"`
		Method     string            `json:"method"`
		Args       []json.RawMessage `json:"args"`
		Metadata   map[string]string `json:"metadata"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal execution data: %w", err)
	}

	// Convert raw args to typed args
	typedArgs := make([]*TypedArgument, len(raw.Args))
	for i, rawArg := range raw.Args {
		arg, err := NewTypedArgument(fmt.Sprintf("arg%d", i), rawArg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse argument %d: %w", i, err)
		}
		typedArgs[i] = arg
	}

	return &TypedExecutionArgs{
		ContractID: raw.ContractID,
		Method:     raw.Method,
		Args:       typedArgs,
		Metadata:   raw.Metadata,
	}, nil
}

// ConvertToInterfaceSlice converts typed arguments to interface slice for compatibility
func ConvertToInterfaceSlice(args []*TypedArgument) []interface{} {
	result := make([]interface{}, len(args))
	for i, arg := range args {
		result[i] = arg.Value.ToInterface(arg.Type)
	}
	return result
}

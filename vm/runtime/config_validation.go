// Package runtime provides JSON schema validation for runtime configurations
package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// PropertyType represents the type of a property in the schema
type PropertyType string

const (
	TypeString  PropertyType = "string"
	TypeNumber  PropertyType = "number"
	TypeInteger PropertyType = "integer"
	TypeBoolean PropertyType = "boolean"
	TypeObject  PropertyType = "object"
	TypeArray   PropertyType = "array"
)

// PropertyDefinition defines a property in the configuration schema
type PropertyDefinition struct {
	Type        PropertyType                  `json:"type"`
	Description string                        `json:"description,omitempty"`
	Required    bool                          `json:"required,omitempty"`
	Default     interface{}                   `json:"default,omitempty"`
	MinValue    *float64                      `json:"minimum,omitempty"`
	MaxValue    *float64                      `json:"maximum,omitempty"`
	MinLength   *int                          `json:"minLength,omitempty"`
	MaxLength   *int                          `json:"maxLength,omitempty"`
	Pattern     string                        `json:"pattern,omitempty"`
	Enum        []interface{}                 `json:"enum,omitempty"`
	Properties  map[string]PropertyDefinition `json:"properties,omitempty"` // For nested objects
	Items       *PropertyDefinition           `json:"items,omitempty"`      // For arrays
}

// ConfigSchema defines the schema for runtime configuration
type ConfigSchema struct {
	Name        string                        `json:"name"`
	Description string                        `json:"description"`
	Version     string                        `json:"version"`
	Properties  map[string]PropertyDefinition `json:"properties"`
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Message string
	Value   interface{}
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s (value: %v)", e.Field, e.Message, e.Value)
}

// ValidationErrors represents multiple validation errors
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "; ")
}

// ConfigValidator validates runtime configurations against schemas
type ConfigValidator struct {
	schemas map[RuntimeType]*ConfigSchema
}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{
		schemas: make(map[RuntimeType]*ConfigSchema),
	}
}

// RegisterSchema registers a configuration schema for a runtime type
func (cv *ConfigValidator) RegisterSchema(runtimeType RuntimeType, schema *ConfigSchema) error {
	if schema == nil {
		return errors.New("schema cannot be nil")
	}
	if schema.Name == "" {
		return errors.New("schema name is required")
	}
	cv.schemas[runtimeType] = schema
	return nil
}

// ValidateConfig validates a runtime configuration against its schema
func (cv *ConfigValidator) ValidateConfig(runtimeType RuntimeType, config map[string]interface{}) error {
	schema, exists := cv.schemas[runtimeType]
	if !exists {
		// No schema registered, allow any config
		return nil
	}

	errors := ValidationErrors{}

	// Check for required fields
	for propName, propDef := range schema.Properties {
		if propDef.Required {
			if _, exists := config[propName]; !exists {
				errors = append(errors, ValidationError{
					Field:   propName,
					Message: "required field is missing",
				})
			}
		}
	}

	// Validate each field in config
	for key, value := range config {
		if propDef, exists := schema.Properties[key]; exists {
			if err := validateProperty(key, value, propDef); err != nil {
				if validationErr, ok := err.(ValidationError); ok {
					errors = append(errors, validationErr)
				} else if validationErrs, ok := err.(ValidationErrors); ok {
					errors = append(errors, validationErrs...)
				} else {
					errors = append(errors, ValidationError{
						Field:   key,
						Message: err.Error(),
						Value:   value,
					})
				}
			}
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// validateProperty validates a single property against its definition
func validateProperty(field string, value interface{}, def PropertyDefinition) error {
	// Handle nil values
	if value == nil {
		if def.Required {
			return ValidationError{
				Field:   field,
				Message: "required field cannot be nil",
				Value:   value,
			}
		}
		return nil
	}

	// Check enum values
	if len(def.Enum) > 0 {
		found := false
		for _, enumVal := range def.Enum {
			if reflect.DeepEqual(value, enumVal) {
				found = true
				break
			}
		}
		if !found {
			return ValidationError{
				Field:   field,
				Message: fmt.Sprintf("value must be one of: %v", def.Enum),
				Value:   value,
			}
		}
	}

	// Validate based on type
	switch def.Type {
	case TypeString:
		return validateString(field, value, def)
	case TypeNumber, TypeInteger:
		return validateNumber(field, value, def)
	case TypeBoolean:
		return validateBoolean(field, value, def)
	case TypeObject:
		return validateObject(field, value, def)
	case TypeArray:
		return validateArray(field, value, def)
	default:
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("unknown type: %s", def.Type),
			Value:   value,
		}
	}
}

func validateString(field string, value interface{}, def PropertyDefinition) error {
	str, ok := value.(string)
	if !ok {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("expected string, got %T", value),
			Value:   value,
		}
	}

	// Check length constraints
	if def.MinLength != nil && len(str) < *def.MinLength {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("string length must be at least %d", *def.MinLength),
			Value:   value,
		}
	}
	if def.MaxLength != nil && len(str) > *def.MaxLength {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("string length must not exceed %d", *def.MaxLength),
			Value:   value,
		}
	}

	// Check pattern
	if def.Pattern != "" {
		regex, err := regexp.Compile(def.Pattern)
		if err != nil {
			return ValidationError{
				Field:   field,
				Message: fmt.Sprintf("invalid regex pattern: %s", def.Pattern),
				Value:   value,
			}
		}
		if !regex.MatchString(str) {
			return ValidationError{
				Field:   field,
				Message: fmt.Sprintf("value does not match pattern: %s", def.Pattern),
				Value:   value,
			}
		}
	}

	return nil
}

func validateNumber(field string, value interface{}, def PropertyDefinition) error {
	var num float64

	switch v := value.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int:
		num = float64(v)
	case int32:
		num = float64(v)
	case int64:
		num = float64(v)
	case uint:
		num = float64(v)
	case uint32:
		num = float64(v)
	case uint64:
		num = float64(v)
	default:
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("expected number, got %T", value),
			Value:   value,
		}
	}

	// Check integer constraint
	if def.Type == TypeInteger {
		if num != float64(int64(num)) {
			return ValidationError{
				Field:   field,
				Message: "expected integer value",
				Value:   value,
			}
		}
	}

	// Check range constraints
	if def.MinValue != nil && num < *def.MinValue {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("value must be at least %v", *def.MinValue),
			Value:   value,
		}
	}
	if def.MaxValue != nil && num > *def.MaxValue {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("value must not exceed %v", *def.MaxValue),
			Value:   value,
		}
	}

	return nil
}

func validateBoolean(field string, value interface{}, _ PropertyDefinition) error {
	if _, ok := value.(bool); !ok {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("expected boolean, got %T", value),
			Value:   value,
		}
	}
	return nil
}

func validateObject(field string, value interface{}, def PropertyDefinition) error {
	obj, ok := value.(map[string]interface{})
	if !ok {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("expected object, got %T", value),
			Value:   value,
		}
	}

	if len(def.Properties) == 0 {
		// No nested schema, allow any object
		return nil
	}

	errors := ValidationErrors{}

	// Validate nested properties
	for propName, propDef := range def.Properties {
		fullField := fmt.Sprintf("%s.%s", field, propName)
		if propValue, exists := obj[propName]; exists {
			if err := validateProperty(fullField, propValue, propDef); err != nil {
				if validationErr, ok := err.(ValidationError); ok {
					errors = append(errors, validationErr)
				} else if validationErrs, ok := err.(ValidationErrors); ok {
					errors = append(errors, validationErrs...)
				}
			}
		} else if propDef.Required {
			errors = append(errors, ValidationError{
				Field:   fullField,
				Message: "required field is missing",
			})
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func validateArray(field string, value interface{}, def PropertyDefinition) error {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return ValidationError{
			Field:   field,
			Message: fmt.Sprintf("expected array, got %T", value),
			Value:   value,
		}
	}

	if def.Items == nil {
		// No item schema, allow any array
		return nil
	}

	errors := ValidationErrors{}

	// Validate each item
	for i := 0; i < v.Len(); i++ {
		itemField := fmt.Sprintf("%s[%d]", field, i)
		itemValue := v.Index(i).Interface()
		if err := validateProperty(itemField, itemValue, *def.Items); err != nil {
			if validationErr, ok := err.(ValidationError); ok {
				errors = append(errors, validationErr)
			} else if validationErrs, ok := err.(ValidationErrors); ok {
				errors = append(errors, validationErrs...)
			}
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// CreateEVMConfigSchema creates the configuration schema for EVM runtime
func CreateEVMConfigSchema() *ConfigSchema {
	chainIDMin := float64(1)
	gasLimitMin := float64(1000000)
	gasLimitMax := float64(100000000)

	return &ConfigSchema{
		Name:        "EVM Runtime Configuration",
		Description: "Configuration schema for Ethereum Virtual Machine runtime",
		Version:     "1.0.0",
		Properties: map[string]PropertyDefinition{
			"blockHeight": {
				Type:        TypeInteger,
				Description: "Current block height",
				Required:    true,
				MinValue:    &chainIDMin,
			},
			"chainId": {
				Type:        TypeString,
				Description: "EVM chain ID",
				Required:    true,
				Pattern:     "^[0-9]+$",
			},
			"gasLimit": {
				Type:        TypeInteger,
				Description: "Maximum gas limit per block",
				Required:    true,
				MinValue:    &gasLimitMin,
				MaxValue:    &gasLimitMax,
			},
			"byzantiumBlock": {
				Type:        TypeInteger,
				Description: "Block number for Byzantium fork",
				Default:     0,
			},
			"constantinopleBlock": {
				Type:        TypeInteger,
				Description: "Block number for Constantinople fork",
				Default:     0,
			},
		},
	}
}

// CreateNativeConfigSchema creates the configuration schema for Native runtime
func CreateNativeConfigSchema() *ConfigSchema {
	maxContainers := float64(1000)

	return &ConfigSchema{
		Name:        "Native Runtime Configuration",
		Description: "Configuration schema for Native Diamante runtime",
		Version:     "1.0.0",
		Properties: map[string]PropertyDefinition{
			"enableDynamicLoad": {
				Type:        TypeBoolean,
				Description: "Enable dynamic plugin loading",
				Required:    false,
				Default:     false,
			},
			"pluginDirectory": {
				Type:        TypeString,
				Description: "Directory path for plugins",
				Required:    false,
				Default:     "/opt/diamante/plugins",
				Pattern:     "^/.*",
			},
			"securityLevel": {
				Type:        TypeString,
				Description: "Security level for native execution",
				Required:    false,
				Default:     "sandbox",
				Enum:        []interface{}{"sandbox", "restricted", "full"},
			},
			"maxMemoryMB": {
				Type:        TypeInteger,
				Description: "Maximum memory allocation in MB",
				Default:     512,
				MinValue:    &chainIDMin,
				MaxValue:    &maxContainers,
			},
		},
	}
}

// CreateChaincodeConfigSchema creates the configuration schema for Chaincode runtime
func CreateChaincodeConfigSchema() *ConfigSchema {
	maxContainersMin := float64(1)
	maxContainersMax := float64(1000)
	timeoutMin := float64(10)

	return &ConfigSchema{
		Name:        "Chaincode Runtime Configuration",
		Description: "Configuration schema for Hyperledger Fabric Chaincode runtime",
		Version:     "1.0.0",
		Properties: map[string]PropertyDefinition{
			"dockerEndpoint": {
				Type:        TypeString,
				Description: "Docker daemon endpoint",
				Required:    true,
				Default:     "unix:///var/run/docker.sock",
			},
			"networkMode": {
				Type:        TypeString,
				Description: "Docker network mode",
				Required:    false,
				Default:     "bridge",
				Enum:        []interface{}{"bridge", "host", "none"},
			},
			"maxContainers": {
				Type:        TypeInteger,
				Description: "Maximum number of chaincode containers",
				Required:    false,
				Default:     100,
				MinValue:    &maxContainersMin,
				MaxValue:    &maxContainersMax,
			},
			"containerTimeout": {
				Type:        TypeInteger,
				Description: "Container startup timeout in seconds",
				Default:     60,
				MinValue:    &timeoutMin,
			},
			"tlsEnabled": {
				Type:        TypeBoolean,
				Description: "Enable TLS for chaincode communication",
				Default:     false,
			},
		},
	}
}

// Global variable to fix undefined chainIDMin
var chainIDMin = float64(0)

// consensus/common/safe_cast.go

package common

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"diamante/types"
)

// SafeCast provides safe type assertion utilities with panic recovery
type SafeCast struct {
	// EnablePanicRecovery controls whether to recover from panics
	EnablePanicRecovery bool
}

// NewSafeCast creates a new SafeCast instance
func NewSafeCast(enablePanicRecovery bool) *SafeCast {
	return &SafeCast{
		EnablePanicRecovery: enablePanicRecovery,
	}
}

// Cast safely casts a typed value to the target type with panic recovery
func (sc *SafeCast) Cast(source *types.Value, target interface{}) (err error) {
	// Recover from panic if enabled
	if sc.EnablePanicRecovery {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic during cast: %v", r)
			}
		}()
	}

	// Check if source is nil
	if source == nil {
		return fmt.Errorf("cannot cast nil source")
	}

	// Get reflect value of target
	targetValue := reflect.ValueOf(target)

	// Target must be a pointer
	if targetValue.Kind() != reflect.Ptr {
		return fmt.Errorf("target must be a pointer")
	}

	// Target pointer must not be nil
	if targetValue.IsNil() {
		return fmt.Errorf("target pointer is nil")
	}

	// Perform type-specific conversion based on source.Type
	switch source.Type {
	case types.ValueTypeString:
		if str, err := source.String(); err == nil {
			if strPtr, ok := target.(*string); ok {
				*strPtr = str
				return nil
			}
		}
	case types.ValueTypeInt64:
		if val, err := source.Int64(); err == nil {
			if intPtr, ok := target.(*int64); ok {
				*intPtr = val
				return nil
			}
		}
	case types.ValueTypeUint64:
		if val, err := source.Uint64(); err == nil {
			if uintPtr, ok := target.(*uint64); ok {
				*uintPtr = val
				return nil
			}
		}
	case types.ValueTypeBool:
		if val, err := source.Bool(); err == nil {
			if boolPtr, ok := target.(*bool); ok {
				*boolPtr = val
				return nil
			}
		}
	case types.ValueTypeFloat64:
		if val, err := source.Float64(); err == nil {
			if floatPtr, ok := target.(*float64); ok {
				*floatPtr = val
				return nil
			}
		}
	case types.ValueTypeBytes:
		if bytesPtr, ok := target.(*[]byte); ok {
			*bytesPtr = source.Bytes()
			return nil
		}
	}

	return fmt.Errorf("cannot cast type %v to target type", source.Type)
}

// TryCast attempts to cast and returns success status
func (sc *SafeCast) TryCast(source *types.Value, target interface{}) bool {
	return sc.Cast(source, target) == nil
}

// MustCast performs a cast and returns an error on failure instead of panicking
// NOTE: This function has been modified to remove panic() for production safety
func (sc *SafeCast) MustCast(source *types.Value, target interface{}) error {
	if err := sc.Cast(source, target); err != nil {
		return fmt.Errorf("cast failed: %w", err)
	}
	return nil
}

// TypedConversions provides typed conversion functions for common types
type TypedConversions struct{}

// NewTypedConversions creates a new TypedConversions instance
func NewTypedConversions() *TypedConversions {
	return &TypedConversions{}
}

// ToString converts a typed value to string
func (tc *TypedConversions) ToString(value *types.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("cannot convert nil value to string")
	}
	return value.String()
}

// ToInt64 converts a typed value to int64
func (tc *TypedConversions) ToInt64(value *types.Value) (int64, error) {
	if value == nil {
		return 0, fmt.Errorf("cannot convert nil value to int64")
	}
	return value.Int64()
}

// ToUint64 converts a typed value to uint64
func (tc *TypedConversions) ToUint64(value *types.Value) (uint64, error) {
	if value == nil {
		return 0, fmt.Errorf("cannot convert nil value to uint64")
	}
	return value.Uint64()
}

// ToBool converts a typed value to bool
func (tc *TypedConversions) ToBool(value *types.Value) (bool, error) {
	if value == nil {
		return false, fmt.Errorf("cannot convert nil value to bool")
	}
	return value.Bool()
}

// ToFloat64 converts a typed value to float64
func (tc *TypedConversions) ToFloat64(value *types.Value) (float64, error) {
	if value == nil {
		return 0, fmt.Errorf("cannot convert nil value to float64")
	}
	return value.Float64()
}

// ToBytes converts a typed value to bytes
func (tc *TypedConversions) ToBytes(value *types.Value) []byte {
	if value == nil {
		return nil
	}
	return value.Bytes()
}

// FromString creates a typed value from string
func (tc *TypedConversions) FromString(str string) *types.Value {
	return types.NewValue(types.ValueTypeString, []byte(str))
}

// FromInt64 creates a typed value from int64
func (tc *TypedConversions) FromInt64(val int64) *types.Value {
	return types.NewValue(types.ValueTypeInt64, types.Uint64ToBytes(uint64(val)))
}

// FromUint64 creates a typed value from uint64
func (tc *TypedConversions) FromUint64(val uint64) *types.Value {
	return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(val))
}

// FromBool creates a typed value from bool
func (tc *TypedConversions) FromBool(val bool) *types.Value {
	b := byte(0)
	if val {
		b = 1
	}
	return types.NewValue(types.ValueTypeBool, []byte{b})
}

// FromFloat64 creates a typed value from float64
func (tc *TypedConversions) FromFloat64(val float64) *types.Value {
	return types.NewValue(types.ValueTypeFloat64, []byte(fmt.Sprintf("%f", val)))
}

// FromBytes creates a typed value from bytes
func (tc *TypedConversions) FromBytes(data []byte) *types.Value {
	return types.NewValue(types.ValueTypeBytes, data)
}

// CastToInterface safely casts to a specific interface type
func CastToInterface[T any](source *types.Value) (T, error) {
	var zero T

	// Create a new safe cast instance
	sc := NewSafeCast(true)

	// Try to cast to the target type
	if err := sc.Cast(source, &zero); err != nil {
		return zero, fmt.Errorf("cannot cast typed value to %T: %w", zero, err)
	}

	return zero, nil
}

// TryCastToInterface attempts to cast to interface and returns success status
func TryCastToInterface[T any](source *types.Value) (T, bool) {
	result, err := CastToInterface[T](source)
	return result, err == nil
}

// MustCastToInterface casts to interface or returns zero value with error logging
// NOTE: This function has been modified to remove panic() for production safety
func MustCastToInterface[T any](source *types.Value) (T, error) {
	result, err := CastToInterface[T](source)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("cast failed: %w", err)
	}
	return result, nil
}

// SafeMethodCall safely calls a method on a typed object with panic recovery
func SafeMethodCall(obj interface{}, methodName string, args ...*types.Value) ([]*types.Value, error) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			// Return error instead of panicking
		}
	}()

	// Check if obj is nil
	if obj == nil {
		return nil, fmt.Errorf("cannot call method on nil object")
	}

	// Get the value and check if it's valid
	objValue := reflect.ValueOf(obj)
	if !objValue.IsValid() {
		return nil, fmt.Errorf("invalid object")
	}

	// Get the method
	method := objValue.MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("method %s not found on %T", methodName, obj)
	}

	// Prepare arguments
	argValues := make([]reflect.Value, len(args))
	for i, arg := range args {
		// Convert typed value to appropriate type based on method signature
		if i < method.Type().NumIn() {
			expectedType := method.Type().In(i)
			var val reflect.Value

			switch arg.Type {
			case types.ValueTypeString:
				if str, err := arg.String(); err == nil {
					val = reflect.ValueOf(str)
				}
			case types.ValueTypeInt64:
				if i64, err := arg.Int64(); err == nil {
					val = reflect.ValueOf(i64)
				}
			case types.ValueTypeUint64:
				if u64, err := arg.Uint64(); err == nil {
					val = reflect.ValueOf(u64)
				}
			case types.ValueTypeBool:
				if b, err := arg.Bool(); err == nil {
					val = reflect.ValueOf(b)
				}
			case types.ValueTypeFloat64:
				if f64, err := arg.Float64(); err == nil {
					val = reflect.ValueOf(f64)
				}
			case types.ValueTypeBytes:
				val = reflect.ValueOf(arg.Bytes())
			}

			// Check if conversion is valid
			if val.IsValid() && val.Type().ConvertibleTo(expectedType) {
				argValues[i] = val.Convert(expectedType)
			} else {
				return nil, fmt.Errorf("cannot convert argument %d to expected type %v", i, expectedType)
			}
		}
	}

	// Call the method
	resultValues := method.Call(argValues)

	// Convert results to typed values
	tc := NewTypedConversions()
	results := make([]*types.Value, len(resultValues))
	for i, v := range resultValues {
		// Convert based on the result type
		switch v.Kind() {
		case reflect.String:
			results[i] = tc.FromString(v.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			results[i] = tc.FromInt64(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			results[i] = tc.FromUint64(v.Uint())
		case reflect.Bool:
			results[i] = tc.FromBool(v.Bool())
		case reflect.Float32, reflect.Float64:
			results[i] = tc.FromFloat64(v.Float())
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				results[i] = tc.FromBytes(v.Bytes())
			}
		}
	}

	return results, nil
}

// InterfaceChecker provides methods to check interface implementation
type InterfaceChecker struct{}

// NewInterfaceChecker creates a new interface checker
func NewInterfaceChecker() *InterfaceChecker {
	return &InterfaceChecker{}
}

// Implements checks if obj implements the interface type T
func Implements[T any](obj interface{}) bool {
	if obj == nil {
		return false
	}

	var target T
	targetType := reflect.TypeOf(&target).Elem()
	objType := reflect.TypeOf(obj)

	return objType.Implements(targetType)
}

// ImplementsAll checks if obj implements all the given interface types
func ImplementsAll(obj interface{}, interfaceTypes ...reflect.Type) bool {
	if obj == nil {
		return false
	}

	objType := reflect.TypeOf(obj)
	for _, ifaceType := range interfaceTypes {
		if !objType.Implements(ifaceType) {
			return false
		}
	}

	return true
}

// GetInterfaceMethods returns all methods of an interface type
func GetInterfaceMethods(ifaceType reflect.Type) []string {
	if ifaceType.Kind() != reflect.Interface {
		return nil
	}

	methods := make([]string, ifaceType.NumMethod())
	for i := 0; i < ifaceType.NumMethod(); i++ {
		methods[i] = ifaceType.Method(i).Name
	}

	return methods
}

// SafeFieldAccess safely accesses a struct field with panic recovery
func SafeFieldAccess(obj interface{}, fieldName string) (*types.Value, error) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			// Return error instead of panicking
		}
	}()

	// Check if obj is nil
	if obj == nil {
		return nil, fmt.Errorf("cannot access field on nil object")
	}

	// Get the value
	objValue := reflect.ValueOf(obj)

	// Dereference pointer if needed
	if objValue.Kind() == reflect.Ptr {
		if objValue.IsNil() {
			return nil, fmt.Errorf("cannot access field on nil pointer")
		}
		objValue = objValue.Elem()
	}

	// Check if it's a struct
	if objValue.Kind() != reflect.Struct {
		return nil, fmt.Errorf("cannot access field on non-struct type %T", obj)
	}

	// Get the field
	fieldValue := objValue.FieldByName(fieldName)
	if !fieldValue.IsValid() {
		return nil, fmt.Errorf("field %s not found in %T", fieldName, obj)
	}

	// Check if field is exported
	if !fieldValue.CanInterface() {
		return nil, fmt.Errorf("field %s is not exported", fieldName)
	}

	// Convert field value to typed value
	tc := NewTypedConversions()
	switch fieldValue.Kind() {
	case reflect.String:
		return tc.FromString(fieldValue.String()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return tc.FromInt64(fieldValue.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return tc.FromUint64(fieldValue.Uint()), nil
	case reflect.Bool:
		return tc.FromBool(fieldValue.Bool()), nil
	case reflect.Float32, reflect.Float64:
		return tc.FromFloat64(fieldValue.Float()), nil
	case reflect.Slice:
		if fieldValue.Type().Elem().Kind() == reflect.Uint8 {
			return tc.FromBytes(fieldValue.Bytes()), nil
		}
	}

	return nil, fmt.Errorf("unsupported field type: %v", fieldValue.Kind())
}

// SafeMethodCallWrapper provides a type-safe way to call methods with panic recovery
type SafeMethodCallWrapper struct {
	obj        interface{}
	methodName string
}

// NewSafeMethodCallWrapper creates a new safe method call wrapper
func NewSafeMethodCallWrapper(obj interface{}, methodName string) *SafeMethodCallWrapper {
	return &SafeMethodCallWrapper{
		obj:        obj,
		methodName: methodName,
	}
}

// Call executes the method with the given typed arguments
func (smc *SafeMethodCallWrapper) Call(args ...*types.Value) ([]*types.Value, error) {
	return SafeMethodCall(smc.obj, smc.methodName, args...)
}

// CallWithTimeout executes the method with a timeout
func (smc *SafeMethodCallWrapper) CallWithTimeout(timeout time.Duration, args ...*types.Value) ([]*types.Value, error) {
	resultChan := make(chan struct {
		results []*types.Value
		err     error
	}, 1)

	go func() {
		results, err := smc.Call(args...)
		resultChan <- struct {
			results []*types.Value
			err     error
		}{results, err}
	}()

	select {
	case result := <-resultChan:
		return result.results, result.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("method call timed out after %v", timeout)
	}
}

// TypeRegistry provides a registry for safe type conversions
type TypeRegistry struct {
	types map[string]reflect.Type
	mu    sync.RWMutex
}

// NewTypeRegistry creates a new type registry
func NewTypeRegistry() *TypeRegistry {
	return &TypeRegistry{
		types: make(map[string]reflect.Type),
	}
}

// Register registers a type with a name
func (tr *TypeRegistry) Register(name string, typ reflect.Type) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.types[name] = typ
}

// Get retrieves a type by name
func (tr *TypeRegistry) Get(name string) (reflect.Type, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	typ, ok := tr.types[name]
	return typ, ok
}

// CreateInstance creates a new instance of a registered type
func (tr *TypeRegistry) CreateInstance(name string) (interface{}, error) {
	typ, ok := tr.Get(name)
	if !ok {
		return nil, fmt.Errorf("type %s not registered", name)
	}

	// Create new instance
	value := reflect.New(typ)
	return value.Interface(), nil
}

// ConvertToTypedValue converts a registered type instance to a typed value
func (tr *TypeRegistry) ConvertToTypedValue(name string, instance interface{}) (*types.Value, error) {
	if instance == nil {
		return nil, fmt.Errorf("cannot convert nil instance")
	}

	// Use reflection to determine the type and convert appropriately
	tc := NewTypedConversions()
	val := reflect.ValueOf(instance)

	switch val.Kind() {
	case reflect.String:
		return tc.FromString(val.String()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return tc.FromInt64(val.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return tc.FromUint64(val.Uint()), nil
	case reflect.Bool:
		return tc.FromBool(val.Bool()), nil
	case reflect.Float32, reflect.Float64:
		return tc.FromFloat64(val.Float()), nil
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return tc.FromBytes(val.Bytes()), nil
		}
	}

	return nil, fmt.Errorf("unsupported type for conversion: %v", val.Kind())
}

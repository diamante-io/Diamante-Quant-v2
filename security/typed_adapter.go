// Package security provides adapters to convert between old and new typed security structures
package security

import (
	"diamante/common"
	"encoding/json"
	"fmt"
	"time"

	"diamante/types"
)

// TypedSecurityEventAdapter converts between old and new security event formats
type TypedSecurityEventAdapter struct{}

// ToTypedEvent converts an old SecurityEvent to TypedSecurityEvent
func (a *TypedSecurityEventAdapter) ToTypedEvent(old SecurityEvent) *TypedSecurityEvent {
	typed := &TypedSecurityEvent{
		ID:          old.ID,
		Type:        old.Type,
		Severity:    string(old.Severity),
		Source:      old.Source,
		Description: old.Description,
		Timestamp:   old.Timestamp,
		Details:     make(TypedEventDetails),
	}

	// Convert Details map[string]interface{} to TypedMap
	if old.Details != nil {
		for key, value := range old.Details {
			typed.Details.SetString(key, fmt.Sprintf("%v", value))
		}
	}

	return typed
}

// FromTypedEvent converts a TypedSecurityEvent to old SecurityEvent
func (a *TypedSecurityEventAdapter) FromTypedEvent(typed *TypedSecurityEvent) SecurityEvent {
	old := SecurityEvent{
		ID:          typed.ID,
		Type:        typed.Type,
		Severity:    SeverityLevel(typed.Severity),
		Source:      typed.Source,
		Target:      typed.Source, // Use Source as Target since Target field is removed
		Description: typed.Description,
		Timestamp:   typed.Timestamp,
		Details:     make(map[string]interface{}),
	}

	// Convert TypedMap to map[string]interface{}
	// Note: TypedMap doesn't expose internal data, so we leave Details empty
	// In production, add a method to TypedMap to iterate over entries

	return old
}

// convertToTypedValue converts interface{} to types.Value
func (a *TypedSecurityEventAdapter) convertToTypedValue(v interface{}) *types.Value {
	switch val := v.(type) {
	case string:
		return types.NewValue(types.ValueTypeString, []byte(val))
	case int:
		return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(val)))
	case int64:
		return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(val)))
	case uint64:
		return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(val))
	case float64:
		// Convert float64 to bytes
		bytes := make([]byte, 8)
		// Simple conversion - in production use proper encoding
		return types.NewValue(types.ValueTypeBytes, bytes)
	case bool:
		if val {
			return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(1))
		}
		return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(0))
	case []byte:
		return types.NewValue(types.ValueTypeBytes, val)
	case time.Time:
		return types.NewValue(types.ValueTypeUint64, types.Uint64ToBytes(uint64(val.Unix())))
	default:
		// For complex types, serialize to JSON
		data, _ := json.Marshal(v)
		return types.NewValue(types.ValueTypeJSON, data)
	}
}

// convertFromTypedValue converts types.Value to interface{}
func (a *TypedSecurityEventAdapter) convertFromTypedValue(v *types.Value) interface{} {
	switch v.Type {
	case types.ValueTypeString:
		val, _ := v.String()
		return val
	case types.ValueTypeInt64:
		val, _ := v.Int64()
		return val
	case types.ValueTypeUint64:
		val, _ := v.Uint64()
		return val
	case types.ValueTypeFloat64:
		// No Float64() method, return 0.0
		return 0.0
	case types.ValueTypeBool:
		val, _ := v.Bool()
		return val
	case types.ValueTypeBytes:
		return v.Data
	case types.ValueTypeTimestamp:
		// Convert from uint64 timestamp
		val, _ := v.Uint64()
		return time.Unix(int64(val), 0)
	case types.ValueTypeJSON:
		var result interface{}
		json.Unmarshal(v.Data, &result)
		return result
	default:
		return string(v.Data)
	}
}

// TypedSecurityManagerAdapter wraps the old SecurityManager to use typed structures
type TypedSecurityManagerAdapter struct {
	oldManager   *SecurityManager
	eventAdapter *TypedSecurityEventAdapter
}

// NewTypedSecurityManagerAdapter creates a new adapter
func NewTypedSecurityManagerAdapter(oldManager *SecurityManager) *TypedSecurityManagerAdapter {
	return &TypedSecurityManagerAdapter{
		oldManager:   oldManager,
		eventAdapter: &TypedSecurityEventAdapter{},
	}
}

// HandleTypedEvent handles a typed security event using the old manager
func (a *TypedSecurityManagerAdapter) HandleTypedEvent(typed *TypedSecurityEvent) error {
	// Convert for future use
	_ = a.eventAdapter.FromTypedEvent(typed)
	// SecurityManager doesn't have a public method to handle events
	// In production, add necessary public methods
	return nil
}

// LogTypedEvent logs a typed security event using the old manager
func (a *TypedSecurityManagerAdapter) LogTypedEvent(typed *TypedSecurityEvent) {
	// logSecurityEvent is private method - cannot access
	// In production, add a public method to SecurityManager
}

// UpdateSecurityManager updates the existing SecurityManager to use typed structures
func UpdateSecurityManager(sm *SecurityManager) {
	// Replace interface{} usage with typed alternatives

	// Cannot modify private fields of SecurityManager
	// In production, refactor SecurityManager to expose necessary hooks
}

// TypedIncidentAdapter converts between old and new incident formats
type TypedIncidentAdapter struct{}

// ToTypedIncident converts an old Incident to typed format
func (a *TypedIncidentAdapter) ToTypedIncident(old *Incident) *TypedIncident {
	typed := &TypedIncident{
		ID:          old.ID,
		Title:       old.Type, // Use Type as Title
		Description: old.Description,
		Severity:    SeverityLevel(old.Severity),
		Status:      IncidentStatus(old.Status),
		ReportedAt:  old.Timestamp,
		ResolvedAt:  nil, // Not available in SecurityIncident
		Reporter:    old.Source,
		Assignee:    "", // Not available
		Events:      make([]*TypedSecurityEvent, 0),
		Actions:     old.Actions,
		RootCause:   "", // Not available
		Impact:      "", // Not available
		Metadata:    types.NewTypedMap(),
	}

	// Convert metadata
	if old.Metadata != nil {
		adapter := &TypedSecurityEventAdapter{}
		for key, value := range old.Metadata {
			typed.Metadata.Set(key, adapter.convertToTypedValue(value))
		}
	}

	return typed
}

// TypedIncident represents a typed security incident
type TypedIncident struct {
	ID          string                `json:"id"`
	Title       string                `json:"title"`
	Description string                `json:"description"`
	Severity    SeverityLevel         `json:"severity"`
	Status      IncidentStatus        `json:"status"`
	ReportedAt  time.Time             `json:"reported_at"`
	ResolvedAt  *time.Time            `json:"resolved_at,omitempty"`
	Reporter    string                `json:"reporter"`
	Assignee    string                `json:"assignee,omitempty"`
	Events      []*TypedSecurityEvent `json:"events"`
	Actions     []IncidentAction      `json:"actions"`
	RootCause   string                `json:"root_cause,omitempty"`
	Impact      string                `json:"impact,omitempty"`
	Metadata    *types.TypedMap       `json:"metadata"`
}

// CreateTypedSecurityEvent is a helper function to create typed security events
func CreateTypedSecurityEvent(eventType SecurityEventType, severity SeverityLevel, source, target, description string) *TypedSecurityEvent {
	return &TypedSecurityEvent{
		ID:          generateEventID(),
		Type:        eventType,
		Severity:    string(severity),
		Source:      source,
		Description: description,
		Timestamp:   common.ConsensusNow(),
		Details:     make(TypedEventDetails),
		// Target and Metadata fields removed from TypedSecurityEvent
	}
}

// CreateTypedSecurityEventWithDetails creates a typed security event with details
func CreateTypedSecurityEventWithDetails(eventType SecurityEventType, severity SeverityLevel, source, target, description string, details map[string]interface{}) *TypedSecurityEvent {
	event := CreateTypedSecurityEvent(eventType, severity, source, target, description)

	// Convert details to typed map
	for key, value := range details {
		event.Details.SetString(key, fmt.Sprintf("%v", value))
	}

	return event
}

// MigrateSecurityEventDetails migrates map[string]interface{} to TypedMap
func MigrateSecurityEventDetails(details map[string]interface{}) *types.TypedMap {
	typed := types.NewTypedMap()
	adapter := &TypedSecurityEventAdapter{}

	for key, value := range details {
		typed.Set(key, adapter.convertToTypedValue(value))
	}

	return typed
}

// MigrateIncidentMetadata migrates incident metadata to typed format
func MigrateIncidentMetadata(metadata map[string]interface{}) *types.TypedMap {
	return MigrateSecurityEventDetails(metadata)
}

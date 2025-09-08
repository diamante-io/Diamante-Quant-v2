package security

import (
	"diamante/common"
	"fmt"
	"time"
)

// EventDetailType represents the type of event detail
type EventDetailType int

const (
	EventDetailTypeString EventDetailType = iota
	EventDetailTypeInt
	EventDetailTypeFloat
	EventDetailTypeBool
	EventDetailTypeTime
	EventDetailTypeStringSlice
	EventDetailTypeMap
)

// TypedEventDetail represents a typed event detail value
type TypedEventDetail struct {
	Type        EventDetailType
	StringValue string
	IntValue    int64
	FloatValue  float64
	BoolValue   bool
	TimeValue   time.Time
	StringSlice []string
	MapValue    map[string]string
}

// NewStringDetail creates a string event detail
func NewStringDetail(value string) TypedEventDetail {
	return TypedEventDetail{
		Type:        EventDetailTypeString,
		StringValue: value,
	}
}

// NewIntDetail creates an int event detail
func NewIntDetail(value int64) TypedEventDetail {
	return TypedEventDetail{
		Type:     EventDetailTypeInt,
		IntValue: value,
	}
}

// NewFloatDetail creates a float event detail
func NewFloatDetail(value float64) TypedEventDetail {
	return TypedEventDetail{
		Type:       EventDetailTypeFloat,
		FloatValue: value,
	}
}

// NewBoolDetail creates a bool event detail
func NewBoolDetail(value bool) TypedEventDetail {
	return TypedEventDetail{
		Type:      EventDetailTypeBool,
		BoolValue: value,
	}
}

// NewTimeDetail creates a time event detail
func NewTimeDetail(value time.Time) TypedEventDetail {
	return TypedEventDetail{
		Type:      EventDetailTypeTime,
		TimeValue: value,
	}
}

// NewStringSliceDetail creates a string slice event detail
func NewStringSliceDetail(value []string) TypedEventDetail {
	return TypedEventDetail{
		Type:        EventDetailTypeStringSlice,
		StringSlice: value,
	}
}

// NewMapDetail creates a map event detail
func NewMapDetail(value map[string]string) TypedEventDetail {
	return TypedEventDetail{
		Type:     EventDetailTypeMap,
		MapValue: value,
	}
}

// String returns the string representation of the detail
func (ted TypedEventDetail) String() string {
	switch ted.Type {
	case EventDetailTypeString:
		return ted.StringValue
	case EventDetailTypeInt:
		return fmt.Sprintf("%d", ted.IntValue)
	case EventDetailTypeFloat:
		return fmt.Sprintf("%f", ted.FloatValue)
	case EventDetailTypeBool:
		return fmt.Sprintf("%t", ted.BoolValue)
	case EventDetailTypeTime:
		return ted.TimeValue.Format(time.RFC3339)
	case EventDetailTypeStringSlice:
		return fmt.Sprintf("%v", ted.StringSlice)
	case EventDetailTypeMap:
		return fmt.Sprintf("%v", ted.MapValue)
	default:
		return ""
	}
}

// TypedEventDetails represents a collection of typed event details
type TypedEventDetails map[string]TypedEventDetail

// SetString sets a string detail
func (ted TypedEventDetails) SetString(key, value string) {
	ted[key] = NewStringDetail(value)
}

// SetInt sets an int detail
func (ted TypedEventDetails) SetInt(key string, value int64) {
	ted[key] = NewIntDetail(value)
}

// SetFloat sets a float detail
func (ted TypedEventDetails) SetFloat(key string, value float64) {
	ted[key] = NewFloatDetail(value)
}

// SetBool sets a bool detail
func (ted TypedEventDetails) SetBool(key string, value bool) {
	ted[key] = NewBoolDetail(value)
}

// SetTime sets a time detail
func (ted TypedEventDetails) SetTime(key string, value time.Time) {
	ted[key] = NewTimeDetail(value)
}

// SetStringSlice sets a string slice detail
func (ted TypedEventDetails) SetStringSlice(key string, value []string) {
	ted[key] = NewStringSliceDetail(value)
}

// SetMap sets a map detail
func (ted TypedEventDetails) SetMap(key string, value map[string]string) {
	ted[key] = NewMapDetail(value)
}

// GetString gets a string detail
func (ted TypedEventDetails) GetString(key string) (string, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeString {
		return detail.StringValue, true
	}
	return "", false
}

// GetInt gets an int detail
func (ted TypedEventDetails) GetInt(key string) (int64, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeInt {
		return detail.IntValue, true
	}
	return 0, false
}

// GetFloat gets a float detail
func (ted TypedEventDetails) GetFloat(key string) (float64, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeFloat {
		return detail.FloatValue, true
	}
	return 0, false
}

// GetBool gets a bool detail
func (ted TypedEventDetails) GetBool(key string) (bool, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeBool {
		return detail.BoolValue, true
	}
	return false, false
}

// GetTime gets a time detail
func (ted TypedEventDetails) GetTime(key string) (time.Time, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeTime {
		return detail.TimeValue, true
	}
	return time.Time{}, false
}

// GetStringSlice gets a string slice detail
func (ted TypedEventDetails) GetStringSlice(key string) ([]string, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeStringSlice {
		return detail.StringSlice, true
	}
	return nil, false
}

// GetMap gets a map detail
func (ted TypedEventDetails) GetMap(key string) (map[string]string, bool) {
	if detail, exists := ted[key]; exists && detail.Type == EventDetailTypeMap {
		return detail.MapValue, true
	}
	return nil, false
}

// ToMap converts to map[string]interface{} for compatibility
func (ted TypedEventDetails) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range ted {
		switch v.Type {
		case EventDetailTypeString:
			result[k] = v.StringValue
		case EventDetailTypeInt:
			result[k] = v.IntValue
		case EventDetailTypeFloat:
			result[k] = v.FloatValue
		case EventDetailTypeBool:
			result[k] = v.BoolValue
		case EventDetailTypeTime:
			result[k] = v.TimeValue
		case EventDetailTypeStringSlice:
			result[k] = v.StringSlice
		case EventDetailTypeMap:
			result[k] = v.MapValue
		}
	}
	return result
}

// TypedSecurityEvent represents a security event with typed details
type TypedSecurityEvent struct {
	ID          string
	Type        SecurityEventType
	Severity    string
	Description string
	Timestamp   time.Time
	Source      string
	IPAddress   string
	UserID      string
	Details     TypedEventDetails
}

// ToSecurityEvent converts to SecurityEvent for compatibility
func (tse *TypedSecurityEvent) ToSecurityEvent() SecurityEvent {
	return SecurityEvent{
		ID:          tse.ID,
		Type:        tse.Type,
		Severity:    SeverityLevel(tse.Severity),
		Description: tse.Description,
		Timestamp:   tse.Timestamp,
		Source:      tse.Source,
		IPAddress:   tse.IPAddress,
		UserID:      tse.UserID,
		Details:     tse.Details.ToMap(),
	}
}

// NewTypedSecurityEvent creates a new typed security event
func NewTypedSecurityEvent(eventType SecurityEventType, severity, description string) *TypedSecurityEvent {
	return &TypedSecurityEvent{
		ID:          fmt.Sprintf("SEC-%d", common.ConsensusNow().UnixNano()),
		Type:        eventType,
		Severity:    severity,
		Description: description,
		Timestamp:   common.ConsensusNow(),
		Details:     make(TypedEventDetails),
	}
}

// TypedAuditLog represents an audit log entry with typed details
type TypedAuditLog struct {
	ID        string
	Timestamp time.Time
	EventType string
	UserID    string
	IPAddress string
	UserAgent string
	Resource  string
	Action    string
	Result    string
	Details   TypedEventDetails
}

// ToAuditLog converts to AuditLog for compatibility
func (tal *TypedAuditLog) ToAuditLog() AuditLog {
	return AuditLog{
		ID:        tal.ID,
		Timestamp: tal.Timestamp,
		EventType: tal.EventType,
		UserID:    tal.UserID,
		IPAddress: tal.IPAddress,
		UserAgent: tal.UserAgent,
		Resource:  tal.Resource,
		Action:    tal.Action,
		Result:    tal.Result,
		Details:   tal.Details.ToMap(),
	}
}

// TypedStatistics represents typed statistics
type TypedStatistics struct {
	CountByType     map[string]int64
	CountBySeverity map[string]int64
	TotalEvents     int64
	LastEventTime   time.Time
	TopUsers        []UserActivity
}

// UserActivity represents user activity statistics
type UserActivity struct {
	UserID string
	Count  int64
}

// ToMap converts statistics to map for compatibility
func (ts *TypedStatistics) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	result["count_by_type"] = ts.CountByType
	result["count_by_severity"] = ts.CountBySeverity
	result["total_events"] = ts.TotalEvents
	result["last_event_time"] = ts.LastEventTime

	// Convert user activities
	userActivities := make([]map[string]interface{}, len(ts.TopUsers))
	for i, ua := range ts.TopUsers {
		userActivities[i] = map[string]interface{}{
			"user_id": ua.UserID,
			"count":   ua.Count,
		}
	}
	result["top_users"] = userActivities

	return result
}

// SIEMEvent represents a typed SIEM event
type SIEMEvent struct {
	Timestamp   int64             `json:"timestamp"`
	EventID     string            `json:"event_id"`
	EventType   string            `json:"event_type"`
	Severity    string            `json:"severity"`
	Source      string            `json:"source"`
	UserID      string            `json:"user_id,omitempty"`
	IPAddress   string            `json:"ip_address,omitempty"`
	Description string            `json:"description"`
	Details     map[string]string `json:"details"`
}

// NewSIEMEvent creates a SIEM event from a typed security event
func NewSIEMEvent(event *TypedSecurityEvent) *SIEMEvent {
	// Convert typed details to string map
	details := make(map[string]string)
	for k, v := range event.Details {
		details[k] = v.String()
	}

	return &SIEMEvent{
		Timestamp:   event.Timestamp.Unix(),
		EventID:     event.ID,
		EventType:   string(event.Type),
		Severity:    event.Severity,
		Source:      event.Source,
		UserID:      event.UserID,
		IPAddress:   event.IPAddress,
		Description: event.Description,
		Details:     details,
	}
}

// IncidentTicket represents a typed incident ticket
type IncidentTicket struct {
	IncidentID  string            `json:"incident_id"`
	Severity    string            `json:"severity"`
	Description string            `json:"description"`
	EventID     string            `json:"event_id"`
	CreatedAt   time.Time         `json:"created_at"`
	Status      string            `json:"status"`
	Assignee    string            `json:"assignee,omitempty"`
	Priority    string            `json:"priority"`
	Details     map[string]string `json:"details"`
}

// NewIncidentTicket creates an incident ticket from a typed security event
func NewIncidentTicket(event *TypedSecurityEvent) *IncidentTicket {
	// Convert typed details to string map
	details := make(map[string]string)
	for k, v := range event.Details {
		details[k] = v.String()
	}

	return &IncidentTicket{
		IncidentID:  fmt.Sprintf("INC-%d", common.ConsensusNow().Unix()),
		Severity:    event.Severity,
		Description: event.Description,
		EventID:     event.ID,
		CreatedAt:   common.ConsensusNow(),
		Status:      "open",
		Priority:    determinePriority(event.Severity),
		Details:     details,
	}
}

func determinePriority(severity string) string {
	switch severity {
	case "CRITICAL":
		return "P1"
	case "HIGH":
		return "P2"
	case "MEDIUM":
		return "P3"
	default:
		return "P4"
	}
}

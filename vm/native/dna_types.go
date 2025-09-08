package native

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"diamante/consensus"

	"github.com/sirupsen/logrus"
)

// ResourceTypeID represents a unique identifier for a resource type
type ResourceTypeID string

// Address represents a blockchain address
type Address = string

// DNAResource represents a resource in the DNA language
type DNAResource struct {
	TypeID     ResourceTypeID    `json:"type_id"`
	ResourceID string            `json:"resource_id"`
	Owner      Address           `json:"owner"`
	Data       []byte            `json:"data"`
	Metadata   map[string]string `json:"metadata"`

	// Resource state tracking
	Borrowed   bool    `json:"borrowed"`
	BorrowedBy Address `json:"borrowed_by,omitempty"`
	BorrowedAt int64   `json:"borrowed_at,omitempty"`
	Consumed   bool    `json:"consumed"`
	ConsumedAt int64   `json:"consumed_at,omitempty"`
	ConsumedBy string  `json:"consumed_by,omitempty"` // Contract that consumed it

	// Lifecycle tracking
	CreatedAt    int64  `json:"created_at"`
	CreatedBy    string `json:"created_by"`
	LastModified int64  `json:"last_modified"`
	Version      uint64 `json:"version"`
}

// ResourceType defines the structure and capabilities of a resource type
type ResourceType struct {
	ID          ResourceTypeID `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Fields      []Field        `json:"fields"`
	Abilities   Abilities      `json:"abilities"`
	Invariants  []Invariant    `json:"invariants"`
	Methods     []Method       `json:"methods"`

	// Type metadata
	CreatedAt int64  `json:"created_at"`
	CreatedBy string `json:"created_by"`
	Version   uint64 `json:"version"`
	IsSystem  bool   `json:"is_system"` // System-defined types like Token, NFT
	IsFrozen  bool   `json:"is_frozen"` // Cannot be modified
}

// Field represents a field in a resource type
type Field struct {
	Name         string       `json:"name"`
	Type         DataType     `json:"type"`
	Required     bool         `json:"required"`
	Mutable      bool         `json:"mutable"`
	DefaultValue interface{}  `json:"default_value,omitempty"`
	Constraints  []Constraint `json:"constraints,omitempty"`
}

// DataType represents the data type of a field
type DataType struct {
	Kind         TypeKind       `json:"kind"`                    // u8, u64, string, bool, address, resource, vector
	ResourceType ResourceTypeID `json:"resource_type,omitempty"` // For resource types
	ElementType  *DataType      `json:"element_type,omitempty"`  // For vectors
	Size         uint32         `json:"size,omitempty"`          // For fixed-size types
}

// TypeKind represents the kind of data type
type TypeKind string

const (
	TypeKindU8       TypeKind = "u8"
	TypeKindU16      TypeKind = "u16"
	TypeKindU32      TypeKind = "u32"
	TypeKindU64      TypeKind = "u64"
	TypeKindU128     TypeKind = "u128"
	TypeKindBool     TypeKind = "bool"
	TypeKindString   TypeKind = "string"
	TypeKindBytes    TypeKind = "bytes"
	TypeKindAddress  TypeKind = "address"
	TypeKindResource TypeKind = "resource"
	TypeKindVector   TypeKind = "vector"
	TypeKindOption   TypeKind = "option"
)

// Abilities define what operations are allowed on a resource type
type Abilities struct {
	Copy  bool `json:"copy"`  // Can be copied (default false for resources)
	Drop  bool `json:"drop"`  // Can be dropped/discarded (default false for resources)
	Store bool `json:"store"` // Can be stored in global storage
	Key   bool `json:"key"`   // Can be used as a key in global storage
}

// Default abilities for different categories
func DefaultResourceAbilities() Abilities {
	return Abilities{
		Copy:  false, // Resources cannot be copied
		Drop:  false, // Resources cannot be dropped
		Store: true,  // Resources can be stored
		Key:   false, // Resources typically cannot be keys
	}
}

func DefaultValueAbilities() Abilities {
	return Abilities{
		Copy:  true, // Values can be copied
		Drop:  true, // Values can be dropped
		Store: true, // Values can be stored
		Key:   true, // Values can be keys
	}
}

// Constraint represents a constraint on a field value
type Constraint struct {
	Type    ConstraintType `json:"type"`
	Value   interface{}    `json:"value"`
	Message string         `json:"message,omitempty"`
}

// ConstraintType represents the type of constraint
type ConstraintType string

const (
	ConstraintMin     ConstraintType = "min"
	ConstraintMax     ConstraintType = "max"
	ConstraintRange   ConstraintType = "range"
	ConstraintPattern ConstraintType = "pattern"
	ConstraintLength  ConstraintType = "length"
	ConstraintUnique  ConstraintType = "unique"
	ConstraintCustom  ConstraintType = "custom"
)

// Invariant represents a mathematical invariant that must hold for the resource type
type Invariant struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Expression  string `json:"expression"` // Mathematical expression in DNA syntax
	Active      bool   `json:"active"`
}

// Method represents a method that can be called on resources of this type
type Method struct {
	Name       string      `json:"name"`
	Parameters []Parameter `json:"parameters"`
	ReturnType *DataType   `json:"return_type,omitempty"`
	Visibility Visibility  `json:"visibility"`
	Mutability Mutability  `json:"mutability"`
	Code       string      `json:"code"` // DNA code for the method
}

// Parameter represents a method parameter
type Parameter struct {
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Required bool     `json:"required"`
}

// Visibility determines who can call a method
type Visibility string

const (
	VisibilityPublic   Visibility = "public"
	VisibilityInternal Visibility = "internal"
	VisibilityPrivate  Visibility = "private"
)

// Mutability determines if a method can modify the resource
type Mutability string

const (
	MutabilityMutable   Mutability = "mutable"
	MutabilityImmutable Mutability = "immutable"
)

// ResourceManager manages resources and their types in the DNA runtime
type ResourceManager struct {
	resourceTypes map[ResourceTypeID]*ResourceType
	resources     map[string]*DNAResource
	ownership     map[Address][]string       // address -> resource IDs
	borrows       map[string]*BorrowInfo     // resource ID -> borrow info
	capabilities  map[string]map[string]bool // contract -> resource type -> allowed
	logger        *logrus.Logger
}

// BorrowInfo tracks information about borrowed resources
type BorrowInfo struct {
	ResourceID string     `json:"resource_id"`
	BorrowedBy Address    `json:"borrowed_by"`
	BorrowedAt time.Time  `json:"borrowed_at"`
	BorrowType BorrowType `json:"borrow_type"`
	ExpiresAt  time.Time  `json:"expires_at,omitempty"`
	Returned   bool       `json:"returned"`
	ReturnedAt time.Time  `json:"returned_at,omitempty"`
}

// BorrowType represents the type of borrow
type BorrowType string

const (
	BorrowTypeShared    BorrowType = "shared"    // Read-only borrow
	BorrowTypeExclusive BorrowType = "exclusive" // Mutable borrow
)

// NewResourceManager creates a new resource manager
func NewResourceManager(logger *logrus.Logger) *ResourceManager {
	rm := &ResourceManager{
		resourceTypes: make(map[ResourceTypeID]*ResourceType),
		resources:     make(map[string]*DNAResource),
		ownership:     make(map[Address][]string),
		borrows:       make(map[string]*BorrowInfo),
		capabilities:  make(map[string]map[string]bool),
		logger:        logger,
	}

	// Register system resource types
	rm.registerSystemTypes()

	return rm
}

// registerSystemTypes registers built-in system resource types
func (rm *ResourceManager) registerSystemTypes() {
	// Token resource type
	tokenType := &ResourceType{
		ID:          "system::Token",
		Name:        "Token",
		Description: "Fungible token resource",
		Fields: []Field{
			{
				Name:     "amount",
				Type:     DataType{Kind: TypeKindU64},
				Required: true,
				Mutable:  false,
			},
			{
				Name:     "symbol",
				Type:     DataType{Kind: TypeKindString},
				Required: true,
				Mutable:  false,
			},
		},
		Abilities: DefaultResourceAbilities(),
		Invariants: []Invariant{
			{
				Name:        "positive_amount",
				Description: "Token amount must be positive",
				Expression:  "self.amount > 0",
				Active:      true,
			},
		},
		Methods: []Method{
			{
				Name: "split",
				Parameters: []Parameter{
					{Name: "amount", Type: DataType{Kind: TypeKindU64}, Required: true},
				},
				ReturnType: &DataType{Kind: TypeKindResource, ResourceType: "system::Token"},
				Visibility: VisibilityPublic,
				Mutability: MutabilityMutable,
				Code:       "// Split token implementation",
			},
			{
				Name: "merge",
				Parameters: []Parameter{
					{Name: "other", Type: DataType{Kind: TypeKindResource, ResourceType: "system::Token"}, Required: true},
				},
				Visibility: VisibilityPublic,
				Mutability: MutabilityMutable,
				Code:       "// Merge token implementation",
			},
		},
		CreatedAt: consensus.ConsensusUnix(),
		CreatedBy: "system",
		Version:   1,
		IsSystem:  true,
		IsFrozen:  true,
	}

	// NFT resource type
	nftType := &ResourceType{
		ID:          "system::NFT",
		Name:        "NFT",
		Description: "Non-fungible token resource",
		Fields: []Field{
			{
				Name:     "token_id",
				Type:     DataType{Kind: TypeKindU64},
				Required: true,
				Mutable:  false,
			},
			{
				Name:     "collection",
				Type:     DataType{Kind: TypeKindString},
				Required: true,
				Mutable:  false,
			},
			{
				Name:     "metadata",
				Type:     DataType{Kind: TypeKindBytes},
				Required: false,
				Mutable:  true,
			},
		},
		Abilities: DefaultResourceAbilities(),
		Invariants: []Invariant{
			{
				Name:        "unique_token_id",
				Description: "Token ID must be unique within collection",
				Expression:  "unique(self.collection, self.token_id)",
				Active:      true,
			},
		},
		Methods: []Method{
			{
				Name: "update_metadata",
				Parameters: []Parameter{
					{Name: "new_metadata", Type: DataType{Kind: TypeKindBytes}, Required: true},
				},
				Visibility: VisibilityPublic,
				Mutability: MutabilityMutable,
				Code:       "// Update NFT metadata implementation",
			},
		},
		CreatedAt: consensus.ConsensusUnix(),
		CreatedBy: "system",
		Version:   1,
		IsSystem:  true,
		IsFrozen:  true,
	}

	rm.resourceTypes[tokenType.ID] = tokenType
	rm.resourceTypes[nftType.ID] = nftType

	rm.logger.Info("Registered system resource types: Token, NFT")
}

// DefineResourceType defines a new resource type
func (rm *ResourceManager) DefineResourceType(resourceType *ResourceType, creator string) error {
	// Validate resource type
	if err := rm.validateResourceType(resourceType); err != nil {
		return fmt.Errorf("invalid resource type: %v", err)
	}

	// Check if type already exists
	if _, exists := rm.resourceTypes[resourceType.ID]; exists {
		return fmt.Errorf("resource type %s already exists", resourceType.ID)
	}

	// Set metadata
	resourceType.CreatedAt = consensus.ConsensusUnix()
	resourceType.CreatedBy = creator
	resourceType.Version = 1

	// Store resource type
	rm.resourceTypes[resourceType.ID] = resourceType

	rm.logger.WithFields(logrus.Fields{
		"type_id":     resourceType.ID,
		"name":        resourceType.Name,
		"creator":     creator,
		"field_count": len(resourceType.Fields),
	}).Info("Defined new resource type")

	return nil
}

// CreateResource creates a new resource instance
func (rm *ResourceManager) CreateResource(typeID ResourceTypeID, owner Address, data map[string]interface{}, creator string) (*DNAResource, error) {
	// Get resource type
	resourceType, exists := rm.resourceTypes[typeID]
	if !exists {
		return nil, fmt.Errorf("resource type %s not found", typeID)
	}

	// Check abilities - must allow creation
	if !resourceType.Abilities.Store {
		return nil, fmt.Errorf("resource type %s does not allow storage", typeID)
	}

	// Validate data against resource type
	if err := rm.validateResourceData(resourceType, data); err != nil {
		return nil, fmt.Errorf("invalid resource data: %v", err)
	}

	// Serialize data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize resource data: %v", err)
	}

	// Generate resource ID
	resourceID := rm.generateResourceID(typeID, owner, dataBytes)

	// Create resource
	resource := &DNAResource{
		TypeID:       typeID,
		ResourceID:   resourceID,
		Owner:        owner,
		Data:         dataBytes,
		Metadata:     make(map[string]string),
		Borrowed:     false,
		Consumed:     false,
		CreatedAt:    consensus.ConsensusUnix(),
		CreatedBy:    creator,
		LastModified: consensus.ConsensusUnix(),
		Version:      1,
	}

	// Store resource
	rm.resources[resourceID] = resource

	// Update ownership
	if rm.ownership[owner] == nil {
		rm.ownership[owner] = make([]string, 0)
	}
	rm.ownership[owner] = append(rm.ownership[owner], resourceID)

	// Check invariants
	if err := rm.checkInvariants(resource); err != nil {
		// Rollback creation
		delete(rm.resources, resourceID)
		rm.removeFromOwnership(owner, resourceID)
		return nil, fmt.Errorf("invariant violation: %v", err)
	}

	rm.logger.WithFields(logrus.Fields{
		"resource_id": resourceID,
		"type_id":     typeID,
		"owner":       owner,
		"creator":     creator,
	}).Info("Created new resource")

	return resource, nil
}

// TransferResource transfers ownership of a resource
func (rm *ResourceManager) TransferResource(resourceID string, fromOwner, toOwner Address) error {
	// Get resource
	resource, exists := rm.resources[resourceID]
	if !exists {
		return fmt.Errorf("resource %s not found", resourceID)
	}

	// Check ownership
	if resource.Owner != fromOwner {
		return fmt.Errorf("resource %s not owned by %s", resourceID, fromOwner)
	}

	// Check if resource is borrowed
	if resource.Borrowed {
		return fmt.Errorf("cannot transfer borrowed resource %s", resourceID)
	}

	// Check if resource is consumed
	if resource.Consumed {
		return fmt.Errorf("cannot transfer consumed resource %s", resourceID)
	}

	// Update ownership
	rm.removeFromOwnership(fromOwner, resourceID)
	if rm.ownership[toOwner] == nil {
		rm.ownership[toOwner] = make([]string, 0)
	}
	rm.ownership[toOwner] = append(rm.ownership[toOwner], resourceID)

	// Update resource
	resource.Owner = toOwner
	resource.LastModified = consensus.ConsensusUnix()
	resource.Version++

	rm.logger.WithFields(logrus.Fields{
		"resource_id": resourceID,
		"from_owner":  fromOwner,
		"to_owner":    toOwner,
	}).Info("Transferred resource ownership")

	return nil
}

// BorrowResource allows temporary borrowing of a resource
func (rm *ResourceManager) BorrowResource(resourceID string, borrower Address, borrowType BorrowType, duration time.Duration) error {
	// Get resource
	resource, exists := rm.resources[resourceID]
	if !exists {
		return fmt.Errorf("resource %s not found", resourceID)
	}

	// Check if already borrowed
	if resource.Borrowed {
		return fmt.Errorf("resource %s is already borrowed", resourceID)
	}

	// Check if consumed
	if resource.Consumed {
		return fmt.Errorf("cannot borrow consumed resource %s", resourceID)
	}

	// Create borrow info
	borrowInfo := &BorrowInfo{
		ResourceID: resourceID,
		BorrowedBy: borrower,
		BorrowedAt: time.Now(),
		BorrowType: borrowType,
		Returned:   false,
	}

	if duration > 0 {
		borrowInfo.ExpiresAt = time.Now().Add(duration)
	}

	// Update resource
	resource.Borrowed = true
	resource.BorrowedBy = borrower
	resource.BorrowedAt = consensus.ConsensusUnix()
	resource.LastModified = consensus.ConsensusUnix()

	// Store borrow info
	rm.borrows[resourceID] = borrowInfo

	rm.logger.WithFields(logrus.Fields{
		"resource_id": resourceID,
		"borrower":    borrower,
		"borrow_type": borrowType,
		"duration":    duration,
	}).Info("Resource borrowed")

	return nil
}

// ReturnResource returns a borrowed resource
func (rm *ResourceManager) ReturnResource(resourceID string, borrower Address) error {
	// Get borrow info
	borrowInfo, exists := rm.borrows[resourceID]
	if !exists {
		return fmt.Errorf("resource %s is not borrowed", resourceID)
	}

	// Check borrower
	if borrowInfo.BorrowedBy != borrower {
		return fmt.Errorf("resource %s not borrowed by %s", resourceID, borrower)
	}

	// Get resource
	resource := rm.resources[resourceID]

	// Update resource
	resource.Borrowed = false
	resource.BorrowedBy = ""
	resource.BorrowedAt = 0
	resource.LastModified = consensus.ConsensusUnix()

	// Update borrow info
	borrowInfo.Returned = true
	borrowInfo.ReturnedAt = time.Now()

	// Remove from active borrows
	delete(rm.borrows, resourceID)

	rm.logger.WithFields(logrus.Fields{
		"resource_id": resourceID,
		"borrower":    borrower,
	}).Info("Resource returned")

	return nil
}

// ConsumeResource marks a resource as consumed (single-use semantics)
func (rm *ResourceManager) ConsumeResource(resourceID string, consumer string) error {
	// Get resource
	resource, exists := rm.resources[resourceID]
	if !exists {
		return fmt.Errorf("resource %s not found", resourceID)
	}

	// Check if already consumed
	if resource.Consumed {
		return fmt.Errorf("resource %s is already consumed", resourceID)
	}

	// Check if borrowed
	if resource.Borrowed {
		return fmt.Errorf("cannot consume borrowed resource %s", resourceID)
	}

	// Mark as consumed
	resource.Consumed = true
	resource.ConsumedAt = consensus.ConsensusUnix()
	resource.ConsumedBy = consumer
	resource.LastModified = consensus.ConsensusUnix()

	// Remove from ownership (consumed resources have no owner)
	rm.removeFromOwnership(resource.Owner, resourceID)

	rm.logger.WithFields(logrus.Fields{
		"resource_id": resourceID,
		"consumer":    consumer,
		"owner":       resource.Owner,
	}).Info("Resource consumed")

	return nil
}

// Helper methods

func (rm *ResourceManager) validateResourceType(resourceType *ResourceType) error {
	if resourceType.ID == "" {
		return fmt.Errorf("resource type ID cannot be empty")
	}
	if resourceType.Name == "" {
		return fmt.Errorf("resource type name cannot be empty")
	}
	if len(resourceType.Fields) == 0 {
		return fmt.Errorf("resource type must have at least one field")
	}

	// Validate fields
	for _, field := range resourceType.Fields {
		if field.Name == "" {
			return fmt.Errorf("field name cannot be empty")
		}
		if err := rm.validateDataType(field.Type); err != nil {
			return fmt.Errorf("invalid field type %s: %v", field.Name, err)
		}
	}

	return nil
}

func (rm *ResourceManager) validateDataType(dataType DataType) error {
	validKinds := map[TypeKind]bool{
		TypeKindU8: true, TypeKindU16: true, TypeKindU32: true, TypeKindU64: true, TypeKindU128: true,
		TypeKindBool: true, TypeKindString: true, TypeKindBytes: true,
		TypeKindAddress: true, TypeKindResource: true, TypeKindVector: true, TypeKindOption: true,
	}

	if !validKinds[dataType.Kind] {
		return fmt.Errorf("invalid type kind: %s", dataType.Kind)
	}

	// Validate resource type reference
	if dataType.Kind == TypeKindResource {
		if dataType.ResourceType == "" {
			return fmt.Errorf("resource type must specify resource_type")
		}
		// Note: We don't validate if the resource type exists here to allow forward references
	}

	// Validate vector element type
	if dataType.Kind == TypeKindVector {
		if dataType.ElementType == nil {
			return fmt.Errorf("vector type must specify element_type")
		}
		return rm.validateDataType(*dataType.ElementType)
	}

	return nil
}

func (rm *ResourceManager) validateResourceData(resourceType *ResourceType, data map[string]interface{}) error {
	// Check all required fields are present
	for _, field := range resourceType.Fields {
		if field.Required {
			if _, exists := data[field.Name]; !exists {
				return fmt.Errorf("required field %s is missing", field.Name)
			}
		}
	}

	// Validate field values
	for fieldName, value := range data {
		// Find field definition
		var field *Field
		for i := range resourceType.Fields {
			if resourceType.Fields[i].Name == fieldName {
				field = &resourceType.Fields[i]
				break
			}
		}

		if field == nil {
			return fmt.Errorf("unknown field: %s", fieldName)
		}

		// Validate value type
		if err := rm.validateFieldValue(field, value); err != nil {
			return fmt.Errorf("invalid value for field %s: %v", fieldName, err)
		}
	}

	return nil
}

func (rm *ResourceManager) validateFieldValue(field *Field, value interface{}) error {
	// Type checking based on field type
	switch field.Type.Kind {
	case TypeKindU8, TypeKindU16, TypeKindU32, TypeKindU64, TypeKindU128:
		if _, ok := value.(float64); !ok {
			if _, ok := value.(int64); !ok {
				return fmt.Errorf("expected numeric value")
			}
		}
	case TypeKindBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean value")
		}
	case TypeKindString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value")
		}
	case TypeKindAddress:
		if addr, ok := value.(string); !ok || addr == "" {
			return fmt.Errorf("expected valid address")
		}
	}

	// Check constraints
	for _, constraint := range field.Constraints {
		if err := rm.validateConstraint(constraint, value); err != nil {
			return err
		}
	}

	return nil
}

func (rm *ResourceManager) validateConstraint(constraint Constraint, value interface{}) error {
	switch constraint.Type {
	case ConstraintMin:
		if minVal, ok := constraint.Value.(float64); ok {
			if val, ok := value.(float64); ok {
				if val < minVal {
					return fmt.Errorf("value %v is less than minimum %v", val, minVal)
				}
			}
		}
	case ConstraintMax:
		if maxVal, ok := constraint.Value.(float64); ok {
			if val, ok := value.(float64); ok {
				if val > maxVal {
					return fmt.Errorf("value %v is greater than maximum %v", val, maxVal)
				}
			}
		}
	}

	return nil
}

func (rm *ResourceManager) checkInvariants(resource *DNAResource) error {
	resourceType := rm.resourceTypes[resource.TypeID]

	for _, invariant := range resourceType.Invariants {
		if !invariant.Active {
			continue
		}

		// Simple invariant checking (in production, would use expression evaluator)
		// For example, checking "self.amount > 0" for tokens
		if invariant.Expression == "self.amount > 0" {
			var data map[string]interface{}
			if err := json.Unmarshal(resource.Data, &data); err != nil {
				continue
			}

			if amount, ok := data["amount"].(float64); ok {
				if amount <= 0 {
					return fmt.Errorf("invariant violation: %s", invariant.Description)
				}
			}
		}
	}

	return nil
}

func (rm *ResourceManager) generateResourceID(typeID ResourceTypeID, owner Address, data []byte) string {
	hasher := sha256.New()
	hasher.Write([]byte(string(typeID)))
	hasher.Write([]byte(owner))
	hasher.Write(data)
	hasher.Write([]byte(fmt.Sprintf("%d", consensus.ConsensusUnixNano())))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (rm *ResourceManager) removeFromOwnership(owner Address, resourceID string) {
	if ownerResources, exists := rm.ownership[owner]; exists {
		for i, id := range ownerResources {
			if id == resourceID {
				rm.ownership[owner] = append(ownerResources[:i], ownerResources[i+1:]...)
				break
			}
		}
	}
}

// Getter methods

// GetResourceType returns a resource type by ID
func (rm *ResourceManager) GetResourceType(id ResourceTypeID) (*ResourceType, error) {
	resourceType, exists := rm.resourceTypes[id]
	if !exists {
		return nil, fmt.Errorf("resource type %s not found", id)
	}
	return resourceType, nil
}

// GetResource returns a resource by ID
func (rm *ResourceManager) GetResource(id string) (*DNAResource, error) {
	resource, exists := rm.resources[id]
	if !exists {
		return nil, fmt.Errorf("resource %s not found", id)
	}
	return resource, nil
}

// GetResourcesByOwner returns all resources owned by an address
func (rm *ResourceManager) GetResourcesByOwner(owner Address) ([]*DNAResource, error) {
	resourceIDs, exists := rm.ownership[owner]
	if !exists {
		return []*DNAResource{}, nil
	}

	resources := make([]*DNAResource, 0, len(resourceIDs))
	for _, id := range resourceIDs {
		if resource, exists := rm.resources[id]; exists && !resource.Consumed {
			resources = append(resources, resource)
		}
	}

	return resources, nil
}

// GetResourcesByType returns all resources of a specific type
func (rm *ResourceManager) GetResourcesByType(typeID ResourceTypeID) ([]*DNAResource, error) {
	resources := make([]*DNAResource, 0)
	for _, resource := range rm.resources {
		if resource.TypeID == typeID && !resource.Consumed {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

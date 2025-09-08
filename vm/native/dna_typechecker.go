package native

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

// DNATypeChecker performs type checking and borrow checking for DNA code
type DNATypeChecker struct {
	resourceManager *ResourceManager
	symbolTable     *SymbolTable
	borrowTracker   *BorrowTracker
	errorReporter   *ErrorReporter
	logger          *logrus.Logger
}

// SymbolTable manages variable and function scope
type SymbolTable struct {
	scopes []map[string]*Symbol
	depth  int
}

// Symbol represents a symbol in the symbol table
type Symbol struct {
	Name       string        `json:"name"`
	Type       *ResolvedType `json:"type"`
	Kind       SymbolKind    `json:"kind"`
	Mutable    bool          `json:"mutable"`
	Borrowed   bool          `json:"borrowed"`
	BorrowType BorrowType    `json:"borrow_type,omitempty"`
	Consumed   bool          `json:"consumed"`
	Scope      int           `json:"scope"`
	Line       int           `json:"line"`
	Column     int           `json:"column"`
}

// SymbolKind represents the kind of symbol
type SymbolKind string

const (
	SymbolVariable SymbolKind = "variable"
	SymbolFunction SymbolKind = "function"
	SymbolType     SymbolKind = "type"
	SymbolResource SymbolKind = "resource"
)

// ResolvedType represents a fully resolved type
type ResolvedType struct {
	Kind         TypeKind      `json:"kind"`
	Name         string        `json:"name,omitempty"`
	Module       string        `json:"module,omitempty"`
	ResourceType *ResourceType `json:"resource_type,omitempty"`
	ElementType  *ResolvedType `json:"element_type,omitempty"`
	Abilities    Abilities     `json:"abilities"`
	Size         uint32        `json:"size,omitempty"`
}

// BorrowTracker tracks resource borrows and ensures borrow safety
type BorrowTracker struct {
	activeBorrows map[string]*BorrowEntry // variable name -> borrow entry
	borrowGraph   map[string][]string     // resource -> borrowers
	logger        *logrus.Logger
}

// BorrowEntry tracks information about an active borrow
type BorrowEntry struct {
	Variable   string     `json:"variable"`
	Resource   string     `json:"resource"`
	BorrowType BorrowType `json:"borrow_type"`
	Scope      int        `json:"scope"`
	Line       int        `json:"line"`
	Column     int        `json:"column"`
}

// ErrorReporter collects and reports type checking errors
type ErrorReporter struct {
	errors   []TypeError
	warnings []TypeWarning
}

// TypeError represents a type checking error
type TypeError struct {
	Message  string `json:"message"`
	Line     int    `json:"line"`
	Column   int    `json:"column"`
	Severity string `json:"severity"`
}

// TypeWarning represents a type checking warning
type TypeWarning struct {
	Message string `json:"message"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
}

// NewDNATypeChecker creates a new type checker
func NewDNATypeChecker(resourceManager *ResourceManager, logger *logrus.Logger) *DNATypeChecker {
	if logger == nil {
		logger = logrus.New()
	}

	return &DNATypeChecker{
		resourceManager: resourceManager,
		symbolTable:     NewSymbolTable(),
		borrowTracker:   NewBorrowTracker(logger),
		errorReporter:   NewErrorReporter(),
		logger:          logger,
	}
}

// NewSymbolTable creates a new symbol table
func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		scopes: make([]map[string]*Symbol, 0),
		depth:  0,
	}
}

// NewBorrowTracker creates a new borrow tracker
func NewBorrowTracker(logger *logrus.Logger) *BorrowTracker {
	return &BorrowTracker{
		activeBorrows: make(map[string]*BorrowEntry),
		borrowGraph:   make(map[string][]string),
		logger:        logger,
	}
}

// NewErrorReporter creates a new error reporter
func NewErrorReporter() *ErrorReporter {
	return &ErrorReporter{
		errors:   make([]TypeError, 0),
		warnings: make([]TypeWarning, 0),
	}
}

// TypeCheck performs type checking on a DNA module
func (tc *DNATypeChecker) TypeCheck(module *Module) error {
	tc.logger.WithField("module", module.Name).Info("Starting type checking")

	// Enter module scope
	tc.symbolTable.EnterScope()
	defer tc.symbolTable.ExitScope()

	// First pass: collect type definitions
	for _, item := range module.Items {
		if err := tc.collectTypeDefinitions(item); err != nil {
			return err
		}
	}

	// Second pass: type check all items
	for _, item := range module.Items {
		if err := tc.typeCheckModuleItem(item); err != nil {
			return err
		}
	}

	// Check for remaining borrow violations
	if err := tc.borrowTracker.CheckFinalBorrows(); err != nil {
		tc.errorReporter.AddError(err.Error(), 0, 0)
	}

	// Report errors
	if len(tc.errorReporter.errors) > 0 {
		return tc.formatErrors()
	}

	tc.logger.WithFields(logrus.Fields{
		"module":   module.Name,
		"warnings": len(tc.errorReporter.warnings),
	}).Info("Type checking completed successfully")

	return nil
}

// collectTypeDefinitions collects all type definitions in the first pass
func (tc *DNATypeChecker) collectTypeDefinitions(item ModuleItem) error {
	switch node := item.(type) {
	case *ResourceDef:
		return tc.collectResourceType(node)
	case *StructDef:
		return tc.collectStructType(node)
	case *Function:
		return tc.collectFunctionType(node)
	}
	return nil
}

// collectResourceType collects a resource type definition
func (tc *DNATypeChecker) collectResourceType(resource *ResourceDef) error {
	// Create resource type
	resourceType := &ResourceType{
		ID:          ResourceTypeID(resource.Name),
		Name:        resource.Name,
		Description: fmt.Sprintf("Resource type %s", resource.Name),
		Fields:      make([]Field, 0, len(resource.Fields)),
		Abilities:   DefaultResourceAbilities(),
		Methods:     make([]Method, 0, len(resource.Methods)),
		CreatedAt:   0, // Will be set by resource manager
		CreatedBy:   "compiler",
		Version:     1,
		IsSystem:    false,
		IsFrozen:    false,
	}

	// Process abilities
	for _, ability := range resource.Abilities {
		switch ability {
		case "copy":
			resourceType.Abilities.Copy = true
		case "drop":
			resourceType.Abilities.Drop = true
		case "store":
			resourceType.Abilities.Store = true
		case "key":
			resourceType.Abilities.Key = true
		}
	}

	// Process fields
	for _, field := range resource.Fields {
		resolvedType, err := tc.resolveType(field.Type)
		if err != nil {
			return fmt.Errorf("failed to resolve field type %s: %v", field.Name, err)
		}

		dataType := tc.resolvedTypeToDataType(resolvedType)
		resourceField := Field{
			Name:     field.Name,
			Type:     dataType,
			Required: true,
			Mutable:  true, // DNA fields are mutable by default
		}

		resourceType.Fields = append(resourceType.Fields, resourceField)
	}

	// Add to symbol table
	symbol := &Symbol{
		Name: resource.Name,
		Type: &ResolvedType{
			Kind:         TypeKindResource,
			Name:         resource.Name,
			ResourceType: resourceType,
			Abilities:    resourceType.Abilities,
		},
		Kind:   SymbolType,
		Scope:  tc.symbolTable.depth,
		Line:   resource.Line,
		Column: resource.Column,
	}

	tc.symbolTable.Define(resource.Name, symbol)

	tc.logger.WithFields(logrus.Fields{
		"resource":  resource.Name,
		"fields":    len(resource.Fields),
		"abilities": resource.Abilities,
	}).Debug("Collected resource type")

	return nil
}

// collectStructType collects a struct type definition
func (tc *DNATypeChecker) collectStructType(structDef *StructDef) error {
	// Create resolved type for struct
	abilities := DefaultValueAbilities()
	for _, ability := range structDef.Abilities {
		switch ability {
		case "copy":
			abilities.Copy = true
		case "drop":
			abilities.Drop = true
		case "store":
			abilities.Store = true
		case "key":
			abilities.Key = true
		}
	}

	resolvedType := &ResolvedType{
		Kind:      TypeKindResource, // Structs are treated as resource types in DNA
		Name:      structDef.Name,
		Abilities: abilities,
	}

	symbol := &Symbol{
		Name:   structDef.Name,
		Type:   resolvedType,
		Kind:   SymbolType,
		Scope:  tc.symbolTable.depth,
		Line:   structDef.Line,
		Column: structDef.Column,
	}

	tc.symbolTable.Define(structDef.Name, symbol)

	tc.logger.WithField("struct", structDef.Name).Debug("Collected struct type")
	return nil
}

// collectFunctionType collects a function type definition
func (tc *DNATypeChecker) collectFunctionType(function *Function) error {
	// Create function type
	paramTypes := make([]*ResolvedType, 0, len(function.Parameters))
	for _, param := range function.Parameters {
		paramType, err := tc.resolveType(param.Type)
		if err != nil {
			return fmt.Errorf("failed to resolve parameter type %s: %v", param.Name, err)
		}
		paramTypes = append(paramTypes, paramType)
	}

	var returnType *ResolvedType
	if function.ReturnType != nil {
		var err error
		returnType, err = tc.resolveType(function.ReturnType)
		if err != nil {
			return fmt.Errorf("failed to resolve return type: %v", err)
		}
	}

	functionType := &ResolvedType{
		Kind: TypeKindResource, // Functions are first-class in DNA
		Name: fmt.Sprintf("fn(%d params)", len(paramTypes)),
	}

	symbol := &Symbol{
		Name:   function.Name,
		Type:   functionType,
		Kind:   SymbolFunction,
		Scope:  tc.symbolTable.depth,
		Line:   function.Line,
		Column: function.Column,
	}

	tc.symbolTable.Define(function.Name, symbol)

	tc.logger.WithFields(logrus.Fields{
		"function":    function.Name,
		"parameters":  len(function.Parameters),
		"return_type": returnType != nil,
	}).Debug("Collected function type")

	return nil
}

// typeCheckModuleItem type checks a module item
func (tc *DNATypeChecker) typeCheckModuleItem(item ModuleItem) error {
	switch node := item.(type) {
	case *ResourceDef:
		return tc.typeCheckResource(node)
	case *StructDef:
		return tc.typeCheckStruct(node)
	case *Function:
		return tc.typeCheckFunction(node)
	}
	return nil
}

// typeCheckResource type checks a resource definition
func (tc *DNATypeChecker) typeCheckResource(resource *ResourceDef) error {
	// Enter resource scope
	tc.symbolTable.EnterScope()
	defer tc.symbolTable.ExitScope()

	// Add 'self' parameter for methods
	resourceType, err := tc.symbolTable.Lookup(resource.Name)
	if err != nil {
		return fmt.Errorf("resource type %s not found", resource.Name)
	}

	selfSymbol := &Symbol{
		Name:    "self",
		Type:    resourceType.Type,
		Kind:    SymbolVariable,
		Mutable: true,
		Scope:   tc.symbolTable.depth,
	}
	tc.symbolTable.Define("self", selfSymbol)

	// Type check methods
	for _, method := range resource.Methods {
		if err := tc.typeCheckFunction(&method); err != nil {
			return fmt.Errorf("error in method %s: %v", method.Name, err)
		}
	}

	// Type check invariants
	for _, invariant := range resource.Invariants {
		if err := tc.typeCheckInvariant(&invariant); err != nil {
			return fmt.Errorf("error in invariant %s: %v", invariant.Name, err)
		}
	}

	return nil
}

// typeCheckStruct type checks a struct definition
func (tc *DNATypeChecker) typeCheckStruct(structDef *StructDef) error {
	// Validate field types
	for _, field := range structDef.Fields {
		_, err := tc.resolveType(field.Type)
		if err != nil {
			return fmt.Errorf("invalid field type %s in struct %s: %v", field.Name, structDef.Name, err)
		}
	}

	return nil
}

// typeCheckFunction type checks a function
func (tc *DNATypeChecker) typeCheckFunction(function *Function) error {
	// Enter function scope
	tc.symbolTable.EnterScope()
	tc.borrowTracker.EnterScope()
	defer tc.symbolTable.ExitScope()
	defer tc.borrowTracker.ExitScope()

	// Add parameters to scope
	for _, param := range function.Parameters {
		paramType, err := tc.resolveType(param.Type)
		if err != nil {
			return fmt.Errorf("invalid parameter type %s: %v", param.Name, err)
		}

		symbol := &Symbol{
			Name:    param.Name,
			Type:    paramType,
			Kind:    SymbolVariable,
			Mutable: false,
			Scope:   tc.symbolTable.depth,
			Line:    param.Line,
			Column:  param.Column,
		}

		tc.symbolTable.Define(param.Name, symbol)
	}

	// Type check function body
	if function.Body != nil {
		if err := tc.typeCheckBlock(function.Body); err != nil {
			return fmt.Errorf("error in function body: %v", err)
		}
	}

	// Check return type consistency
	if function.ReturnType != nil {
		// TODO: Check that all return statements match the declared return type
	}

	return nil
}

// typeCheckBlock type checks a block
func (tc *DNATypeChecker) typeCheckBlock(block *Block) error {
	tc.symbolTable.EnterScope()
	tc.borrowTracker.EnterScope()
	defer tc.symbolTable.ExitScope()
	defer tc.borrowTracker.ExitScope()

	for _, stmt := range block.Statements {
		if err := tc.typeCheckStatement(stmt); err != nil {
			return err
		}
	}

	return nil
}

// typeCheckStatement type checks a statement
func (tc *DNATypeChecker) typeCheckStatement(stmt Statement) error {
	switch node := stmt.(type) {
	case *LetStatement:
		return tc.typeCheckLetStatement(node)
	case *AssignStatement:
		return tc.typeCheckAssignStatement(node)
	case *ExpressionStatement:
		_, err := tc.typeCheckExpression(node.Expression)
		return err
	case *ReturnStatement:
		if node.Value != nil {
			_, err := tc.typeCheckExpression(node.Value)
			return err
		}
		return nil
	}

	return fmt.Errorf("unknown statement type: %T", stmt)
}

// typeCheckLetStatement type checks a let statement
func (tc *DNATypeChecker) typeCheckLetStatement(stmt *LetStatement) error {
	// Type check the value
	valueType, err := tc.typeCheckExpression(stmt.Value)
	if err != nil {
		return fmt.Errorf("error in let statement value: %v", err)
	}

	// Check declared type if present
	var declaredType *ResolvedType
	if stmt.Type != nil {
		declaredType, err = tc.resolveType(stmt.Type)
		if err != nil {
			return fmt.Errorf("invalid type declaration: %v", err)
		}

		// Check type compatibility
		if !tc.isTypeCompatible(valueType, declaredType) {
			return fmt.Errorf("type mismatch: cannot assign %s to %s",
				tc.typeToString(valueType), tc.typeToString(declaredType))
		}
	} else {
		declaredType = valueType
	}

	// Check resource handling
	if tc.isResourceType(valueType) {
		// Resource is being moved to new variable
		if err := tc.borrowTracker.MoveResource(stmt.Name, stmt.Line, stmt.Column); err != nil {
			return fmt.Errorf("resource move error: %v", err)
		}
	}

	// Add to symbol table
	symbol := &Symbol{
		Name:    stmt.Name,
		Type:    declaredType,
		Kind:    SymbolVariable,
		Mutable: stmt.Mutable,
		Scope:   tc.symbolTable.depth,
		Line:    stmt.Line,
		Column:  stmt.Column,
	}

	tc.symbolTable.Define(stmt.Name, symbol)

	tc.logger.WithFields(logrus.Fields{
		"variable": stmt.Name,
		"type":     tc.typeToString(declaredType),
		"mutable":  stmt.Mutable,
	}).Debug("Type checked let statement")

	return nil
}

// typeCheckAssignStatement type checks an assignment statement
func (tc *DNATypeChecker) typeCheckAssignStatement(stmt *AssignStatement) error {
	// Type check target and value
	targetType, err := tc.typeCheckLValue(stmt.Target)
	if err != nil {
		return fmt.Errorf("invalid assignment target: %v", err)
	}

	valueType, err := tc.typeCheckExpression(stmt.Value)
	if err != nil {
		return fmt.Errorf("error in assignment value: %v", err)
	}

	// Check type compatibility
	if !tc.isTypeCompatible(valueType, targetType) {
		return fmt.Errorf("type mismatch in assignment: cannot assign %s to %s",
			tc.typeToString(valueType), tc.typeToString(targetType))
	}

	// Check mutability
	if ident, ok := stmt.Target.(*Identifier); ok {
		symbol, err := tc.symbolTable.Lookup(ident.Name)
		if err != nil {
			return fmt.Errorf("undefined variable: %s", ident.Name)
		}

		if !symbol.Mutable {
			return fmt.Errorf("cannot assign to immutable variable: %s", ident.Name)
		}

		// Check borrow constraints
		if tc.isResourceType(valueType) {
			if err := tc.borrowTracker.CheckAssignment(ident.Name, stmt.Line, stmt.Column); err != nil {
				return fmt.Errorf("borrow checker violation: %v", err)
			}
		}
	}

	return nil
}

// typeCheckLValue type checks an l-value (assignable expression)
func (tc *DNATypeChecker) typeCheckLValue(expr Expression) (*ResolvedType, error) {
	switch node := expr.(type) {
	case *Identifier:
		symbol, err := tc.symbolTable.Lookup(node.Name)
		if err != nil {
			return nil, fmt.Errorf("undefined variable: %s", node.Name)
		}
		return symbol.Type, nil
	case *FieldAccess:
		objectType, err := tc.typeCheckExpression(node.Object)
		if err != nil {
			return nil, err
		}

		// Look up field type in resource/struct
		if objectType.ResourceType != nil {
			for _, field := range objectType.ResourceType.Fields {
				if field.Name == node.Field {
					return tc.dataTypeToResolvedType(field.Type), nil
				}
			}
		}

		return nil, fmt.Errorf("field %s not found in type %s", node.Field, tc.typeToString(objectType))
	default:
		return nil, fmt.Errorf("invalid l-value: %T", expr)
	}
}

// typeCheckExpression type checks an expression and returns its type
func (tc *DNATypeChecker) typeCheckExpression(expr Expression) (*ResolvedType, error) {
	switch node := expr.(type) {
	case *Literal:
		return tc.typeCheckLiteral(node)
	case *Identifier:
		return tc.typeCheckIdentifier(node)
	case *BinaryOp:
		return tc.typeCheckBinaryOp(node)
	case *UnaryOp:
		return tc.typeCheckUnaryOp(node)
	case *Call:
		return tc.typeCheckCall(node)
	case *FieldAccess:
		return tc.typeCheckFieldAccess(node)
	default:
		return nil, fmt.Errorf("unknown expression type: %T", expr)
	}
}

// typeCheckLiteral type checks a literal
func (tc *DNATypeChecker) typeCheckLiteral(literal *Literal) (*ResolvedType, error) {
	switch literal.Kind {
	case TokenNumber:
		return &ResolvedType{
			Kind:      TypeKindU64,
			Abilities: DefaultValueAbilities(),
		}, nil
	case TokenString:
		return &ResolvedType{
			Kind:      TypeKindString,
			Abilities: DefaultValueAbilities(),
		}, nil
	case TokenBool:
		return &ResolvedType{
			Kind:      TypeKindBool,
			Abilities: DefaultValueAbilities(),
		}, nil
	case TokenAddress:
		return &ResolvedType{
			Kind:      TypeKindAddress,
			Abilities: DefaultValueAbilities(),
		}, nil
	default:
		return nil, fmt.Errorf("unknown literal type: %s", literal.Kind)
	}
}

// typeCheckIdentifier type checks an identifier
func (tc *DNATypeChecker) typeCheckIdentifier(ident *Identifier) (*ResolvedType, error) {
	symbol, err := tc.symbolTable.Lookup(ident.Name)
	if err != nil {
		return nil, fmt.Errorf("undefined identifier: %s", ident.Name)
	}

	// Check if resource is consumed
	if symbol.Consumed {
		return nil, fmt.Errorf("use of consumed resource: %s", ident.Name)
	}

	// Track resource usage
	if tc.isResourceType(symbol.Type) {
		if err := tc.borrowTracker.UseResource(ident.Name, ident.Line, ident.Column); err != nil {
			return nil, fmt.Errorf("resource usage error: %v", err)
		}
	}

	return symbol.Type, nil
}

// typeCheckBinaryOp type checks a binary operation
func (tc *DNATypeChecker) typeCheckBinaryOp(binop *BinaryOp) (*ResolvedType, error) {
	leftType, err := tc.typeCheckExpression(binop.Left)
	if err != nil {
		return nil, fmt.Errorf("error in left operand: %v", err)
	}

	rightType, err := tc.typeCheckExpression(binop.Right)
	if err != nil {
		return nil, fmt.Errorf("error in right operand: %v", err)
	}

	// Type check based on operator
	switch binop.Operator {
	case "+", "-", "*", "/", "%":
		// Arithmetic operators require numeric types
		if !tc.isNumericType(leftType) || !tc.isNumericType(rightType) {
			return nil, fmt.Errorf("arithmetic operator %s requires numeric operands", binop.Operator)
		}

		// Result type is the wider of the two operand types
		resultType := tc.getWiderType(leftType, rightType)
		return resultType, nil

	case "==", "!=":
		// Equality operators require compatible types
		if !tc.isTypeCompatible(leftType, rightType) {
			return nil, fmt.Errorf("equality operator %s requires compatible operands", binop.Operator)
		}
		return &ResolvedType{Kind: TypeKindBool, Abilities: DefaultValueAbilities()}, nil

	case "<", "<=", ">", ">=":
		// Comparison operators require comparable types
		if !tc.isComparableType(leftType) || !tc.isComparableType(rightType) {
			return nil, fmt.Errorf("comparison operator %s requires comparable operands", binop.Operator)
		}
		if !tc.isTypeCompatible(leftType, rightType) {
			return nil, fmt.Errorf("comparison operator %s requires compatible operands", binop.Operator)
		}
		return &ResolvedType{Kind: TypeKindBool, Abilities: DefaultValueAbilities()}, nil

	case "&&", "||":
		// Logical operators require boolean operands
		if leftType.Kind != TypeKindBool || rightType.Kind != TypeKindBool {
			return nil, fmt.Errorf("logical operator %s requires boolean operands", binop.Operator)
		}
		return &ResolvedType{Kind: TypeKindBool, Abilities: DefaultValueAbilities()}, nil

	default:
		return nil, fmt.Errorf("unknown binary operator: %s", binop.Operator)
	}
}

// typeCheckUnaryOp type checks a unary operation
func (tc *DNATypeChecker) typeCheckUnaryOp(unaryop *UnaryOp) (*ResolvedType, error) {
	operandType, err := tc.typeCheckExpression(unaryop.Operand)
	if err != nil {
		return nil, fmt.Errorf("error in unary operand: %v", err)
	}

	switch unaryop.Operator {
	case "-":
		if !tc.isNumericType(operandType) {
			return nil, fmt.Errorf("unary minus requires numeric operand")
		}
		return operandType, nil
	case "!":
		if operandType.Kind != TypeKindBool {
			return nil, fmt.Errorf("logical not requires boolean operand")
		}
		return &ResolvedType{Kind: TypeKindBool, Abilities: DefaultValueAbilities()}, nil
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", unaryop.Operator)
	}
}

// typeCheckCall type checks a function call
func (tc *DNATypeChecker) typeCheckCall(call *Call) (*ResolvedType, error) {
	// Type check function expression
	_, err := tc.typeCheckExpression(call.Function)
	if err != nil {
		return nil, fmt.Errorf("error in function expression: %v", err)
	}

	// Type check arguments
	argTypes := make([]*ResolvedType, 0, len(call.Arguments))
	for i, arg := range call.Arguments {
		argType, err := tc.typeCheckExpression(arg)
		if err != nil {
			return nil, fmt.Errorf("error in argument %d: %v", i, err)
		}
		argTypes = append(argTypes, argType)
	}

	// For now, assume function calls return u64
	// In a full implementation, we would look up the function signature
	return &ResolvedType{
		Kind:      TypeKindU64,
		Abilities: DefaultValueAbilities(),
	}, nil
}

// typeCheckFieldAccess type checks field access
func (tc *DNATypeChecker) typeCheckFieldAccess(access *FieldAccess) (*ResolvedType, error) {
	objectType, err := tc.typeCheckExpression(access.Object)
	if err != nil {
		return nil, fmt.Errorf("error in field access object: %v", err)
	}

	// Look up field in resource type
	if objectType.ResourceType != nil {
		for _, field := range objectType.ResourceType.Fields {
			if field.Name == access.Field {
				return tc.dataTypeToResolvedType(field.Type), nil
			}
		}
	}

	return nil, fmt.Errorf("field %s not found in type %s", access.Field, tc.typeToString(objectType))
}

// typeCheckInvariant type checks an invariant
func (tc *DNATypeChecker) typeCheckInvariant(invariant *InvariantDecl) error {
	// Type check the invariant expression
	exprType, err := tc.typeCheckExpression(invariant.Expression)
	if err != nil {
		return fmt.Errorf("error in invariant expression: %v", err)
	}

	// Invariants must be boolean expressions
	if exprType.Kind != TypeKindBool {
		return fmt.Errorf("invariant expression must be boolean, got %s", tc.typeToString(exprType))
	}

	return nil
}

// Helper methods

// resolveType resolves a type expression to a resolved type
func (tc *DNATypeChecker) resolveType(typeExpr *TypeExpr) (*ResolvedType, error) {
	switch typeExpr.Kind {
	case "u8":
		return &ResolvedType{Kind: TypeKindU8, Abilities: DefaultValueAbilities()}, nil
	case "u16":
		return &ResolvedType{Kind: TypeKindU16, Abilities: DefaultValueAbilities()}, nil
	case "u32":
		return &ResolvedType{Kind: TypeKindU32, Abilities: DefaultValueAbilities()}, nil
	case "u64":
		return &ResolvedType{Kind: TypeKindU64, Abilities: DefaultValueAbilities()}, nil
	case "u128":
		return &ResolvedType{Kind: TypeKindU128, Abilities: DefaultValueAbilities()}, nil
	case "bool":
		return &ResolvedType{Kind: TypeKindBool, Abilities: DefaultValueAbilities()}, nil
	case "string":
		return &ResolvedType{Kind: TypeKindString, Abilities: DefaultValueAbilities()}, nil
	case "address":
		return &ResolvedType{Kind: TypeKindAddress, Abilities: DefaultValueAbilities()}, nil
	case "vector":
		if typeExpr.ElementType == nil {
			return nil, fmt.Errorf("vector type requires element type")
		}
		elementType, err := tc.resolveType(typeExpr.ElementType)
		if err != nil {
			return nil, err
		}
		return &ResolvedType{
			Kind:        TypeKindVector,
			ElementType: elementType,
			Abilities:   DefaultValueAbilities(),
		}, nil
	default:
		// Named type - look up in symbol table
		if typeExpr.Name != "" {
			symbol, err := tc.symbolTable.Lookup(typeExpr.Name)
			if err != nil {
				return nil, fmt.Errorf("undefined type: %s", typeExpr.Name)
			}
			if symbol.Kind != SymbolType {
				return nil, fmt.Errorf("%s is not a type", typeExpr.Name)
			}
			return symbol.Type, nil
		}
		return nil, fmt.Errorf("unknown type: %s", typeExpr.Kind)
	}
}

// isTypeCompatible checks if two types are compatible
func (tc *DNATypeChecker) isTypeCompatible(from, to *ResolvedType) bool {
	// Exact match
	if from.Kind == to.Kind && from.Name == to.Name {
		return true
	}

	// Numeric type widening
	if tc.isNumericType(from) && tc.isNumericType(to) {
		return tc.canWiden(from, to)
	}

	return false
}

// isResourceType checks if a type is a resource type
func (tc *DNATypeChecker) isResourceType(t *ResolvedType) bool {
	return t.Kind == TypeKindResource || !t.Abilities.Copy
}

// isNumericType checks if a type is numeric
func (tc *DNATypeChecker) isNumericType(t *ResolvedType) bool {
	switch t.Kind {
	case TypeKindU8, TypeKindU16, TypeKindU32, TypeKindU64, TypeKindU128:
		return true
	}
	return false
}

// isComparableType checks if a type is comparable
func (tc *DNATypeChecker) isComparableType(t *ResolvedType) bool {
	switch t.Kind {
	case TypeKindU8, TypeKindU16, TypeKindU32, TypeKindU64, TypeKindU128:
		return true
	case TypeKindBool, TypeKindString, TypeKindAddress:
		return true
	}
	return false
}

// canWiden checks if one numeric type can be widened to another
func (tc *DNATypeChecker) canWiden(from, to *ResolvedType) bool {
	typeOrder := map[TypeKind]int{
		TypeKindU8: 1, TypeKindU16: 2, TypeKindU32: 3, TypeKindU64: 4, TypeKindU128: 5,
	}

	fromOrder, fromOk := typeOrder[from.Kind]
	toOrder, toOk := typeOrder[to.Kind]

	return fromOk && toOk && fromOrder <= toOrder
}

// getWiderType returns the wider of two numeric types
func (tc *DNATypeChecker) getWiderType(left, right *ResolvedType) *ResolvedType {
	if tc.canWiden(left, right) {
		return right
	}
	if tc.canWiden(right, left) {
		return left
	}
	return left // Fallback
}

// resolvedTypeToDataType converts a resolved type to a data type
func (tc *DNATypeChecker) resolvedTypeToDataType(resolved *ResolvedType) DataType {
	dataType := DataType{
		Kind: resolved.Kind,
	}

	if resolved.Name != "" {
		dataType.ResourceType = ResourceTypeID(resolved.Name)
	}

	if resolved.ElementType != nil {
		elementType := tc.resolvedTypeToDataType(resolved.ElementType)
		dataType.ElementType = &elementType
	}

	return dataType
}

// dataTypeToResolvedType converts a data type to a resolved type
func (tc *DNATypeChecker) dataTypeToResolvedType(dataType DataType) *ResolvedType {
	resolved := &ResolvedType{
		Kind: dataType.Kind,
	}

	if dataType.ResourceType != "" {
		resolved.Name = string(dataType.ResourceType)
		// Look up resource type for abilities
		if resourceType, err := tc.resourceManager.GetResourceType(dataType.ResourceType); err == nil {
			resolved.ResourceType = resourceType
			resolved.Abilities = resourceType.Abilities
		}
	} else {
		resolved.Abilities = DefaultValueAbilities()
	}

	if dataType.ElementType != nil {
		resolved.ElementType = tc.dataTypeToResolvedType(*dataType.ElementType)
	}

	return resolved
}

// typeToString converts a resolved type to a string representation
func (tc *DNATypeChecker) typeToString(t *ResolvedType) string {
	if t.Name != "" {
		if t.Module != "" {
			return fmt.Sprintf("%s::%s", t.Module, t.Name)
		}
		return t.Name
	}

	switch t.Kind {
	case TypeKindVector:
		if t.ElementType != nil {
			return fmt.Sprintf("vector<%s>", tc.typeToString(t.ElementType))
		}
		return "vector"
	default:
		return string(t.Kind)
	}
}

// formatErrors formats all collected errors into a single error
func (tc *DNATypeChecker) formatErrors() error {
	var errorMessages []string

	for _, err := range tc.errorReporter.errors {
		errorMessages = append(errorMessages, fmt.Sprintf("line %d:%d: %s", err.Line, err.Column, err.Message))
	}

	return fmt.Errorf("type checking failed:\n%s", strings.Join(errorMessages, "\n"))
}

// Symbol table methods

// EnterScope enters a new scope
func (st *SymbolTable) EnterScope() {
	st.scopes = append(st.scopes, make(map[string]*Symbol))
	st.depth++
}

// ExitScope exits the current scope
func (st *SymbolTable) ExitScope() {
	if len(st.scopes) > 0 {
		st.scopes = st.scopes[:len(st.scopes)-1]
		st.depth--
	}
}

// Define defines a symbol in the current scope
func (st *SymbolTable) Define(name string, symbol *Symbol) {
	if len(st.scopes) > 0 {
		st.scopes[len(st.scopes)-1][name] = symbol
	}
}

// Lookup looks up a symbol in all scopes
func (st *SymbolTable) Lookup(name string) (*Symbol, error) {
	// Search from innermost to outermost scope
	for i := len(st.scopes) - 1; i >= 0; i-- {
		if symbol, exists := st.scopes[i][name]; exists {
			return symbol, nil
		}
	}
	return nil, fmt.Errorf("undefined symbol: %s", name)
}

// Borrow tracker methods

// EnterScope enters a new borrow scope
func (bt *BorrowTracker) EnterScope() {
	// Borrow scopes are tracked implicitly through symbol table depth
}

// ExitScope exits the current borrow scope
func (bt *BorrowTracker) ExitScope() {
	// Remove borrows from the current scope
	for name, borrow := range bt.activeBorrows {
		if borrow.Scope >= len(bt.activeBorrows) {
			delete(bt.activeBorrows, name)
		}
	}
}

// MoveResource tracks a resource move
func (bt *BorrowTracker) MoveResource(variable string, line, column int) error {
	// Check if resource is currently borrowed
	if borrow, exists := bt.activeBorrows[variable]; exists {
		return fmt.Errorf("cannot move borrowed resource %s (borrowed at line %d)", variable, borrow.Line)
	}

	bt.logger.WithFields(logrus.Fields{
		"variable": variable,
		"line":     line,
		"column":   column,
	}).Debug("Resource moved")

	return nil
}

// UseResource tracks resource usage
func (bt *BorrowTracker) UseResource(variable string, line, column int) error {
	// For now, just log usage
	bt.logger.WithFields(logrus.Fields{
		"variable": variable,
		"line":     line,
		"column":   column,
	}).Debug("Resource used")

	return nil
}

// CheckAssignment checks if an assignment is valid
func (bt *BorrowTracker) CheckAssignment(variable string, line, column int) error {
	// Check if resource is borrowed
	if borrow, exists := bt.activeBorrows[variable]; exists {
		if borrow.BorrowType == BorrowTypeShared {
			return fmt.Errorf("cannot assign to shared borrow %s", variable)
		}
	}

	return nil
}

// CheckFinalBorrows checks for any remaining borrows at the end of type checking
func (bt *BorrowTracker) CheckFinalBorrows() error {
	if len(bt.activeBorrows) > 0 {
		var unreturned []string
		for name := range bt.activeBorrows {
			unreturned = append(unreturned, name)
		}
		return fmt.Errorf("unreturned borrows: %s", strings.Join(unreturned, ", "))
	}
	return nil
}

// Error reporter methods

// AddError adds a type checking error
func (er *ErrorReporter) AddError(message string, line, column int) {
	er.errors = append(er.errors, TypeError{
		Message:  message,
		Line:     line,
		Column:   column,
		Severity: "error",
	})
}

// AddWarning adds a type checking warning
func (er *ErrorReporter) AddWarning(message string, line, column int) {
	er.warnings = append(er.warnings, TypeWarning{
		Message: message,
		Line:    line,
		Column:  column,
	})
}

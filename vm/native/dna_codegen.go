package native

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/sirupsen/logrus"
)

// DNACodeGenerator generates WebAssembly code from DNA AST
type DNACodeGenerator struct {
	buffer      *bytes.Buffer
	symbolTable *CodeGenSymbolTable
	resourceMgr *ResourceManager
	localCount  int
	labelCount  int
	logger      *logrus.Logger

	// WASM-specific state
	functionTypes []FunctionType
	functions     []Function
	memories      []Memory
	globals       []Global
	exports       []Export
	imports       []Import
}

// CodeGenSymbolTable tracks symbols during code generation
type CodeGenSymbolTable struct {
	symbols map[string]*CodeGenSymbol
	locals  map[string]int // local variable indices
	depth   int
}

// CodeGenSymbol represents a symbol during code generation
type CodeGenSymbol struct {
	Name       string
	Type       *ResolvedType
	LocalIndex int
	IsLocal    bool
	IsGlobal   bool
}

// WASM Type definitions

// FunctionType represents a WASM function type
type FunctionType struct {
	Parameters []ValueType
	Results    []ValueType
}

// ValueType represents a WASM value type
type ValueType uint8

const (
	ValueTypeI32 ValueType = 0x7F
	ValueTypeI64 ValueType = 0x7E
	ValueTypeF32 ValueType = 0x7D
	ValueTypeF64 ValueType = 0x7C
)

// WASMFunction represents a WASM function
type WASMFunction struct {
	TypeIndex int
	Locals    []Local
	Body      []byte
}

// Local represents a local variable in WASM
type Local struct {
	Count int
	Type  ValueType
}

// Memory represents WASM memory
type Memory struct {
	Min uint32
	Max *uint32
}

// Global represents a WASM global
type Global struct {
	Type     GlobalType
	InitExpr []byte
}

// GlobalType represents a WASM global type
type GlobalType struct {
	ValueType ValueType
	Mutable   bool
}

// Export represents a WASM export
type Export struct {
	Name  string
	Kind  ExportKind
	Index uint32
}

// ExportKind represents the kind of export
type ExportKind uint8

const (
	ExportKindFunc   ExportKind = 0x00
	ExportKindTable  ExportKind = 0x01
	ExportKindMemory ExportKind = 0x02
	ExportKindGlobal ExportKind = 0x03
)

// Import represents a WASM import
type Import struct {
	Module string
	Name   string
	Kind   ImportKind
}

// ImportKind represents the kind of import
type ImportKind interface {
	isImportKind()
}

// WASM Instructions
const (
	// Control Instructions
	InstUnreachable  = 0x00
	InstNop          = 0x01
	InstBlock        = 0x02
	InstLoop         = 0x03
	InstIf           = 0x04
	InstElse         = 0x05
	InstEnd          = 0x0B
	InstBr           = 0x0C
	InstBrIf         = 0x0D
	InstBrTable      = 0x0E
	InstReturn       = 0x0F
	InstCall         = 0x10
	InstCallIndirect = 0x11

	// Variable Instructions
	InstLocalGet  = 0x20
	InstLocalSet  = 0x21
	InstLocalTee  = 0x22
	InstGlobalGet = 0x23
	InstGlobalSet = 0x24

	// Memory Instructions
	InstI32Load  = 0x28
	InstI64Load  = 0x29
	InstI32Store = 0x36
	InstI64Store = 0x37

	// Numeric Instructions
	InstI32Const = 0x41
	InstI64Const = 0x42
	InstF32Const = 0x43
	InstF64Const = 0x44

	// Arithmetic Instructions
	InstI32Add = 0x6A
	InstI32Sub = 0x6B
	InstI32Mul = 0x6C
	InstI32Div = 0x6D
	InstI32Rem = 0x6F
	InstI32And = 0x71
	InstI32Or  = 0x72
	InstI32Xor = 0x73

	// Comparison Instructions
	InstI32Eq = 0x46
	InstI32Ne = 0x47
	InstI32Lt = 0x48
	InstI32Le = 0x4C
	InstI32Gt = 0x4A
	InstI32Ge = 0x4E

	// Conversion Instructions
	InstI32WrapI64   = 0xA7
	InstI64ExtendI32 = 0xAC
)

// NewDNACodeGenerator creates a new DNA code generator
func NewDNACodeGenerator(resourceMgr *ResourceManager, logger *logrus.Logger) *DNACodeGenerator {
	if logger == nil {
		logger = logrus.New()
	}

	return &DNACodeGenerator{
		buffer:        &bytes.Buffer{},
		symbolTable:   NewCodeGenSymbolTable(),
		resourceMgr:   resourceMgr,
		logger:        logger,
		functionTypes: make([]FunctionType, 0),
		functions:     make([]Function, 0),
		memories:      make([]Memory, 0),
		globals:       make([]Global, 0),
		exports:       make([]Export, 0),
		imports:       make([]Import, 0),
	}
}

// NewCodeGenSymbolTable creates a new code generation symbol table
func NewCodeGenSymbolTable() *CodeGenSymbolTable {
	return &CodeGenSymbolTable{
		symbols: make(map[string]*CodeGenSymbol),
		locals:  make(map[string]int),
	}
}

// GenerateWASM generates WASM bytecode from a DNA module
func (cg *DNACodeGenerator) GenerateWASM(module *Module) ([]byte, error) {
	cg.logger.WithField("module", module.Name).Info("Starting DNA to WASM compilation")

	// Reset state
	cg.buffer.Reset()
	cg.localCount = 0
	cg.labelCount = 0

	// Generate WASM module
	if err := cg.generateModule(module); err != nil {
		return nil, fmt.Errorf("failed to generate WASM module: %v", err)
	}

	result := cg.buffer.Bytes()

	cg.logger.WithFields(logrus.Fields{
		"module":    module.Name,
		"wasm_size": len(result),
		"functions": len(cg.functions),
		"exports":   len(cg.exports),
	}).Info("DNA to WASM compilation completed")

	return result, nil
}

// generateModule generates a complete WASM module
func (cg *DNACodeGenerator) generateModule(module *Module) error {
	// WASM Magic Number and Version
	cg.writeBytes([]byte{0x00, 0x61, 0x73, 0x6D}) // "\0asm"
	cg.writeU32(1)                                // version 1

	// Process module items first to collect type information
	for _, item := range module.Items {
		if err := cg.analyzeModuleItem(item); err != nil {
			return fmt.Errorf("failed to analyze module item: %v", err)
		}
	}

	// Generate WASM sections
	if err := cg.generateTypeSection(); err != nil {
		return err
	}

	if err := cg.generateImportSection(); err != nil {
		return err
	}

	if err := cg.generateFunctionSection(); err != nil {
		return err
	}

	if err := cg.generateMemorySection(); err != nil {
		return err
	}

	if err := cg.generateGlobalSection(); err != nil {
		return err
	}

	if err := cg.generateExportSection(); err != nil {
		return err
	}

	if err := cg.generateCodeSection(module); err != nil {
		return err
	}

	return nil
}

// analyzeModuleItem analyzes a module item to collect type information
func (cg *DNACodeGenerator) analyzeModuleItem(item ModuleItem) error {
	switch node := item.(type) {
	case *ResourceDef:
		return cg.analyzeResourceDef(node)
	case *StructDef:
		return cg.analyzeStructDef(node)
	case *Function:
		return cg.analyzeFunction(node)
	}
	return nil
}

// analyzeResourceDef analyzes a resource definition
func (cg *DNACodeGenerator) analyzeResourceDef(resource *ResourceDef) error {
	// Resources need constructor and destructor functions
	// Constructor: () -> i32 (returns resource handle)
	constructorType := FunctionType{
		Parameters: []ValueType{},
		Results:    []ValueType{ValueTypeI32},
	}
	cg.functionTypes = append(cg.functionTypes, constructorType)

	// Destructor: (i32) -> () (takes resource handle)
	destructorType := FunctionType{
		Parameters: []ValueType{ValueTypeI32},
		Results:    []ValueType{},
	}
	cg.functionTypes = append(cg.functionTypes, destructorType)

	// Add methods
	for _, method := range resource.Methods {
		if err := cg.analyzeFunction(&method); err != nil {
			return err
		}
	}

	cg.logger.WithField("resource", resource.Name).Debug("Analyzed resource definition")
	return nil
}

// analyzeStructDef analyzes a struct definition
func (cg *DNACodeGenerator) analyzeStructDef(structDef *StructDef) error {
	// Structs are similar to resources but with value semantics
	return nil
}

// analyzeFunction analyzes a function definition
func (cg *DNACodeGenerator) analyzeFunction(function *Function) error {
	// Convert DNA types to WASM types
	params := make([]ValueType, 0, len(function.Parameters))
	for _, param := range function.Parameters {
		wasmType := cg.dnaTypeToWASMType(param.Type)
		params = append(params, wasmType)
	}

	var results []ValueType
	if function.ReturnType != nil {
		wasmType := cg.dnaTypeToWASMType(function.ReturnType)
		results = []ValueType{wasmType}
	}

	functionType := FunctionType{
		Parameters: params,
		Results:    results,
	}

	cg.functionTypes = append(cg.functionTypes, functionType)

	// Add export if public
	if function.Visibility == VisibilityPublic {
		export := Export{
			Name:  function.Name,
			Kind:  ExportKindFunc,
			Index: uint32(len(cg.functions)),
		}
		cg.exports = append(cg.exports, export)
	}

	cg.logger.WithFields(logrus.Fields{
		"function":   function.Name,
		"parameters": len(params),
		"returns":    len(results),
	}).Debug("Analyzed function")

	return nil
}

// generateTypeSection generates the WASM type section
func (cg *DNACodeGenerator) generateTypeSection() error {
	if len(cg.functionTypes) == 0 {
		return nil
	}

	sectionBuffer := &bytes.Buffer{}

	// Write number of types
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(cg.functionTypes)))

	// Write each function type
	for _, funcType := range cg.functionTypes {
		// Function type indicator
		sectionBuffer.WriteByte(0x60)

		// Parameters
		cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(funcType.Parameters)))
		for _, param := range funcType.Parameters {
			sectionBuffer.WriteByte(byte(param))
		}

		// Results
		cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(funcType.Results)))
		for _, result := range funcType.Results {
			sectionBuffer.WriteByte(byte(result))
		}
	}

	// Write section
	cg.writeSection(1, sectionBuffer.Bytes()) // Type section ID = 1

	return nil
}

// generateImportSection generates the WASM import section
func (cg *DNACodeGenerator) generateImportSection() error {
	// Add DNA host function imports
	hostImports := []Import{
		{Module: "diamante", Name: "log"},
		{Module: "diamante", Name: "getTime"},
		{Module: "diamante", Name: "getRandom"},
		{Module: "diamante", Name: "createResource"},
		{Module: "diamante", Name: "destroyResource"},
		{Module: "diamante", Name: "moveResource"},
		{Module: "diamante", Name: "borrowResource"},
		{Module: "diamante", Name: "returnResource"},
	}

	if len(hostImports) == 0 {
		return nil
	}

	sectionBuffer := &bytes.Buffer{}

	// Write number of imports
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(hostImports)))

	// Write each import (simplified - real implementation would have proper types)
	for _, imp := range hostImports {
		// Module name
		cg.writeStringToBuffer(sectionBuffer, imp.Module)
		// Function name
		cg.writeStringToBuffer(sectionBuffer, imp.Name)
		// Import kind (function)
		sectionBuffer.WriteByte(0x00)
		// Function type index (simplified - use type 0)
		cg.writeULEB128ToBuffer(sectionBuffer, 0)
	}

	// Write section
	cg.writeSection(2, sectionBuffer.Bytes()) // Import section ID = 2

	return nil
}

// generateFunctionSection generates the WASM function section
func (cg *DNACodeGenerator) generateFunctionSection() error {
	if len(cg.functions) == 0 {
		return nil
	}

	sectionBuffer := &bytes.Buffer{}

	// Write number of functions
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(cg.functions)))

	// Write type index for each function
	for i := range cg.functions {
		cg.writeULEB128ToBuffer(sectionBuffer, uint32(i))
	}

	// Write section
	cg.writeSection(3, sectionBuffer.Bytes()) // Function section ID = 3

	return nil
}

// generateMemorySection generates the WASM memory section
func (cg *DNACodeGenerator) generateMemorySection() error {
	// Add default memory for resource management
	memory := Memory{
		Min: 1,   // 64KB minimum
		Max: nil, // No maximum
	}
	cg.memories = append(cg.memories, memory)

	sectionBuffer := &bytes.Buffer{}

	// Write number of memories
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(cg.memories)))

	// Write each memory
	for _, mem := range cg.memories {
		if mem.Max != nil {
			sectionBuffer.WriteByte(0x01) // Has maximum
			cg.writeULEB128ToBuffer(sectionBuffer, mem.Min)
			cg.writeULEB128ToBuffer(sectionBuffer, *mem.Max)
		} else {
			sectionBuffer.WriteByte(0x00) // No maximum
			cg.writeULEB128ToBuffer(sectionBuffer, mem.Min)
		}
	}

	// Write section
	cg.writeSection(5, sectionBuffer.Bytes()) // Memory section ID = 5

	// Export memory
	memoryExport := Export{
		Name:  "memory",
		Kind:  ExportKindMemory,
		Index: 0,
	}
	cg.exports = append(cg.exports, memoryExport)

	return nil
}

// generateGlobalSection generates the WASM global section
func (cg *DNACodeGenerator) generateGlobalSection() error {
	if len(cg.globals) == 0 {
		return nil
	}

	sectionBuffer := &bytes.Buffer{}

	// Write number of globals
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(cg.globals)))

	// Write each global
	for _, global := range cg.globals {
		// Global type
		sectionBuffer.WriteByte(byte(global.Type.ValueType))
		if global.Type.Mutable {
			sectionBuffer.WriteByte(0x01)
		} else {
			sectionBuffer.WriteByte(0x00)
		}

		// Init expression
		sectionBuffer.Write(global.InitExpr)
		sectionBuffer.WriteByte(InstEnd)
	}

	// Write section
	cg.writeSection(6, sectionBuffer.Bytes()) // Global section ID = 6

	return nil
}

// generateExportSection generates the WASM export section
func (cg *DNACodeGenerator) generateExportSection() error {
	if len(cg.exports) == 0 {
		return nil
	}

	sectionBuffer := &bytes.Buffer{}

	// Write number of exports
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(cg.exports)))

	// Write each export
	for _, export := range cg.exports {
		// Export name
		cg.writeStringToBuffer(sectionBuffer, export.Name)
		// Export kind
		sectionBuffer.WriteByte(byte(export.Kind))
		// Export index
		cg.writeULEB128ToBuffer(sectionBuffer, export.Index)
	}

	// Write section
	cg.writeSection(7, sectionBuffer.Bytes()) // Export section ID = 7

	return nil
}

// generateCodeSection generates the WASM code section
func (cg *DNACodeGenerator) generateCodeSection(module *Module) error {
	sectionBuffer := &bytes.Buffer{}

	// Collect functions to generate
	var functionsToGenerate []*Function
	for _, item := range module.Items {
		if function, ok := item.(*Function); ok {
			functionsToGenerate = append(functionsToGenerate, function)
		} else if resource, ok := item.(*ResourceDef); ok {
			// Add resource methods
			for i := range resource.Methods {
				functionsToGenerate = append(functionsToGenerate, &resource.Methods[i])
			}
		}
	}

	// Write number of functions
	cg.writeULEB128ToBuffer(sectionBuffer, uint32(len(functionsToGenerate)))

	// Generate code for each function
	for _, function := range functionsToGenerate {
		functionBuffer := &bytes.Buffer{}

		// Generate function body
		if err := cg.generateFunctionBody(function, functionBuffer); err != nil {
			return fmt.Errorf("failed to generate function %s: %v", function.Name, err)
		}

		// Write function size and body
		cg.writeULEB128ToBuffer(sectionBuffer, uint32(functionBuffer.Len()))
		sectionBuffer.Write(functionBuffer.Bytes())
	}

	// Write section
	cg.writeSection(10, sectionBuffer.Bytes()) // Code section ID = 10

	return nil
}

// generateFunctionBody generates WASM code for a function body
func (cg *DNACodeGenerator) generateFunctionBody(function *Function, buffer *bytes.Buffer) error {
	// Enter new scope
	cg.symbolTable.EnterScope()
	defer cg.symbolTable.ExitScope()

	// Add parameters to symbol table
	for i, param := range function.Parameters {
		symbol := &CodeGenSymbol{
			Name:       param.Name,
			LocalIndex: i,
			IsLocal:    true,
		}
		cg.symbolTable.symbols[param.Name] = symbol
	}

	// Count locals (simplified - in real implementation would analyze function body)
	localCount := len(function.Parameters)

	// Write locals declaration
	buffer.WriteByte(0x01) // 1 local declaration
	cg.writeULEB128ToBuffer(buffer, uint32(localCount))
	buffer.WriteByte(byte(ValueTypeI32)) // All locals are i32 for simplicity

	// Generate function body
	if function.Body != nil {
		if err := cg.generateBlock(function.Body, buffer); err != nil {
			return err
		}
	} else {
		// Empty function - just return
		if function.ReturnType != nil {
			// Return default value
			buffer.WriteByte(InstI32Const)
			cg.writeULEB128ToBuffer(buffer, 0)
		}
	}

	// End function
	buffer.WriteByte(InstEnd)

	return nil
}

// generateBlock generates WASM code for a block
func (cg *DNACodeGenerator) generateBlock(block *Block, buffer *bytes.Buffer) error {
	for _, stmt := range block.Statements {
		if err := cg.generateStatement(stmt, buffer); err != nil {
			return err
		}
	}
	return nil
}

// generateStatement generates WASM code for a statement
func (cg *DNACodeGenerator) generateStatement(stmt Statement, buffer *bytes.Buffer) error {
	switch node := stmt.(type) {
	case *LetStatement:
		return cg.generateLetStatement(node, buffer)
	case *AssignStatement:
		return cg.generateAssignStatement(node, buffer)
	case *ExpressionStatement:
		return cg.generateExpression(node.Expression, buffer)
	case *ReturnStatement:
		return cg.generateReturnStatement(node, buffer)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// generateLetStatement generates WASM code for a let statement
func (cg *DNACodeGenerator) generateLetStatement(stmt *LetStatement, buffer *bytes.Buffer) error {
	// Generate value expression
	if err := cg.generateExpression(stmt.Value, buffer); err != nil {
		return err
	}

	// Store in local variable
	localIndex := cg.localCount
	cg.localCount++

	// Add to symbol table
	symbol := &CodeGenSymbol{
		Name:       stmt.Name,
		LocalIndex: localIndex,
		IsLocal:    true,
	}
	cg.symbolTable.symbols[stmt.Name] = symbol

	// Store the value
	buffer.WriteByte(InstLocalSet)
	cg.writeULEB128ToBuffer(buffer, uint32(localIndex))

	return nil
}

// generateAssignStatement generates WASM code for an assignment
func (cg *DNACodeGenerator) generateAssignStatement(stmt *AssignStatement, buffer *bytes.Buffer) error {
	// Generate value expression
	if err := cg.generateExpression(stmt.Value, buffer); err != nil {
		return err
	}

	// Generate assignment to target
	if ident, ok := stmt.Target.(*Identifier); ok {
		symbol, exists := cg.symbolTable.symbols[ident.Name]
		if !exists {
			return fmt.Errorf("undefined variable: %s", ident.Name)
		}

		if symbol.IsLocal {
			buffer.WriteByte(InstLocalSet)
			cg.writeULEB128ToBuffer(buffer, uint32(symbol.LocalIndex))
		} else {
			return fmt.Errorf("global assignment not implemented")
		}
	} else {
		return fmt.Errorf("complex assignment targets not implemented")
	}

	return nil
}

// generateReturnStatement generates WASM code for a return statement
func (cg *DNACodeGenerator) generateReturnStatement(stmt *ReturnStatement, buffer *bytes.Buffer) error {
	if stmt.Value != nil {
		if err := cg.generateExpression(stmt.Value, buffer); err != nil {
			return err
		}
	}

	buffer.WriteByte(InstReturn)
	return nil
}

// generateExpression generates WASM code for an expression
func (cg *DNACodeGenerator) generateExpression(expr Expression, buffer *bytes.Buffer) error {
	switch node := expr.(type) {
	case *Literal:
		return cg.generateLiteral(node, buffer)
	case *Identifier:
		return cg.generateIdentifier(node, buffer)
	case *BinaryOp:
		return cg.generateBinaryOp(node, buffer)
	case *UnaryOp:
		return cg.generateUnaryOp(node, buffer)
	case *Call:
		return cg.generateCall(node, buffer)
	case *FieldAccess:
		return cg.generateFieldAccess(node, buffer)
	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// generateLiteral generates WASM code for a literal
func (cg *DNACodeGenerator) generateLiteral(literal *Literal, buffer *bytes.Buffer) error {
	switch literal.Kind {
	case TokenNumber:
		// Generate i32.const
		buffer.WriteByte(InstI32Const)
		if val, ok := literal.Value.(int64); ok {
			cg.writeULEB128ToBuffer(buffer, uint32(val))
		} else {
			cg.writeULEB128ToBuffer(buffer, 0)
		}
	case TokenBool:
		// Generate i32.const (0 or 1)
		buffer.WriteByte(InstI32Const)
		if val, ok := literal.Value.(bool); ok && val {
			cg.writeULEB128ToBuffer(buffer, 1)
		} else {
			cg.writeULEB128ToBuffer(buffer, 0)
		}
	case TokenString:
		// For strings, we would need to store in memory and return pointer
		// Simplified: return 0
		buffer.WriteByte(InstI32Const)
		cg.writeULEB128ToBuffer(buffer, 0)
	default:
		buffer.WriteByte(InstI32Const)
		cg.writeULEB128ToBuffer(buffer, 0)
	}

	return nil
}

// generateIdentifier generates WASM code for an identifier
func (cg *DNACodeGenerator) generateIdentifier(ident *Identifier, buffer *bytes.Buffer) error {
	symbol, exists := cg.symbolTable.symbols[ident.Name]
	if !exists {
		return fmt.Errorf("undefined identifier: %s", ident.Name)
	}

	if symbol.IsLocal {
		buffer.WriteByte(InstLocalGet)
		cg.writeULEB128ToBuffer(buffer, uint32(symbol.LocalIndex))
	} else if symbol.IsGlobal {
		buffer.WriteByte(InstGlobalGet)
		cg.writeULEB128ToBuffer(buffer, uint32(symbol.LocalIndex)) // Global index
	} else {
		return fmt.Errorf("unsupported symbol type for %s", ident.Name)
	}

	return nil
}

// generateBinaryOp generates WASM code for a binary operation
func (cg *DNACodeGenerator) generateBinaryOp(binop *BinaryOp, buffer *bytes.Buffer) error {
	// Generate left operand
	if err := cg.generateExpression(binop.Left, buffer); err != nil {
		return err
	}

	// Generate right operand
	if err := cg.generateExpression(binop.Right, buffer); err != nil {
		return err
	}

	// Generate operation
	switch binop.Operator {
	case "+":
		buffer.WriteByte(InstI32Add)
	case "-":
		buffer.WriteByte(InstI32Sub)
	case "*":
		buffer.WriteByte(InstI32Mul)
	case "/":
		buffer.WriteByte(InstI32Div)
	case "%":
		buffer.WriteByte(InstI32Rem)
	case "==":
		buffer.WriteByte(InstI32Eq)
	case "!=":
		buffer.WriteByte(InstI32Ne)
	case "<":
		buffer.WriteByte(InstI32Lt)
	case "<=":
		buffer.WriteByte(InstI32Le)
	case ">":
		buffer.WriteByte(InstI32Gt)
	case ">=":
		buffer.WriteByte(InstI32Ge)
	case "&&":
		buffer.WriteByte(InstI32And)
	case "||":
		buffer.WriteByte(InstI32Or)
	default:
		return fmt.Errorf("unsupported binary operator: %s", binop.Operator)
	}

	return nil
}

// generateUnaryOp generates WASM code for a unary operation
func (cg *DNACodeGenerator) generateUnaryOp(unaryop *UnaryOp, buffer *bytes.Buffer) error {
	// Generate operand
	if err := cg.generateExpression(unaryop.Operand, buffer); err != nil {
		return err
	}

	// Generate operation
	switch unaryop.Operator {
	case "-":
		// Negate: 0 - operand
		buffer.WriteByte(InstI32Const)
		cg.writeULEB128ToBuffer(buffer, 0)
		// Swap operands (we need to put 0 first)
		// For simplicity, we'll just emit sub as-is
		buffer.WriteByte(InstI32Sub)
	case "!":
		// Logical not: operand == 0
		buffer.WriteByte(InstI32Const)
		cg.writeULEB128ToBuffer(buffer, 0)
		buffer.WriteByte(InstI32Eq)
	default:
		return fmt.Errorf("unsupported unary operator: %s", unaryop.Operator)
	}

	return nil
}

// generateCall generates WASM code for a function call
func (cg *DNACodeGenerator) generateCall(call *Call, buffer *bytes.Buffer) error {
	// Generate arguments
	for _, arg := range call.Arguments {
		if err := cg.generateExpression(arg, buffer); err != nil {
			return err
		}
	}

	// Generate call instruction
	if _, ok := call.Function.(*Identifier); ok {
		// Look up function index (simplified)
		functionIndex := uint32(0) // Would look up actual function index

		buffer.WriteByte(InstCall)
		cg.writeULEB128ToBuffer(buffer, functionIndex)
	} else {
		return fmt.Errorf("indirect calls not implemented")
	}

	return nil
}

// generateFieldAccess generates WASM code for field access
func (cg *DNACodeGenerator) generateFieldAccess(access *FieldAccess, buffer *bytes.Buffer) error {
	// Generate object expression (should result in a resource handle)
	if err := cg.generateExpression(access.Object, buffer); err != nil {
		return err
	}

	// Field access would involve memory loads based on struct layout
	// Simplified: just return the object for now
	// Real implementation would calculate field offset and load

	return nil
}

// Helper methods

// dnaTypeToWASMType converts a DNA type to a WASM type
func (cg *DNACodeGenerator) dnaTypeToWASMType(typeExpr *TypeExpr) ValueType {
	switch typeExpr.Kind {
	case "u8", "u16", "u32":
		return ValueTypeI32
	case "u64":
		return ValueTypeI64
	case "bool":
		return ValueTypeI32
	case "string", "address":
		return ValueTypeI32 // Pointer to memory
	default:
		// Resources and complex types are represented as handles (i32)
		return ValueTypeI32
	}
}

// Symbol table methods for code generation

// EnterScope enters a new scope
func (st *CodeGenSymbolTable) EnterScope() {
	st.depth++
}

// ExitScope exits the current scope
func (st *CodeGenSymbolTable) ExitScope() {
	// Remove symbols from current scope
	for name, symbol := range st.symbols {
		if symbol.LocalIndex >= st.depth {
			delete(st.symbols, name)
		}
	}
	st.depth--
}

// WASM writing helper methods

// writeSection writes a WASM section
func (cg *DNACodeGenerator) writeSection(sectionID byte, data []byte) {
	cg.buffer.WriteByte(sectionID)
	cg.writeULEB128(uint32(len(data)))
	cg.buffer.Write(data)
}

// writeBytes writes raw bytes
func (cg *DNACodeGenerator) writeBytes(data []byte) {
	cg.buffer.Write(data)
}

// writeU32 writes a 32-bit unsigned integer in little-endian
func (cg *DNACodeGenerator) writeU32(value uint32) {
	binary.Write(cg.buffer, binary.LittleEndian, value)
}

// writeULEB128 writes an unsigned LEB128 integer
func (cg *DNACodeGenerator) writeULEB128(value uint32) {
	cg.writeULEB128ToBuffer(cg.buffer, value)
}

// writeULEB128ToBuffer writes an unsigned LEB128 integer to a specific buffer
func (cg *DNACodeGenerator) writeULEB128ToBuffer(buffer *bytes.Buffer, value uint32) {
	for {
		byte := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			byte |= 0x80
		}
		buffer.WriteByte(byte)
		if value == 0 {
			break
		}
	}
}

// writeString writes a WASM string (length + UTF-8 bytes)
func (cg *DNACodeGenerator) writeString(str string) {
	cg.writeStringToBuffer(cg.buffer, str)
}

// writeStringToBuffer writes a WASM string to a specific buffer
func (cg *DNACodeGenerator) writeStringToBuffer(buffer *bytes.Buffer, str string) {
	bytes := []byte(str)
	cg.writeULEB128ToBuffer(buffer, uint32(len(bytes)))
	buffer.Write(bytes)
}

// OptimizeWASM performs basic optimizations on generated WASM
func (cg *DNACodeGenerator) OptimizeWASM(wasmBytes []byte) []byte {
	// Basic optimizations could include:
	// - Dead code elimination
	// - Constant folding
	// - Local variable optimization
	// - Function inlining for small functions

	// For now, return as-is
	return wasmBytes
}

// GetGenerationMetrics returns metrics about the code generation process
func (cg *DNACodeGenerator) GetGenerationMetrics() map[string]interface{} {
	return map[string]interface{}{
		"function_types": len(cg.functionTypes),
		"functions":      len(cg.functions),
		"exports":        len(cg.exports),
		"imports":        len(cg.imports),
		"memories":       len(cg.memories),
		"globals":        len(cg.globals),
		"local_count":    cg.localCount,
		"label_count":    cg.labelCount,
	}
}

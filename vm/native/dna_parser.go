package native

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/sirupsen/logrus"
)

// DNAParser parses DNA language source code into an AST
type DNAParser struct {
	lexer  *DNALexer
	tokens []Token
	pos    int
	logger *logrus.Logger
}

// Token represents a lexical token in DNA source code
type Token struct {
	Type     TokenType `json:"type"`
	Value    string    `json:"value"`
	Line     int       `json:"line"`
	Column   int       `json:"column"`
	Position int       `json:"position"`
}

// TokenType represents the type of a token
type TokenType string

const (
	// Literals
	TokenNumber  TokenType = "NUMBER"
	TokenString  TokenType = "STRING"
	TokenBool    TokenType = "BOOL"
	TokenAddress TokenType = "ADDRESS"
	TokenIdent   TokenType = "IDENT"

	// Keywords
	TokenResource  TokenType = "RESOURCE"
	TokenModule    TokenType = "MODULE"
	TokenStruct    TokenType = "STRUCT"
	TokenFun       TokenType = "FUN"
	TokenPublic    TokenType = "PUBLIC"
	TokenInternal  TokenType = "INTERNAL"
	TokenPrivate   TokenType = "PRIVATE"
	TokenMove      TokenType = "MOVE"
	TokenCopy      TokenType = "COPY"
	TokenDrop      TokenType = "DROP"
	TokenBorrow    TokenType = "BORROW"
	TokenLet       TokenType = "LET"
	TokenMut       TokenType = "MUT"
	TokenIf        TokenType = "IF"
	TokenElse      TokenType = "ELSE"
	TokenWhile     TokenType = "WHILE"
	TokenFor       TokenType = "FOR"
	TokenReturn    TokenType = "RETURN"
	TokenAbort     TokenType = "ABORT"
	TokenAssert    TokenType = "ASSERT"
	TokenInvariant TokenType = "INVARIANT"
	TokenRequires  TokenType = "REQUIRES"
	TokenEnsures   TokenType = "ENSURES"

	// Types
	TokenU8       TokenType = "U8"
	TokenU16      TokenType = "U16"
	TokenU32      TokenType = "U32"
	TokenU64      TokenType = "U64"
	TokenU128     TokenType = "U128"
	TokenBoolType TokenType = "BOOL_TYPE"
	TokenVector   TokenType = "VECTOR"
	TokenOption   TokenType = "OPTION"

	// Abilities
	TokenCopyAbility  TokenType = "COPY_ABILITY"
	TokenDropAbility  TokenType = "DROP_ABILITY"
	TokenStoreAbility TokenType = "STORE_ABILITY"
	TokenKeyAbility   TokenType = "KEY_ABILITY"

	// Operators
	TokenPlus    TokenType = "PLUS"
	TokenMinus   TokenType = "MINUS"
	TokenStar    TokenType = "STAR"
	TokenSlash   TokenType = "SLASH"
	TokenPercent TokenType = "PERCENT"
	TokenEq      TokenType = "EQ"
	TokenNe      TokenType = "NE"
	TokenLt      TokenType = "LT"
	TokenLe      TokenType = "LE"
	TokenGt      TokenType = "GT"
	TokenGe      TokenType = "GE"
	TokenAnd     TokenType = "AND"
	TokenOr      TokenType = "OR"
	TokenNot     TokenType = "NOT"
	TokenAssign  TokenType = "ASSIGN"

	// Delimiters
	TokenLParen     TokenType = "LPAREN"
	TokenRParen     TokenType = "RPAREN"
	TokenLBrace     TokenType = "LBRACE"
	TokenRBrace     TokenType = "RBRACE"
	TokenLBracket   TokenType = "LBRACKET"
	TokenRBracket   TokenType = "RBRACKET"
	TokenSemicolon  TokenType = "SEMICOLON"
	TokenComma      TokenType = "COMMA"
	TokenDot        TokenType = "DOT"
	TokenColon      TokenType = "COLON"
	TokenColonColon TokenType = "COLON_COLON"
	TokenArrow      TokenType = "ARROW"

	// Special
	TokenEOF     TokenType = "EOF"
	TokenNewline TokenType = "NEWLINE"
	TokenComment TokenType = "COMMENT"
)

// AST Node types

// ASTNode is the base interface for all AST nodes
type ASTNode interface {
	String() string
	GetType() ASTNodeType
}

// ASTNodeType represents the type of an AST node
type ASTNodeType string

const (
	NodeModule      ASTNodeType = "MODULE"
	NodeResourceDef ASTNodeType = "RESOURCE_DEF"
	NodeStructDef   ASTNodeType = "STRUCT_DEF"
	NodeFunction    ASTNodeType = "FUNCTION"
	NodeParameter   ASTNodeType = "PARAMETER"
	NodeField       ASTNodeType = "FIELD"
	NodeStatement   ASTNodeType = "STATEMENT"
	NodeExpression  ASTNodeType = "EXPRESSION"
	NodeType        ASTNodeType = "TYPE"
	NodeBlock       ASTNodeType = "BLOCK"
	NodeLiteral     ASTNodeType = "LITERAL"
	NodeIdentifier  ASTNodeType = "IDENTIFIER"
	NodeBinaryOp    ASTNodeType = "BINARY_OP"
	NodeUnaryOp     ASTNodeType = "UNARY_OP"
	NodeCall        ASTNodeType = "CALL"
	NodeFieldAccess ASTNodeType = "FIELD_ACCESS"
	NodeInvariant   ASTNodeType = "INVARIANT"
)

// Module represents a DNA module
type Module struct {
	Name       string       `json:"name"`
	Items      []ModuleItem `json:"items"`
	Imports    []ImportDecl `json:"imports"`
	Attributes []Attribute  `json:"attributes"`
	Line       int          `json:"line"`
	Column     int          `json:"column"`
}

func (m *Module) String() string       { return fmt.Sprintf("module %s", m.Name) }
func (m *Module) GetType() ASTNodeType { return NodeModule }

// ModuleItem represents an item in a module
type ModuleItem interface {
	ASTNode
	IsModuleItem()
}

// ImportDecl represents an import declaration
type ImportDecl struct {
	Module string   `json:"module"`
	Items  []string `json:"items"`
	Alias  string   `json:"alias,omitempty"`
}

// Attribute represents metadata attributes
type Attribute struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value,omitempty"`
}

// ResourceDef represents a resource definition
type ResourceDef struct {
	Name       string          `json:"name"`
	Fields     []FieldDef      `json:"fields"`
	Abilities  []string        `json:"abilities"`
	Methods    []Function      `json:"methods"`
	Invariants []InvariantDecl `json:"invariants"`
	Visibility Visibility      `json:"visibility"`
	Line       int             `json:"line"`
	Column     int             `json:"column"`
}

func (r *ResourceDef) String() string       { return fmt.Sprintf("resource %s", r.Name) }
func (r *ResourceDef) GetType() ASTNodeType { return NodeResourceDef }
func (r *ResourceDef) IsModuleItem()        {}

// StructDef represents a struct definition
type StructDef struct {
	Name       string     `json:"name"`
	Fields     []FieldDef `json:"fields"`
	Abilities  []string   `json:"abilities"`
	Visibility Visibility `json:"visibility"`
	Line       int        `json:"line"`
	Column     int        `json:"column"`
}

func (s *StructDef) String() string       { return fmt.Sprintf("struct %s", s.Name) }
func (s *StructDef) GetType() ASTNodeType { return NodeStructDef }
func (s *StructDef) IsModuleItem()        {}

// Function represents a function definition
type Function struct {
	Name       string         `json:"name"`
	Parameters []ParameterDef `json:"parameters"`
	ReturnType *TypeExpr      `json:"return_type,omitempty"`
	Body       *Block         `json:"body"`
	Visibility Visibility     `json:"visibility"`
	Mutability Mutability     `json:"mutability"`
	Requires   []Expression   `json:"requires,omitempty"`
	Ensures    []Expression   `json:"ensures,omitempty"`
	Line       int            `json:"line"`
	Column     int            `json:"column"`
}

func (f *Function) String() string       { return fmt.Sprintf("fun %s", f.Name) }
func (f *Function) GetType() ASTNodeType { return NodeFunction }
func (f *Function) IsModuleItem()        {}

// FieldDef represents a field definition
type FieldDef struct {
	Name   string    `json:"name"`
	Type   *TypeExpr `json:"type"`
	Line   int       `json:"line"`
	Column int       `json:"column"`
}

func (f *FieldDef) String() string       { return fmt.Sprintf("%s: %s", f.Name, f.Type) }
func (f *FieldDef) GetType() ASTNodeType { return NodeField }

// ParameterDef represents a parameter definition
type ParameterDef struct {
	Name   string    `json:"name"`
	Type   *TypeExpr `json:"type"`
	Line   int       `json:"line"`
	Column int       `json:"column"`
}

func (p *ParameterDef) String() string       { return fmt.Sprintf("%s: %s", p.Name, p.Type) }
func (p *ParameterDef) GetType() ASTNodeType { return NodeParameter }

// TypeExpr represents a type expression
type TypeExpr struct {
	Kind        string     `json:"kind"`                   // u64, bool, vector, etc.
	Name        string     `json:"name,omitempty"`         // For named types
	Module      string     `json:"module,omitempty"`       // For module-qualified types
	TypeArgs    []TypeExpr `json:"type_args,omitempty"`    // For generic types
	ElementType *TypeExpr  `json:"element_type,omitempty"` // For vector, option
	Line        int        `json:"line"`
	Column      int        `json:"column"`
}

func (t *TypeExpr) String() string {
	if t.Module != "" && t.Name != "" {
		return fmt.Sprintf("%s::%s", t.Module, t.Name)
	}
	if t.Name != "" {
		return t.Name
	}
	return t.Kind
}
func (t *TypeExpr) GetType() ASTNodeType { return NodeType }

// Statement represents a statement
type Statement interface {
	ASTNode
	IsStatement()
}

// Expression represents an expression
type Expression interface {
	ASTNode
	IsExpression()
}

// Block represents a block of statements
type Block struct {
	Statements []Statement `json:"statements"`
	Line       int         `json:"line"`
	Column     int         `json:"column"`
}

func (b *Block) String() string       { return fmt.Sprintf("block(%d statements)", len(b.Statements)) }
func (b *Block) GetType() ASTNodeType { return NodeBlock }

// Specific statement types

// LetStatement represents a let binding
type LetStatement struct {
	Name    string     `json:"name"`
	Type    *TypeExpr  `json:"type,omitempty"`
	Value   Expression `json:"value"`
	Mutable bool       `json:"mutable"`
	Line    int        `json:"line"`
	Column  int        `json:"column"`
}

func (l *LetStatement) String() string       { return fmt.Sprintf("let %s = ...", l.Name) }
func (l *LetStatement) GetType() ASTNodeType { return NodeStatement }
func (l *LetStatement) IsStatement()         {}

// AssignStatement represents an assignment
type AssignStatement struct {
	Target Expression `json:"target"`
	Value  Expression `json:"value"`
	Line   int        `json:"line"`
	Column int        `json:"column"`
}

func (a *AssignStatement) String() string       { return "assignment" }
func (a *AssignStatement) GetType() ASTNodeType { return NodeStatement }
func (a *AssignStatement) IsStatement()         {}

// ExpressionStatement represents an expression used as a statement
type ExpressionStatement struct {
	Expression Expression `json:"expression"`
	Line       int        `json:"line"`
	Column     int        `json:"column"`
}

func (e *ExpressionStatement) String() string       { return "expression statement" }
func (e *ExpressionStatement) GetType() ASTNodeType { return NodeStatement }
func (e *ExpressionStatement) IsStatement()         {}

// ReturnStatement represents a return statement
type ReturnStatement struct {
	Value  Expression `json:"value,omitempty"`
	Line   int        `json:"line"`
	Column int        `json:"column"`
}

func (r *ReturnStatement) String() string       { return "return" }
func (r *ReturnStatement) GetType() ASTNodeType { return NodeStatement }
func (r *ReturnStatement) IsStatement()         {}

// Specific expression types

// Literal represents a literal value
type Literal struct {
	Kind   TokenType   `json:"kind"`
	Value  interface{} `json:"value"`
	Line   int         `json:"line"`
	Column int         `json:"column"`
}

func (l *Literal) String() string       { return fmt.Sprintf("%v", l.Value) }
func (l *Literal) GetType() ASTNodeType { return NodeLiteral }
func (l *Literal) IsExpression()        {}

// Identifier represents an identifier
type Identifier struct {
	Name   string `json:"name"`
	Line   int    `json:"line"`
	Column int    `json:"column"`
}

func (i *Identifier) String() string       { return i.Name }
func (i *Identifier) GetType() ASTNodeType { return NodeIdentifier }
func (i *Identifier) IsExpression()        {}

// BinaryOp represents a binary operation
type BinaryOp struct {
	Left     Expression `json:"left"`
	Operator string     `json:"operator"`
	Right    Expression `json:"right"`
	Line     int        `json:"line"`
	Column   int        `json:"column"`
}

func (b *BinaryOp) String() string       { return fmt.Sprintf("(%s %s %s)", b.Left, b.Operator, b.Right) }
func (b *BinaryOp) GetType() ASTNodeType { return NodeBinaryOp }
func (b *BinaryOp) IsExpression()        {}

// UnaryOp represents a unary operation
type UnaryOp struct {
	Operator string     `json:"operator"`
	Operand  Expression `json:"operand"`
	Line     int        `json:"line"`
	Column   int        `json:"column"`
}

func (u *UnaryOp) String() string       { return fmt.Sprintf("(%s %s)", u.Operator, u.Operand) }
func (u *UnaryOp) GetType() ASTNodeType { return NodeUnaryOp }
func (u *UnaryOp) IsExpression()        {}

// Call represents a function call
type Call struct {
	Function  Expression   `json:"function"`
	Arguments []Expression `json:"arguments"`
	Line      int          `json:"line"`
	Column    int          `json:"column"`
}

func (c *Call) String() string       { return fmt.Sprintf("call %s", c.Function) }
func (c *Call) GetType() ASTNodeType { return NodeCall }
func (c *Call) IsExpression()        {}

// FieldAccess represents field access
type FieldAccess struct {
	Object Expression `json:"object"`
	Field  string     `json:"field"`
	Line   int        `json:"line"`
	Column int        `json:"column"`
}

func (f *FieldAccess) String() string       { return fmt.Sprintf("%s.%s", f.Object, f.Field) }
func (f *FieldAccess) GetType() ASTNodeType { return NodeFieldAccess }
func (f *FieldAccess) IsExpression()        {}

// InvariantDecl represents an invariant declaration
type InvariantDecl struct {
	Name        string     `json:"name"`
	Expression  Expression `json:"expression"`
	Description string     `json:"description,omitempty"`
	Line        int        `json:"line"`
	Column      int        `json:"column"`
}

func (i *InvariantDecl) String() string       { return fmt.Sprintf("invariant %s", i.Name) }
func (i *InvariantDecl) GetType() ASTNodeType { return NodeInvariant }

// DNALexer tokenizes DNA source code
type DNALexer struct {
	input    string
	position int
	line     int
	column   int
	logger   *logrus.Logger
}

// NewDNAParser creates a new DNA parser
func NewDNAParser(logger *logrus.Logger) *DNAParser {
	if logger == nil {
		logger = logrus.New()
	}

	return &DNAParser{
		logger: logger,
	}
}

// Parse parses DNA source code into an AST
func (p *DNAParser) Parse(source string) (*Module, error) {
	// Initialize lexer
	p.lexer = &DNALexer{
		input:  source,
		line:   1,
		column: 1,
		logger: p.logger,
	}

	// Tokenize
	var err error
	p.tokens, err = p.lexer.tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization error: %v", err)
	}

	p.pos = 0

	// Parse module
	return p.parseModule()
}

// Lexer implementation

func (l *DNALexer) tokenize() ([]Token, error) {
	var tokens []Token

	for l.position < len(l.input) {
		token, err := l.nextToken()
		if err != nil {
			return nil, err
		}

		// Skip comments and whitespace
		if token.Type == TokenComment || token.Type == TokenNewline {
			continue
		}

		tokens = append(tokens, token)
	}

	// Add EOF token
	tokens = append(tokens, Token{
		Type:   TokenEOF,
		Line:   l.line,
		Column: l.column,
	})

	return tokens, nil
}

func (l *DNALexer) nextToken() (Token, error) {
	l.skipWhitespace()

	if l.position >= len(l.input) {
		return Token{Type: TokenEOF, Line: l.line, Column: l.column}, nil
	}

	start := l.position
	startLine := l.line
	startColumn := l.column

	ch := l.currentChar()

	// Numbers
	if isDigit(ch) {
		return l.readNumber(start, startLine, startColumn)
	}

	// Strings
	if ch == '"' {
		return l.readString(start, startLine, startColumn)
	}

	// Identifiers and keywords
	if isAlpha(ch) || ch == '_' {
		return l.readIdentifier(start, startLine, startColumn)
	}

	// Single character tokens
	switch ch {
	case '+':
		l.advance()
		return Token{Type: TokenPlus, Value: "+", Line: startLine, Column: startColumn}, nil
	case '-':
		l.advance()
		return Token{Type: TokenMinus, Value: "-", Line: startLine, Column: startColumn}, nil
	case '*':
		l.advance()
		return Token{Type: TokenStar, Value: "*", Line: startLine, Column: startColumn}, nil
	case '/':
		// Check for comments
		if l.peek() == '/' {
			return l.readLineComment(start, startLine, startColumn)
		}
		l.advance()
		return Token{Type: TokenSlash, Value: "/", Line: startLine, Column: startColumn}, nil
	case '%':
		l.advance()
		return Token{Type: TokenPercent, Value: "%", Line: startLine, Column: startColumn}, nil
	case '(':
		l.advance()
		return Token{Type: TokenLParen, Value: "(", Line: startLine, Column: startColumn}, nil
	case ')':
		l.advance()
		return Token{Type: TokenRParen, Value: ")", Line: startLine, Column: startColumn}, nil
	case '{':
		l.advance()
		return Token{Type: TokenLBrace, Value: "{", Line: startLine, Column: startColumn}, nil
	case '}':
		l.advance()
		return Token{Type: TokenRBrace, Value: "}", Line: startLine, Column: startColumn}, nil
	case '[':
		l.advance()
		return Token{Type: TokenLBracket, Value: "[", Line: startLine, Column: startColumn}, nil
	case ']':
		l.advance()
		return Token{Type: TokenRBracket, Value: "]", Line: startLine, Column: startColumn}, nil
	case ';':
		l.advance()
		return Token{Type: TokenSemicolon, Value: ";", Line: startLine, Column: startColumn}, nil
	case ',':
		l.advance()
		return Token{Type: TokenComma, Value: ",", Line: startLine, Column: startColumn}, nil
	case '.':
		l.advance()
		return Token{Type: TokenDot, Value: ".", Line: startLine, Column: startColumn}, nil
	case ':':
		if l.peek() == ':' {
			l.advance()
			l.advance()
			return Token{Type: TokenColonColon, Value: "::", Line: startLine, Column: startColumn}, nil
		}
		l.advance()
		return Token{Type: TokenColon, Value: ":", Line: startLine, Column: startColumn}, nil
	case '=':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenEq, Value: "==", Line: startLine, Column: startColumn}, nil
		}
		l.advance()
		return Token{Type: TokenAssign, Value: "=", Line: startLine, Column: startColumn}, nil
	case '!':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenNe, Value: "!=", Line: startLine, Column: startColumn}, nil
		}
		l.advance()
		return Token{Type: TokenNot, Value: "!", Line: startLine, Column: startColumn}, nil
	case '<':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenLe, Value: "<=", Line: startLine, Column: startColumn}, nil
		}
		l.advance()
		return Token{Type: TokenLt, Value: "<", Line: startLine, Column: startColumn}, nil
	case '>':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenGe, Value: ">=", Line: startLine, Column: startColumn}, nil
		}
		l.advance()
		return Token{Type: TokenGt, Value: ">", Line: startLine, Column: startColumn}, nil
	case '&':
		if l.peek() == '&' {
			l.advance()
			l.advance()
			return Token{Type: TokenAnd, Value: "&&", Line: startLine, Column: startColumn}, nil
		}
	case '|':
		if l.peek() == '|' {
			l.advance()
			l.advance()
			return Token{Type: TokenOr, Value: "||", Line: startLine, Column: startColumn}, nil
		}
	case '\n':
		l.advance()
		return Token{Type: TokenNewline, Value: "\\n", Line: startLine, Column: startColumn}, nil
	}

	return Token{}, fmt.Errorf("unexpected character '%c' at line %d, column %d", ch, l.line, l.column)
}

func (l *DNALexer) readNumber(start, startLine, startColumn int) (Token, error) {
	for l.position < len(l.input) && isDigit(l.currentChar()) {
		l.advance()
	}

	value := l.input[start:l.position]
	return Token{Type: TokenNumber, Value: value, Line: startLine, Column: startColumn}, nil
}

func (l *DNALexer) readString(start, startLine, startColumn int) (Token, error) {
	l.advance() // Skip opening quote

	for l.position < len(l.input) && l.currentChar() != '"' {
		if l.currentChar() == '\\' {
			l.advance() // Skip escape character
		}
		l.advance()
	}

	if l.position >= len(l.input) {
		return Token{}, fmt.Errorf("unterminated string at line %d", startLine)
	}

	l.advance() // Skip closing quote

	value := l.input[start+1 : l.position-1] // Exclude quotes
	return Token{Type: TokenString, Value: value, Line: startLine, Column: startColumn}, nil
}

func (l *DNALexer) readIdentifier(start, startLine, startColumn int) (Token, error) {
	for l.position < len(l.input) && (isAlphaNumeric(l.currentChar()) || l.currentChar() == '_') {
		l.advance()
	}

	value := l.input[start:l.position]
	tokenType := l.getKeywordType(value)

	return Token{Type: tokenType, Value: value, Line: startLine, Column: startColumn}, nil
}

func (l *DNALexer) readLineComment(start, startLine, startColumn int) (Token, error) {
	for l.position < len(l.input) && l.currentChar() != '\n' {
		l.advance()
	}

	value := l.input[start:l.position]
	return Token{Type: TokenComment, Value: value, Line: startLine, Column: startColumn}, nil
}

func (l *DNALexer) getKeywordType(value string) TokenType {
	keywords := map[string]TokenType{
		"resource":  TokenResource,
		"module":    TokenModule,
		"struct":    TokenStruct,
		"fun":       TokenFun,
		"public":    TokenPublic,
		"internal":  TokenInternal,
		"private":   TokenPrivate,
		"move":      TokenMove,
		"copy":      TokenCopy,
		"drop":      TokenDrop,
		"borrow":    TokenBorrow,
		"let":       TokenLet,
		"mut":       TokenMut,
		"if":        TokenIf,
		"else":      TokenElse,
		"while":     TokenWhile,
		"for":       TokenFor,
		"return":    TokenReturn,
		"abort":     TokenAbort,
		"assert":    TokenAssert,
		"invariant": TokenInvariant,
		"requires":  TokenRequires,
		"ensures":   TokenEnsures,
		"u8":        TokenU8,
		"u16":       TokenU16,
		"u32":       TokenU32,
		"u64":       TokenU64,
		"u128":      TokenU128,
		"bool":      TokenBoolType,
		"vector":    TokenVector,
		"option":    TokenOption,
		"true":      TokenBool,
		"false":     TokenBool,
	}

	if tokenType, exists := keywords[value]; exists {
		return tokenType
	}

	// Check for address literal (0x...)
	if matched, _ := regexp.MatchString(`^0x[0-9a-fA-F]+$`, value); matched {
		return TokenAddress
	}

	return TokenIdent
}

// Lexer helper methods

func (l *DNALexer) currentChar() byte {
	if l.position >= len(l.input) {
		return 0
	}
	return l.input[l.position]
}

func (l *DNALexer) peek() byte {
	if l.position+1 >= len(l.input) {
		return 0
	}
	return l.input[l.position+1]
}

func (l *DNALexer) advance() {
	if l.position < len(l.input) && l.input[l.position] == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
	l.position++
}

func (l *DNALexer) skipWhitespace() {
	for l.position < len(l.input) && (l.currentChar() == ' ' || l.currentChar() == '\t' || l.currentChar() == '\r') {
		l.advance()
	}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isAlphaNumeric(ch byte) bool {
	return isAlpha(ch) || isDigit(ch)
}

// Parser implementation

func (p *DNAParser) parseModule() (*Module, error) {
	// Expect module keyword
	if !p.match(TokenModule) {
		return nil, fmt.Errorf("expected 'module' keyword")
	}

	// Parse module name
	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected module name: %v", err)
	}

	// Expect semicolon
	if !p.match(TokenSemicolon) {
		return nil, fmt.Errorf("expected ';' after module name")
	}

	module := &Module{
		Name:    name,
		Items:   make([]ModuleItem, 0),
		Imports: make([]ImportDecl, 0),
	}

	// Parse module items
	for !p.isAtEnd() && !p.check(TokenEOF) {
		item, err := p.parseModuleItem()
		if err != nil {
			return nil, err
		}
		if item != nil {
			module.Items = append(module.Items, item)
		}
	}

	return module, nil
}

func (p *DNAParser) parseModuleItem() (ModuleItem, error) {
	// Check for visibility modifiers
	visibility := p.parseVisibility()

	switch {
	case p.check(TokenResource):
		return p.parseResourceDef(visibility)
	case p.check(TokenStruct):
		return p.parseStructDef(visibility)
	case p.check(TokenFun):
		return p.parseFunction(visibility)
	default:
		return nil, fmt.Errorf("unexpected token: %v", p.peek())
	}
}

func (p *DNAParser) parseResourceDef(visibility Visibility) (*ResourceDef, error) {
	if !p.match(TokenResource) {
		return nil, fmt.Errorf("expected 'resource'")
	}

	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected resource name: %v", err)
	}

	resource := &ResourceDef{
		Name:       name,
		Visibility: visibility,
		Fields:     make([]FieldDef, 0),
		Methods:    make([]Function, 0),
		Invariants: make([]InvariantDecl, 0),
	}

	// Parse abilities
	if p.match(TokenColon) {
		resource.Abilities, err = p.parseAbilities()
		if err != nil {
			return nil, err
		}
	}

	// Parse body
	if !p.match(TokenLBrace) {
		return nil, fmt.Errorf("expected '{' after resource declaration")
	}

	for !p.check(TokenRBrace) && !p.isAtEnd() {
		// Parse fields and methods
		if p.check(TokenIdent) {
			field, err := p.parseFieldDef()
			if err != nil {
				return nil, err
			}
			resource.Fields = append(resource.Fields, *field)
		} else if p.check(TokenFun) {
			method, err := p.parseFunction(VisibilityPublic)
			if err != nil {
				return nil, err
			}
			resource.Methods = append(resource.Methods, *method)
		} else if p.check(TokenInvariant) {
			invariant, err := p.parseInvariant()
			if err != nil {
				return nil, err
			}
			resource.Invariants = append(resource.Invariants, *invariant)
		} else {
			return nil, fmt.Errorf("unexpected token in resource body: %v", p.peek())
		}
	}

	if !p.match(TokenRBrace) {
		return nil, fmt.Errorf("expected '}' to close resource body")
	}

	return resource, nil
}

func (p *DNAParser) parseStructDef(visibility Visibility) (*StructDef, error) {
	// Implementation similar to parseResourceDef but for structs
	if !p.match(TokenStruct) {
		return nil, fmt.Errorf("expected 'struct'")
	}

	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected struct name: %v", err)
	}

	structDef := &StructDef{
		Name:       name,
		Visibility: visibility,
		Fields:     make([]FieldDef, 0),
	}

	// Parse abilities
	if p.match(TokenColon) {
		structDef.Abilities, err = p.parseAbilities()
		if err != nil {
			return nil, err
		}
	}

	// Parse body
	if !p.match(TokenLBrace) {
		return nil, fmt.Errorf("expected '{' after struct declaration")
	}

	for !p.check(TokenRBrace) && !p.isAtEnd() {
		field, err := p.parseFieldDef()
		if err != nil {
			return nil, err
		}
		structDef.Fields = append(structDef.Fields, *field)
	}

	if !p.match(TokenRBrace) {
		return nil, fmt.Errorf("expected '}' to close struct body")
	}

	return structDef, nil
}

func (p *DNAParser) parseFunction(visibility Visibility) (*Function, error) {
	if !p.match(TokenFun) {
		return nil, fmt.Errorf("expected 'fun'")
	}

	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected function name: %v", err)
	}

	function := &Function{
		Name:       name,
		Visibility: visibility,
		Parameters: make([]ParameterDef, 0),
	}

	// Parse parameters
	if !p.match(TokenLParen) {
		return nil, fmt.Errorf("expected '(' after function name")
	}

	for !p.check(TokenRParen) && !p.isAtEnd() {
		param, err := p.parseParameter()
		if err != nil {
			return nil, err
		}
		function.Parameters = append(function.Parameters, *param)

		if !p.check(TokenRParen) {
			if !p.match(TokenComma) {
				return nil, fmt.Errorf("expected ',' between parameters")
			}
		}
	}

	if !p.match(TokenRParen) {
		return nil, fmt.Errorf("expected ')' after parameters")
	}

	// Parse return type
	if p.match(TokenColon) {
		function.ReturnType, err = p.parseType()
		if err != nil {
			return nil, err
		}
	}

	// Parse body
	function.Body, err = p.parseBlock()
	if err != nil {
		return nil, err
	}

	return function, nil
}

func (p *DNAParser) parseFieldDef() (*FieldDef, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected field name: %v", err)
	}

	if !p.match(TokenColon) {
		return nil, fmt.Errorf("expected ':' after field name")
	}

	fieldType, err := p.parseType()
	if err != nil {
		return nil, fmt.Errorf("expected field type: %v", err)
	}

	if !p.match(TokenSemicolon) {
		return nil, fmt.Errorf("expected ';' after field definition")
	}

	return &FieldDef{
		Name: name,
		Type: fieldType,
	}, nil
}

func (p *DNAParser) parseParameter() (*ParameterDef, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected parameter name: %v", err)
	}

	if !p.match(TokenColon) {
		return nil, fmt.Errorf("expected ':' after parameter name")
	}

	paramType, err := p.parseType()
	if err != nil {
		return nil, fmt.Errorf("expected parameter type: %v", err)
	}

	return &ParameterDef{
		Name: name,
		Type: paramType,
	}, nil
}

func (p *DNAParser) parseType() (*TypeExpr, error) {
	token := p.peek()

	typeExpr := &TypeExpr{
		Line:   token.Line,
		Column: token.Column,
	}

	switch token.Type {
	case TokenU8, TokenU16, TokenU32, TokenU64, TokenU128, TokenBoolType:
		p.advance()
		typeExpr.Kind = token.Value
		return typeExpr, nil
	case TokenVector:
		p.advance()
		if !p.match(TokenLt) {
			return nil, fmt.Errorf("expected '<' after 'vector'")
		}
		elementType, err := p.parseType()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenGt) {
			return nil, fmt.Errorf("expected '>' after vector element type")
		}
		typeExpr.Kind = "vector"
		typeExpr.ElementType = elementType
		return typeExpr, nil
	case TokenIdent:
		// Named type or module-qualified type
		name := token.Value
		p.advance()

		if p.match(TokenColonColon) {
			// Module-qualified type
			typeName, err := p.expectIdent()
			if err != nil {
				return nil, fmt.Errorf("expected type name after '::'")
			}
			typeExpr.Module = name
			typeExpr.Name = typeName
		} else {
			typeExpr.Name = name
		}
		return typeExpr, nil
	default:
		return nil, fmt.Errorf("expected type, got %v", token.Type)
	}
}

func (p *DNAParser) parseBlock() (*Block, error) {
	if !p.match(TokenLBrace) {
		return nil, fmt.Errorf("expected '{'")
	}

	block := &Block{
		Statements: make([]Statement, 0),
	}

	for !p.check(TokenRBrace) && !p.isAtEnd() {
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if stmt != nil {
			block.Statements = append(block.Statements, stmt)
		}
	}

	if !p.match(TokenRBrace) {
		return nil, fmt.Errorf("expected '}' to close block")
	}

	return block, nil
}

func (p *DNAParser) parseStatement() (Statement, error) {
	switch {
	case p.check(TokenLet):
		return p.parseLetStatement()
	case p.check(TokenReturn):
		return p.parseReturnStatement()
	default:
		// Try to parse as expression statement
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		// Check for assignment
		if p.match(TokenAssign) {
			value, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			if !p.match(TokenSemicolon) {
				return nil, fmt.Errorf("expected ';' after assignment")
			}
			return &AssignStatement{
				Target: expr,
				Value:  value,
			}, nil
		}

		if !p.match(TokenSemicolon) {
			return nil, fmt.Errorf("expected ';' after expression")
		}
		return &ExpressionStatement{
			Expression: expr,
		}, nil
	}
}

func (p *DNAParser) parseLetStatement() (Statement, error) {
	if !p.match(TokenLet) {
		return nil, fmt.Errorf("expected 'let'")
	}

	mutable := p.match(TokenMut)

	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected variable name: %v", err)
	}

	var varType *TypeExpr
	if p.match(TokenColon) {
		varType, err = p.parseType()
		if err != nil {
			return nil, err
		}
	}

	if !p.match(TokenAssign) {
		return nil, fmt.Errorf("expected '=' in let statement")
	}

	value, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if !p.match(TokenSemicolon) {
		return nil, fmt.Errorf("expected ';' after let statement")
	}

	return &LetStatement{
		Name:    name,
		Type:    varType,
		Value:   value,
		Mutable: mutable,
	}, nil
}

func (p *DNAParser) parseReturnStatement() (Statement, error) {
	if !p.match(TokenReturn) {
		return nil, fmt.Errorf("expected 'return'")
	}

	stmt := &ReturnStatement{}

	if !p.check(TokenSemicolon) {
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Value = value
	}

	if !p.match(TokenSemicolon) {
		return nil, fmt.Errorf("expected ';' after return statement")
	}

	return stmt, nil
}

func (p *DNAParser) parseExpression() (Expression, error) {
	return p.parseLogicalOr()
}

func (p *DNAParser) parseLogicalOr() (Expression, error) {
	expr, err := p.parseLogicalAnd()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOr) {
		operator := p.previous().Value
		right, err := p.parseLogicalAnd()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseLogicalAnd() (Expression, error) {
	expr, err := p.parseEquality()
	if err != nil {
		return nil, err
	}

	for p.match(TokenAnd) {
		operator := p.previous().Value
		right, err := p.parseEquality()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseEquality() (Expression, error) {
	expr, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenEq, TokenNe) {
		operator := p.previous().Value
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseComparison() (Expression, error) {
	expr, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenGt, TokenGe, TokenLt, TokenLe) {
		operator := p.previous().Value
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseTerm() (Expression, error) {
	expr, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenMinus, TokenPlus) {
		operator := p.previous().Value
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseFactor() (Expression, error) {
	expr, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.matchAny(TokenSlash, TokenStar, TokenPercent) {
		operator := p.previous().Value
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		expr = &BinaryOp{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *DNAParser) parseUnary() (Expression, error) {
	if p.matchAny(TokenNot, TokenMinus) {
		operator := p.previous().Value
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{
			Operator: operator,
			Operand:  expr,
		}, nil
	}

	return p.parseCall()
}

func (p *DNAParser) parseCall() (Expression, error) {
	expr, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		if p.match(TokenLParen) {
			// Function call
			args := make([]Expression, 0)

			if !p.check(TokenRParen) {
				for {
					arg, err := p.parseExpression()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)

					if !p.match(TokenComma) {
						break
					}
				}
			}

			if !p.match(TokenRParen) {
				return nil, fmt.Errorf("expected ')' after arguments")
			}

			expr = &Call{
				Function:  expr,
				Arguments: args,
			}
		} else if p.match(TokenDot) {
			// Field access
			field, err := p.expectIdent()
			if err != nil {
				return nil, fmt.Errorf("expected field name after '.'")
			}
			expr = &FieldAccess{
				Object: expr,
				Field:  field,
			}
		} else {
			break
		}
	}

	return expr, nil
}

func (p *DNAParser) parsePrimary() (Expression, error) {
	switch {
	case p.match(TokenBool):
		value := p.previous().Value == "true"
		return &Literal{
			Kind:  TokenBool,
			Value: value,
		}, nil
	case p.match(TokenNumber):
		value, err := strconv.ParseInt(p.previous().Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %v", err)
		}
		return &Literal{
			Kind:  TokenNumber,
			Value: value,
		}, nil
	case p.match(TokenString):
		return &Literal{
			Kind:  TokenString,
			Value: p.previous().Value,
		}, nil
	case p.match(TokenAddress):
		return &Literal{
			Kind:  TokenAddress,
			Value: p.previous().Value,
		}, nil
	case p.match(TokenIdent):
		return &Identifier{
			Name: p.previous().Value,
		}, nil
	case p.match(TokenLParen):
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenRParen) {
			return nil, fmt.Errorf("expected ')' after expression")
		}
		return expr, nil
	}

	return nil, fmt.Errorf("expected expression, got %v", p.peek())
}

func (p *DNAParser) parseVisibility() Visibility {
	switch {
	case p.match(TokenPublic):
		return VisibilityPublic
	case p.match(TokenInternal):
		return VisibilityInternal
	case p.match(TokenPrivate):
		return VisibilityPrivate
	default:
		return VisibilityInternal // Default visibility
	}
}

func (p *DNAParser) parseAbilities() ([]string, error) {
	abilities := make([]string, 0)

	for {
		if p.check(TokenIdent) {
			ability := p.advance().Value
			abilities = append(abilities, ability)
		} else {
			break
		}

		if !p.match(TokenComma) {
			break
		}
	}

	return abilities, nil
}

func (p *DNAParser) parseInvariant() (*InvariantDecl, error) {
	if !p.match(TokenInvariant) {
		return nil, fmt.Errorf("expected 'invariant'")
	}

	name, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("expected invariant name: %v", err)
	}

	if !p.match(TokenLBrace) {
		return nil, fmt.Errorf("expected '{' after invariant name")
	}

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if !p.match(TokenRBrace) {
		return nil, fmt.Errorf("expected '}' after invariant expression")
	}

	return &InvariantDecl{
		Name:       name,
		Expression: expr,
	}, nil
}

// Parser helper methods

func (p *DNAParser) match(tokenType TokenType) bool {
	if p.check(tokenType) {
		p.advance()
		return true
	}
	return false
}

func (p *DNAParser) matchAny(types ...TokenType) bool {
	for _, tokenType := range types {
		if p.check(tokenType) {
			p.advance()
			return true
		}
	}
	return false
}

func (p *DNAParser) check(tokenType TokenType) bool {
	if p.isAtEnd() {
		return false
	}
	return p.peek().Type == tokenType
}

func (p *DNAParser) advance() Token {
	if !p.isAtEnd() {
		p.pos++
	}
	return p.previous()
}

func (p *DNAParser) isAtEnd() bool {
	return p.pos >= len(p.tokens) || p.peek().Type == TokenEOF
}

func (p *DNAParser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *DNAParser) previous() Token {
	if p.pos == 0 {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos-1]
}

func (p *DNAParser) expectIdent() (string, error) {
	if !p.check(TokenIdent) {
		return "", fmt.Errorf("expected identifier, got %v", p.peek().Type)
	}
	return p.advance().Value, nil
}

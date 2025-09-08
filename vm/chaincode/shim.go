package chaincode

import (
	"context"
	"fmt"
	"time"

	"diamante/vm/runtime"

	"github.com/sirupsen/logrus"
)

// ChaincodeStubInterface provides the Fabric-compatible chaincode shim interface
type ChaincodeStubInterface interface {
	// State management
	GetState(key string) ([]byte, error)
	PutState(key string, value []byte) error
	DelState(key string) error
	GetStateByRange(startKey, endKey string) (StateQueryIteratorInterface, error)
	GetStateByPartialCompositeKey(objectType string, keys []string) (StateQueryIteratorInterface, error)
	CreateCompositeKey(objectType string, attributes []string) (string, error)
	SplitCompositeKey(compositeKey string) (string, []string, error)

	// Private data management
	GetPrivateData(collection, key string) ([]byte, error)
	PutPrivateData(collection, key string, value []byte) error
	DelPrivateData(collection, key string) error
	GetPrivateDataByRange(collection, startKey, endKey string) (StateQueryIteratorInterface, error)
	GetPrivateDataByPartialCompositeKey(collection, objectType string, keys []string) (StateQueryIteratorInterface, error)
	GetPrivateDataHash(collection, key string) ([]byte, error)

	// Transaction context
	GetTxID() string
	GetChannelID() string
	GetMSPID() string
	GetCreator() ([]byte, error)
	GetSignedProposal() (*SignedProposal, error)
	GetTxTimestamp() (*Timestamp, error)
	GetBinding() ([]byte, error)
	GetTransient() (map[string][]byte, error)

	// Function and parameter access
	GetFunctionAndParameters() (string, []string)
	GetStringArgs() []string
	GetArgs() [][]byte

	// Chaincode invocation
	InvokeChaincode(chaincodeName string, args [][]byte, channel string) Response

	// Event management
	SetEvent(name string, payload []byte) error

	// History and queries
	GetHistoryForKey(key string) (HistoryQueryIteratorInterface, error)
	GetQueryResult(query string) (StateQueryIteratorInterface, error)
	GetPrivateDataQueryResult(collection, query string) (StateQueryIteratorInterface, error)
}

// ChaincodeStub implements the ChaincodeStubInterface
type ChaincodeStub struct {
	runtime        *ChaincodeRuntime
	txID           string
	channelID      string
	chaincodeID    string
	function       string
	args           []string
	transientData  map[string][]byte
	creator        []byte
	signedProposal *SignedProposal
	timestamp      *Timestamp
	logger         *logrus.Logger
	stateChanges   map[string][]byte
	events         []*ChaincodeEvent
}

// StateQueryIteratorInterface provides iterator interface for state queries
type StateQueryIteratorInterface interface {
	HasNext() bool
	Next() (*QueryResponse, error)
	Close() error
}

// HistoryQueryIteratorInterface provides iterator interface for history queries
type HistoryQueryIteratorInterface interface {
	HasNext() bool
	Next() (*KeyModification, error)
	Close() error
}

// QueryResponse represents a query result
type QueryResponse struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Namespace string `json:"namespace"`
}

// KeyModification represents a historical modification to a key
type KeyModification struct {
	TxID      string    `json:"tx_id"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	IsDelete  bool      `json:"is_delete"`
}

// SignedProposal represents a signed transaction proposal
type SignedProposal struct {
	ProposalBytes []byte `json:"proposal_bytes"`
	Signature     []byte `json:"signature"`
}

// Timestamp represents a timestamp
type Timestamp struct {
	Seconds int64 `json:"seconds"`
	Nanos   int32 `json:"nanos"`
}

// Response represents a chaincode response
type Response struct {
	Status  int32  `json:"status"`
	Message string `json:"message"`
	Payload []byte `json:"payload"`
}

// ChaincodeEvent represents an event emitted by chaincode
type ChaincodeEvent struct {
	EventName string `json:"event_name"`
	Payload   []byte `json:"payload"`
	TxID      string `json:"tx_id"`
}

// Success creates a successful response
func Success(payload []byte) Response {
	return Response{
		Status:  200,
		Message: "OK",
		Payload: payload,
	}
}

// Error creates an error response
func Error(msg string) Response {
	return Response{
		Status:  500,
		Message: msg,
		Payload: nil,
	}
}

// NewChaincodeStub creates a new chaincode stub
func NewChaincodeStub(runtime *ChaincodeRuntime, txID, channelID, chaincodeID string, function string, args []string, transientData map[string][]byte, logger *logrus.Logger) *ChaincodeStub {
	return &ChaincodeStub{
		runtime:       runtime,
		txID:          txID,
		channelID:     channelID,
		chaincodeID:   chaincodeID,
		function:      function,
		args:          args,
		transientData: transientData,
		logger:        logger,
		stateChanges:  make(map[string][]byte),
		events:        make([]*ChaincodeEvent, 0),
		timestamp: &Timestamp{
			Seconds: time.Now().Unix(),
			Nanos:   int32(time.Now().Nanosecond()),
		},
	}
}

// State management implementation

func (s *ChaincodeStub) GetState(key string) ([]byte, error) {
	// Check local state changes first
	if value, exists := s.stateChanges[key]; exists {
		return value, nil
	}

	// Query from storage
	stateKey := fmt.Sprintf("%s:%s", s.chaincodeID, key)
	return s.runtime.stateStore.GetState([]byte(stateKey))
}

func (s *ChaincodeStub) PutState(key string, value []byte) error {
	// Store in local state changes for transaction
	s.stateChanges[key] = value

	// Also persist immediately for read-your-writes consistency
	stateKey := fmt.Sprintf("%s:%s", s.chaincodeID, key)
	return s.runtime.stateStore.SaveState([]byte(stateKey), value)
}

func (s *ChaincodeStub) DelState(key string) error {
	// Mark as deleted in local state changes
	s.stateChanges[key] = nil

	// Delete from storage
	stateKey := fmt.Sprintf("%s:%s", s.chaincodeID, key)
	return s.runtime.stateStore.SaveState([]byte(stateKey), []byte{})
}

func (s *ChaincodeStub) GetStateByRange(startKey, endKey string) (StateQueryIteratorInterface, error) {
	// Simple implementation - in production this would use proper range queries
	return NewSimpleStateIterator(s, startKey, endKey), nil
}

func (s *ChaincodeStub) GetStateByPartialCompositeKey(objectType string, keys []string) (StateQueryIteratorInterface, error) {
	compositePrefix, err := s.CreateCompositeKey(objectType, keys)
	if err != nil {
		return nil, err
	}
	return s.GetStateByRange(compositePrefix, compositePrefix+"\xff")
}

func (s *ChaincodeStub) CreateCompositeKey(objectType string, attributes []string) (string, error) {
	if objectType == "" {
		return "", fmt.Errorf("object type cannot be empty")
	}

	key := objectType
	for _, attr := range attributes {
		key += "\x00" + attr
	}
	return key, nil
}

func (s *ChaincodeStub) SplitCompositeKey(compositeKey string) (string, []string, error) {
	parts := []byte(compositeKey)
	objectType := ""
	attributes := []string{}

	// Simple parsing - in production would handle escape sequences
	currentPart := ""
	for _, b := range parts {
		if b == 0x00 {
			if objectType == "" {
				objectType = currentPart
			} else {
				attributes = append(attributes, currentPart)
			}
			currentPart = ""
		} else {
			currentPart += string(b)
		}
	}

	if currentPart != "" {
		if objectType == "" {
			objectType = currentPart
		} else {
			attributes = append(attributes, currentPart)
		}
	}

	return objectType, attributes, nil
}

// Private data management implementation

func (s *ChaincodeStub) GetPrivateData(collection, key string) ([]byte, error) {
	// Get the organization ID (in a real implementation this would come from the certificate)
	orgID := "default_org"
	return s.runtime.GetPrivateData(collection, key, orgID)
}

func (s *ChaincodeStub) PutPrivateData(collection, key string, value []byte) error {
	// Get current block height (simplified)
	blockHeight := uint64(1)
	return s.runtime.PutPrivateData(collection, key, value, s.txID, blockHeight)
}

func (s *ChaincodeStub) DelPrivateData(collection, key string) error {
	return s.PutPrivateData(collection, key, []byte{})
}

func (s *ChaincodeStub) GetPrivateDataByRange(collection, startKey, endKey string) (StateQueryIteratorInterface, error) {
	return NewSimplePrivateDataIterator(s, collection, startKey, endKey), nil
}

func (s *ChaincodeStub) GetPrivateDataByPartialCompositeKey(collection, objectType string, keys []string) (StateQueryIteratorInterface, error) {
	compositePrefix, err := s.CreateCompositeKey(objectType, keys)
	if err != nil {
		return nil, err
	}
	return s.GetPrivateDataByRange(collection, compositePrefix, compositePrefix+"\xff")
}

func (s *ChaincodeStub) GetPrivateDataHash(collection, key string) ([]byte, error) {
	hash, err := s.runtime.privateDataMgr.GetPrivateDataHash(collection, key)
	if err != nil {
		return nil, err
	}
	return []byte(hash), nil
}

// Transaction context implementation

func (s *ChaincodeStub) GetTxID() string {
	return s.txID
}

func (s *ChaincodeStub) GetChannelID() string {
	return s.channelID
}

func (s *ChaincodeStub) GetMSPID() string {
	return "DiamanteMSP"
}

func (s *ChaincodeStub) GetCreator() ([]byte, error) {
	if s.creator == nil {
		// Default creator certificate
		s.creator = []byte(`{"mspid":"DiamanteMSP","id":"default_user"}`)
	}
	return s.creator, nil
}

func (s *ChaincodeStub) GetSignedProposal() (*SignedProposal, error) {
	if s.signedProposal == nil {
		s.signedProposal = &SignedProposal{
			ProposalBytes: []byte("mock_proposal"),
			Signature:     []byte("mock_signature"),
		}
	}
	return s.signedProposal, nil
}

func (s *ChaincodeStub) GetTxTimestamp() (*Timestamp, error) {
	return s.timestamp, nil
}

func (s *ChaincodeStub) GetBinding() ([]byte, error) {
	return []byte("mock_binding"), nil
}

func (s *ChaincodeStub) GetTransient() (map[string][]byte, error) {
	if s.transientData == nil {
		s.transientData = make(map[string][]byte)
	}
	return s.transientData, nil
}

// Function and parameter access implementation

func (s *ChaincodeStub) GetFunctionAndParameters() (string, []string) {
	return s.function, s.args
}

func (s *ChaincodeStub) GetStringArgs() []string {
	args := make([]string, len(s.args)+1)
	args[0] = s.function
	copy(args[1:], s.args)
	return args
}

func (s *ChaincodeStub) GetArgs() [][]byte {
	args := make([][]byte, len(s.args)+1)
	args[0] = []byte(s.function)
	for i, arg := range s.args {
		args[i+1] = []byte(arg)
	}
	return args
}

// Chaincode invocation implementation

func (s *ChaincodeStub) InvokeChaincode(chaincodeName string, args [][]byte, channel string) Response {
	// Convert args to strings
	stringArgs := make([]string, len(args))
	for i, arg := range args {
		stringArgs[i] = string(arg)
	}

	// Convert string args to ContractParameters
	params := runtime.ContractParameters{
		StringParams: make(map[string]string),
	}
	for i, arg := range stringArgs[1:] {
		params.StringParams[fmt.Sprintf("arg%d", i)] = arg
	}

	// Create chaincode call
	call := runtime.ContractCall{
		ContractID: chaincodeName,
		Function:   stringArgs[0],
		Args:       params,
		Caller:     s.chaincodeID,
		// Note: TxID is not in ContractCall anymore
	}

	// Execute chaincode
	result, err := s.runtime.Execute(context.Background(), call)
	if err != nil {
		return Error(fmt.Sprintf("Failed to invoke chaincode: %v", err))
	}

	// Return raw data as Success expects []byte
	return Success(result.RawReturnData)
}

// Event management implementation

func (s *ChaincodeStub) SetEvent(name string, payload []byte) error {
	event := &ChaincodeEvent{
		EventName: name,
		Payload:   payload,
		TxID:      s.txID,
	}
	s.events = append(s.events, event)

	s.logger.WithFields(logrus.Fields{
		"event":   name,
		"tx_id":   s.txID,
		"payload": len(payload),
	}).Debug("Set chaincode event")

	return nil
}

// History and query implementation

func (s *ChaincodeStub) GetHistoryForKey(key string) (HistoryQueryIteratorInterface, error) {
	return NewSimpleHistoryIterator(s, key), nil
}

func (s *ChaincodeStub) GetQueryResult(query string) (StateQueryIteratorInterface, error) {
	return NewSimpleQueryIterator(s, query), nil
}

func (s *ChaincodeStub) GetPrivateDataQueryResult(collection, query string) (StateQueryIteratorInterface, error) {
	return NewSimplePrivateQueryIterator(s, collection, query), nil
}

// Simple iterator implementations

type SimpleStateIterator struct {
	stub     *ChaincodeStub
	startKey string
	endKey   string
	current  int
	results  []*QueryResponse
}

func NewSimpleStateIterator(stub *ChaincodeStub, startKey, endKey string) *SimpleStateIterator {
	return &SimpleStateIterator{
		stub:     stub,
		startKey: startKey,
		endKey:   endKey,
		current:  0,
		results:  []*QueryResponse{}, // Would be populated with actual range query
	}
}

func (iter *SimpleStateIterator) HasNext() bool {
	return iter.current < len(iter.results)
}

func (iter *SimpleStateIterator) Next() (*QueryResponse, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more results")
	}
	result := iter.results[iter.current]
	iter.current++
	return result, nil
}

func (iter *SimpleStateIterator) Close() error {
	return nil
}

type SimplePrivateDataIterator struct {
	stub       *ChaincodeStub
	collection string
	startKey   string
	endKey     string
	current    int
	results    []*QueryResponse
}

func NewSimplePrivateDataIterator(stub *ChaincodeStub, collection, startKey, endKey string) *SimplePrivateDataIterator {
	return &SimplePrivateDataIterator{
		stub:       stub,
		collection: collection,
		startKey:   startKey,
		endKey:     endKey,
		current:    0,
		results:    []*QueryResponse{}, // Would be populated with actual query
	}
}

func (iter *SimplePrivateDataIterator) HasNext() bool {
	return iter.current < len(iter.results)
}

func (iter *SimplePrivateDataIterator) Next() (*QueryResponse, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more results")
	}
	result := iter.results[iter.current]
	iter.current++
	return result, nil
}

func (iter *SimplePrivateDataIterator) Close() error {
	return nil
}

type SimpleHistoryIterator struct {
	stub    *ChaincodeStub
	key     string
	current int
	results []*KeyModification
}

func NewSimpleHistoryIterator(stub *ChaincodeStub, key string) *SimpleHistoryIterator {
	return &SimpleHistoryIterator{
		stub:    stub,
		key:     key,
		current: 0,
		results: []*KeyModification{}, // Would be populated with actual history
	}
}

func (iter *SimpleHistoryIterator) HasNext() bool {
	return iter.current < len(iter.results)
}

func (iter *SimpleHistoryIterator) Next() (*KeyModification, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more results")
	}
	result := iter.results[iter.current]
	iter.current++
	return result, nil
}

func (iter *SimpleHistoryIterator) Close() error {
	return nil
}

type SimpleQueryIterator struct {
	stub    *ChaincodeStub
	query   string
	current int
	results []*QueryResponse
}

func NewSimpleQueryIterator(stub *ChaincodeStub, query string) *SimpleQueryIterator {
	return &SimpleQueryIterator{
		stub:    stub,
		query:   query,
		current: 0,
		results: []*QueryResponse{}, // Would be populated with actual query results
	}
}

func (iter *SimpleQueryIterator) HasNext() bool {
	return iter.current < len(iter.results)
}

func (iter *SimpleQueryIterator) Next() (*QueryResponse, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more results")
	}
	result := iter.results[iter.current]
	iter.current++
	return result, nil
}

func (iter *SimpleQueryIterator) Close() error {
	return nil
}

type SimplePrivateQueryIterator struct {
	stub       *ChaincodeStub
	collection string
	query      string
	current    int
	results    []*QueryResponse
}

func NewSimplePrivateQueryIterator(stub *ChaincodeStub, collection, query string) *SimplePrivateQueryIterator {
	return &SimplePrivateQueryIterator{
		stub:       stub,
		collection: collection,
		query:      query,
		current:    0,
		results:    []*QueryResponse{}, // Would be populated with actual query results
	}
}

func (iter *SimplePrivateQueryIterator) HasNext() bool {
	return iter.current < len(iter.results)
}

func (iter *SimplePrivateQueryIterator) Next() (*QueryResponse, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more results")
	}
	result := iter.results[iter.current]
	iter.current++
	return result, nil
}

func (iter *SimplePrivateQueryIterator) Close() error {
	return nil
}

// ledger/evm/events.go

package evm

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sync"

	"diamante/consensus"
	"diamante/storage"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// EventLog represents an EVM event log
type EventLog struct {
	// Address of the contract that generated the log
	Address ethcommon.Address `json:"address"`

	// Topics are indexed parameters of the event
	Topics []ethcommon.Hash `json:"topics"`

	// Data contains non-indexed parameters
	Data []byte `json:"data"`

	// Block number where this log was included
	BlockNumber uint64 `json:"blockNumber"`

	// Transaction hash that generated this log
	TxHash ethcommon.Hash `json:"transactionHash"`

	// Index of the log in the transaction
	LogIndex uint `json:"logIndex"`

	// Index of the transaction in the block
	TxIndex uint `json:"transactionIndex"`

	// Whether the log was removed due to a chain reorganization
	Removed bool `json:"removed"`
}

// EventFilter represents a filter for querying events
type EventFilter struct {
	// Block range
	FromBlock *big.Int `json:"fromBlock,omitempty"`
	ToBlock   *big.Int `json:"toBlock,omitempty"`

	// Contract addresses to filter by
	Addresses []ethcommon.Address `json:"addresses,omitempty"`

	// Topics to filter by (each element can be nil, a single topic, or a list of topics)
	Topics [][]ethcommon.Hash `json:"topics,omitempty"`

	// Limit the number of results
	Limit int `json:"limit,omitempty"`
}

// EventManager manages EVM events
type EventManager struct {
	logs []EventLog
	mu   sync.RWMutex
}

// NewEventManager creates a new event manager
func NewEventManager() *EventManager {
	return &EventManager{
		logs: make([]EventLog, 0),
	}
}

// AddLog adds a new event log
func (em *EventManager) AddLog(log EventLog) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.logs = append(em.logs, log)
}

// GetLogs returns all logs that match the given filter
func (em *EventManager) GetLogs(filter EventFilter) []EventLog {
	var result []EventLog

	for _, log := range em.logs {
		if em.matchesFilter(log, filter) {
			result = append(result, log)

			// Apply limit if specified
			if filter.Limit > 0 && len(result) >= filter.Limit {
				break
			}
		}
	}

	return result
}

// GetLogsByTxHash returns all logs for a specific transaction
func (em *EventManager) GetLogsByTxHash(txHash ethcommon.Hash) []EventLog {
	var result []EventLog

	for _, log := range em.logs {
		if log.TxHash == txHash {
			result = append(result, log)
		}
	}

	return result
}

// GetLogsByAddress returns all logs for a specific contract address
func (em *EventManager) GetLogsByAddress(address ethcommon.Address) []EventLog {
	var result []EventLog

	for _, log := range em.logs {
		if log.Address == address {
			result = append(result, log)
		}
	}

	return result
}

// GetLogsByBlockRange returns all logs within a block range
func (em *EventManager) GetLogsByBlockRange(fromBlock, toBlock uint64) []EventLog {
	var result []EventLog

	for _, log := range em.logs {
		if log.BlockNumber >= fromBlock && log.BlockNumber <= toBlock {
			result = append(result, log)
		}
	}

	return result
}

// matchesFilter checks if a log matches the given filter
func (em *EventManager) matchesFilter(log EventLog, filter EventFilter) bool {
	// Check block range
	if filter.FromBlock != nil && log.BlockNumber < filter.FromBlock.Uint64() {
		return false
	}
	if filter.ToBlock != nil && log.BlockNumber > filter.ToBlock.Uint64() {
		return false
	}

	// Check addresses
	if len(filter.Addresses) > 0 {
		addressMatch := false
		for _, addr := range filter.Addresses {
			if log.Address == addr {
				addressMatch = true
				break
			}
		}
		if !addressMatch {
			return false
		}
	}

	// Check topics
	if len(filter.Topics) > 0 {
		for i, topicFilter := range filter.Topics {
			if i >= len(log.Topics) {
				return false
			}

			if len(topicFilter) == 0 {
				// Empty topic filter matches any topic
				continue
			}

			topicMatch := false
			for _, topic := range topicFilter {
				if log.Topics[i] == topic {
					topicMatch = true
					break
				}
			}
			if !topicMatch {
				return false
			}
		}
	}

	return true
}

// Clear removes all logs
func (em *EventManager) Clear() {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.logs = make([]EventLog, 0)
}

// Count returns the number of logs
func (em *EventManager) Count() int {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return len(em.logs)
}

// Persist writes all collected logs to the provided ledger store as a receipt.
func (em *EventManager) Persist(store storage.LedgerStore, txID string, blockHeight uint64, blockHash string, status bool, gasUsed uint64) error {
	em.mu.RLock()
	defer em.mu.RUnlock()

	if store == nil {
		return storage.ErrInvalidData
	}

	receipt := &storage.Receipt{
		TxID:        txID,
		BlockHeight: blockHeight,
		BlockHash:   blockHash,
		Status:      status,
		GasUsed:     gasUsed,
		Logs:        make([]storage.EventLog, len(em.logs)),
		Metadata: storage.ReceiptMetadata{
			CumulativeGasUsed: gasUsed,
			Type:              "evm_transaction",
		},
		CreatedAt: consensus.ConsensusNow(),
	}

	for i, log := range em.logs {
		topics := make([]string, len(log.Topics))
		for j, t := range log.Topics {
			topics[j] = t.Hex()
		}
		receipt.Logs[i] = storage.EventLog{
			Address:     log.Address.Hex(),
			Topics:      topics,
			Data:        log.Data,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash.Hex(),
			Index:       log.LogIndex,
		}
	}

	return store.SaveReceipt(receipt)
}

// LoadByTx retrieves logs for a transaction from the ledger store.
func LoadByTx(store storage.LedgerStore, txID string) ([]EventLog, error) {
	receipt, err := store.GetReceipt(txID)
	if err != nil {
		return nil, err
	}
	logs := make([]EventLog, len(receipt.Logs))
	for i, rlog := range receipt.Logs {
		topics := make([]ethcommon.Hash, len(rlog.Topics))
		for j, t := range rlog.Topics {
			topics[j] = ethcommon.HexToHash(t)
		}
		logs[i] = EventLog{
			Address:     ethcommon.HexToAddress(rlog.Address),
			Topics:      topics,
			Data:        rlog.Data,
			BlockNumber: rlog.BlockNumber,
			TxHash:      ethcommon.HexToHash(rlog.TxHash),
			LogIndex:    rlog.Index,
			TxIndex:     rlog.Index,
		}
	}
	return logs, nil
}

// EventBuilder helps build event logs
type EventBuilder struct {
	address     ethcommon.Address
	blockNumber uint64
	txHash      ethcommon.Hash
	txIndex     uint
	logIndex    uint
}

// NewEventBuilder creates a new event builder
func NewEventBuilder(address ethcommon.Address, blockNumber uint64, txHash ethcommon.Hash, txIndex uint) *EventBuilder {
	return &EventBuilder{
		address:     address,
		blockNumber: blockNumber,
		txHash:      txHash,
		txIndex:     txIndex,
		logIndex:    0,
	}
}

// BuildLog creates an event log with the given topics and data
func (eb *EventBuilder) BuildLog(topics []ethcommon.Hash, data []byte) EventLog {
	log := EventLog{
		Address:     eb.address,
		Topics:      topics,
		Data:        data,
		BlockNumber: eb.blockNumber,
		TxHash:      eb.txHash,
		LogIndex:    eb.logIndex,
		TxIndex:     eb.txIndex,
		Removed:     false,
	}

	eb.logIndex++
	return log
}

// BuildTransferLog creates a Transfer event log (ERC-20 standard)
func (eb *EventBuilder) BuildTransferLog(from, to ethcommon.Address, amount *big.Int) EventLog {
	// Transfer event signature: Transfer(address,address,uint256)
	transferSig := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

	topics := []ethcommon.Hash{
		transferSig,
		ethcommon.BytesToHash(from.Bytes()),
		ethcommon.BytesToHash(to.Bytes()),
	}

	// Amount is stored in data (non-indexed)
	data := ethcommon.LeftPadBytes(amount.Bytes(), 32)

	return eb.BuildLog(topics, data)
}

// BuildApprovalLog creates an Approval event log (ERC-20 standard)
func (eb *EventBuilder) BuildApprovalLog(owner, spender ethcommon.Address, amount *big.Int) EventLog {
	// Approval event signature: Approval(address,address,uint256)
	approvalSig := crypto.Keccak256Hash([]byte("Approval(address,address,uint256)"))

	topics := []ethcommon.Hash{
		approvalSig,
		ethcommon.BytesToHash(owner.Bytes()),
		ethcommon.BytesToHash(spender.Bytes()),
	}

	// Amount is stored in data (non-indexed)
	data := ethcommon.LeftPadBytes(amount.Bytes(), 32)

	return eb.BuildLog(topics, data)
}

// BuildCustomLog creates a custom event log with string signature
func (eb *EventBuilder) BuildCustomLog(signature string, indexedParams []ethcommon.Hash, data []byte) EventLog {
	// Calculate event signature hash
	eventSig := crypto.Keccak256Hash([]byte(signature))

	// Build topics (signature + indexed parameters)
	topics := []ethcommon.Hash{eventSig}
	topics = append(topics, indexedParams...)

	return eb.BuildLog(topics, data)
}

// EventDecoder helps decode event logs
type EventDecoder struct{}

// NewEventDecoder creates a new event decoder
func NewEventDecoder() *EventDecoder {
	return &EventDecoder{}
}

// DecodeTransferLog decodes a Transfer event log
func (ed *EventDecoder) DecodeTransferLog(log EventLog) (from, to ethcommon.Address, amount *big.Int, err error) {
	if len(log.Topics) < 3 {
		return ethcommon.Address{}, ethcommon.Address{}, nil, fmt.Errorf("invalid Transfer log: insufficient topics")
	}

	// Check if this is a Transfer event
	transferSig := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	if log.Topics[0] != transferSig {
		return ethcommon.Address{}, ethcommon.Address{}, nil, fmt.Errorf("not a Transfer event")
	}

	from = ethcommon.BytesToAddress(log.Topics[1].Bytes())
	to = ethcommon.BytesToAddress(log.Topics[2].Bytes())
	amount = new(big.Int).SetBytes(log.Data)

	return from, to, amount, nil
}

// DecodeApprovalLog decodes an Approval event log
func (ed *EventDecoder) DecodeApprovalLog(log EventLog) (owner, spender ethcommon.Address, amount *big.Int, err error) {
	if len(log.Topics) < 3 {
		return ethcommon.Address{}, ethcommon.Address{}, nil, fmt.Errorf("invalid Approval log: insufficient topics")
	}

	// Check if this is an Approval event
	approvalSig := crypto.Keccak256Hash([]byte("Approval(address,address,uint256)"))
	if log.Topics[0] != approvalSig {
		return ethcommon.Address{}, ethcommon.Address{}, nil, fmt.Errorf("not an Approval event")
	}

	owner = ethcommon.BytesToAddress(log.Topics[1].Bytes())
	spender = ethcommon.BytesToAddress(log.Topics[2].Bytes())
	amount = new(big.Int).SetBytes(log.Data)

	return owner, spender, amount, nil
}

// ToJSON converts an event log to JSON
func (log *EventLog) ToJSON() ([]byte, error) {
	return json.Marshal(log)
}

// FromJSON creates an event log from JSON
func EventLogFromJSON(data []byte) (*EventLog, error) {
	var log EventLog
	err := json.Unmarshal(data, &log)
	if err != nil {
		return nil, err
	}
	return &log, nil
}

// String returns a string representation of the event log
func (log *EventLog) String() string {
	return fmt.Sprintf("EventLog{Address: %s, Topics: %v, BlockNumber: %d, TxHash: %s}",
		log.Address.Hex(), log.Topics, log.BlockNumber, log.TxHash.Hex())
}

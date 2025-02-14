// consensus/types/types.go

package types

import (
	"time"
)

type Event struct {
	ID        [32]byte
	Creator   [32]byte
	ParentIDs [][32]byte
	Data      []byte
	Timestamp time.Time
	Height    uint64
	Finalized bool
	PoHState  [32]byte
	PoHCount  uint64
	PoHProof  [32]byte
}

// Lachesis interface represents the Lachesis consensus algorithm
type Lachesis interface {
	AddNode(id [32]byte, stake uint64)
	UpdateNodeStake(id [32]byte, newStake uint64)
	CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *Event
	ProcessEvent(event *Event) bool
	GetNetworkLoad() float64
	GetGossipDelay() time.Duration
	SetGossipDelay(delay time.Duration)
	AdjustNetworkLoad(adjustment float64)
	GetVotingThreshold() float64
	SetVotingThreshold(threshold float64)
	GetState() ([]byte, error)
	RestoreState(state []byte) error
	GetFinalizedEvents(fromHeight, toHeight uint64) ([]*Event, error)
	// Removed Start() and Stop() methods if they are not implemented
}

// DPoS interface represents the Delegated Proof of Stake consensus algorithm
type DPoS interface {
	AddValidator(id [32]byte, stake uint64)
	UpdateStake(id [32]byte, newStake uint64)
	IsActiveValidator(id [32]byte) bool
	GetValidators() []*Validator
	GetActiveValidators() []*Validator
	GetTotalStake() uint64
	GetValidatorStake(validatorID [32]byte) uint64
	GetSetSize() int
	SetSetSize(size int)
	GetEpochDuration() uint64
	SetEpochDuration(duration uint64)
	GetNextValidator(blockNumber uint64, lastBlockHash [32]byte) *Validator // Updated signature
	ProcessEpoch(blockNumber uint64) error
	RewardValidator(id [32]byte)
	GetState() ([]byte, error)
	RestoreState(stateData []byte) error
	// Removed Start() and Stop() methods as they are no longer present
}

// PoH interface represents the Proof of History component
type PoH interface {
	Record(data []byte) [32]byte
	Verify(prevState [32]byte, data []byte, proof [32]byte, count uint64) bool
	GetState() [32]byte
	GetCount() uint64
	GetTickDelay() time.Duration
	SetTickDelay(delay time.Duration) error
	Tick()
	Synchronize(targetState [32]byte, targetCount uint64) error
	AdvanceState(iterations uint64)
	GenerateProof(data []byte, iterations uint64) ([32]byte, [32]byte, uint64, error)
	VerifyProof(startState [32]byte, data []byte, proof [32]byte, startCount, iterations uint64) (bool, error)
	EstimateTimeToCount(targetCount uint64) time.Duration
	VerifyHashRange(startState [32]byte, startCount uint64, hashes [][32]byte) bool
	// Removed Start() and Stop() methods if they are not implemented
}

type Validator struct {
	ID    [32]byte
	Stake uint64
}

// Consensus interface represents the overall consensus mechanism
type Consensus interface {
	GetNetworkLoad() float64
	GetLachesis() Lachesis
	GetDPoS() DPoS
	GetPoH() PoH
	Start() error
	Stop() error
	ProcessBlock(blockNumber uint64) error
	CreateEvent(creator [32]byte, parentIDs [][32]byte, data []byte) *Event
	FinalizeEvent(event *Event) (bool, error)
	SynchronizeState(targetState [32]byte, targetCount uint64) error
	GetValidators() []*Validator
	GetActiveValidators() []*Validator
	GetPendingEvents() []*Event
	GetFinalizedEvents(fromHeight, toHeight uint64) ([]*Event, error)
}

// Add this type to represent the full state structure
type lachesisTestState struct {
	DAGState        []byte              `json:"DAGState"`
	GossipState     []byte              `json:"GossipState"`
	VotingState     []byte              `json:"VotingState"`
	FinalizerState  []byte              `json:"FinalizerState"`
	FinalizedEvents map[uint64][]string `json:"FinalizedEvents"`
}

type dagTestState struct {
	Events    map[string]struct{} `json:"events"`
	Nodes     map[string]struct{} `json:"nodes"`
	MaxHeight uint64              `json:"max_height"`
}

// --------------------------------------------------------------------
// ADD/UPDATE THESE DEFINITIONS TO FIX LEDGER REFERENCES
// --------------------------------------------------------------------

// Transaction now includes Sender, Receiver, and Amount to match your ledger logic.
type Transaction struct {
	ID       string `json:"id"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Amount   uint64 `json:"amount"`
	// Add more fields (e.g. Fee, Nonce) if needed by your code
}

// Block references the above Transaction struct.
type Block struct {
	Number       uint64        `json:"number"`
	Transactions []Transaction `json:"transactions"`
}

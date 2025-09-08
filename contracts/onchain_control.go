package contracts

import (
	"encoding/hex"
	"fmt"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

// OnChainController handles actual on-chain contract control operations
type OnChainController struct {
	evmExecutor EVMExecutor
	logger      *logrus.Logger
}

// NewOnChainController creates a new on-chain controller
func NewOnChainController(evmExecutor EVMExecutor, logger *logrus.Logger) *OnChainController {
	if logger == nil {
		logger = logrus.New()
	}
	return &OnChainController{
		evmExecutor: evmExecutor,
		logger:      logger,
	}
}

// PauseContract calls the pause function on a contract
func (oc *OnChainController) PauseContract(contractID string, pauser string) error {
	if contractID == "" || pauser == "" {
		return fmt.Errorf("contract ID and pauser cannot be empty")
	}

	contractAddr := ethcommon.HexToAddress(contractID)
	pauserAddr := ethcommon.HexToAddress(pauser)

	// Check if contract supports ERC-165 and Pausable interface
	isPausable, err := oc.checkPausableInterface(contractAddr)
	if err != nil {
		return fmt.Errorf("failed to check pausable interface: %w", err)
	}

	if !isPausable {
		return fmt.Errorf("contract does not implement pausable interface")
	}

	// Encode the pause() function call
	pauseData := oc.encodeFunctionCall("pause()")

	// Call the pause function
	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		result, err := adapter.CallContract(pauserAddr, contractAddr, pauseData, 100000)
		if err != nil {
			return fmt.Errorf("failed to call pause function: %w", err)
		}

		oc.logger.WithFields(logrus.Fields{
			"contract": contractID,
			"pauser":   pauser,
			"result":   hex.EncodeToString(result),
		}).Info("Contract paused on-chain")
	}

	return nil
}

// ResumeContract calls the unpause/resume function on a contract
func (oc *OnChainController) ResumeContract(contractID string, resumer string) error {
	if contractID == "" || resumer == "" {
		return fmt.Errorf("contract ID and resumer cannot be empty")
	}

	contractAddr := ethcommon.HexToAddress(contractID)
	resumerAddr := ethcommon.HexToAddress(resumer)

	// Check if contract is paused
	isPaused, err := oc.checkIfPaused(contractAddr)
	if err != nil {
		return fmt.Errorf("failed to check pause status: %w", err)
	}

	if !isPaused {
		return fmt.Errorf("contract is not paused")
	}

	// Encode the unpause() function call
	unpauseData := oc.encodeFunctionCall("unpause()")

	// Call the unpause function
	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		result, err := adapter.CallContract(resumerAddr, contractAddr, unpauseData, 100000)
		if err != nil {
			return fmt.Errorf("failed to call unpause function: %w", err)
		}

		oc.logger.WithFields(logrus.Fields{
			"contract": contractID,
			"resumer":  resumer,
			"result":   hex.EncodeToString(result),
		}).Info("Contract resumed on-chain")
	}

	return nil
}

// DestroyContract calls the self-destruct function on a contract
func (oc *OnChainController) DestroyContract(contractID string, destroyer string) error {
	if contractID == "" || destroyer == "" {
		return fmt.Errorf("contract ID and destroyer cannot be empty")
	}

	contractAddr := ethcommon.HexToAddress(contractID)
	destroyerAddr := ethcommon.HexToAddress(destroyer)

	// Check if contract supports destruction
	isDestructible, err := oc.checkDestructibleInterface(contractAddr)
	if err != nil {
		return fmt.Errorf("failed to check destructible interface: %w", err)
	}

	if !isDestructible {
		return fmt.Errorf("contract does not implement destructible interface")
	}

	// Encode the destroy() function call
	destroyData := oc.encodeFunctionCall("destroy()")

	// Call the destroy function
	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		result, err := adapter.CallContract(destroyerAddr, contractAddr, destroyData, 500000)
		if err != nil {
			return fmt.Errorf("failed to call destroy function: %w", err)
		}

		oc.logger.WithFields(logrus.Fields{
			"contract":  contractID,
			"destroyer": destroyer,
			"result":    hex.EncodeToString(result),
		}).Info("Contract destroyed on-chain")
	}

	return nil
}

// checkPausableInterface checks if a contract implements the Pausable interface using ERC-165
func (oc *OnChainController) checkPausableInterface(addr ethcommon.Address) (bool, error) {
	// ERC-165 interface ID for Pausable
	// bytes4(keccak256('pause()')) ^ bytes4(keccak256('unpause()')) ^ bytes4(keccak256('paused()'))
	pausableInterfaceID := [4]byte{0x36, 0x61, 0xea, 0x85}

	// Check if contract supports ERC-165
	supportsERC165, err := oc.supportsInterface(addr, [4]byte{0x01, 0xff, 0xc9, 0xa7})
	if err != nil || !supportsERC165 {
		// Contract doesn't support ERC-165, check for pause function directly
		return oc.hasPauseFunction(addr)
	}

	// Check if contract supports Pausable interface
	return oc.supportsInterface(addr, pausableInterfaceID)
}

// checkDestructibleInterface checks if a contract implements the Destructible interface
func (oc *OnChainController) checkDestructibleInterface(addr ethcommon.Address) (bool, error) {
	// Check for common destroy/kill function signatures
	destroyFuncSig := crypto.Keccak256([]byte("destroy()"))[:4]
	killFuncSig := crypto.Keccak256([]byte("kill()"))[:4]

	// Try to get the code and check for SELFDESTRUCT opcode
	code, err := oc.evmExecutor.GetCode(addr)
	if err != nil {
		return false, fmt.Errorf("failed to get contract code: %w", err)
	}

	// Check if code contains SELFDESTRUCT opcode (0xff)
	for i := 0; i < len(code); i++ {
		if code[i] == 0xff {
			return true, nil
		}
	}

	// Check for destroy function signature in code
	destroySig := hex.EncodeToString(destroyFuncSig)
	killSig := hex.EncodeToString(killFuncSig)
	codeHex := hex.EncodeToString(code)

	return contains(codeHex, destroySig) || contains(codeHex, killSig), nil
}

// supportsInterface checks if a contract supports a specific interface using ERC-165
func (oc *OnChainController) supportsInterface(addr ethcommon.Address, interfaceID [4]byte) (bool, error) {
	// Encode supportsInterface(bytes4) call
	data := append(crypto.Keccak256([]byte("supportsInterface(bytes4)"))[:4], interfaceID[:]...)

	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		// Use a dummy caller for view functions
		dummyCaller := ethcommon.HexToAddress("0x0000000000000000000000000000000000000000")
		result, err := adapter.CallContract(dummyCaller, addr, data, 50000)
		if err != nil {
			return false, err
		}

		// Check if result is true (non-zero)
		if len(result) >= 32 {
			return new(big.Int).SetBytes(result[len(result)-32:]).Cmp(big.NewInt(0)) != 0, nil
		}
	}

	return false, fmt.Errorf("unable to check interface support")
}

// hasPauseFunction checks if a contract has a pause function
func (oc *OnChainController) hasPauseFunction(addr ethcommon.Address) (bool, error) {
	// Try to call paused() view function
	pausedData := crypto.Keccak256([]byte("paused()"))[:4]

	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		dummyCaller := ethcommon.HexToAddress("0x0000000000000000000000000000000000000000")
		_, err := adapter.CallContract(dummyCaller, addr, pausedData, 50000)
		// If call succeeds, contract likely has pause functionality
		return err == nil, nil
	}

	return false, nil
}

// checkIfPaused checks if a contract is currently paused
func (oc *OnChainController) checkIfPaused(addr ethcommon.Address) (bool, error) {
	// Encode paused() view function call
	pausedData := crypto.Keccak256([]byte("paused()"))[:4]

	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		dummyCaller := ethcommon.HexToAddress("0x0000000000000000000000000000000000000000")
		result, err := adapter.CallContract(dummyCaller, addr, pausedData, 50000)
		if err != nil {
			return false, fmt.Errorf("failed to check pause status: %w", err)
		}

		// Parse boolean result
		if len(result) >= 32 {
			return new(big.Int).SetBytes(result[len(result)-32:]).Cmp(big.NewInt(0)) != 0, nil
		}
	}

	return false, fmt.Errorf("unable to check pause status")
}

// encodeFunctionCall encodes a simple function call without parameters
func (oc *OnChainController) encodeFunctionCall(signature string) []byte {
	return crypto.Keccak256([]byte(signature))[:4]
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsAt(s, substr) >= 0
}

// containsAt finds the index of substr in s, or -1 if not found
func containsAt(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// ContractControlInterface defines standard control functions for smart contracts
type ContractControlInterface struct {
	// Pausable interface
	Pause    string `json:"pause"`
	Unpause  string `json:"unpause"`
	IsPaused string `json:"isPaused"`

	// Ownable interface
	Owner             string `json:"owner"`
	TransferOwnership string `json:"transferOwnership"`
	RenounceOwnership string `json:"renounceOwnership"`

	// Destructible interface
	Destroy        string `json:"destroy"`
	DestroyAndSend string `json:"destroyAndSend"`
	EmergencyPause string `json:"emergencyPause"`
}

// StandardInterfaces provides common interface definitions
var StandardInterfaces = map[string]ContractControlInterface{
	"OpenZeppelin": {
		Pause:             "pause()",
		Unpause:           "unpause()",
		IsPaused:          "paused()",
		Owner:             "owner()",
		TransferOwnership: "transferOwnership(address)",
		RenounceOwnership: "renounceOwnership()",
		Destroy:           "destroy()",
		DestroyAndSend:    "destroyAndSend(address)",
		EmergencyPause:    "pause()",
	},
	"Custom": {
		Pause:             "pauseContract()",
		Unpause:           "resumeContract()",
		IsPaused:          "isContractPaused()",
		Owner:             "getOwner()",
		TransferOwnership: "changeOwner(address)",
		RenounceOwnership: "removeOwner()",
		Destroy:           "kill()",
		DestroyAndSend:    "killAndWithdraw(address)",
		EmergencyPause:    "emergencyStop()",
	},
}

// GetInterfaceForContract determines which interface a contract uses
func (oc *OnChainController) GetInterfaceForContract(addr ethcommon.Address) (string, error) {
	// Try OpenZeppelin interface first
	if oc.tryInterface(addr, StandardInterfaces["OpenZeppelin"]) {
		return "OpenZeppelin", nil
	}

	// Try custom interface
	if oc.tryInterface(addr, StandardInterfaces["Custom"]) {
		return "Custom", nil
	}

	return "", fmt.Errorf("contract does not match any known interface")
}

// tryInterface tests if a contract responds to a specific interface
func (oc *OnChainController) tryInterface(addr ethcommon.Address, iface ContractControlInterface) bool {
	// Try to call the owner function as a basic test
	ownerData := crypto.Keccak256([]byte(iface.Owner))[:4]

	if adapter, ok := oc.evmExecutor.(*EVMExecutorAdapter); ok {
		dummyCaller := ethcommon.HexToAddress("0x0000000000000000000000000000000000000000")
		_, err := adapter.CallContract(dummyCaller, addr, ownerData, 50000)
		return err == nil
	}

	return false
}

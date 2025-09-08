// ledger/evm/precompiled.go

package evm

import (
	"crypto/sha256"
	"errors"
	"math/big"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/ripemd160"
)

// PrecompiledContract is the interface for native Go implementations of Ethereum precompiled contracts
type PrecompiledContract interface {
	// RequiredGas returns the gas required to execute the precompiled contract
	RequiredGas(input []byte) uint64
	// Run runs the precompiled contract
	Run(input []byte) ([]byte, error)
}

// PrecompiledContractsDiamante contains the set of pre-compiled Ethereum contracts used in Diamante
var PrecompiledContractsDiamante = map[ethcommon.Address]PrecompiledContract{
	ethcommon.BytesToAddress([]byte{1}): &ecrecover{},
	ethcommon.BytesToAddress([]byte{2}): &sha256hash{},
	ethcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	ethcommon.BytesToAddress([]byte{4}): &dataCopy{},
}

// Gas costs for precompiled contracts
const (
	EcrecoverGas        = 3000
	Sha256BaseGas       = 60
	Sha256PerWordGas    = 12
	Ripemd160BaseGas    = 600
	Ripemd160PerWordGas = 120
	IdentityBaseGas     = 15
	IdentityPerWordGas  = 3
)

// ecrecover implements the ECDSA public key recovery from signature
type ecrecover struct{}

func (c *ecrecover) RequiredGas(input []byte) uint64 {
	return EcrecoverGas
}

func (c *ecrecover) Run(input []byte) ([]byte, error) {
	const ecRecoverInputLength = 128

	input = ethcommon.RightPadBytes(input, ecRecoverInputLength)
	// "input" is (hash, v, r, s), each 32 bytes
	// but for ecrecover we want (r, s, v)

	r := new(big.Int).SetBytes(input[64:96])
	s := new(big.Int).SetBytes(input[96:128])
	v := input[63] // the signature is in the last byte of the 32-byte v value
	if v != 27 && v != 28 {
		v += 27 // Ethereum adds 27 to v
	}

	// We need to convert from Ethereum signature format to go-ethereum signature format
	sig := make([]byte, 65)
	copy(sig[32-len(r.Bytes()):32], r.Bytes())
	copy(sig[64-len(s.Bytes()):64], s.Bytes())
	sig[64] = v

	// Recover the public key
	hash := input[:32]
	pub, err := crypto.Ecrecover(hash, sig)
	if err != nil {
		return nil, err
	}

	// Convert the public key to an Ethereum address
	if len(pub) == 0 || pub[0] != 4 {
		return nil, errors.New("invalid public key")
	}
	result := crypto.Keccak256(pub[1:])
	return ethcommon.LeftPadBytes(result[12:], 32), nil
}

// sha256hash implements the SHA256 hashing function
type sha256hash struct{}

func (c *sha256hash) RequiredGas(input []byte) uint64 {
	return Sha256BaseGas + Sha256PerWordGas*uint64((len(input)+31)/32)
}

func (c *sha256hash) Run(input []byte) ([]byte, error) {
	h := sha256.Sum256(input)
	return h[:], nil
}

// ripemd160hash implements the RIPEMD160 hashing function
type ripemd160hash struct{}

func (c *ripemd160hash) RequiredGas(input []byte) uint64 {
	return Ripemd160BaseGas + Ripemd160PerWordGas*uint64((len(input)+31)/32)
}

func (c *ripemd160hash) Run(input []byte) ([]byte, error) {
	// RIPEMD-160 is required for Ethereum compatibility
	// Use the ripemd160 package from golang.org/x/crypto
	h := ripemd160.New()
	h.Write(input)
	sum := h.Sum(nil)
	return ethcommon.LeftPadBytes(sum, 32), nil
}

// dataCopy implements the data copy operation
type dataCopy struct{}

func (c *dataCopy) RequiredGas(input []byte) uint64 {
	return IdentityBaseGas + IdentityPerWordGas*uint64((len(input)+31)/32)
}

func (c *dataCopy) Run(input []byte) ([]byte, error) {
	return input, nil
}

// getData returns a slice from the input data with the given start and length
func getData(data []byte, start, length uint64) []byte {
	if start > uint64(len(data)) {
		start = uint64(len(data))
	}
	if start+length > uint64(len(data)) {
		length = uint64(len(data)) - start
	}
	return data[start : start+length]
}

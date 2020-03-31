package eth

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"

	"chainlink/core/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// WeiPerEth is amount of Wei currency units in one Eth.
var WeiPerEth = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// This data can contain anything and is submitted by user on-chain, so we must
// be extra careful how we interact with it
type UntrustedBytes []byte

//go:generate gencodec -type Log -field-override logMarshaling -out gen_log_json.go

// Log represents a contract log event. These events are generated by the LOG opcode and
// stored/indexed by the node.
// NOTE: This is almost (but not quite) a copy of go-ethereum/core/types.Log, in log.go
type Log struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address common.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []common.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data UntrustedBytes `json:"data" gencodec:"required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash common.Hash `json:"transactionHash"`
	// index of the transaction in the block
	TxIndex uint `json:"transactionIndex"`
	// hash of the block in which the transaction was included
	BlockHash common.Hash `json:"blockHash"`
	// index of the log in the receipt
	Index uint `json:"logIndex"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}

// GetTopic returns the hash for the topic at the passed index, or error.
func (log Log) GetTopic(idx uint) (common.Hash, error) {
	if len(log.Topics) <= int(idx) {
		return common.Hash{}, fmt.Errorf("Log: Unable to get topic #%v for %v", idx, log)
	}

	return log.Topics[idx], nil
}

// Copy creates a deep copy of a log.  The LogBroadcaster creates a single websocket
// subscription for all log events that we're interested in and distributes them to
// the relevant subscribers elsewhere in the codebase.  If a given log needs to be
// distributed to multiple subscribers while avoiding data races, it's necessary
// to make copies.
func (log Log) Copy() Log {
	var cpy Log
	cpy.Address = log.Address
	if log.Topics != nil {
		cpy.Topics = make([]common.Hash, len(log.Topics))
		copy(cpy.Topics, log.Topics)
	}
	if log.Data != nil {
		cpy.Data = make([]byte, len(log.Data))
		copy(cpy.Data, log.Data)
	}
	cpy.BlockNumber = log.BlockNumber
	cpy.TxHash = log.TxHash
	cpy.TxIndex = log.TxIndex
	cpy.BlockHash = log.BlockHash
	cpy.Index = log.Index
	cpy.Removed = log.Removed
	return cpy
}

// logMarshaling represents an ethereum event log.
//
// NOTE: If this is changed, gen_log_json.go must be changed accordingly. It was
// generated by the above "//go:generate gencodec" command, which is currently
// broken. (It seems as though the problem might be that gencodec doesn't work
// with modules-based packages, in which case it could probably be run outside
// chainlink. https://github.com/fjl/gencodec/issues/10)
type logMarshaling struct {
	Data        hexutil.Bytes
	BlockNumber hexutil.Uint64
	TxIndex     hexutil.Uint
	Index       hexutil.Uint
}

// BlockHeader represents a block header in the Ethereum blockchain.
// Deliberately does not have required fields because some fields aren't
// present depending on the Ethereum node.
// i.e. Parity does not always send mixHash
type BlockHeader struct {
	ParentHash  common.Hash      `json:"parentHash"`
	UncleHash   common.Hash      `json:"sha3Uncles"`
	Coinbase    common.Address   `json:"miner"`
	Root        common.Hash      `json:"stateRoot"`
	TxHash      common.Hash      `json:"transactionsRoot"`
	ReceiptHash common.Hash      `json:"receiptsRoot"`
	Bloom       types.Bloom      `json:"logsBloom"`
	Difficulty  hexutil.Big      `json:"difficulty"`
	Number      hexutil.Big      `json:"number"`
	GasLimit    hexutil.Uint64   `json:"gasLimit"`
	GasUsed     hexutil.Uint64   `json:"gasUsed"`
	Time        hexutil.Big      `json:"timestamp"`
	Extra       hexutil.Bytes    `json:"extraData"`
	Nonce       types.BlockNonce `json:"nonce"`
	GethHash    common.Hash      `json:"mixHash"`
	ParityHash  common.Hash      `json:"hash"`
}

type Transaction struct {
	GasPrice hexutil.Uint64 `json:"gasPrice"`
}

// Block represents a full block
// See: https://github.com/ethereum/go-ethereum/blob/0e6ea9199ca701ee4c96220e873884327c8d18ff/core/types/block.go#L147
type Block struct {
	Transactions []Transaction  `json:"transactions"`
	Difficulty   hexutil.Uint64 `json:"difficulty"`
}

var emptyHash = common.Hash{}

// Hash will return GethHash if it exists otherwise it returns the ParityHash
func (h BlockHeader) Hash() common.Hash {
	if h.GethHash != emptyHash {
		return h.GethHash
	}
	return h.ParityHash
}

// TxReceipt holds the block number and the transaction hash of a signed
// transaction that has been written to the blockchain.
type TxReceipt struct {
	BlockNumber *utils.Big   `json:"blockNumber"`
	BlockHash   *common.Hash `json:"blockHash"`
	Hash        common.Hash  `json:"transactionHash"`
	Logs        []Log        `json:"logs"`
}

// Unconfirmed returns true if the transaction is not confirmed.
func (txr *TxReceipt) Unconfirmed() bool {
	return txr.Hash == emptyHash || txr.BlockNumber == nil
}

// ChainlinkFulfilledTopic is the signature for the event emitted after calling
// ChainlinkClient.validateChainlinkCallback(requestId). See
// ../../evm-contracts/src/v0.6/ChainlinkClient.sol
var ChainlinkFulfilledTopic = utils.MustHash("ChainlinkFulfilled(bytes32)")

// FulfilledRunLog returns true if this tx receipt is the result of a
// fulfilled run log.
func (txr TxReceipt) FulfilledRunLog() bool {
	for _, log := range txr.Logs {
		if log.Topics[0] == ChainlinkFulfilledTopic {
			return true
		}
	}
	return false
}

// FunctionSelector is the first four bytes of the call data for a
// function call and specifies the function to be called.
type FunctionSelector [FunctionSelectorLength]byte

// FunctionSelectorLength should always be a length of 4 as a byte.
const FunctionSelectorLength = 4

// BytesToFunctionSelector converts the given bytes to a FunctionSelector.
func BytesToFunctionSelector(b []byte) FunctionSelector {
	var f FunctionSelector
	f.SetBytes(b)
	return f
}

// HexToFunctionSelector converts the given string to a FunctionSelector.
func HexToFunctionSelector(s string) FunctionSelector {
	return BytesToFunctionSelector(common.FromHex(s))
}

// String returns the FunctionSelector as a string type.
func (f FunctionSelector) String() string { return hexutil.Encode(f[:]) }

// Bytes returns the FunctionSelector as a byte slice
func (f FunctionSelector) Bytes() []byte { return f[:] }

// WithoutPrefix returns the FunctionSelector as a string without the '0x' prefix.
func (f FunctionSelector) WithoutPrefix() string { return f.String()[2:] }

// SetBytes sets the FunctionSelector to that of the given bytes (will trim).
func (f *FunctionSelector) SetBytes(b []byte) { copy(f[:], b[:FunctionSelectorLength]) }

var hexRegexp *regexp.Regexp = regexp.MustCompile("^[0-9a-fA-F]*$")

func unmarshalFromString(s string, f *FunctionSelector) error {
	if utils.HasHexPrefix(s) {
		if !hexRegexp.Match([]byte(s)[2:]) {
			return fmt.Errorf("function selector %s must be 0x-hex encoded", s)
		}
		bytes := common.FromHex(s)
		if len(bytes) != FunctionSelectorLength {
			return errors.New("Function ID must be 4 bytes in length")
		}
		f.SetBytes(bytes)
	} else {
		bytes, err := utils.Keccak256([]byte(s))
		if err != nil {
			return err
		}
		f.SetBytes(bytes[0:4])
	}
	return nil
}

// UnmarshalJSON parses the raw FunctionSelector and sets the FunctionSelector
// type to the given input.
func (f *FunctionSelector) UnmarshalJSON(input []byte) error {
	var s string
	err := json.Unmarshal(input, &s)
	if err != nil {
		return err
	}
	return unmarshalFromString(s, f)
}

// MarshalJSON returns the JSON encoding of f
func (f FunctionSelector) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.String())
}

// Value returns this instance serialized for database storage
func (f FunctionSelector) Value() (driver.Value, error) {
	return f.Bytes(), nil
}

// Scan returns the selector from its serialization in the database
func (f FunctionSelector) Scan(value interface{}) error {
	temp, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("unable to convent %v of type %T to FunctionSelector", value, value)
	}
	if len(temp) != FunctionSelectorLength {
		return fmt.Errorf("function selector %v should have length %d, but has length %d",
			temp, FunctionSelectorLength, len(temp))
	}
	copy(f[:], temp)
	return nil
}

// SafeByteSlice returns an error on out of bounds access to a byte array, where a
// normal slice would panic instead
func (ary UntrustedBytes) SafeByteSlice(start int, end int) ([]byte, error) {
	if end > len(ary) || start > end || start < 0 || end < 0 {
		var empty []byte
		return empty, errors.New("out of bounds slice access")
	}
	return ary[start:end], nil
}

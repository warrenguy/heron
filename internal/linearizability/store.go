package linearizability

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	opIDLength              = 8
	opKindLength            = 1
	requestLengthPrefixSize = 2
	replyLengthPrefixSize   = 4
)

// OpKind names one key-value operation.
type OpKind uint8

const (
	// OpGet reads the value of a key.
	OpGet OpKind = iota + 1

	// OpPut replaces the value of a key.
	OpPut

	// OpAppend extends the value of a key.
	OpAppend
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpAppend:
		return "append"
	default:
		return fmt.Sprintf("kind%d", uint8(k))
	}
}

// Input is one key-value request.
type Input struct {
	Kind  OpKind
	Key   string
	Value string
}

// Output is one key-value reply.
type Output struct {
	// Value is the key's value after the operation. Empty for a put.
	Value string

	// Unknown marks a write whose client stopped waiting.
	Unknown bool
}

// Store is the key-value state a service replica holds. Every key starts empty.
type Store struct {
	values map[string]string
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{values: make(map[string]string)}
}

// Apply runs one operation against the store. Panics on an unknown kind.
func (s *Store) Apply(in Input) Output {
	switch in.Kind {
	case OpGet:
		return Output{Value: s.values[in.Key]}
	case OpPut:
		s.values[in.Key] = in.Value
		return Output{}
	case OpAppend:
		value := s.values[in.Key] + in.Value
		s.values[in.Key] = value
		return Output{Value: value}
	default:
		panic(fmt.Sprintf("linearizability: apply: unknown kind %d", in.Kind))
	}
}

// EncodeRequest appends one request to dst.
//
// Wire layout, little-endian: op id int64, kind uint8, key length uint16, key,
// value length uint16, value.
//
// Panics when the key or the value exceeds 65535 bytes.
func EncodeRequest(dst []byte, opID int64, in Input) []byte {
	if len(in.Key) > math.MaxUint16 {
		panic(fmt.Sprintf("linearizability: encode request: key length %d exceeds %d",
			len(in.Key), math.MaxUint16))
	}
	if len(in.Value) > math.MaxUint16 {
		panic(fmt.Sprintf("linearizability: encode request: value length %d exceeds %d",
			len(in.Value), math.MaxUint16))
	}

	dst = binary.LittleEndian.AppendUint64(dst, uint64(opID))
	dst = append(dst, byte(in.Kind))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(in.Key)))
	dst = append(dst, in.Key...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(in.Value)))
	return append(dst, in.Value...)
}

// DecodeRequest reads one request that [EncodeRequest] wrote.
//
// Returns an error when the kind is unknown, the payload ends early, or the
// payload holds trailing bytes.
func DecodeRequest(payload []byte) (int64, Input, error) {
	headerLength := opIDLength + opKindLength + requestLengthPrefixSize
	if len(payload) < headerLength {
		return 0, Input{}, fmt.Errorf("decode request: payload length %d below header length %d",
			len(payload), headerLength)
	}

	opID := int64(binary.LittleEndian.Uint64(payload))
	kind := OpKind(payload[opIDLength])
	if kind < OpGet || kind > OpAppend {
		return 0, Input{}, fmt.Errorf("decode request: unknown kind %d", kind)
	}

	keyLength := int(binary.LittleEndian.Uint16(payload[opIDLength+opKindLength:]))
	rest := payload[headerLength:]
	if keyLength > len(rest) {
		return 0, Input{}, fmt.Errorf("decode request: key length %d exceeds remaining %d bytes",
			keyLength, len(rest))
	}
	key := string(rest[:keyLength])

	rest = rest[keyLength:]
	if len(rest) < requestLengthPrefixSize {
		return 0, Input{}, fmt.Errorf("decode request: remaining %d bytes below value length field",
			len(rest))
	}
	valueLength := int(binary.LittleEndian.Uint16(rest))
	rest = rest[requestLengthPrefixSize:]
	if valueLength != len(rest) {
		return 0, Input{}, fmt.Errorf("decode request: value length %d differs from remaining %d bytes",
			valueLength, len(rest))
	}

	return opID, Input{Kind: kind, Key: key, Value: string(rest[:valueLength])}, nil
}

// EncodeReply appends one reply to dst.
//
// Wire layout, little-endian: op id int64, value length uint32, value. The
// layout does not carry [Output.Unknown].
//
// Panics when the value exceeds [math.MaxUint32] bytes.
func EncodeReply(dst []byte, opID int64, out Output) []byte {
	if len(out.Value) > math.MaxUint32 {
		panic(fmt.Sprintf("linearizability: encode reply: value length %d exceeds %d",
			len(out.Value), math.MaxUint32))
	}

	dst = binary.LittleEndian.AppendUint64(dst, uint64(opID))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(out.Value)))
	return append(dst, out.Value...)
}

// DecodeReply reads one reply that [EncodeReply] wrote.
//
// Returns an error when the payload ends early or holds trailing bytes.
func DecodeReply(payload []byte) (int64, Output, error) {
	headerLength := opIDLength + replyLengthPrefixSize
	if len(payload) < headerLength {
		return 0, Output{}, fmt.Errorf("decode reply: payload length %d below header length %d",
			len(payload), headerLength)
	}

	opID := int64(binary.LittleEndian.Uint64(payload))
	valueLength := int(binary.LittleEndian.Uint32(payload[opIDLength:]))
	rest := payload[headerLength:]
	if valueLength != len(rest) {
		return 0, Output{}, fmt.Errorf("decode reply: value length %d differs from remaining %d bytes",
			valueLength, len(rest))
	}

	return opID, Output{Value: string(rest[:valueLength])}, nil
}

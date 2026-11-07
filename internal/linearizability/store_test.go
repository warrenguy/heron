package linearizability

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStore_apply(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		prep []Input
		in   Input
		want Output

		// wantValue is what a later get on the key reads.
		wantValue string
	}{
		{
			name: "get of an empty key",
			in:   Input{Kind: OpGet, Key: "k0"},
			want: Output{},
		},
		{
			name:      "get returns the value",
			prep:      []Input{{Kind: OpPut, Key: "k0", Value: "1,"}},
			in:        Input{Kind: OpGet, Key: "k0"},
			want:      Output{Value: "1,"},
			wantValue: "1,",
		},
		{
			name:      "put replaces the value and returns empty",
			prep:      []Input{{Kind: OpPut, Key: "k0", Value: "1,"}},
			in:        Input{Kind: OpPut, Key: "k0", Value: "2,"},
			want:      Output{},
			wantValue: "2,",
		},
		{
			name:      "append to an empty key",
			in:        Input{Kind: OpAppend, Key: "k0", Value: "1,"},
			want:      Output{Value: "1,"},
			wantValue: "1,",
		},
		{
			name:      "append extends the value",
			prep:      []Input{{Kind: OpAppend, Key: "k0", Value: "1,"}},
			in:        Input{Kind: OpAppend, Key: "k0", Value: "2,"},
			want:      Output{Value: "1,2,"},
			wantValue: "1,2,",
		},
		{
			name: "keys stay separate",
			prep: []Input{{Kind: OpPut, Key: "k1", Value: "1,"}},
			in:   Input{Kind: OpGet, Key: "k0"},
			want: Output{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := NewStore()
			for _, in := range test.prep {
				store.Apply(in)
			}

			require.Equal(t, test.want, store.Apply(test.in))
			got := store.Apply(Input{Kind: OpGet, Key: test.in.Key})
			require.Equal(t, test.wantValue, got.Value)
		})
	}

	t.Run("unknown kind panics", func(t *testing.T) {
		t.Parallel()

		require.Panics(t, func() { NewStore().Apply(Input{Key: "k0"}) })
	})
}

func TestCodec_encodesWireLayout(t *testing.T) {
	t.Parallel()

	request := EncodeRequest([]byte{0xff}, 0x0102030405060708, Input{
		Kind:  OpAppend,
		Key:   "k1",
		Value: "9,",
	})
	require.Equal(t, []byte{
		0xff,
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
		0x03,
		0x02, 0x00,
		'k', '1',
		0x02, 0x00,
		'9', ',',
	}, request)

	reply := EncodeReply([]byte{0xff}, 0x0102030405060708, Output{Value: "9,"})
	require.Equal(t, []byte{
		0xff,
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
		0x02, 0x00, 0x00, 0x00,
		'9', ',',
	}, reply)
}

func TestCodec_decodeRejects(t *testing.T) {
	t.Parallel()

	validRequest := EncodeRequest(nil, 7, Input{Kind: OpAppend, Key: "k0", Value: "1,"})
	validReply := EncodeReply(nil, 7, Output{Value: "1,"})

	const (
		kindOffset          = 8
		keyLengthOffset     = 9
		requestHeaderLength = 11
		valueLengthOffset   = requestHeaderLength + len("k0")
		replyLengthOffset   = 8
		replyHeaderLength   = 12
	)

	withByte := func(payload []byte, offset int, value byte) []byte {
		changed := slices.Clone(payload)
		changed[offset] = value
		return changed
	}

	maxLengthReply := slices.Clone(validReply)
	binary.LittleEndian.PutUint32(maxLengthReply[replyLengthOffset:], math.MaxUint32)

	requestTests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "empty payload", payload: nil, want: "header length"},
		{
			name:    "short header",
			payload: validRequest[:requestHeaderLength-1],
			want:    "header length",
		},
		{name: "zero kind", payload: withByte(validRequest, kindOffset, 0), want: "unknown kind 0"},
		{
			name:    "kind past the last",
			payload: withByte(validRequest, kindOffset, byte(OpAppend)+1),
			want:    "unknown kind 4",
		},
		{
			name:    "key length past the end",
			payload: withByte(validRequest, keyLengthOffset, 200),
			want:    "key length 200",
		},
		{
			name:    "missing value length",
			payload: validRequest[:valueLengthOffset+1],
			want:    "value length field",
		},
		{
			name:    "value length past the end",
			payload: withByte(validRequest, valueLengthOffset, 200),
			want:    "value length 200",
		},
		{
			name:    "trailing byte",
			payload: append(slices.Clone(validRequest), 0),
			want:    "remaining 3 bytes",
		},
	}
	for _, test := range requestTests {
		t.Run("request "+test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := DecodeRequest(test.payload)
			require.ErrorContains(t, err, test.want)
		})
	}

	replyTests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "empty payload", payload: nil, want: "header length"},
		{name: "short header", payload: validReply[:replyHeaderLength-1], want: "header length"},
		{
			name:    "value length past the end",
			payload: withByte(validReply, replyLengthOffset, 200),
			want:    "value length 200",
		},
		{
			name:    "value length at the uint32 limit",
			payload: maxLengthReply,
			want:    "value length 4294967295",
		},
		{
			name:    "trailing byte",
			payload: append(slices.Clone(validReply), 0),
			want:    "remaining 3 bytes",
		},
	}
	for _, test := range replyTests {
		t.Run("reply "+test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := DecodeReply(test.payload)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestCodec_encodePanicsPastLengthField(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", math.MaxUint16+1)
	longest := tooLong[:math.MaxUint16]

	// Phase: a request length past 16 bits panics.
	require.Panics(t, func() { EncodeRequest(nil, 1, Input{Kind: OpGet, Key: tooLong}) })
	require.Panics(t, func() { EncodeRequest(nil, 1, Input{Kind: OpPut, Key: "k0", Value: tooLong}) })

	// Phase: the longest key and value round-trip.
	longestInput := Input{Kind: OpPut, Key: longest, Value: longest}
	opID, in, err := DecodeRequest(EncodeRequest(nil, 1, longestInput))
	require.NoError(t, err)
	require.Equal(t, int64(1), opID)
	require.Equal(t, longestInput, in)

	// Phase: a reply value past 16 bits round-trips. Its length field is uint32.
	opID, out, err := DecodeReply(EncodeReply(nil, 1, Output{Value: tooLong}))
	require.NoError(t, err)
	require.Equal(t, int64(1), opID)
	require.Equal(t, Output{Value: tooLong}, out)
}

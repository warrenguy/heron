package conductor_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	counterTestClientID      int64 = 1
	counterTestCorrelationID int64 = 42
	counterTestTypeID        int32 = 1001
	counterTestLabel               = "counter1"
)

var counterTestKey = []byte{0x01, 0x02, 0x03, 0x04, 0x05}

func TestCounterMessageBytes_golden(t *testing.T) {
	keyAndLabel := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xe9, 0x03, 0x00, 0x00,
		0x05, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x31,
	}
	tests := []struct {
		name  string
		key   []byte
		label string
		fill  byte
		want  []byte
	}{
		{
			name:  "keyAndLabel",
			key:   counterTestKey,
			label: counterTestLabel,
			want:  keyAndLabel,
		},
		{
			// The encode zeroes the key padding over stale bytes.
			name:  "keyAndLabelDirtyBuffer",
			key:   counterTestKey,
			label: counterTestLabel,
			fill:  0xEE,
			want:  keyAndLabel,
		},
		{
			name: "emptyKeyAndLabel",
			want: []byte{
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0xe9, 0x03, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := conductor.CounterMessageBytes(
				counterTestClientID,
				counterTestCorrelationID,
				counterTestTypeID,
				test.key,
				test.label,
			)
			require.Equal(t, test.want, got)
			require.Len(t, got, conductor.CounterMessageBodyLength(len(test.key), len(test.label)))

			raw := bytes.Repeat([]byte{test.fill}, len(test.want))
			require.NoError(t, conductor.EncodeCounterMessage(
				atomicbuffer.Wrap(raw),
				0,
				counterTestClientID,
				counterTestCorrelationID,
				counterTestTypeID,
				test.key,
				test.label,
			))
			require.Equal(t, test.want, raw)
		})
	}
}

func TestCounterMessageRoundtrip(t *testing.T) {
	tests := []struct {
		name      string
		offset    int
		key       []byte
		label     string
		extraBody int
	}{
		{name: "offsetZero", key: counterTestKey, label: counterTestLabel},
		{name: "offsetEight", offset: 8, key: counterTestKey, label: counterTestLabel},
		{name: "emptyKeyAndLabel"},
		{name: "alignedKey", key: bytes.Repeat([]byte{0xAB}, 8), label: "x"},
		{
			name:  "maximumKeyAndLabel",
			key:   bytes.Repeat([]byte{0xCD}, cnc.CounterMetadataMaxKeyLength),
			label: strings.Repeat("l", cnc.CounterMetadataMaxLabelLength),
		},
		{name: "paddedBody", key: counterTestKey, label: counterTestLabel, extraBody: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encodedLength := conductor.CounterMessageBodyLength(len(test.key), len(test.label))
			bodyLength := encodedLength + test.extraBody
			buffer := atomicbuffer.Wrap(make([]byte, test.offset+bodyLength))
			require.NoError(t, conductor.EncodeCounterMessage(
				buffer,
				test.offset,
				counterTestClientID,
				counterTestCorrelationID,
				counterTestTypeID,
				test.key,
				test.label,
			))

			got, err := conductor.DecodeCounterMessage(buffer, test.offset, bodyLength)
			require.NoError(t, err)
			wantKey := test.key
			if wantKey == nil {
				wantKey = []byte{}
			}
			require.Equal(t, conductor.CounterMessage{
				ClientID:      counterTestClientID,
				CorrelationID: counterTestCorrelationID,
				TypeID:        counterTestTypeID,
				Key:           wantKey,
				Label:         test.label,
			}, got)

			// The decoded key is a copy. Zeroing the buffer leaves it unchanged.
			buffer.SetBytes(test.offset, bodyLength, 0)
			require.Equal(t, wantKey, got.Key)
		})
	}
}

func TestEncodeCounterMessage_rejects(t *testing.T) {
	tests := []struct {
		name           string
		key            []byte
		label          string
		bufferLength   int
		wantErr        error
		wantErrMessage string
	}{
		{
			name:           "keyTooLong",
			key:            make([]byte, cnc.CounterMetadataMaxKeyLength+1),
			bufferLength:   256,
			wantErr:        conductor.ErrCounterKeyTooLong,
			wantErrMessage: "conductor: counter key exceeds 112 bytes",
		},
		{
			name:           "labelTooLong",
			label:          strings.Repeat("l", cnc.CounterMetadataMaxLabelLength+1),
			bufferLength:   512,
			wantErr:        conductor.ErrCounterLabelTooLong,
			wantErrMessage: "conductor: counter label exceeds 380 bytes",
		},
		{
			name:           "nonASCIILabel",
			label:          "caf\xc3\xa9",
			bufferLength:   64,
			wantErr:        atomicbuffer.ErrStringASCIINonASCII,
			wantErrMessage: "atomicbuffer: string ascii non-ascii",
		},
		{
			name:           "outOfBounds",
			key:            counterTestKey,
			label:          counterTestLabel,
			bufferLength:   43,
			wantErr:        conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: offset=0 capacity=43",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := bytes.Repeat([]byte{0xEE}, test.bufferLength)
			err := conductor.EncodeCounterMessage(
				atomicbuffer.Wrap(raw),
				0,
				counterTestClientID,
				counterTestCorrelationID,
				counterTestTypeID,
				test.key,
				test.label,
			)
			require.ErrorIs(t, err, test.wantErr)
			require.EqualError(t, err, test.wantErrMessage)
			require.Equal(t, bytes.Repeat([]byte{0xEE}, test.bufferLength), raw)
		})
	}
}

func TestDecodeCounterMessage_rejects(t *testing.T) {
	golden := func() []byte {
		return conductor.CounterMessageBytes(
			counterTestClientID,
			counterTestCorrelationID,
			counterTestTypeID,
			counterTestKey,
			counterTestLabel,
		)
	}
	labelLengthOffset := conductor.CounterMessageLabelLengthOffset(len(counterTestKey))

	tests := []struct {
		name           string
		body           []byte
		bodyLength     int
		wantErr        error
		wantErrMessage string
	}{
		{
			name:       "shortBody",
			body:       golden(),
			bodyLength: conductor.CounterMessageMinimumBodyLength - 1,
			wantErr:    conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: " +
				"bodyLength=27 minimum=28",
		},
		{
			name:       "bodyPastBuffer",
			body:       golden(),
			bodyLength: 45,
			wantErr:    conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: " +
				"offset=0 capacity=44",
		},
		{
			name:       "truncatedKey",
			body:       golden(),
			bodyLength: conductor.CounterMessageKeyOffset + len(counterTestKey) - 1,
			wantErr:    conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: " +
				"bodyLength=28 minimum=36",
		},
		{
			name:       "truncatedLabelLength",
			body:       golden(),
			bodyLength: labelLengthOffset + 3,
			wantErr:    conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: " +
				"bodyLength=35 minimum=36",
		},
		{
			name:       "truncatedLabel",
			body:       golden(),
			bodyLength: 43,
			wantErr:    conductor.ErrCounterMessageOutOfBounds,
			wantErrMessage: "conductor: counter message out of bounds: " +
				"bodyLength=43 minimum=44",
		},
		{
			name:           "negativeKeyLength",
			body:           withInt32(golden(), conductor.CounterMessageKeyLengthOffset, -1),
			bodyLength:     44,
			wantErr:        conductor.ErrCounterMessageLengthNegative,
			wantErrMessage: "conductor: counter message length negative: keyLength=-1",
		},
		{
			name: "keyLengthOverCap",
			body: withInt32(
				make([]byte, 256),
				conductor.CounterMessageKeyLengthOffset,
				cnc.CounterMetadataMaxKeyLength+1,
			),
			bodyLength:     256,
			wantErr:        conductor.ErrCounterKeyTooLong,
			wantErrMessage: "conductor: counter key exceeds 112 bytes: length=113",
		},
		{
			name:           "negativeLabelLength",
			body:           withInt32(golden(), labelLengthOffset, -1),
			bodyLength:     44,
			wantErr:        conductor.ErrCounterMessageLengthNegative,
			wantErrMessage: "conductor: counter message length negative: labelLength=-1",
		},
		{
			name: "labelLengthOverCap",
			body: withInt32(
				make([]byte, 512),
				conductor.CounterMessageLabelLengthOffset(0),
				cnc.CounterMetadataMaxLabelLength+1,
			),
			bodyLength:     512,
			wantErr:        conductor.ErrCounterLabelTooLong,
			wantErrMessage: "conductor: counter label exceeds 380 bytes: length=381",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := conductor.DecodeCounterMessage(
				atomicbuffer.Wrap(test.body),
				0,
				test.bodyLength,
			)
			require.ErrorIs(t, err, test.wantErr)
			require.EqualError(t, err, test.wantErrMessage)
			require.Zero(t, got)
		})
	}
}

func withInt32(body []byte, offset int, value int32) []byte {
	atomicbuffer.Wrap(body).PutInt32(offset, value)
	return body
}

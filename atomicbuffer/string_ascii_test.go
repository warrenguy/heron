package atomicbuffer_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
)

func TestPutGetStringASCII_roundtrip(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "single char", value: "a"},
		{name: "64 chars", value: strings.Repeat("x", 64)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encodedLength := atomicbuffer.StringASCIIEncodedLength(len(test.value))
			buffer := atomicbuffer.Wrap(make([]byte, encodedLength+8))
			gotEncodedLength, err := atomicbuffer.PutStringASCII(buffer, 0, test.value)
			require.NoError(t, err)
			require.Equal(t, encodedLength, gotEncodedLength)

			got, decodedLength, err := atomicbuffer.GetStringASCII(buffer, 0)
			require.NoError(t, err)
			require.Equal(t, test.value, got)
			require.Equal(t, gotEncodedLength, decodedLength)
		})
	}
}

func TestPutStringASCII_golden(t *testing.T) {
	const value = "aeron:ipc"

	buffer := atomicbuffer.Wrap(make([]byte, 32))
	_, err := atomicbuffer.PutStringASCII(buffer, 0, value)
	require.NoError(t, err)

	require.Equal(
		t,
		[]byte{
			0x09, 0x00, 0x00, 0x00,
			0x61, 0x65, 0x72, 0x6f, 0x6e, 0x3a, 0x69, 0x70, 0x63,
		},
		buffer.Bytes()[:atomicbuffer.StringASCIIEncodedLength(len(value))],
	)
}

func TestPutStringASCII_errors(t *testing.T) {
	tests := []struct {
		name                 string
		buffer               []byte
		offset               int
		value                string
		wantErr              error
		wantErrMsg           string
		checkBufferUnchanged bool
	}{
		{
			name:       "buffer too small",
			buffer:     []byte{0x00, 0x00, 0x00, 0x00},
			offset:     0,
			value:      "a",
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=0 capacity=4",
		},
		{
			name:                 "non-ascii rejected before out of bounds",
			buffer:               []byte{0x00, 0x00, 0x00, 0x00},
			offset:               0,
			value:                "\x80",
			wantErr:              atomicbuffer.ErrStringASCIINonASCII,
			wantErrMsg:           "atomicbuffer: string ascii non-ascii",
			checkBufferUnchanged: true,
		},
		{
			name:       "negative offset",
			buffer:     make([]byte, 8),
			offset:     -1,
			value:      "",
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=-1 capacity=8",
		},
		{
			name:       "non-zero offset out of bounds",
			buffer:     make([]byte, 8),
			offset:     8,
			value:      "",
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=8 capacity=8",
		},
		{
			name:       "non-zero offset payload out of bounds",
			buffer:     make([]byte, 10),
			offset:     8,
			value:      "abc",
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=8 capacity=10",
		},
		{
			name:       "non-ascii utf-8",
			buffer:     make([]byte, 32),
			offset:     0,
			value:      "caf\u00e9",
			wantErr:    atomicbuffer.ErrStringASCIINonASCII,
			wantErrMsg: "atomicbuffer: string ascii non-ascii",
		},
		{
			name:       "high byte",
			buffer:     make([]byte, 32),
			offset:     0,
			value:      "\x80",
			wantErr:    atomicbuffer.ErrStringASCIINonASCII,
			wantErrMsg: "atomicbuffer: string ascii non-ascii",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := atomicbuffer.Wrap(test.buffer)
			before := append([]byte(nil), buffer.Bytes()...)
			_, err := atomicbuffer.PutStringASCII(buffer, test.offset, test.value)
			require.ErrorIs(t, err, test.wantErr)
			require.EqualError(t, err, test.wantErrMsg)
			if test.checkBufferUnchanged {
				require.Equal(t, before, buffer.Bytes())
			}
		})
	}
}

func TestPutGetStringASCII_roundtrip_nonZeroOffset(t *testing.T) {
	const offset = 8
	const value = "aeron:ipc"

	encodedLength := atomicbuffer.StringASCIIEncodedLength(len(value))
	buffer := atomicbuffer.Wrap(make([]byte, offset+encodedLength+4))
	prefix := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	copy(buffer.Bytes(), prefix)

	gotEncodedLength, err := atomicbuffer.PutStringASCII(buffer, offset, value)
	require.NoError(t, err)
	require.Equal(t, encodedLength, gotEncodedLength)
	require.Equal(t, prefix, buffer.Bytes()[:offset])

	got, decodedLength, err := atomicbuffer.GetStringASCII(buffer, offset)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, gotEncodedLength, decodedLength)
}

func TestGetStringASCII_errors(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		offset     int
		wantErr    error
		wantErrMsg string
	}{
		{
			name:       "out of bounds offset",
			buffer:     []byte{0x00, 0x00, 0x00, 0x00},
			offset:     4,
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=4 capacity=4",
		},
		{
			name:       "length exceeds payload",
			buffer:     []byte{0x08, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63},
			offset:     0,
			wantErr:    atomicbuffer.ErrStringASCIIOutOfBounds,
			wantErrMsg: "atomicbuffer: string ascii out of bounds: offset=4 capacity=7",
		},
		{
			name:       "negative length",
			buffer:     []byte{0xff, 0xff, 0xff, 0xff},
			offset:     0,
			wantErr:    atomicbuffer.ErrStringASCIILengthInvalid,
			wantErrMsg: "atomicbuffer: string ascii length invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := atomicbuffer.GetStringASCII(atomicbuffer.Wrap(test.buffer), test.offset)
			require.ErrorIs(t, err, test.wantErr)
			require.EqualError(t, err, test.wantErrMsg)
		})
	}
}

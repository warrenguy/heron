package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestCorrelatedMessageBytes_golden(t *testing.T) {
	require.Equal(
		t,
		[]byte{
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		},
		conductor.CorrelatedMessageBytes(1, -1),
	)
}

func TestCorrelatedMessageRoundtrip(t *testing.T) {
	tests := []struct {
		name          string
		offset        int
		clientID      int64
		correlationID int64
	}{
		{
			name:          "positive ids at offset zero",
			offset:        0,
			clientID:      42,
			correlationID: 99,
		},
		{
			name:          "max int64 values at offset zero",
			offset:        0,
			clientID:      9223372036854775807,
			correlationID: -9223372036854775808,
		},
		{
			name:          "non-zero offset",
			offset:        8,
			clientID:      11,
			correlationID: 22,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := atomicbuffer.Wrap(make([]byte, test.offset+conductor.CorrelatedMessageLength))
			err := conductor.EncodeCorrelated(buffer, test.offset, test.clientID, test.correlationID)
			require.NoError(t, err)

			got, err := conductor.DecodeCorrelated(buffer, test.offset)
			require.NoError(t, err)
			require.Equal(t, conductor.CorrelatedMessage{
				ClientID:      test.clientID,
				CorrelationID: test.correlationID,
			}, got)
		})
	}
}

func TestCorrelatedMessageOutOfBounds(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.CorrelatedMessageLength))
	wantErr := "conductor: correlated message out of bounds: offset=8 capacity=16"

	t.Run("encode", func(t *testing.T) {
		err := conductor.EncodeCorrelated(buffer, 8, 1, 2)
		require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
		require.EqualError(t, err, wantErr)
	})

	t.Run("decode", func(t *testing.T) {
		_, err := conductor.DecodeCorrelated(buffer, 8)
		require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
		require.EqualError(t, err, wantErr)
	})
}

func TestCorrelatedMessageNegativeOffset(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.CorrelatedMessageLength))
	wantErr := "conductor: correlated message out of bounds: offset=-8 capacity=16"

	t.Run("encode", func(t *testing.T) {
		err := conductor.EncodeCorrelated(buffer, -8, 1, 2)
		require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
		require.EqualError(t, err, wantErr)
	})

	t.Run("decode", func(t *testing.T) {
		_, err := conductor.DecodeCorrelated(buffer, -8)
		require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
		require.EqualError(t, err, wantErr)
	})
}

func TestCorrelationAndInt32NegativeOffset(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.CorrelationAndInt32BodyLength))

	_, err := conductor.DecodeCorrelationAndInt32(buffer, -1)
	require.ErrorIs(t, err, conductor.ErrCorrelationAndInt32OutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: correlation and int32 body out of bounds: offset=-1 capacity=12",
	)
}

func TestDecodeTrailingStringASCII_stringOffsetBeyondBodyLength(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, 32))

	_, err := conductor.DecodeTrailingStringASCIIForTest(
		buffer,
		0,
		16,
		20,
		conductor.ErrCorrelatedMessageOutOfBounds,
	)
	require.ErrorIs(t, err, conductor.ErrCorrelatedMessageOutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: correlated message out of bounds: stringOffset=20 bodyLength=16",
	)
}

func TestNextAvailableSessionIDBytes_golden(t *testing.T) {
	require.Equal(
		t,
		[]byte{
			0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0xe9, 0x03, 0x00, 0x00,
		},
		conductor.CorrelationAndInt32Bytes(99, 1001),
	)
}

func TestDecodeNextAvailableSessionID_golden(t *testing.T) {
	buffer := atomicbuffer.Wrap(conductor.CorrelationAndInt32Bytes(99, 1001))

	got, err := conductor.DecodeNextAvailableSessionID(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, conductor.CorrelationAndInt32{
		CorrelationID: 99,
		Int32Value:    1001,
	}, got)
}

func TestCorrelationAndInt32(t *testing.T) {
	t.Run("decode golden", func(t *testing.T) {
		buffer := atomicbuffer.Wrap(conductor.CorrelationAndInt32Bytes(42, 12345))

		got, err := conductor.DecodeCorrelationAndInt32(buffer, 0)
		require.NoError(t, err)
		require.Equal(t, conductor.CorrelationAndInt32{
			CorrelationID: 42,
			Int32Value:    12345,
		}, got)
	})

	t.Run("decode out of bounds", func(t *testing.T) {
		buffer := atomicbuffer.Wrap(make([]byte, 8))

		_, err := conductor.DecodeCorrelationAndInt32(buffer, 0)
		require.ErrorIs(t, err, conductor.ErrCorrelationAndInt32OutOfBounds)
		require.EqualError(
			t,
			err,
			"conductor: correlation and int32 body out of bounds: offset=0 capacity=8",
		)
	})
}

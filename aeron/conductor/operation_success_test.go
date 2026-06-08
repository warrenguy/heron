package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/conductor"
)

func TestDecodeOperationSuccess_golden(t *testing.T) {
	payload := conductor.OperationSuccessBytes(7)
	buffer := atomic.Wrap(payload)

	got, err := conductor.DecodeOperationSuccess(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, conductor.OperationSuccess{CorrelationID: 7}, got)
}

func TestDecodeOperationSuccess_tooShort(t *testing.T) {
	buffer := atomic.Wrap([]byte{0x01, 0x02, 0x03})

	_, err := conductor.DecodeOperationSuccess(buffer, 0)
	require.ErrorIs(t, err, conductor.ErrOperationSuccessTooShort)
	require.EqualError(
		t,
		err,
		"conductor: operation success too short: offset=0 capacity=3",
	)
}

func TestEncodeOperationSuccess_outOfBounds(t *testing.T) {
	buffer := atomic.Wrap(make([]byte, 4))

	err := conductor.EncodeOperationSuccess(buffer, 0, 7)
	require.ErrorIs(t, err, conductor.ErrOperationSuccessTooShort)
	require.EqualError(
		t,
		err,
		"conductor: operation success too short: offset=0 capacity=4",
	)
}

func TestOperationSuccess_roundtrip(t *testing.T) {
	const correlationID int64 = 42
	buffer := atomic.Wrap(make([]byte, conductor.OperationSuccessBodyLength))

	require.NoError(t, conductor.EncodeOperationSuccess(buffer, 0, correlationID))

	got, err := conductor.DecodeOperationSuccess(buffer, 0)
	require.NoError(t, err)
	require.Equal(t, conductor.OperationSuccess{CorrelationID: correlationID}, got)
}

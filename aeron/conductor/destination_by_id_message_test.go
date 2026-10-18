package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestDestinationByIDMessage_goldenBytes(t *testing.T) {
	const (
		clientID                  int64 = 1
		correlationID             int64 = 42
		resourceRegistrationID    int64 = 9001
		destinationRegistrationID int64 = 9002
	)

	buffer := atomicbuffer.Wrap(make([]byte, conductor.DestinationByIDMessageBodyLength))
	err := conductor.EncodeDestinationByIDMessage(
		buffer,
		0,
		clientID,
		correlationID,
		resourceRegistrationID,
		destinationRegistrationID,
	)
	require.NoError(t, err)
	require.Equal(t, clientID, buffer.GetInt64(0))
	require.Equal(t, correlationID, buffer.GetInt64(8))
	require.Equal(t, resourceRegistrationID, buffer.GetInt64(16))
	require.Equal(t, destinationRegistrationID, buffer.GetInt64(24))
	require.Equal(t, buffer.Bytes(), conductor.DestinationByIDMessageBytes(
		clientID,
		correlationID,
		resourceRegistrationID,
		destinationRegistrationID,
	))

	got, err := conductor.DecodeDestinationByIDMessage(
		buffer,
		0,
		conductor.DestinationByIDMessageBodyLength,
	)
	require.NoError(t, err)
	require.Equal(t, conductor.DestinationByIDMessage{
		ClientID:                  clientID,
		CorrelationID:             correlationID,
		ResourceRegistrationID:    resourceRegistrationID,
		DestinationRegistrationID: destinationRegistrationID,
	}, got)
}

func TestDecodeDestinationByIDMessage_bodyTooShort(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.DestinationByIDMessageBodyLength))

	_, err := conductor.DecodeDestinationByIDMessage(buffer, 0, 24)
	require.ErrorIs(t, err, conductor.ErrDestinationByIDMessageOutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: destination by id message out of bounds: bodyLength=24 minimum=32",
	)
}

func TestEncodeDestinationByIDMessage_outOfBounds(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.DestinationByIDMessageBodyLength))
	wantErr := "conductor: destination by id message out of bounds: offset=8 capacity=32"

	err := conductor.EncodeDestinationByIDMessage(buffer, 8, 1, 42, 9001, 9002)
	require.ErrorIs(t, err, conductor.ErrDestinationByIDMessageOutOfBounds)
	require.EqualError(t, err, wantErr)
}

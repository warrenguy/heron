package conductor_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestRemoveMessageRoundtrip(t *testing.T) {
	const (
		clientID       int64 = 1
		correlationID  int64 = 42
		registrationID int64 = 9001
	)

	buffer := atomicbuffer.Wrap(make([]byte, conductor.RemoveMessageBodyLength))
	err := conductor.EncodeRemoveMessage(
		buffer,
		0,
		clientID,
		correlationID,
		registrationID,
	)
	require.NoError(t, err)

	got, err := conductor.DecodeRemoveMessage(buffer, 0, conductor.RemoveMessageBodyLength)
	require.NoError(t, err)
	require.Equal(t, conductor.RemoveMessage{
		ClientID:       clientID,
		CorrelationID:  correlationID,
		RegistrationID: registrationID,
	}, got)
}

func TestRemovePublicationMessage_revokeFlag(t *testing.T) {
	const (
		clientID       int64 = 1
		correlationID  int64 = 42
		registrationID int64 = 9001
	)

	tests := []struct {
		name      string
		revoke    bool
		wantFlags int64
	}{
		{name: "revoke true", revoke: true, wantFlags: conductor.RemovePublicationFlagRevoke},
		{name: "revoke false", revoke: false, wantFlags: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := atomicbuffer.Wrap(make([]byte, conductor.RemovePublicationBodyLength))
			err := conductor.EncodeRemovePublicationMessage(
				buffer,
				0,
				clientID,
				correlationID,
				registrationID,
				test.revoke,
			)
			require.NoError(t, err)
			require.Equal(t, test.wantFlags, buffer.GetInt64(conductor.RemovePublicationFlagsOffset))

			got, err := conductor.DecodeRemovePublicationMessage(
				buffer,
				0,
				conductor.RemovePublicationBodyLength,
			)
			require.NoError(t, err)
			require.Equal(t, test.revoke, got.Revoke)
		})
	}
}

func TestDecodeRemovePublicationMessage_shortBody(t *testing.T) {
	buffer := atomicbuffer.Wrap(conductor.RemoveMessageBytes(1, 42, 9001))

	got, err := conductor.DecodeRemovePublicationMessage(buffer, 0, conductor.RemoveMessageBodyLength)
	require.NoError(t, err)
	require.False(t, got.Revoke)
	require.Equal(t, conductor.RemoveMessage{
		ClientID:       1,
		CorrelationID:  42,
		RegistrationID: 9001,
	}, got.RemoveMessage)
}

func TestDecodeRemovePublicationMessage_flagsTruncated(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.RemoveMessageBodyLength))

	_, err := conductor.DecodeRemovePublicationMessage(
		buffer,
		0,
		conductor.RemovePublicationBodyLength,
	)
	require.ErrorIs(t, err, conductor.ErrRemoveMessageOutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: remove message out of bounds: offset=0 capacity=24",
	)
}

func TestDecodeRemovePublicationMessage_bodyTooShort(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.RemoveMessageBodyLength))

	_, err := conductor.DecodeRemovePublicationMessage(buffer, 0, 20)
	require.ErrorIs(t, err, conductor.ErrRemoveMessageOutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: remove message out of bounds: bodyLength=20 minimum=24",
	)
}

func TestDecodeRemoveMessage_bodyTooShort(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.RemoveMessageBodyLength))

	_, err := conductor.DecodeRemoveMessage(buffer, 0, 20)
	require.ErrorIs(t, err, conductor.ErrRemoveMessageOutOfBounds)
	require.EqualError(
		t,
		err,
		"conductor: remove message out of bounds: bodyLength=20 minimum=24",
	)
}

func TestEncodeRemoveMessage_outOfBounds(t *testing.T) {
	buffer := atomicbuffer.Wrap(make([]byte, conductor.RemoveMessageBodyLength))
	wantErr := "conductor: remove message out of bounds: offset=8 capacity=24"

	err := conductor.EncodeRemoveMessage(buffer, 8, 1, 42, 9001)
	require.ErrorIs(t, err, conductor.ErrRemoveMessageOutOfBounds)
	require.EqualError(t, err, wantErr)
}

package conductor_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
)

type streamChannelNonASCIICase struct {
	name    string
	channel string
}

func streamChannelNonASCIICases() []streamChannelNonASCIICase {
	return []streamChannelNonASCIICase{
		{name: "high byte", channel: "aeron:\x80"},
		{name: "utf8 multibyte", channel: "ü"},
	}
}

type streamChannelMessageHarness struct {
	minimumBodyLength int
	errOutOfBounds    error
	bodyLength        func(string) int
	encode            func(
		buffer atomic.AtomicBuffer,
		offset int,
		clientID int64,
		correlationID int64,
		streamID int32,
		channel string,
	) error
	decode func(
		buffer atomic.AtomicBuffer,
		offset int,
		bodyLength int,
	) (clientID, correlationID int64, streamID int32, channel string, err error)
	bytes func(
		clientID int64,
		correlationID int64,
		streamID int32,
		channel string,
	) []byte
}

func testStreamChannelMessageRoundtrip(
	t *testing.T,
	harness streamChannelMessageHarness,
	tests []struct {
		name          string
		offset        int
		clientID      int64
		correlationID int64
		streamID      int32
		channel       string
	},
) {
	t.Helper()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bodyLength := harness.bodyLength(test.channel)
			buffer := atomic.Wrap(make([]byte, test.offset+bodyLength))
			err := harness.encode(
				buffer,
				test.offset,
				test.clientID,
				test.correlationID,
				test.streamID,
				test.channel,
			)
			require.NoError(t, err)

			gotClientID, gotCorrelationID, gotStreamID, gotChannel, err := harness.decode(
				buffer,
				test.offset,
				bodyLength,
			)
			require.NoError(t, err)
			require.Equal(t, test.clientID, gotClientID)
			require.Equal(t, test.correlationID, gotCorrelationID)
			require.Equal(t, test.streamID, gotStreamID)
			require.Equal(t, test.channel, gotChannel)
		})
	}
}

func testStreamChannelMessageBodyLength(
	t *testing.T,
	harness streamChannelMessageHarness,
) {
	t.Helper()

	const channel = "aeron:ipc"
	require.Equal(
		t,
		harness.minimumBodyLength+atomic.StringASCIIEncodedLength(len(channel))-4,
		harness.bodyLength(channel),
	)
}

func testDecodeStreamChannelMessageBodyTooShort(
	t *testing.T,
	harness streamChannelMessageHarness,
	shortBodyLength int,
) {
	t.Helper()

	buffer := atomic.Wrap(make([]byte, harness.minimumBodyLength))

	_, _, _, _, err := harness.decode(buffer, 0, shortBodyLength)
	require.ErrorIs(t, err, harness.errOutOfBounds)
	require.EqualError(
		t,
		err,
		harness.errOutOfBounds.Error()+": bodyLength="+
			strconv.Itoa(shortBodyLength)+
			" minimum="+
			strconv.Itoa(harness.minimumBodyLength),
	)
}

func testDecodeStreamChannelMessageChannelTruncated(
	t *testing.T,
	harness streamChannelMessageHarness,
	payload []byte,
	shortBodyLength int,
	wantErr string,
) {
	t.Helper()

	buffer := atomic.Wrap(payload)

	_, _, _, _, err := harness.decode(buffer, 0, shortBodyLength)
	require.ErrorIs(t, err, atomic.ErrStringASCIIOutOfBounds)
	require.EqualError(t, err, wantErr)
}

func testDecodeStreamChannelMessageDecodedLengthMismatch(
	t *testing.T,
	harness streamChannelMessageHarness,
	payload []byte,
	bodyLength int,
	decodedLength int,
) {
	t.Helper()

	buf := make([]byte, bodyLength)
	copy(buf, payload)
	buffer := atomic.Wrap(buf)

	_, _, _, _, err := harness.decode(buffer, 0, bodyLength)
	require.ErrorIs(t, err, harness.errOutOfBounds)
	require.EqualError(
		t,
		err,
		harness.errOutOfBounds.Error()+": decodedLength="+
			strconv.Itoa(decodedLength)+
			" bodyLength="+
			strconv.Itoa(bodyLength),
	)
}

func testEncodeStreamChannelMessageOutOfBounds(
	t *testing.T,
	harness streamChannelMessageHarness,
) {
	t.Helper()

	buffer := atomic.Wrap(make([]byte, harness.minimumBodyLength))
	wantErr := harness.errOutOfBounds.Error() +
		": offset=8 capacity=" +
		strconv.Itoa(harness.minimumBodyLength)

	err := harness.encode(buffer, 8, 1, 42, 1001, "aeron:ipc")
	require.ErrorIs(t, err, harness.errOutOfBounds)
	require.EqualError(t, err, wantErr)
}

func testStreamChannelMessageRejectsNonASCII(
	t *testing.T,
	harness streamChannelMessageHarness,
) {
	t.Helper()

	const (
		clientID      int64 = 1
		correlationID int64 = 42
		streamID      int32 = 1001
	)
	for _, channelTest := range streamChannelNonASCIICases() {
		t.Run(channelTest.name, func(t *testing.T) {
			bodyLength := harness.bodyLength(channelTest.channel)
			buffer := atomic.Wrap(make([]byte, bodyLength))
			err := harness.encode(
				buffer,
				0,
				clientID,
				correlationID,
				streamID,
				channelTest.channel,
			)
			require.ErrorIs(t, err, atomic.ErrStringASCIINonASCII)
		})
	}
}

func TestStreamChannelMessage_rejectsNonASCII(t *testing.T) {
	t.Run("publication", func(t *testing.T) {
		testStreamChannelMessageRejectsNonASCII(t, publicationMessageHarness())
	})
	t.Run("subscription", func(t *testing.T) {
		testStreamChannelMessageRejectsNonASCII(t, subscriptionMessageHarness())
	})
}

func TestStreamChannelMessageBytes_panicsOnNonASCII(t *testing.T) {
	const (
		clientID      int64 = 1
		correlationID int64 = 42
		streamID      int32 = 1001
	)
	for _, channelTest := range streamChannelNonASCIICases() {
		t.Run("publication/"+channelTest.name, func(t *testing.T) {
			require.Panics(t, func() {
				conductor.PublicationMessageBytes(
					clientID,
					correlationID,
					streamID,
					channelTest.channel,
				)
			})
		})
		t.Run("subscription/"+channelTest.name, func(t *testing.T) {
			require.Panics(t, func() {
				conductor.SubscriptionMessageBytes(
					clientID,
					correlationID,
					conductor.NullRegistrationCorrelationID,
					streamID,
					channelTest.channel,
				)
			})
		})
	}
}

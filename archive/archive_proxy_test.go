package archive

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	archiveProxyTestOfferPosition      int64 = 2048
	archiveProxyTestCorrelationID      int64 = 9001
	archiveProxyTestResponseStreamID   int32 = 10
	archiveProxyTestRecordingStreamID  int32 = 33
	archiveProxyTestRecordingID        int64 = 7700
	archiveProxyTestResponseChannel          = "aeron:ipc"
	archiveProxyWireBufferHighWaterLen       = 500
)

var archiveProxyTestResponseChannelBytes = []byte(archiveProxyTestResponseChannel)

type stubControlPublication struct {
	offerPosition int64
	offerResult   int64
	offered       [][]byte
	maxMsgLen     int
	closed        bool
}

func newStubControlPublication() *stubControlPublication {
	return &stubControlPublication{
		offerPosition: archiveProxyTestOfferPosition,
		maxMsgLen:     1024 * 1024,
	}
}

func newTestProxy(ctx Context) (*stubControlPublication, *Proxy) {
	pub := newStubControlPublication()
	return pub, NewProxy(pub, ctx)
}

func (publication *stubControlPublication) Offer(buffer []byte) int64 {
	publication.offered = append(
		publication.offered,
		bytes.Clone(buffer),
	)
	if publication.offerResult != 0 {
		return publication.offerResult
	}
	publication.offerPosition += int64(len(buffer))
	return publication.offerPosition
}

func (publication *stubControlPublication) IsConnected() bool { return true }

func (publication *stubControlPublication) IsClosed() bool { return false }

func (publication *stubControlPublication) Close() error {
	publication.closed = true
	return nil
}

func (publication *stubControlPublication) MaxMessageLength() int { return publication.maxMsgLen }

func (publication *stubControlPublication) MaxPayloadLength() int {
	return publication.maxMsgLen - 1
}

func (publication *stubControlPublication) lastOffer() []byte {
	if len(publication.offered) == 0 {
		return nil
	}
	return publication.offered[len(publication.offered)-1]
}

func requireProxyWireFields[T any](
	t *testing.T,
	wire []byte,
	wantTemplateID uint16,
) T {
	t.Helper()
	require.NotEmpty(t, wire)

	decoded, ok := codecs.TryDecodeByTemplate(wire, 0, codecs.ArchiveDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, wantTemplateID, decoded.TemplateID)

	fields, ok := decoded.Fields.(T)
	require.True(t, ok)
	return fields
}

func archiveProxyTestCredBytes(marker byte) []byte {
	return []byte{marker, marker + 1, marker + 2}
}

type stubCredentialsSupplier struct {
	encoded []byte
}

func (supplier stubCredentialsSupplier) EncodedCredentials() []byte {
	return supplier.encoded
}

func (supplier stubCredentialsSupplier) RespondToChallenge([]byte) []byte { return nil }

func TestProxy_TryConnect_wireTemplateAndLength(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	pos, ok := proxy.TryConnect(
		archiveProxyTestResponseChannelBytes,
		archiveProxyTestResponseStreamID,
		archiveProxyTestCorrelationID,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.AuthConnectRequestFields](
		t,
		wire,
		archivewire.AuthConnectRequestSbeTemplateID,
	)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestResponseStreamID, fields.ResponseStreamID)
	require.Equal(t, ProtocolSemanticVersion(), fields.Version)
	require.Equal(t, archiveProxyTestResponseChannelBytes, fields.ResponseChannel)

	expectedLen, lenOK := codecs.AuthConnectRequestWireLen(
		archivewire.MessageHeaderSbeSchemaVersion,
		len(archiveProxyTestResponseChannelBytes),
		0,
		len(proxy.clientInfo),
	)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryConnect_clientInfoOnWire(t *testing.T) {
	cases := []struct {
		name       string
		clientName string
		want       []byte
	}{
		{
			name:       "default",
			clientName: "",
			want:       []byte("name=archive-client heron"),
		},
		{
			name:       "custom",
			clientName: "my-archive",
			want:       []byte("name=my-archive heron"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub, proxy := newTestProxy(Context{ClientName: tc.clientName})

			_, ok := proxy.TryConnect(
				archiveProxyTestResponseChannelBytes,
				archiveProxyTestResponseStreamID,
				archiveProxyTestCorrelationID,
			)
			require.True(t, ok)

			fields := requireProxyWireFields[codecs.AuthConnectRequestFields](
				t,
				pub.lastOffer(),
				archivewire.AuthConnectRequestSbeTemplateID,
			)
			require.Equal(t, tc.want, fields.ClientInfo)
		})
	}
}

func TestProxy_TryConnect_encodedCredentialsOnWire(t *testing.T) {
	credentials := archiveProxyTestCredBytes(0x5A)
	pub, proxy := newTestProxy(Context{
		CredentialsSupplier: stubCredentialsSupplier{encoded: credentials},
	})

	_, ok := proxy.TryConnect(
		archiveProxyTestResponseChannelBytes,
		archiveProxyTestResponseStreamID,
		archiveProxyTestCorrelationID,
	)
	require.True(t, ok)

	fields := requireProxyWireFields[codecs.AuthConnectRequestFields](
		t,
		pub.lastOffer(),
		archivewire.AuthConnectRequestSbeTemplateID,
	)
	require.Equal(t, credentials, fields.EncodedCredentials)
}

func TestProxy_TryCloseSession_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	_, ok := proxy.TryCloseSession(controlResponseTestControlSessionID)
	require.True(t, ok)

	fields := requireProxyWireFields[codecs.CloseSessionRequestFields](
		t,
		pub.lastOffer(),
		archivewire.CloseSessionRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
}

func TestProxy_TryKeepAlive_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	_, ok := proxy.TryKeepAlive(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
	)
	require.True(t, ok)

	fields := requireProxyWireFields[codecs.KeepAliveRequestFields](
		t,
		pub.lastOffer(),
		archivewire.KeepAliveRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
}

func TestProxy_TryChallengeResponse_fieldValuesOnWire(t *testing.T) {
	encodedCredentials := archiveProxyTestCredBytes(0x3C)
	pub, proxy := newTestProxy(Context{})

	_, ok := proxy.TryChallengeResponse(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		encodedCredentials,
	)
	require.True(t, ok)

	fields := requireProxyWireFields[codecs.ChallengeResponseFields](
		t,
		pub.lastOffer(),
		archivewire.ChallengeResponseSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, encodedCredentials, fields.EncodedCredentials)
}

func TestProxy_TryStartRecording_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	channel := []byte("aeron:udp?endpoint=localhost:9999")
	pos, ok := proxy.TryStartRecording(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		archiveProxyTestRecordingStreamID,
		archivewire.SourceLocation_REMOTE,
		channel,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.StartRecordingRequestFields](
		t,
		wire,
		archivewire.StartRecordingRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, archivewire.SourceLocation_REMOTE, fields.SourceLocation)
	require.Equal(t, channel, fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StartRecordingRequestSbeBlockLength,
		len(channel),
	)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryStopRecording_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	channel := []byte("aeron:udp?endpoint=localhost:9999")
	pos, ok := proxy.TryStopRecording(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		archiveProxyTestRecordingStreamID,
		channel,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.StopRecordingRequestFields](
		t,
		wire,
		archivewire.StopRecordingRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingStreamID, fields.StreamID)
	require.Equal(t, channel, fields.Channel)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.StopRecordingRequestSbeBlockLength,
		len(channel),
	)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryListRecording_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	pos, ok := proxy.TryListRecording(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		archiveProxyTestRecordingID,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.ListRecordingRequestFields](
		t,
		wire,
		archivewire.ListRecordingRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingID, fields.RecordingID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(archivewire.ListRecordingRequestSbeBlockLength)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryReplay_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	const (
		replayPosition int64 = 4096
		replayLength   int64 = 8192
		replayStreamID int32 = 1201
	)
	channel := []byte("aeron:ipc?term-length=64k")
	pos, ok := proxy.TryReplay(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		archiveProxyTestRecordingID,
		replayPosition,
		replayLength,
		replayStreamID,
		channel,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.ReplayRequestFields](
		t,
		wire,
		archivewire.ReplayRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, archiveProxyTestRecordingID, fields.RecordingID)
	require.Equal(t, replayPosition, fields.Position)
	require.Equal(t, replayLength, fields.Length)
	require.Equal(t, replayStreamID, fields.ReplayStreamID)
	require.Equal(t, channel, fields.ReplayChannel)

	expectedLen, lenOK := codecs.ReplayRequestWireLen(
		archivewire.MessageHeaderSbeSchemaVersion,
		len(channel),
	)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryStopReplay_fieldValuesOnWire(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	const replaySessionID int64 = 8800
	pos, ok := proxy.TryStopReplay(
		controlResponseTestControlSessionID,
		archiveProxyTestCorrelationID,
		replaySessionID,
	)
	require.True(t, ok)

	wire := pub.lastOffer()
	require.Equal(t, archiveProxyTestOfferPosition+int64(len(wire)), pos)

	fields := requireProxyWireFields[codecs.StopReplayRequestFields](
		t,
		wire,
		archivewire.StopReplayRequestSbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, archiveProxyTestCorrelationID, fields.CorrelationID)
	require.Equal(t, replaySessionID, fields.ReplaySessionID)

	expectedLen, lenOK := codecs.ArchiveWireMessageLen(archivewire.StopReplayRequestSbeBlockLength)
	require.True(t, lenOK)
	require.NotZero(t, expectedLen)
	require.Len(t, wire, expectedLen)
}

func TestProxy_TryChallengeResponse_clearsWireBufferSlackBetweenEncodes(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	longCreds := bytes.Repeat([]byte{0xAB}, archiveProxyWireBufferHighWaterLen)
	_, ok := proxy.TryChallengeResponse(10, 20, longCreds)
	require.True(t, ok)

	backing := proxy.wireBuffer[:cap(proxy.wireBuffer)]
	largeWire := pub.lastOffer()
	require.NotEmpty(t, largeWire)

	shortCreds := []byte("ok")
	smallWireLen, lenOK := codecs.ArchiveWireMessageLen(
		archivewire.ChallengeResponseSbeBlockLength,
		len(shortCreds),
	)
	require.True(t, lenOK)
	require.NotZero(t, smallWireLen)
	require.Less(t, smallWireLen, len(largeWire))

	const sentinel = 0xCD
	for i := smallWireLen; i < len(backing); i++ {
		backing[i] = sentinel
	}

	_, ok = proxy.TryChallengeResponse(11, 21, shortCreds)
	require.True(t, ok)
	require.Len(t, pub.lastOffer(), smallWireLen)

	for i := smallWireLen; i < len(backing); i++ {
		require.Zero(t, backing[i], "slack byte %d not cleared between challenge encodes", i)
	}
}

func TestProxy_tryOffer_negativeOfferCodes(t *testing.T) {
	cases := []struct {
		name        string
		offerResult int64
	}{
		{name: "back_pressured", offerResult: aeron.BackPressured},
		{name: "admin_action", offerResult: aeron.AdminAction},
		{name: "not_connected", offerResult: aeron.NotConnected},
		{name: "closed", offerResult: aeron.Closed},
		{name: "max_position_exceeded", offerResult: aeron.MaxPositionExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub, proxy := newTestProxy(Context{})
			pub.offerResult = tc.offerResult

			pos, ok := proxy.TryKeepAlive(1, 2)
			require.False(t, ok)
			require.Equal(t, tc.offerResult, pos)
			require.NotEmpty(t, pub.lastOffer())
		})
	}
}

func TestProxy_tryOffer_encodeFailure(t *testing.T) {
	pub, proxy := newTestProxy(Context{})

	pos, ok := proxy.tryOffer(32, true, func([]byte) (int, bool) {
		return 0, false
	})
	require.False(t, ok)
	require.Zero(t, pos)
	require.Empty(t, pub.offered)
}

func TestProxy_encodePreflightFailure(t *testing.T) {
	oversized := make([]byte, codecs.ArchiveEncodeMaxVarLen+1)

	cases := []struct {
		name  string
		ctx   Context
		offer func(proxy *Proxy) (int64, bool)
	}{
		{
			name: "try_connect_response_channel",
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryConnect(oversized, archiveProxyTestResponseStreamID, 1)
			},
		},
		{
			name: "try_challenge_response_credentials",
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryChallengeResponse(1, 2, oversized)
			},
		},
		{
			name: "try_connect_encoded_credentials",
			ctx: Context{
				CredentialsSupplier: stubCredentialsSupplier{encoded: oversized},
			},
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryConnect(
					archiveProxyTestResponseChannelBytes,
					archiveProxyTestResponseStreamID,
					1,
				)
			},
		},
		{
			name: "try_start_recording_channel",
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryStartRecording(
					controlResponseTestControlSessionID,
					archiveProxyTestCorrelationID,
					archiveProxyTestRecordingStreamID,
					archivewire.SourceLocation_LOCAL,
					oversized,
				)
			},
		},
		{
			name: "try_stop_recording_channel",
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryStopRecording(
					controlResponseTestControlSessionID,
					archiveProxyTestCorrelationID,
					archiveProxyTestRecordingStreamID,
					oversized,
				)
			},
		},
		{
			name: "try_replay_replay_channel",
			offer: func(proxy *Proxy) (int64, bool) {
				return proxy.TryReplay(
					controlResponseTestControlSessionID,
					archiveProxyTestCorrelationID,
					archiveProxyTestRecordingID,
					4096,
					8192,
					int32(1201),
					oversized,
				)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub, proxy := newTestProxy(tc.ctx)

			pos, ok := tc.offer(proxy)
			require.False(t, ok)
			require.Zero(t, pos)
			require.Empty(t, pub.offered)
		})
	}
}

func TestProxy_resizeWireBuffer_clearsAbandonedBackingOnGrow(t *testing.T) {
	_, proxy := newTestProxy(Context{})

	shortCreds := []byte("x")
	_, ok := proxy.TryChallengeResponse(1, 2, shortCreds)
	require.True(t, ok)

	oldBacking := proxy.wireBuffer[:cap(proxy.wireBuffer)]
	const sentinel = 0xCC
	for i := range oldBacking {
		oldBacking[i] = sentinel
	}

	longCreds := bytes.Repeat([]byte{0xAB}, archiveProxyWireBufferHighWaterLen)
	_, ok = proxy.TryChallengeResponse(3, 4, longCreds)
	require.True(t, ok)
	require.Greater(t, cap(proxy.wireBuffer), len(oldBacking))

	for i := range oldBacking {
		require.Zero(t, oldBacking[i], "abandoned backing byte %d not cleared on grow", i)
	}

	evenLongerCreds := bytes.Repeat([]byte{0xBB}, archiveProxyWireBufferHighWaterLen+100)
	_, ok = proxy.TryChallengeResponse(5, 6, evenLongerCreds)
	require.True(t, ok)
	require.Greater(t, cap(proxy.wireBuffer), len(oldBacking))
}

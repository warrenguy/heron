//go:build testhooks

package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

type asyncConnectCredentialsSupplier struct {
	encodedLen   int
	challengeLen int
}

func (supplier asyncConnectCredentialsSupplier) EncodedCredentials() []byte {
	if supplier.encodedLen == 0 {
		return nil
	}
	return make([]byte, supplier.encodedLen)
}

func (supplier asyncConnectCredentialsSupplier) RespondToChallenge([]byte) []byte {
	if supplier.challengeLen == 0 {
		return nil
	}
	return make([]byte, supplier.challengeLen)
}

type challengeCapturingCredentialsSupplier struct {
	captured *[]byte
}

func (supplier challengeCapturingCredentialsSupplier) EncodedCredentials() []byte {
	return nil
}

func (supplier challengeCapturingCredentialsSupplier) RespondToChallenge(
	encodedChallenge []byte,
) []byte {
	*supplier.captured = encodedChallenge
	return nil
}

func newAsyncConnectFixtureWithCredentialLens(
	t *testing.T,
	encodedLen, challengeLen int,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	return newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext {
		ctx.CredentialsSupplier = asyncConnectCredentialsSupplier{
			encodedLen:   encodedLen,
			challengeLen: challengeLen,
		}
		return ctx
	})
}

func setupAsyncConnectAtPollResponse(t *testing.T) *AsyncConnect {
	t.Helper()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)
	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})
	return ac
}

func setupAsyncConnectPreparedSendMessage(t *testing.T) (*AsyncConnect, *stubControlPublication) {
	t.Helper()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)
	require.NoError(t, ac.PrepareConnectRequestForTest())

	pub := newStubControlPublication()
	ac.transport.requestPublication = pub
	ac.state = ConnectStateSendMessage
	return ac, pub
}

func TestAsyncConnect_pollResponse_ignoresRecordingSignalEventDuringHandshake(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)

	correlation := ac.CorrelationIDForTest()
	require.NotEqual(t, conductor.NullCorrelationID, correlation)

	termBuffer := controlResponseTermBufferForAsyncConnect(t, ac)
	writeRecordingSignalEventForConnect(t, termBuffer, correlation)
	result, err := ac.Poll()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStatePollResponse, ac.State())
}

func TestAsyncConnect_pollResponse_ignoresMismatchedControlResponseCorrelation(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)

	termBuffer := controlResponseTermBufferForAsyncConnect(t, ac)
	correlation := ac.CorrelationIDForTest()
	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, codecs.ControlResponseFields{
			ControlSessionID: asyncConnectFixtureControlSessionID,
			CorrelationID:    correlation + 999,
			RelevantID:       asyncConnectFixtureArchiveID,
			Code:             archivewire.ControlResponseCode_OK,
			Version:          asyncConnectFixtureServerVersion,
		}),
	)
	result, err := ac.Poll()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStatePollResponse, ac.State())

	appendControlResponseOKForConnect(t, termBuffer, correlation)
	_, err = ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateConcludeConnect, ac.State())

	result, err = ac.Poll()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
}

func TestAsyncConnect_pollResponse_ignoresMismatchedChallengeCorrelation(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)

	connectCorrelation := ac.CorrelationIDForTest()
	termBuffer := controlResponseTermBufferForAsyncConnect(t, ac)
	writeChallengeForConnect(t, termBuffer, connectCorrelation+999)
	result, err := ac.Poll()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStatePollResponse, ac.State())

	appendChallengeForConnect(t, termBuffer, connectCorrelation)
	result, err = ac.Poll()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStateSendMessage, ac.State())

	challengeCorrelation := ac.CorrelationIDForTest()
	require.NotEqual(t, conductor.NullCorrelationID, challengeCorrelation)
	require.NotEqual(t, connectCorrelation, challengeCorrelation)

	wireLen := ac.ConnectWireLengthForTest()
	require.Positive(t, wireLen)
	decoded, ok := codecs.TryDecodeByTemplate(
		ac.ConnectWireBufferForTest()[:wireLen],
		0,
		codecs.ArchiveDecodeMaxVarLen,
	)
	require.True(t, ok)
	require.Equal(t, archivewire.ChallengeResponseSbeTemplateID, decoded.TemplateID)
	challengeFields, ok := decoded.Fields.(codecs.ChallengeResponseFields)
	require.True(t, ok)
	require.Equal(t, challengeCorrelation, challengeFields.CorrelationID)
}

func TestAsyncConnect_pollResponse_controlResponseError(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)

	withAsyncConnectControlResponseTerm(
		t,
		ac,
		func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
			writeUnfragmentedWireToTerm(
				t,
				termBuffer,
				encodeControlResponseWire(t, codecs.ControlResponseFields{
					ControlSessionID: asyncConnectFixtureControlSessionID,
					CorrelationID:    correlation,
					RelevantID:       ArchiveErrorAuthenticationRejected,
					Code:             archivewire.ControlResponseCode_ERROR,
					ErrorMessage:     []byte("bad credentials"),
				}),
			)
		},
	)
	_, err := ac.Poll()
	require.Error(t, err)
	var archiveErr *ArchiveException
	require.ErrorAs(t, err, &archiveErr)
	require.Equal(t, int32(ArchiveErrorAuthenticationRejected), archiveErr.ErrorCode)
	require.Contains(t, archiveErr.Error(), "authentication rejected")
}

func TestAsyncConnect_prepareConnectRequest_unresolvedResponseChannel(t *testing.T) {
	t.Parallel()

	_, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)

	err := ac.PrepareConnectRequestForTest()
	require.EqualError(
		t,
		err,
		"archive client: control response subscription cannot resolve response channel",
	)
}

func TestAsyncConnect_prepareConnectRequest_credentialsSizing(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixtureWithCredentialLens(
		t,
		connectEncodedCredentialsPastLayoutMax,
		0,
	)
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	requireConnectRequestWireLength(
		t,
		ac,
		archiveConnectResponseChannelLen,
		connectEncodedCredentialsPastLayoutMax,
		archiveConnectClientInfoLen,
	)
}

func TestAsyncConnect_prepareChallengeResponse_credentialsSizing(t *testing.T) {
	t.Parallel()

	_, ac := newAsyncConnectFixtureWithCredentialLens(t, 0, challengeResponseCredentialsAtLayoutMax)
	registerAsyncConnectClose(t, ac)
	ac.wire.encodedChallenge = []byte("challenge")
	ac.session.controlSessionID = asyncConnectFixtureControlSessionID

	require.NoError(t, ac.PrepareChallengeResponseForTest())
	requireChallengeResponseWireLength(t, ac, challengeResponseCredentialsAtLayoutMax)
}

func TestAsyncConnect_prepareChallengeResponse_nilChallengeNormalized(t *testing.T) {
	t.Parallel()

	var captured []byte
	_, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext {
		ctx.CredentialsSupplier = challengeCapturingCredentialsSupplier{captured: &captured}
		return ctx
	})
	registerAsyncConnectClose(t, ac)
	ac.wire.encodedChallenge = nil
	ac.session.controlSessionID = asyncConnectFixtureControlSessionID

	require.NoError(t, ac.PrepareChallengeResponseForTest())
	require.NotNil(t, captured)
	require.Empty(t, captured)
}

func TestAsyncConnect_pollResponse_emptyChallengeNormalized(t *testing.T) {
	t.Parallel()

	var captured []byte
	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext {
		ctx.CredentialsSupplier = challengeCapturingCredentialsSupplier{captured: &captured}
		return ctx
	})
	registerAsyncConnectClose(t, ac)
	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})

	withAsyncConnectControlResponseTerm(
		t,
		ac,
		func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
			writeUnfragmentedWireToTerm(
				t,
				termBuffer,
				encodeChallengeWire(
					t,
					asyncConnectFixtureControlSessionID,
					correlation,
					nil,
				),
			)
		},
	)
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateSendMessage, ac.State())
	require.NotNil(t, captured)
	require.Empty(t, captured)
}

func TestAsyncConnect_prepareConnectRequest_encodeFailure(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)

	setupAsyncConnectControlReady(t, harness, ac)
	ac.ctx.CredentialsSupplier = asyncConnectCredentialsSupplier{
		encodedLen: codecs.ArchiveEncodeMaxVarLen + 1,
	}
	err := ac.PrepareConnectRequestForTest()
	require.EqualError(t, err, "archive client: auth connect request wire length invalid")
}

func TestAsyncConnect_prepareChallengeResponse_encodeFailure(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)

	withAsyncConnectControlResponseTerm(
		t,
		ac,
		func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
			writeChallengeForConnect(t, termBuffer, correlation)
		},
	)
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateSendMessage, ac.State())

	ac.session.correlationID = conductor.NullCorrelationID
	ac.ctx.CredentialsSupplier = asyncConnectCredentialsSupplier{
		challengeLen: codecs.ArchiveEncodeMaxVarLen + 1,
	}
	err = ac.PrepareChallengeResponseForTest()
	require.EqualError(t, err, "archive client: challenge response wire length invalid")
}

func TestAsyncConnect_timeout(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	ctx := validArchiveConnectFixtureContext()
	ctx.Aeron = harness.Client
	ctx.ConnectTimeout = 5 * time.Millisecond
	ctx.AeronContext.NanoClock = func() int64 { return time.Now().UnixNano() }
	ac, err := NewAsyncConnect(ctx)
	require.NoError(t, err)
	registerAsyncConnectClose(t, ac)

	deadline := time.Now().Add(200 * time.Millisecond)
	var pollErr error
	for time.Now().Before(deadline) && pollErr == nil {
		harness.DoWork()
		harness.RespondPendingAdds()
		_, pollErr = ac.Poll()
	}
	require.Error(t, pollErr)
	require.ErrorContains(t, pollErr, "archive client: connect timeout in state")
	require.ErrorContains(t, pollErr, DefaultLocalControlChannel)
}

func TestAsyncConnect_pollSendMessage_backPressuredStaysInSendMessage(t *testing.T) {
	t.Parallel()

	ac, pub := setupAsyncConnectPreparedSendMessage(t)
	pub.offerResult = aeron.BackPressured

	result, err := ac.PollSendMessageForTest()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStateSendMessage, ac.State())
	require.Len(t, pub.offered, 1)
}

func TestAsyncConnect_pollSendMessage_terminalErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		offerResult int64
		wantSubstr  string
	}{
		{name: "closed", offerResult: aeron.Closed, wantSubstr: "unexpected loss of connection"},
		{
			name:        "notConnected",
			offerResult: aeron.NotConnected,
			wantSubstr:  "unexpected loss of connection",
		},
		{
			name:        "maxPositionExceeded",
			offerResult: aeron.MaxPositionExceeded,
			wantSubstr:  "stream position exceeded",
		},
		{
			name:        "unsupportedMessageLength",
			offerResult: aeron.UnsupportedMessageLength,
			wantSubstr:  "max payload length",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ac, pub := setupAsyncConnectPreparedSendMessage(t)
			pub.offerResult = tc.offerResult

			_, err := ac.PollSendMessageForTest()
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantSubstr)
		})
	}
}

func TestAsyncConnect_pollSendMessage_fragmentRequiredWireFailsFast(t *testing.T) {
	t.Parallel()

	pub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)
	maxPayloadLength := pub.MaxPayloadLength()
	maxMessageLength := pub.MaxMessageLength()
	require.Equal(t, 8192, maxMessageLength)
	require.Greater(t, maxMessageLength, maxPayloadLength)

	var credentialsLen int
	var wantWireLen int
	for credentialsLen = 1; credentialsLen <= codecs.ArchiveEncodeMaxVarLen; credentialsLen++ {
		wireLen, ok := authConnectRequestWireLen(
			archiveConnectResponseChannelLen,
			credentialsLen,
			archiveConnectClientInfoLen,
		)
		if !ok {
			continue
		}
		if wireLen > maxPayloadLength && wireLen <= maxMessageLength {
			wantWireLen = wireLen
			break
		}
	}
	require.Positive(t, wantWireLen, "need connect wire in (maxPayloadLength, maxMessageLength]")

	harness, ac := newAsyncConnectFixtureWithCredentialLens(t, credentialsLen, 0)
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())

	ac.transport.requestPublication = pub
	ac.state = ConnectStateSendMessage

	start := time.Now()
	_, err := ac.PollSendMessageForTest()
	require.Error(t, err)
	require.ErrorContains(t, err, "max payload length")
	require.ErrorContains(t, err, "does not fragment")
	require.Less(t, time.Since(start), time.Second)
}

func TestAsyncConnect_pollSendMessage_oversizeWireFailsFast(t *testing.T) {
	t.Parallel()

	pub := aeron.NewTestExclusivePublicationForHooksWithConnected(t, true)
	maxMessageLength := pub.MaxMessageLength()
	require.Equal(t, 8192, maxMessageLength)

	wantWireLen, ok := authConnectRequestWireLen(
		archiveConnectResponseChannelLen,
		pollSendMessageOversizeWireCredentialsLen,
		archiveConnectClientInfoLen,
	)
	require.True(t, ok)
	require.Greater(t, wantWireLen, maxMessageLength)
	require.Less(t, wantWireLen, logbuffer.MaxMessageLength)

	harness, ac := newAsyncConnectFixtureWithCredentialLens(
		t,
		pollSendMessageOversizeWireCredentialsLen,
		0,
	)
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())

	ac.transport.requestPublication = pub
	ac.state = ConnectStateSendMessage

	start := time.Now()
	_, err := ac.PollSendMessageForTest()
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds control publication max message length")
	require.Less(t, time.Since(start), time.Second)
}

func TestAsyncConnect_resizeConnectWireBuffer_clearsAbandonedBackingOnGrow(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	buf := ac.ConnectWireBufferForTest()
	require.NotNil(t, buf)
	oldBacking := buf[:cap(buf)]
	smallWireLen := ac.ConnectWireLengthForTest()
	require.Equal(t, len(oldBacking), smallWireLen)

	const sentinel = 0xCC
	for i := range oldBacking {
		oldBacking[i] = sentinel
	}

	ac.ctx.CredentialsSupplier = asyncConnectCredentialsSupplier{
		challengeLen: connectWireBufferCloseHighWaterCredentialsLen,
	}
	ac.wire.encodedChallenge = []byte("challenge")
	ac.session.controlSessionID = asyncConnectFixtureControlSessionID
	require.NoError(t, ac.PrepareChallengeResponseForTest())

	newBuf := ac.ConnectWireBufferForTest()
	require.NotNil(t, newBuf)
	require.Greater(t, cap(newBuf), cap(oldBacking))

	for i := range oldBacking {
		require.Zero(t, oldBacking[i], "abandoned backing byte %d not cleared on grow", i)
	}
}

func TestAsyncConnect_prepareChallengeResponse_clearsConnectWireBufferSlack(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixtureWithCredentialLens(
		t,
		connectWireBufferCloseHighWaterCredentialsLen,
		0,
	)
	registerAsyncConnectClose(t, ac)

	backing, smallWireLen := establishConnectWireBufferHighWater(t, harness, ac)
	requireConnectWireSlackCleared(
		t,
		backing,
		smallWireLen,
		"slack byte %d retained prior connect credentials",
	)
}

func TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnClose(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixtureWithCredentialLens(
		t,
		connectWireBufferCloseHighWaterCredentialsLen,
		0,
	)

	backing, wireLen := establishConnectWireBufferHighWater(t, harness, ac)
	plantConnectWireBufferSlackSentinels(backing, wireLen, 0xCC)

	require.NoError(t, ac.Close())
	requireConnectWireSlackCleared(t, backing, wireLen, "slack byte %d not cleared on Close")
	require.Nil(t, ac.ConnectWireBufferForTest())
}

func TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnSuccess(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixtureWithCredentialLens(
		t,
		connectWireBufferPollHighWaterCredentialsLen,
		connectWireBufferSlackChallengeLen,
	)
	registerAsyncConnectClose(t, ac)
	setupAsyncConnectControlReady(t, harness, ac)

	var backing []byte
	var slackWireLen int
	sentinelsPlanted := false
	var roundTrip asyncConnectChallengeRoundTripState

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		if capturedBacking, capturedWireLen := captureConnectWireBufferSlackOnce(
			&sentinelsPlanted,
			ac,
			roundTrip.challengeWritten,
			0xDD,
		); capturedBacking != nil {
			backing = capturedBacking
			slackWireLen = capturedWireLen
		}
		if roundTrip.controlResponseWritten {
			return
		}
		roundTrip.onStep(t, ac)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, roundTrip.challengeWritten)
	require.True(t, roundTrip.challengeResponseOffered)
	require.True(t, roundTrip.controlResponseWritten)
	require.True(t, sentinelsPlanted)
	require.NotNil(t, backing)
	requireConnectWireSlackCleared(t, backing, slackWireLen, "slack byte %d not cleared on success")
	require.Nil(t, ac.ConnectWireBufferForTest())
}

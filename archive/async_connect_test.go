//go:build testhooks

package archive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
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

	return newAsyncConnectFixture(t, func(cfg Config) Config {
		cfg.CredentialsSupplier = asyncConnectCredentialsSupplier{
			encodedLen:   encodedLen,
			challengeLen: challengeLen,
		}
		return cfg
	})
}

func setupAsyncConnectAtPollResponse(t *testing.T) *AsyncConnect {
	t.Helper()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)
	pollUntilConnectPollResponse(t, harness, ac, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	})
	return ac
}

// setupAsyncConnectAtArchiveIDResponse returns an AsyncConnect in
// ConnectStateAwaitArchiveIDResponse and its control response term buffer.
func setupAsyncConnectAtArchiveIDResponse(
	t *testing.T,
) (*AsyncConnect, atomicbuffer.AtomicBuffer) {
	t.Helper()

	ac := setupAsyncConnectAtPollResponse(t)
	termBuffer := controlResponseTermBufferForAsyncConnect(t, ac)
	writeControlResponseOKForConnect(t, termBuffer, ac.CorrelationIDForTest())
	for _, want := range []ConnectState{
		ConnectStateSendArchiveIDRequest,
		ConnectStateAwaitArchiveIDResponse,
	} {
		result, err := ac.Poll()
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, want, ac.State())
	}
	return ac, termBuffer
}

func setupAsyncConnectPreparedSendMessage(t *testing.T) (*AsyncConnect, *stubControlPublication) {
	t.Helper()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
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
			RelevantID:       asyncConnectFixtureControlSessionID,
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
	require.Equal(t, ConnectStateSendArchiveIDRequest, ac.State())
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

func TestAsyncConnect_pollResponse_ignoresMismatchedArchiveIDCorrelation(t *testing.T) {
	t.Parallel()

	ac, termBuffer := setupAsyncConnectAtArchiveIDResponse(t)
	correlation := ac.CorrelationIDForTest()

	appendArchiveIDResponseOK(t, termBuffer, correlation+999)
	result, err := ac.Poll()
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, ConnectStateAwaitArchiveIDResponse, ac.State())

	appendArchiveIDResponseOK(t, termBuffer, correlation)
	_, err = ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateConcludeConnect, ac.State())

	result, err = ac.Poll()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.Equal(t, asyncConnectFixtureArchiveID, result.ArchiveID)
	require.Equal(t, asyncConnectFixtureServerVersion, result.ServerVersion)
}

func TestAsyncConnect_pollResponse_archiveIDResponseError(t *testing.T) {
	t.Parallel()

	ac, termBuffer := setupAsyncConnectAtArchiveIDResponse(t)
	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, codecs.ControlResponseFields{
			ControlSessionID: asyncConnectFixtureControlSessionID,
			CorrelationID:    ac.CorrelationIDForTest(),
			RelevantID:       ErrorGeneric,
			Code:             archivewire.ControlResponseCode_ERROR,
			ErrorMessage:     []byte("archive id unavailable"),
		}),
	)
	_, err := ac.Poll()
	require.Error(t, err)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
	require.Equal(t, int32(ErrorGeneric), archiveErr.ErrorCode)
	require.Contains(t, archiveErr.Error(), "archive id unavailable")

	pub := newStubControlPublication()
	ac.transport.requestPublication = pub
	require.NoError(t, ac.Close())
	require.True(t, pub.closed)
	require.Len(t, pub.offered, 1)
	fields := requireProxyWireFields[codecs.CloseSessionRequestFields](
		t,
		pub.offered[0],
		archivewire.CloseSessionRequestSbeTemplateID,
	)
	require.Equal(t, asyncConnectFixtureControlSessionID, fields.ControlSessionID)
}

func TestAsyncConnect_pollResponse_legacyServerSkipsArchiveIDRequest(t *testing.T) {
	t.Parallel()

	ac := setupAsyncConnectAtPollResponse(t)
	fields := connectControlResponseOKFields(ac.CorrelationIDForTest())
	fields.Version = asyncConnectFixtureLegacyServerVersion
	writeUnfragmentedWireToTerm(
		t,
		controlResponseTermBufferForAsyncConnect(t, ac),
		encodeControlResponseWire(t, fields),
	)
	_, err := ac.Poll()
	require.NoError(t, err)
	require.Equal(t, ConnectStateConcludeConnect, ac.State())

	result, err := ac.Poll()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.Equal(t, cnc.NullValue, result.ArchiveID)
	require.Equal(t, asyncConnectFixtureLegacyServerVersion, result.ServerVersion)
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
					RelevantID:       ErrorAuthenticationRejected,
					Code:             archivewire.ControlResponseCode_ERROR,
					ErrorMessage:     []byte("bad credentials"),
				}),
			)
		},
	)
	_, err := ac.Poll()
	require.Error(t, err)
	var archiveErr *Exception
	require.ErrorAs(t, err, &archiveErr)
	require.Equal(t, int32(ErrorAuthenticationRejected), archiveErr.ErrorCode)
	require.Contains(t, archiveErr.Error(), "authentication rejected")

	pub := newStubControlPublication()
	ac.transport.requestPublication = pub
	require.NoError(t, ac.Close())
	require.True(t, pub.closed)
	require.Empty(t, pub.offered, "no session to close before the connect OK")
}

func TestAsyncConnect_prepareConnectRequest_unresolvedResponseChannel(t *testing.T) {
	t.Parallel()

	_, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)

	err := ac.PrepareConnectRequestForTest()
	require.EqualError(
		t,
		err,
		"archive: control response subscription cannot resolve response channel",
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
	_, ac := newAsyncConnectFixture(t, func(cfg Config) Config {
		cfg.CredentialsSupplier = challengeCapturingCredentialsSupplier{captured: &captured}
		return cfg
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
	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config {
		cfg.CredentialsSupplier = challengeCapturingCredentialsSupplier{captured: &captured}
		return cfg
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

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)

	setupAsyncConnectControlReady(t, harness, ac)
	ac.cfg.CredentialsSupplier = asyncConnectCredentialsSupplier{
		encodedLen: codecs.ArchiveEncodeMaxVarLen + 1,
	}
	err := ac.PrepareConnectRequestForTest()
	require.EqualError(t, err, "archive: auth connect request wire length invalid")
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
	ac.cfg.CredentialsSupplier = asyncConnectCredentialsSupplier{
		challengeLen: codecs.ArchiveEncodeMaxVarLen + 1,
	}
	err = ac.PrepareChallengeResponseForTest()
	require.EqualError(t, err, "archive: challenge response wire length invalid")
}

func TestAsyncConnect_timeout(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := validConnectFixtureConfig()
	cfg.Aeron = harness.Client
	cfg.ConnectTimeout = 5 * time.Millisecond
	cfg.AeronConfig.NanoClock = func() int64 { return time.Now().UnixNano() }
	ac, err := NewAsyncConnect(t.Context(), cfg)
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
	require.ErrorContains(t, pollErr, "archive: connect timeout in state")
	require.ErrorContains(t, pollErr, DefaultLocalControlChannel)
}

// TestAsyncConnect_ownedClientFaultFailsPoll asserts that a fault that
// terminates the owned transport client fails Poll with the cause. The fault
// check runs before the connect deadline check. A later Poll returns the
// same error.
func TestAsyncConnect_ownedClientFaultFailsPoll(t *testing.T) {
	t.Parallel()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := ownedConnectConfigForTest(t, harness)
	cfg.ConnectTimeout = time.Minute
	ac, err := NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	registerAsyncConnectClose(t, ac)
	require.True(t, ac.OwnsAeronClient())

	// Phase: the control response subscription add completes.
	setupAsyncConnectControlReady(t, harness, ac)
	require.Equal(t, ConnectStateCreateControlRequestPublication, ac.State())

	// Phase: the client timeout terminates the owned client. The fault wins
	// over the expired deadline.
	transmitClientTimeout(t, harness)
	harness.DoWork()
	require.True(t, harness.Client.IsTerminating())
	ac.deadlineNs = ac.nanoClock() - 1

	result, err := ac.Poll()
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrTransportTerminating)
	require.ErrorIs(t, err, conductor.ErrClientTimeout)

	// Phase: a later Poll returns the same error.
	result, again := ac.Poll()
	require.Nil(t, result)
	require.Same(t, err, again)

	// Phase: Close closes the owned client.
	require.NoError(t, ac.Close())
	require.True(t, aeron.ClientIsClosedForTest(harness.Client))
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
			wantSubstr:  "max message length",
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

func TestAsyncConnect_pollSendMessage_fragmentRequiredWireOffers(t *testing.T) {
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

	_, err := ac.PollSendMessageForTest()
	require.NoError(t, err)
	require.Equal(t, ConnectStatePollResponse, ac.State())
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

	_, err := ac.PollSendMessageForTest()
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds control publication max message length")
}

func TestAsyncConnect_resizeConnectWireBuffer_clearsAbandonedBackingOnGrow(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
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

	ac.cfg.CredentialsSupplier = asyncConnectCredentialsSupplier{
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

// TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnSuccess plants
// sentinels in the wire slack of the last encode before the connect concludes:
// the archive id request on a current server, the challenge response on a
// legacy server. Both must leave the slack cleared on success.
func TestAsyncConnect_zeroSensitiveBuffers_clearsConnectWireCapOnSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		legacyServer bool
		plantReady   func(*AsyncConnect, *asyncConnectChallengeRoundTripState) bool
	}{
		{
			name: "archiveIDRequest",
			plantReady: func(ac *AsyncConnect, _ *asyncConnectChallengeRoundTripState) bool {
				return ac.State() == ConnectStateAwaitArchiveIDResponse
			},
		},
		{
			name:         "legacyServer",
			legacyServer: true,
			plantReady: func(_ *AsyncConnect, roundTrip *asyncConnectChallengeRoundTripState) bool {
				return roundTrip.challengeWritten
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			roundTrip := asyncConnectChallengeRoundTripState{legacyServer: tt.legacyServer}

			result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
				if capturedBacking, capturedWireLen := captureConnectWireBufferSlackOnce(
					&sentinelsPlanted,
					ac,
					tt.plantReady(ac, &roundTrip),
					0xDD,
				); capturedBacking != nil {
					backing = capturedBacking
					slackWireLen = capturedWireLen
				}
				roundTrip.onStep(t, ac)
			})
			require.NoError(t, err)
			require.NotNil(t, result)
			require.True(t, roundTrip.challengeWritten)
			require.True(t, roundTrip.challengeResponseOffered)
			require.True(t, roundTrip.controlResponseWritten)
			require.Equal(t, !tt.legacyServer, roundTrip.archiveIDResponseWritten)
			require.True(t, sentinelsPlanted)
			require.NotNil(t, backing)
			requireConnectWireSlackCleared(t, backing, slackWireLen, "slack byte %d not cleared on success")
			require.Nil(t, ac.ConnectWireBufferForTest())
		})
	}
}

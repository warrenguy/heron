//go:build testhooks

package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

func TestAsyncConnect_connectAndKeepAliveRoundtrip(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)

	var connectWire []byte
	var connectCorrelation int64
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		if ac.State() == ConnectStatePollResponse {
			connectCorrelation = ac.CorrelationIDForTest()
			wireLen := ac.ConnectWireLengthForTest()
			require.Positive(t, wireLen)
			connectWire = append([]byte(nil), ac.ConnectWireBufferForTest()[:wireLen]...)
			withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
		}
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.Equal(t, asyncConnectFixtureArchiveID, result.ArchiveID)
	require.Equal(t, asyncConnectFixtureServerVersion, result.ServerVersion)
	require.NotNil(t, result.Proxy)
	require.NotNil(t, result.Poller)
	require.NotNil(t, result.ControlPublication)
	require.NotNil(t, result.ControlSubscription)
	require.False(t, result.OwnsAeronClient)

	require.NotEqual(t, conductor.NullCorrelationID, connectCorrelation)
	require.NotEmpty(t, connectWire)
	decoded, ok := codecs.TryDecodeByTemplate(connectWire, 0, codecs.ArchiveDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, gensbe.AuthConnectRequestSbeTemplateID, decoded.TemplateID)
	authFields, ok := decoded.Fields.(codecs.AuthConnectRequestFields)
	require.True(t, ok)
	require.Equal(t, connectCorrelation, authFields.CorrelationID)

	keepAliveCorrelation := harness.Client.NextCorrelationID()
	archiveCtx := result.Context
	archiveCtx.ControlOfferRetryTimeout = 5 * time.Second
	pos := ControlOfferUntilPositive(harness.Client, archiveCtx, ControlOfferConfig{
		IdleStrategy: NewDefaultIdleStrategy(),
		NanoClock:    result.Context.AeronContext.NanoClock,
		PollFn:       result.Poller.Poll,
	}, func() int64 {
		position, _ := result.Proxy.TryKeepAlive(
			result.ControlSessionID,
			keepAliveCorrelation,
		)
		return position
	})
	require.Positive(t, pos)

	termBuffer := controlResponseTermBufferForSubscription(t, result.ControlSubscription)
	appendControlResponseOKForConnect(t, termBuffer, keepAliveCorrelation)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		result.Poller.Poll()
		resp, ok := result.Poller.Response()
		if ok && resp.CorrelationID == keepAliveCorrelation {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, result.Poller.IsPollComplete())
	resp, ok := result.Poller.Response()
	require.True(t, ok)
	require.Equal(t, keepAliveCorrelation, resp.CorrelationID)
	require.Equal(t, gensbe.ControlResponseCode_OK, resp.Code)

	err = MapControlResponse(resp)
	require.NoError(t, err)
}

func TestAsyncConnect_happyPathIPC(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.NotNil(t, result.ControlPublication)
	require.NotNil(t, result.ControlSubscription)
	require.False(t, result.OwnsAeronClient)
	require.Equal(t, ConnectStateDone, ac.State())
}

type asyncConnectChallengeRoundTripState struct {
	challengeWritten         bool
	challengeResponseOffered bool
	controlResponseWritten   bool
}

func (state *asyncConnectChallengeRoundTripState) onStep(
	t *testing.T,
	ac *AsyncConnect,
) {
	t.Helper()

	if state.challengeWritten && ac.State() == ConnectStateSendMessage {
		state.challengeResponseOffered = true
	}
	withAsyncConnectControlResponseTerm(
		t,
		ac,
		func(t *testing.T, termBuffer atomic.AtomicBuffer, correlation int64) {
			switch {
			case !state.challengeWritten && ac.State() == ConnectStatePollResponse:
				writeChallengeForConnect(t, termBuffer, correlation)
				state.challengeWritten = true
			case state.challengeResponseOffered && !state.controlResponseWritten &&
				ac.State() == ConnectStatePollResponse:
				appendControlResponseOKForConnect(t, termBuffer, correlation)
				state.controlResponseWritten = true
			}
		},
	)
}

func TestAsyncConnect_challengeRoundTrip(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(ctx ArchiveContext) ArchiveContext { return ctx })
	registerAsyncConnectClose(t, ac)

	var roundTrip asyncConnectChallengeRoundTripState
	var connectCorrelation int64
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		if ac.State() == ConnectStatePollResponse && connectCorrelation == 0 {
			connectCorrelation = ac.CorrelationIDForTest()
		}
		roundTrip.onStep(t, ac)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, roundTrip.challengeWritten)
	require.True(t, roundTrip.challengeResponseOffered)
	require.True(t, roundTrip.controlResponseWritten)
	require.Equal(t, ConnectStateDone, ac.State())
	require.NotEqual(t, conductor.NullCorrelationID, connectCorrelation)
}

//go:build testhooks

package archive

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestAsyncConnect_connectAndKeepAliveRoundtrip(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)

	var connectWire, archiveIDWire []byte
	var connectCorrelation, archiveIDCorrelation int64
	captureWire := func(ac *AsyncConnect) (int64, []byte) {
		wireLen := ac.ConnectWireLengthForTest()
		require.Positive(t, wireLen)
		return ac.CorrelationIDForTest(),
			append([]byte(nil), ac.ConnectWireBufferForTest()[:wireLen]...)
	}
	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		switch ac.State() {
		case ConnectStatePollResponse:
			connectCorrelation, connectWire = captureWire(ac)
		case ConnectStateAwaitArchiveIDResponse:
			archiveIDCorrelation, archiveIDWire = captureWire(ac)
		}
		writeConnectHandshakeResponses(t, ac)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.Equal(t, asyncConnectFixtureArchiveID, result.ArchiveID)
	require.NotEqual(t, result.ControlSessionID, result.ArchiveID)
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
	require.Equal(t, archivewire.AuthConnectRequestSbeTemplateID, decoded.TemplateID)
	authFields, ok := decoded.Fields.(codecs.AuthConnectRequestFields)
	require.True(t, ok)
	require.Equal(t, connectCorrelation, authFields.CorrelationID)

	require.NotEmpty(t, archiveIDWire)
	decoded, ok = codecs.TryDecodeByTemplate(archiveIDWire, 0, codecs.ArchiveDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, archivewire.ArchiveIdRequestSbeTemplateID, decoded.TemplateID)
	archiveIDFields, ok := decoded.Fields.(codecs.ArchiveIDRequestFields)
	require.True(t, ok)
	require.Equal(t, asyncConnectFixtureControlSessionID, archiveIDFields.ControlSessionID)
	require.Equal(t, archiveIDCorrelation, archiveIDFields.CorrelationID)
	require.NotEqual(t, connectCorrelation, archiveIDCorrelation)

	keepAliveCorrelation := harness.Client.NextCorrelationID()
	cfg := result.Config
	cfg.ControlOfferRetryTimeout = 5 * time.Second
	pos, err := controlOfferUntilPositive(
		t.Context(),
		harness.Client,
		cfg,
		ControlOfferConfig{PollFn: result.Poller.Poll},
		func() int64 {
			position, _ := result.Proxy.TryKeepAlive(
				result.ControlSessionID,
				keepAliveCorrelation,
			)
			return position
		},
	)
	require.NoError(t, err)
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
		runtime.Gosched()
	}
	require.True(t, result.Poller.IsPollComplete())
	resp, ok := result.Poller.Response()
	require.True(t, ok)
	require.Equal(t, keepAliveCorrelation, resp.CorrelationID)
	require.Equal(t, archivewire.ControlResponseCode_OK, resp.Code)

	err = MapControlResponse(resp)
	require.NoError(t, err)
}

func TestAsyncConnect_happyPathIPC(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)

	result, err := pollAsyncConnectUntil(t, ac, harness, func(ac *AsyncConnect) {
		writeConnectHandshakeResponses(t, ac)
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, asyncConnectFixtureControlSessionID, result.ControlSessionID)
	require.Equal(t, asyncConnectFixtureArchiveID, result.ArchiveID)
	require.NotNil(t, result.ControlPublication)
	require.NotNil(t, result.ControlSubscription)
	require.False(t, result.OwnsAeronClient)
	require.Equal(t, ConnectStateDone, ac.State())
}

func TestAsyncConnect_responseModeRequestChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestChannel  string
		responseChannel string
		// localControl skips WithRemoteControl. IPC-first selection then
		// replaces both control channels.
		localControl bool
		// wantRequestPrefix is the request channel up to the registration id.
		// Empty means the request channel is unchanged.
		wantRequestPrefix string
	}{
		{
			name:              "response_mode",
			requestChannel:    "aeron:udp?endpoint=localhost:8010",
			responseChannel:   "aeron:udp?control-mode=response|control=localhost:10002",
			wantRequestPrefix: "aeron:udp?endpoint=localhost:8010|response-correlation-id=",
		},
		{
			name:            "default_mode_unchanged",
			requestChannel:  "aeron:ipc",
			responseChannel: "aeron:ipc",
		},
		{
			name:            "local_control_ignores_response_mode",
			requestChannel:  "aeron:ipc",
			responseChannel: "aeron:ipc?control-mode=response",
			localControl:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config {
				if !tc.localControl {
					cfg = cfg.WithRemoteControl()
				}
				cfg.ControlRequestChannel = tc.requestChannel
				cfg.ControlResponseChannel = tc.responseChannel
				return cfg
			})
			registerAsyncConnectClose(t, ac)

			setupAsyncConnectControlReady(t, harness, ac)
			require.Equal(t, ConnectStateCreateControlRequestPublication, ac.State())

			wantRequestChannel := tc.requestChannel
			if tc.localControl {
				wantRequestChannel = DefaultLocalControlChannel
			}
			if tc.wantRequestPrefix != "" {
				registrationID := ac.ControlResponseSubscriptionForTest().RegistrationID()
				wantRequestChannel = tc.wantRequestPrefix + strconv.FormatInt(registrationID, 10)
			}
			require.Equal(t, wantRequestChannel, ac.ConfigForTest().EffectiveControlRequestChannel())
		})
	}
}

type asyncConnectChallengeRoundTripState struct {
	// legacyServer answers the connect with a version below 1.11.0, so the
	// connect concludes without an archive id round trip.
	legacyServer             bool
	challengeWritten         bool
	challengeResponseOffered bool
	controlResponseWritten   bool
	archiveIDResponseWritten bool
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
		func(t *testing.T, termBuffer atomicbuffer.AtomicBuffer, correlation int64) {
			switch {
			case !state.challengeWritten && ac.State() == ConnectStatePollResponse:
				writeChallengeForConnect(t, termBuffer, correlation)
				state.challengeWritten = true
			case state.challengeResponseOffered && !state.controlResponseWritten &&
				ac.State() == ConnectStatePollResponse:
				fields := connectControlResponseOKFields(correlation)
				if state.legacyServer {
					fields.Version = asyncConnectFixtureLegacyServerVersion
				}
				appendUnfragmentedWireToTerm(t, termBuffer, encodeControlResponseWire(t, fields))
				state.controlResponseWritten = true
			case state.controlResponseWritten && !state.archiveIDResponseWritten &&
				ac.State() == ConnectStateAwaitArchiveIDResponse:
				appendArchiveIDResponseOK(t, termBuffer, correlation)
				state.archiveIDResponseWritten = true
			}
		},
	)
}

func TestAsyncConnect_challengeRoundTrip(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
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
	require.True(t, roundTrip.archiveIDResponseWritten)
	require.Equal(t, asyncConnectFixtureArchiveID, result.ArchiveID)
	require.Equal(t, ConnectStateDone, ac.State())
	require.NotEqual(t, conductor.NullCorrelationID, connectCorrelation)
}

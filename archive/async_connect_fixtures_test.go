//go:build testhooks

package archive

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

const (
	asyncConnectFixtureControlSessionID int64 = 42
	asyncConnectFixtureArchiveID        int64 = 7
	asyncConnectFixtureTimestamp        int64 = 1700000000123

	asyncConnectPollTimeout    = 5 * time.Second
	asyncConnectHarnessDoWorkN = 4

	archiveConnectResponseChannelLen = len(DefaultLocalControlChannel)

	connectEncodedCredentialsPastLayoutMax  = 1245
	challengeResponseCredentialsAtLayoutMax = 1372

	// pollSendMessageOversizeWireCredentialsLen yields connect wire larger than harness pub max.
	pollSendMessageOversizeWireCredentialsLen = 8430

	connectWireBufferCloseHighWaterCredentialsLen = 1500
	connectWireBufferPollHighWaterCredentialsLen  = 200
	connectWireBufferSlackChallengeLen            = 32

	asyncConnectFixtureChallenge = "challenge-bytes"
)

var archiveConnectClientInfoLen = len(archiveConnectClientInfo(""))

var (
	asyncConnectFixtureServerVersion       = cnc.ComposeSemanticVersion(1, 12, 0)
	asyncConnectFixtureLegacyServerVersion = cnc.ComposeSemanticVersion(1, 10, 0)
)

func validConnectFixtureConfig() Config {
	return Config{
		LocalControlChannel: DefaultLocalControlChannel,
		CredentialsSupplier: NullCredentialsSupplier{},
		IdleStrategy:        NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
			NanoClock: func() int64 { return asyncConnectFixtureTimestamp },
		},
	}.Normalize()
}

func newAsyncConnectFixture(
	t *testing.T,
	mutate func(Config) Config,
) (*aeron.AsyncConnectTestHarness, *AsyncConnect) {
	t.Helper()

	harness := aeron.NewAsyncConnectTestHarness(t)
	cfg := mutate(validConnectFixtureConfig())
	cfg.Aeron = harness.Client
	cfg.ConnectTimeout = 30 * time.Second
	ac, err := NewAsyncConnect(t.Context(), cfg)
	require.NoError(t, err)
	return harness, ac
}

func registerAsyncConnectClose(t *testing.T, ac *AsyncConnect) {
	t.Helper()
	t.Cleanup(func() { _ = ac.Close() })
}

type asyncConnectHarnessResponder func(*aeron.AsyncConnectTestHarness)

func ensureSubscriptionImage(t *testing.T, sub *aeron.Subscription) atomicbuffer.AtomicBuffer {
	t.Helper()

	require.NotNil(t, sub)
	for i := range 8 {
		if sub.IsConnected() {
			break
		}
		image, ok := sub.ImageAt(0)
		if ok && image != nil {
			return aeron.ImageTermBufferForHooks(image)
		}
		image = aeron.NewTestImageForSubscriptionHooks(t, sub, int64(i))
		require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	}
	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	return aeron.ImageTermBufferForHooks(image)
}

func asyncConnectHarnessStep(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
	respond asyncConnectHarnessResponder,
) {
	t.Helper()

	for range asyncConnectHarnessDoWorkN {
		harness.DoWork()
	}
	respond(harness)
	sub := ac.ControlResponseSubscriptionForTest()
	if sub != nil && !sub.IsConnected() {
		ensureSubscriptionImage(t, sub)
	}
}

func failAsyncConnectPollTimeout(t *testing.T, ac *AsyncConnect) {
	t.Helper()

	require.FailNowf(
		t,
		"async connect poll timed out",
		"state=%v correlation=%v",
		ac.State(),
		ac.CorrelationIDForTest(),
	)
}

func pollAsyncConnectLoop(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	respond asyncConnectHarnessResponder,
	onStep func(ac *AsyncConnect),
	done func(ac *AsyncConnect, result *ConnectResult, err error) bool,
) (*ConnectResult, error) {
	t.Helper()

	deadline := time.Now().Add(asyncConnectPollTimeout)
	for time.Now().Before(deadline) {
		asyncConnectHarnessStep(t, harness, ac, respond)
		if onStep != nil {
			onStep(ac)
		}
		result, err := ac.Poll()
		if done(ac, result, err) {
			return result, err
		}
		runtime.Gosched()
	}
	failAsyncConnectPollTimeout(t, ac)
	return nil, nil
}

func pollAsyncConnectWithResponder(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	respond asyncConnectHarnessResponder,
	onStep func(ac *AsyncConnect),
) (*ConnectResult, error) {
	t.Helper()

	return pollAsyncConnectLoop(
		t, ac, harness, respond, onStep,
		func(_ *AsyncConnect, result *ConnectResult, err error) bool {
			return result != nil || err != nil
		},
	)
}

func pollAsyncConnectUntil(
	t *testing.T,
	ac *AsyncConnect,
	harness *aeron.AsyncConnectTestHarness,
	onStep func(ac *AsyncConnect),
) (*ConnectResult, error) {
	t.Helper()

	return pollAsyncConnectWithResponder(t, ac, harness, func(h *aeron.AsyncConnectTestHarness) {
		h.RespondPendingAdds()
	}, onStep)
}

func pollUntilConnectPollResponse(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
	respond asyncConnectHarnessResponder,
) {
	t.Helper()

	_, err := pollAsyncConnectLoop(
		t, ac, harness, respond, nil,
		func(ac *AsyncConnect, _ *ConnectResult, err error) bool {
			if err != nil {
				require.NoError(t, err)
			}
			return ac.State() == ConnectStatePollResponse &&
				ac.CorrelationIDForTest() != conductor.NullCorrelationID
		},
	)
	require.NoError(t, err)
}

func controlResponseTermBufferForSubscription(
	t *testing.T,
	sub *aeron.Subscription,
) atomicbuffer.AtomicBuffer {
	t.Helper()
	return ensureSubscriptionImage(t, sub)
}

func controlResponseTermBufferForAsyncConnect(
	t *testing.T,
	ac *AsyncConnect,
) atomicbuffer.AtomicBuffer {
	t.Helper()

	return controlResponseTermBufferForSubscription(t, ac.ControlResponseSubscriptionForTest())
}

func withAsyncConnectControlResponseTerm(
	t *testing.T,
	ac *AsyncConnect,
	fn func(*testing.T, atomicbuffer.AtomicBuffer, int64),
) {
	t.Helper()

	correlation := ac.CorrelationIDForTest()
	if correlation == conductor.NullCorrelationID {
		return
	}
	fn(t, controlResponseTermBufferForAsyncConnect(t, ac), correlation)
}

func connectControlResponseOKFields(correlationID int64) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: asyncConnectFixtureControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       asyncConnectFixtureControlSessionID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          asyncConnectFixtureServerVersion,
	}
}

func archiveIDResponseOKFields(correlationID int64) codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: asyncConnectFixtureControlSessionID,
		CorrelationID:    correlationID,
		RelevantID:       asyncConnectFixtureArchiveID,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          asyncConnectFixtureServerVersion,
	}
}

func encodeConnectChallengeWire(t *testing.T, correlationID int64) []byte {
	t.Helper()

	return encodeChallengeWire(
		t,
		asyncConnectFixtureControlSessionID,
		correlationID,
		[]byte(asyncConnectFixtureChallenge),
	)
}

func writeControlResponseOKForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, connectControlResponseOKFields(correlationID)),
	)
}

func writeChallengeForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(t, termBuffer, encodeConnectChallengeWire(t, correlationID))
}

func appendChallengeForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	appendUnfragmentedWireToTerm(t, termBuffer, encodeConnectChallengeWire(t, correlationID))
}

func appendControlResponseOKForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, connectControlResponseOKFields(correlationID)),
	)
}

func appendArchiveIDResponseOK(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	appendUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeControlResponseWire(t, archiveIDResponseOKFields(correlationID)),
	)
}

// writeConnectHandshakeResponses writes an OK response to the connect or
// archive id request that the current state awaits. Other states write nothing.
func writeConnectHandshakeResponses(t *testing.T, ac *AsyncConnect) {
	t.Helper()

	switch ac.State() {
	case ConnectStatePollResponse:
		withAsyncConnectControlResponseTerm(t, ac, writeControlResponseOKForConnect)
	case ConnectStateAwaitArchiveIDResponse:
		withAsyncConnectControlResponseTerm(t, ac, appendArchiveIDResponseOK)
	}
}

func setupAsyncConnectControlReady(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		harness.DoWork()
		harness.RespondSubscriptionAdd()
		_, err := ac.Poll()
		require.NoError(t, err)
		if ac.ControlResponseSubscriptionForTest() != nil {
			break
		}
		runtime.Gosched()
	}
	sub := ac.ControlResponseSubscriptionForTest()
	require.NotNil(t, sub)
	ensureSubscriptionImage(t, sub)
}

func requireConnectRequestWireLength(
	t *testing.T,
	ac *AsyncConnect,
	responseChannelLen, encodedCredentialsLen, clientInfoLen int,
) {
	t.Helper()

	wantWireLen, ok := authConnectRequestWireLen(
		responseChannelLen,
		encodedCredentialsLen,
		clientInfoLen,
	)
	require.True(t, ok)
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())
}

func requireChallengeResponseWireLength(
	t *testing.T,
	ac *AsyncConnect,
	encodedCredentialsLen int,
) {
	t.Helper()

	wantWireLen, ok := challengeResponseWireLen(encodedCredentialsLen)
	require.True(t, ok)
	require.Equal(t, wantWireLen, ac.ConnectWireLengthForTest())
}

func requireConnectWireSlackCleared(t *testing.T, backing []byte, wireLen int, msg string) {
	t.Helper()

	for i := wireLen; i < len(backing); i++ {
		require.Zero(t, backing[i], msg, i)
	}
}

func establishConnectWireBufferHighWater(
	t *testing.T,
	harness *aeron.AsyncConnectTestHarness,
	ac *AsyncConnect,
) (backing []byte, activeWireLen int) {
	t.Helper()

	setupAsyncConnectControlReady(t, harness, ac)

	require.NoError(t, ac.PrepareConnectRequestForTest())
	buf := ac.ConnectWireBufferForTest()
	require.NotNil(t, buf)
	backing = buf[:cap(buf)]
	largeWireLen := ac.ConnectWireLengthForTest()

	ac.cfg.CredentialsSupplier = asyncConnectCredentialsSupplier{
		challengeLen: connectWireBufferSlackChallengeLen,
	}
	ac.wire.encodedChallenge = []byte("challenge")
	ac.session.controlSessionID = asyncConnectFixtureControlSessionID
	require.NoError(t, ac.PrepareChallengeResponseForTest())

	activeWireLen = ac.ConnectWireLengthForTest()
	require.Greater(t, len(backing), activeWireLen)
	require.Less(t, activeWireLen, largeWireLen)

	return backing, activeWireLen
}

func plantConnectWireBufferSlackSentinels(backing []byte, wireLen int, sentinel byte) {
	for i := wireLen; i < len(backing); i++ {
		backing[i] = sentinel
	}
}

func captureConnectWireBufferSlackOnce(
	planted *bool,
	ac *AsyncConnect,
	ready bool,
	sentinel byte,
) (backing []byte, slackWireLen int) {
	if *planted || !ready {
		return nil, 0
	}
	buf := ac.ConnectWireBufferForTest()
	if buf == nil {
		return nil, 0
	}
	capLen := cap(buf)
	wireLen := ac.ConnectWireLengthForTest()
	if capLen <= wireLen || wireLen <= 0 {
		return nil, 0
	}
	backing = buf[:capLen]
	plantConnectWireBufferSlackSentinels(backing, wireLen, sentinel)
	*planted = true
	return backing, wireLen
}

func writeRecordingSignalEventForConnect(
	t *testing.T,
	termBuffer atomicbuffer.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeRecordingSignalEventWire(t, codecs.RecordingSignalEventFields{
			ControlSessionID: asyncConnectFixtureControlSessionID,
			CorrelationID:    correlationID,
			RecordingID:      99,
			SubscriptionID:   88,
			Position:         77,
			Signal:           archivewire.RecordingSignal_START,
		}),
	)
}

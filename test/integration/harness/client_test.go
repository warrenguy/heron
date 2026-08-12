//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestPollUntilBool_earlyExit(t *testing.T) {
	t.Parallel()
	calls := 0
	ok := PollUntilBool(
		100*time.Millisecond,
		time.Millisecond,
		func() bool {
			calls++
			return calls >= 2
		},
	)
	require.True(t, ok)
	require.Equal(t, 2, calls)
}

func TestPollUntilBool_timesOut(t *testing.T) {
	t.Parallel()
	calls := 0
	ok := PollUntilBool(
		50*time.Millisecond,
		10*time.Millisecond,
		func() bool {
			calls++
			return false
		},
	)
	require.False(t, ok)
	require.Positive(t, calls)
}

type connectedWaitFn func(
	t testing.TB,
	capture *CaptureFailTB,
	client *aeron.Client,
	streamID int32,
) (io.Closer, string)

func TestConnectedWait_timeoutMessages(t *testing.T) {
	const connectedWaitTestTimeout = 50 * time.Millisecond

	tests := []struct {
		name   string
		stream int32
		wait   connectedWaitFn
	}{
		{
			name:   "publication",
			stream: ConnectedWaitPublicationTimeoutStreamID,
			wait: func(
				t testing.TB,
				capture *CaptureFailTB,
				client *aeron.Client,
				streamID int32,
			) (io.Closer, string) {
				pub, err := client.AddPublication(streamID, IPCChannel)
				require.NoError(t, err)

				WaitUntilConnected(capture, pub)
				wantMsg := fmt.Sprintf(
					"publication %d (stream %d session %d) never connected; position=0 within %s",
					pub.RegistrationID(),
					pub.StreamID(),
					pub.SessionID(),
					connectedWaitTestTimeout,
				)
				return pub, wantMsg
			},
		},
		{
			name:   "subscription",
			stream: ConnectedWaitSubscriptionTimeoutStreamID,
			wait: func(
				t testing.TB,
				capture *CaptureFailTB,
				client *aeron.Client,
				streamID int32,
			) (io.Closer, string) {
				sub, err := client.AddSubscription(streamID, IPCChannel)
				require.NoError(t, err)

				waitForSubscriptionConnectedCtx(
					capture,
					t.Context(),
					client,
					sub,
					connectedWaitTestTimeout,
				)
				wantMsg := fmt.Sprintf(
					"subscription %d never connected within %s",
					sub.RegistrationID(),
					connectedWaitTestTimeout,
				)
				return sub, wantMsg
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := SharedParallel(t)
			client := ConnectClient(t, md, shortPollAeronConfig())

			capture := &CaptureFailTB{TB: t}
			closer, wantMsg := tt.wait(t, capture, client, tt.stream)
			require.True(t, capture.failed)
			require.Equal(t, wantMsg, capture.lastMsg)
			CleanupClose(t, "close registration", closer.Close)
		})
	}
}

func TestPollClientUntilDynamicCtx_contextCanceled(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		time.Sleep(aeron.DefaultIdleSleepDuration * 2)
		cancel()
	}()

	capture := &CaptureFailTB{TB: t}
	pollClientUntilDynamicCtx(
		capture,
		ctx,
		client,
		ConnectedPollTimeout,
		func() bool { return false },
		func() string { return "condition not met" },
	)
	require.True(t, capture.failed)
	require.Equal(t, "poll: context canceled", capture.lastMsg)
}

func TestRequirePublicationLookupMiss_earlyExit(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	pub, err := client.AddPublication(RequirePublicationLookupMissStreamID, IPCChannel)
	require.NoError(t, err)
	regID := pub.RegistrationID()
	require.NoError(t, pub.Close())

	RequirePublicationLookupMiss(t, client, regID)

	// Bound zero allows one poll. A miss here asserts first-iteration exit.
	lookups := 0
	requireLookupMissCtx(
		t,
		t.Context(),
		client,
		0,
		regID,
		"publication",
		func(id int64) (*aeron.Publication, bool) {
			lookups++
			return client.Publication(id)
		},
	)
	require.Equal(t, 1, lookups)
}

func TestRequirePublicationLookupMiss_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	pub, err := client.AddPublication(RequirePublicationLookupMissTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)
	regID := pub.RegistrationID()

	capture := &CaptureFailTB{TB: t}
	requireLookupMissCtx(
		capture,
		t.Context(),
		client,
		pollHarnessShortTimeout,
		regID,
		"publication",
		client.Publication,
	)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf(
		"publication %d still registered after close within %s",
		regID,
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestWaitForIPCLinkConnected_liveIPC(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	pub, err := client.AddPublication(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	WaitForIPCLinkConnected(t, client, pub, sub)
}

func TestWaitForIPCLinkConnected_subscriptionTimeout(t *testing.T) {
	const linkWaitTestTimeout = 50 * time.Millisecond

	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	pub, err := client.AddPublication(WaitForIPCLinkConnectedSubTimeoutPubStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(
		WaitForIPCLinkConnectedSubscriptionOnlyTimeoutStreamID,
		IPCChannel,
	)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	ctx, cancel := context.WithTimeout(t.Context(), linkWaitTestTimeout)
	defer cancel()

	capture := &CaptureFailTB{TB: t}
	waitForIPCLinkConnectedCtx(capture, ctx, client, pub, sub)
	require.True(t, capture.failed)
	wantSubMsg := fmt.Sprintf("subscription %d never connected", sub.RegistrationID())
	require.Contains(t, capture.lastMsg, wantSubMsg)
	require.NotContains(t, capture.lastMsg, "publication")
	require.NotContains(t, capture.lastMsg, "poll: context deadline exceeded")

	// Fail text carries the effective bound after " within ". The parent context
	// must clamp that bound to at most linkWaitTestTimeout.
	_, within, ok := strings.Cut(capture.lastMsg, " within ")
	require.True(t, ok)
	effective, err := time.ParseDuration(within)
	require.NoError(t, err)
	require.LessOrEqual(t, effective, linkWaitTestTimeout)
}

func TestPollClientDuring_happyPath(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})
	hadClientError := &atomic.Bool{}

	PollClientDuring(
		t,
		client,
		hadClientError,
		ConnectedPollTimeout,
		"transport error",
		func() string { return "unexpected timeout" },
		func() error { return nil },
	)
}

func TestPollClientDuring_fnError(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})
	hadClientError := &atomic.Bool{}
	fnErr := errors.New("fn failed")

	capture := &CaptureFailTB{TB: t}
	PollClientDuring(
		capture,
		client,
		hadClientError,
		ConnectedPollTimeout,
		"transport error",
		func() string { return "unexpected timeout" },
		func() error { return fnErr },
	)
	require.True(t, capture.failed)
	require.Equal(t, "fn failed", capture.lastMsg)
}

func TestPollClientDuring_fnReturnsDeadlineExceeded(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})
	hadClientError := &atomic.Bool{}

	capture := &CaptureFailTB{TB: t}
	timeoutLabel := "poll client during timed out"
	PollClientDuring(
		capture,
		client,
		hadClientError,
		ConnectedPollTimeout,
		"transport error",
		func() string { return timeoutLabel },
		func() error { return context.DeadlineExceeded },
	)
	require.True(t, capture.failed)
	require.NotEqual(t, timeoutLabel, capture.lastMsg)
	require.Equal(t, "poll: context deadline exceeded", capture.lastMsg)
}

func TestPollClientDuring_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, shortPollAeronConfig())
	hadClientError := &atomic.Bool{}

	capture := &CaptureFailTB{TB: t}
	PollClientDuring(
		capture,
		client,
		hadClientError,
		pollHarnessShortTimeout,
		"transport error",
		func() string { return "poll client during timed out" },
		func() error {
			time.Sleep(pollHarnessShortTimeout * 4)
			return nil
		},
	)
	require.True(t, capture.failed)
	require.Equal(t, "poll client during timed out", capture.lastMsg)
}

func TestPollClientDuring_transportErrorGate(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})
	hadClientError := &atomic.Bool{}
	hadClientError.Store(true)

	capture := &CaptureFailTB{TB: t}
	PollClientDuring(
		capture,
		client,
		hadClientError,
		ConnectedPollTimeout,
		"transport error during poll",
		func() string { return "unexpected timeout" },
		func() error {
			time.Sleep(ConnectedPollTimeout)
			return nil
		},
	)
	require.True(t, capture.failed)
	require.Equal(t, "transport error during poll", capture.lastMsg)
}

func TestPollClientDuring_transportErrorGateAsync(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})
	hadClientError := &atomic.Bool{}

	capture := &CaptureFailTB{TB: t}

	// ErrorHandler records transport error mid-poll while fn still runs.
	go func() {
		time.Sleep(aeron.DefaultIdleSleepDuration * 3)
		hadClientError.Store(true)
	}()
	PollClientDuring(
		capture,
		client,
		hadClientError,
		ConnectedPollTimeout,
		"transport error during async poll",
		func() string { return "unexpected timeout" },
		func() error {
			time.Sleep(ConnectedPollTimeout)
			return nil
		},
	)
	require.True(t, capture.failed)
	require.Equal(t, "transport error during async poll", capture.lastMsg)
}

func TestPollClientUntilDynamicCtx_zeroBoundPollsOnce(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Config{})

	calls := 0
	capture := &CaptureFailTB{TB: t}
	pollClientUntilDynamicCtx(
		capture,
		t.Context(),
		client,
		0,
		func() bool {
			calls++
			return true
		},
		func() string { return "condition not met" },
	)
	require.False(t, capture.failed)
	require.Equal(t, 1, calls)
}

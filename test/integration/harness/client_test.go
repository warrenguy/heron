//go:build integration

package harness

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestPollUntilBool_earlyExit(t *testing.T) {
	t.Parallel()
	calls := 0
	ok := pollUntilBool(
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
	ok := pollUntilBool(
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

				PollUntilConnected(capture, client, pub, connectedWaitTestTimeout)
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

				WaitForSubscriptionConnected(capture, client, sub, connectedWaitTestTimeout)
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
			client := ConnectClient(t, md, aeron.Context{})

			capture := &CaptureFailTB{TB: t}
			closer, wantMsg := tt.wait(t, capture, client, tt.stream)
			require.True(t, capture.failed)
			require.Equal(t, wantMsg, capture.lastMsg)
			CleanupClose(t, "close registration", closer.Close)
		})
	}
}

func TestRequirePublicationLookupMiss_earlyExit(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(RequirePublicationLookupMissStreamID, IPCChannel)
	require.NoError(t, err)
	regID := pub.RegistrationID()
	require.NoError(t, pub.Close())

	start := time.Now()
	RequirePublicationLookupMiss(t, client, regID)
	require.Less(t, time.Since(start), CommandPollDeadline)
}

func TestWaitForIPCLinkConnected_liveIPC(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	WaitForIPCLinkConnected(t, client, pub, sub, ConnectedPollTimeout)
}

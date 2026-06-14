//go:build integration

package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestPollUntilBool_earlyExit(t *testing.T) {
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

func TestConnectedWait_timeoutMessages(t *testing.T) {
	const connectedWaitTestTimeout = 50 * time.Millisecond

	tests := []struct {
		name   string
		stream int32
		wait   func(*CaptureFailTB, *aeron.Client, int32)
	}{
		{
			name:   "publication",
			stream: ConnectedWaitPublicationTimeoutStreamID,
			wait: func(capture *CaptureFailTB, client *aeron.Client, streamID int32) {
				pub, err := client.AddPublication(streamID, IPCChannel)
				require.NoError(capture, err)
				PollUntilConnected(capture, client, pub, connectedWaitTestTimeout)
				wantPublicationMsg := fmt.Sprintf(
					"publication %d (stream %d session %d) never connected; position=0 within %s",
					pub.RegistrationID(),
					pub.StreamID(),
					pub.SessionID(),
					connectedWaitTestTimeout,
				)
				require.Equal(capture, wantPublicationMsg, capture.lastMsg)
			},
		},
		{
			name:   "subscription",
			stream: ConnectedWaitSubscriptionTimeoutStreamID,
			wait: func(capture *CaptureFailTB, client *aeron.Client, streamID int32) {
				sub, err := client.AddSubscription(streamID, IPCChannel)
				require.NoError(capture, err)
				WaitForSubscriptionConnected(capture, client, sub, connectedWaitTestTimeout)
				wantSubscriptionMsg := fmt.Sprintf(
					"subscription %d never connected within %s",
					sub.RegistrationID(),
					connectedWaitTestTimeout,
				)
				require.Equal(capture, wantSubscriptionMsg, capture.lastMsg)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := Start(t)
			client := ConnectClient(t, md, aeron.Context{})

			capture := &CaptureFailTB{TB: t}
			tt.wait(capture, client, tt.stream)
			require.True(t, capture.failed)
		})
	}
}

func TestRequirePublicationLookupMiss_earlyExit(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(RequirePublicationLookupMissStreamID, IPCChannel)
	require.NoError(t, err)
	regID := pub.RegistrationID()
	require.NoError(t, pub.Close())

	start := time.Now()
	RequirePublicationLookupMiss(t, client, regID)
	require.Less(t, time.Since(start), commandPollDeadline)
}

func TestWaitForIPCLinkConnected_liveIPC(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	WaitForIPCLinkConnected(t, client, pub, sub, ConnectedPollTimeout)
}

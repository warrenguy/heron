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

type captureFailTB struct {
	testing.TB
	failed  bool
	lastMsg string
}

func (capture *captureFailTB) Errorf(format string, args ...any) {
	capture.lastMsg = fmt.Sprintf(format, args...)
}

func (capture *captureFailTB) FailNow() {
	capture.failed = true
}

func TestConnectedWait_timeoutMessages(t *testing.T) {
	tests := []struct {
		name   string
		stream int32
		wait   func(testing.TB, *captureFailTB, *aeron.Client, int32)
	}{
		{
			name:   "publication",
			stream: 7991,
			wait: func(t testing.TB, capture *captureFailTB, client *aeron.Client, streamID int32) {
				pub, err := client.AddPublication(streamID, IPCChannel)
				require.NoError(t, err)
				WaitForPublicationConnected(capture, client, pub, 50*time.Millisecond)
				wantPublicationMsg := fmt.Sprintf(
					"publication %d never connected",
					pub.RegistrationID(),
				)
				require.Contains(t, capture.lastMsg, wantPublicationMsg)
			},
		},
		{
			name:   "subscription",
			stream: 7994,
			wait: func(t testing.TB, capture *captureFailTB, client *aeron.Client, streamID int32) {
				sub, err := client.AddSubscription(streamID, IPCChannel)
				require.NoError(t, err)
				WaitForSubscriptionConnected(capture, client, sub, 50*time.Millisecond)
				wantSubscriptionMsg := fmt.Sprintf(
					"subscription %d never connected",
					sub.RegistrationID(),
				)
				require.Contains(t, capture.lastMsg, wantSubscriptionMsg)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := Start(t)
			client := ConnectClient(t, md, aeron.Context{})

			capture := &captureFailTB{TB: t}
			tt.wait(t, capture, client, tt.stream)
			require.True(t, capture.failed)
			require.Contains(t, capture.lastMsg, "within 50ms")
		})
	}
}

func TestRequirePublicationLookupMiss_earlyExit(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	const streamID int32 = 7992
	pub, err := client.AddPublication(streamID, IPCChannel)
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

	const streamID int32 = 7993
	pub, err := client.AddPublication(streamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(streamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	WaitForIPCLinkConnected(t, client, pub, sub, connectedPollTimeout)
}

//go:build integration

package integration_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/test/integration/harness"
)

// TestPubSubIPCRoundtrip proves IPC offer, poll delivery, and position advance on live aeronmd.
func TestPubSubIPCRoundtrip(t *testing.T) {
	md := harness.Start(t)

	var hadClientError atomic.Bool
	client := harness.ConnectClient(t, md, aeron.Context{
		ErrorHandler: func(err error) {
			hadClientError.Store(true)
			t.Errorf("client error: %v", err)
		},
	})

	const (
		streamID     = harness.PubSubRoundtripStreamID
		messageCount = 16
		payloadSize  = 64
	)
	payloads := make([][]byte, messageCount)
	for index := range messageCount {
		payloads[index] = make([]byte, payloadSize)
		payloads[index][0] = byte(0xA0 | index)
		for offset := 1; offset < payloadSize; offset++ {
			payloads[index][offset] = byte(index)
		}
	}

	pub, err := client.AddPublication(streamID, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(streamID, harness.IPCChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "close subscription", sub.Close)

	// Poll publication connect before image attach; harness does not use WaitForIPCLinkConnected.
	linkDeadline := time.Now().Add(harness.ConnectedPollTimeout)
	harness.PollUntilConnected(t, client, pub, time.Until(linkDeadline))
	harness.PollUntilImageCount(t, client, sub, 1, time.Until(linkDeadline))

	pubPosBefore := pub.Position()
	// Position advance uses aligned DATA frame length, not raw payload sum.
	alignedFrameLen := int64(logbuffer.AlignFragmentChunkLength(payloadSize))
	wantAdvance := alignedFrameLen * int64(messageCount)

	for _, payload := range payloads {
		require.Positive(t, pub.Offer(payload))
		client.DoWork()
	}

	received := harness.PollDelivered(t, client, sub, messageCount, 0)

	require.Equal(t, payloads, received)

	require.Equal(t, pubPosBefore+wantAdvance, pub.Position())

	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	require.Equal(t, image.JoinPosition()+wantAdvance, image.Position())

	require.Equal(t, streamID, pub.StreamID())
	require.Equal(t, streamID, sub.StreamID())
	require.Equal(t, streamID, image.StreamID())
	require.Equal(t, pub.SessionID(), image.SessionID())

	require.False(t, hadClientError.Load())
	md.AssertProcessAlive(t)
}

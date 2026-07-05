//go:build integration

package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const pollHarnessShortTimeout = 50 * time.Millisecond

const invalidHarnessTimeout = -time.Second

var invalidWantCases = []struct {
	name string
	want int
}{
	{name: "zero", want: 0},
	{name: "negative", want: -1},
}

func connectIPCPubSub(
	t testing.TB,
	client *aeron.Client,
	streamID int32,
) (*aeron.Publication, *aeron.Subscription) {
	return ConnectIPCPubSubClients(t, client, client, streamID)
}

func offerSingleAlignedFragment(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
) int64 {
	t.Helper()

	payload := []byte{0x01, 0x02, 0x03}
	result := pub.Offer(payload)
	require.Positive(t, result)
	client.DoWork()
	return AlignedOfferAdvance(len(payload), 1)
}

func TestPollUntilConnected_invalidTimeoutFails(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(PollUntilConnectedInvalidTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	capture := &CaptureFailTB{TB: t}
	PollUntilConnected(capture, client, pub, invalidHarnessTimeout)
	require.True(t, capture.failed)
	require.Equal(
		t,
		fmt.Sprintf("invalid timeout: %s", invalidHarnessTimeout),
		capture.lastMsg,
	)
}

func TestPollUntilConnected_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(PollUntilConnectedTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	capture := &CaptureFailTB{TB: t}
	PollUntilConnected(capture, client, pub, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf(
		"publication %d (stream %d session %d) never connected; position=0 within %s",
		pub.RegistrationID(),
		pub.StreamID(),
		pub.SessionID(),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollUntilImageCount_timeoutMessage(t *testing.T) {
	t.Run("zeroActiveImages", func(t *testing.T) {
		md := SharedParallel(t)
		client := ConnectClient(t, md, aeron.Context{})

		sub, err := client.AddSubscription(PollUntilImageCountZeroActiveStreamID, IPCChannel)
		require.NoError(t, err)
		CleanupClose(t, "close subscription", sub.Close)

		capture := &CaptureFailTB{TB: t}
		PollUntilImageCount(capture, client, sub, 1, pollHarnessShortTimeout)
		require.True(t, capture.failed)
		wantMsg := fmt.Sprintf(
			"subscription %d (stream %d) active image count=0 want=1; no images within %s",
			sub.RegistrationID(),
			sub.StreamID(),
			pollHarnessShortTimeout,
		)
		require.Equal(t, wantMsg, capture.lastMsg)
	})

	t.Run("oneActiveImageWantTwo", func(t *testing.T) {
		md := SharedParallel(t)
		client := ConnectClient(t, md, aeron.Context{})

		pub, sub := connectIPCPubSub(t, client, PollUntilImageCountOneWantTwoStreamID)

		capture := &CaptureFailTB{TB: t}
		PollUntilImageCount(capture, client, sub, 2, pollHarnessShortTimeout)
		require.True(t, capture.failed)
		wantMsg := fmt.Sprintf(
			"subscription %d (stream %d) active image count=1 want=2; "+
				"active=1 [0 stream=%d session=%d position=0] within %s",
			sub.RegistrationID(),
			sub.StreamID(),
			sub.StreamID(),
			pub.SessionID(),
			pollHarnessShortTimeout,
		)
		require.Equal(t, wantMsg, capture.lastMsg)
	})
}

func TestPollUntilNoActiveImages_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	_, sub := connectIPCPubSub(t, client, PollUntilNoActiveImagesTimeoutStreamID)

	capture := &CaptureFailTB{TB: t}
	PollUntilNoActiveImages(capture, client, sub, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf(
		"%s within %s",
		subscriptionImageCountFailMsg(sub, 0),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollSplitClientsUntil_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	clientA := ConnectClient(t, md, aeron.Context{})
	clientB := ConnectClient(t, md, aeron.Context{})

	const failMsg = "split poll condition not met"
	capture := &CaptureFailTB{TB: t}
	PollSplitClientsUntil(
		capture,
		clientA,
		clientB,
		pollHarnessShortTimeout,
		func() bool { return false },
		failMsg,
	)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf("%s within %s", failMsg, pollHarnessShortTimeout)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollSplitClientIPCLink_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	pubClient := ConnectClient(t, md, aeron.Context{})
	subClient := ConnectClient(t, md, aeron.Context{})

	pub, err := pubClient.AddPublication(PollSplitClientIPCLinkTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := subClient.AddSubscription(PollSplitClientIPCLinkSubStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	capture := &CaptureFailTB{TB: t}
	PollSplitClientIPCLink(
		capture,
		pubClient,
		subClient,
		pub,
		sub,
		pollHarnessShortTimeout,
	)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf(
		"split-client IPC link not ready within %s",
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollUntilNoActiveImagesSplit_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	pubClient := ConnectClient(t, md, aeron.Context{})
	subClient := ConnectClient(t, md, aeron.Context{})

	_, sub := ConnectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		PollUntilNoActiveImagesSplitTimeoutStreamID,
	)

	capture := &CaptureFailTB{TB: t}
	PollUntilNoActiveImagesSplit(
		capture,
		pubClient,
		subClient,
		sub,
		pollHarnessShortTimeout,
	)
	require.True(t, capture.failed)
	wantMsg := fmt.Sprintf(
		"%s within %s",
		subscriptionImageCountFailMsg(sub, 0),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollUntilImageCount_invalidWantFails(t *testing.T) {
	testInvalidPollWant(t, PollUntilImageCountInvalidWantStreamID, func(
		capture *CaptureFailTB,
		client *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		PollUntilImageCount(capture, client, sub, want, time.Second)
	})
}

func testInvalidPollWant(
	t *testing.T,
	streamID int32,
	poll func(*CaptureFailTB, *aeron.Client, *aeron.Subscription, int),
) {
	t.Helper()
	for _, tt := range invalidWantCases {
		t.Run(tt.name, func(t *testing.T) {
			md := SharedParallel(t)
			client := ConnectClient(t, md, aeron.Context{})

			sub, err := client.AddSubscription(streamID, IPCChannel)
			require.NoError(t, err)
			CleanupClose(t, "close subscription", sub.Close)

			capture := &CaptureFailTB{TB: t}
			poll(capture, client, sub, tt.want)
			require.True(t, capture.failed)
			require.Equal(t, fmt.Sprintf(`"%d" is not positive`, tt.want), capture.lastMsg)
		})
	}
}

func TestPollDelivered_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollDeliveredTimeoutStreamID)

	capture := &CaptureFailTB{TB: t}
	got := PollDelivered(capture, client, sub, 1, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Empty(t, got)
	wantMsg := fmt.Sprintf(
		"subscription %d delivered 0/1 fragments; "+
			"active=1 [0 stream=%d session=%d position=0] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollDelivered_partialDelivery_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollDeliveredPartialDeliveryStreamID)

	alignedAdvance := offerSingleAlignedFragment(t, client, pub)

	capture := &CaptureFailTB{TB: t}
	got := PollDelivered(capture, client, sub, 2, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Len(t, got, 1)
	wantMsg := fmt.Sprintf(
		"subscription %d delivered 1/2 fragments; "+
			"active=1 [0 stream=%d session=%d position=%d] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		alignedAdvance,
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollDelivered_invalidWantFails(t *testing.T) {
	testInvalidPollWant(t, PollDeliveredInvalidWantStreamID, func(
		capture *CaptureFailTB,
		client *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		got := PollDelivered(capture, client, sub, want, time.Second)
		require.Empty(t, got)
	})
}

func TestPollDelivered_liveIPC(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	payload := []byte{0xAB, 0xCD, 0xEF}

	pub, sub := connectIPCPubSub(t, client, PollDeliveredLiveIPCStreamID)

	pubPosBefore := pub.Position()
	alignedAdvance := AlignedOfferAdvance(len(payload), 1)

	result := pub.Offer(payload)
	require.Positive(t, result)
	client.DoWork()

	got := PollDelivered(t, client, sub, 1, ConnectedPollTimeout)
	require.Len(t, got, 1)
	require.Equal(t, payload, got[0])

	AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, alignedAdvance)
}

func TestPollDeliveredSplit_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	pubClient := ConnectClient(t, md, aeron.Context{})
	subClient := ConnectClient(t, md, aeron.Context{})

	pub, sub := ConnectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		PollDeliveredSplitTimeoutStreamID,
	)

	capture := &CaptureFailTB{TB: t}
	got := PollDeliveredSplit(
		capture,
		pubClient,
		subClient,
		sub,
		1,
		pollHarnessShortTimeout,
	)
	require.True(t, capture.failed)
	require.Empty(t, got)
	wantMsg := fmt.Sprintf(
		"subscription %d delivered 0/1 fragments; "+
			"active=1 [0 stream=%d session=%d position=0] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollDeliveredSplit_partialDelivery_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	pubClient := ConnectClient(t, md, aeron.Context{})
	subClient := ConnectClient(t, md, aeron.Context{})

	pub, sub := ConnectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		PollDeliveredSplitPartialDeliveryStreamID,
	)

	alignedAdvance := offerSingleAlignedFragmentSplit(t, pubClient, pub)

	capture := &CaptureFailTB{TB: t}
	got := PollDeliveredSplit(
		capture,
		pubClient,
		subClient,
		sub,
		2,
		pollHarnessShortTimeout,
	)
	require.True(t, capture.failed)
	require.Len(t, got, 1)
	wantMsg := fmt.Sprintf(
		"subscription %d delivered 1/2 fragments; "+
			"active=1 [0 stream=%d session=%d position=%d] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		alignedAdvance,
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollDeliveredSplit_invalidWantFails(t *testing.T) {
	testInvalidPollWantSplit(t, PollDeliveredSplitInvalidWantStreamID, func(
		capture *CaptureFailTB,
		pubClient *aeron.Client,
		subClient *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		got := PollDeliveredSplit(
			capture,
			pubClient,
			subClient,
			sub,
			want,
			time.Second,
		)
		require.Empty(t, got)
	})
}

func offerSingleAlignedFragmentSplit(
	t testing.TB,
	pubClient *aeron.Client,
	pub *aeron.Publication,
) int64 {
	t.Helper()

	payload := []byte{0x01, 0x02, 0x03}
	result := pub.Offer(payload)
	require.Positive(t, result)
	pubClient.DoWork()
	return AlignedOfferAdvance(len(payload), 1)
}

func testInvalidPollWantSplit(
	t *testing.T,
	streamID int32,
	poll func(*CaptureFailTB, *aeron.Client, *aeron.Client, *aeron.Subscription, int),
) {
	t.Helper()
	for _, tt := range invalidWantCases {
		t.Run(tt.name, func(t *testing.T) {
			md := SharedParallel(t)
			pubClient := ConnectClient(t, md, aeron.Context{})
			subClient := ConnectClient(t, md, aeron.Context{})

			sub, err := subClient.AddSubscription(streamID, IPCChannel)
			require.NoError(t, err)
			CleanupClose(t, "close subscription", sub.Close)

			capture := &CaptureFailTB{TB: t}
			poll(capture, pubClient, subClient, sub, tt.want)
			require.True(t, capture.failed)
			require.Equal(t, fmt.Sprintf(`"%d" is not positive`, tt.want), capture.lastMsg)
		})
	}
}

func TestPollReassembled_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollReassembledTimeoutStreamID)

	capture := &CaptureFailTB{TB: t}
	got, fragments := PollReassembled(capture, client, sub, 1, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Empty(t, got)
	require.Zero(t, fragments)
	wantMsg := fmt.Sprintf(
		"subscription %d reassembled 0/1 messages; fragments=0; "+
			"active=1 [0 stream=%d session=%d position=0] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollReassembled_noActiveImage_failsFast(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	sub, err := client.AddSubscription(PollReassembledNoImageStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	capture := &CaptureFailTB{TB: t}
	got, fragments := PollReassembled(capture, client, sub, 1, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Empty(t, got)
	require.Zero(t, fragments)
	wantMsg := fmt.Sprintf(
		"subscription %d reassembled 0/1 messages; fragments=0; no images",
		sub.RegistrationID(),
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollReassembled_partialDelivery_timeoutMessage(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollReassembledPartialDeliveryStreamID)

	alignedAdvance := offerSingleAlignedFragment(t, client, pub)

	capture := &CaptureFailTB{TB: t}
	got, fragments := PollReassembled(capture, client, sub, 2, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Len(t, got, 1)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, got[0])
	require.Equal(t, 1, fragments)
	wantMsg := fmt.Sprintf(
		"subscription %d reassembled 1/2 messages; fragments=1; "+
			"active=1 [0 stream=%d session=%d position=%d] within %s",
		sub.RegistrationID(),
		sub.StreamID(),
		pub.SessionID(),
		alignedAdvance,
		pollHarnessShortTimeout,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollReassembled_invalidWantFails(t *testing.T) {
	testInvalidPollWant(t, PollReassembledInvalidWantStreamID, func(
		capture *CaptureFailTB,
		client *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		got, fragments := PollReassembled(capture, client, sub, want, time.Second)
		require.Empty(t, got)
		require.Zero(t, fragments)
	})
}

func TestOfferWithRetry_invalidTimeoutFails(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(OfferWithRetryInvalidTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	capture := &CaptureFailTB{TB: t}
	got := OfferWithRetry(capture, client, pub, []byte{0x01}, invalidHarnessTimeout)
	require.True(t, capture.failed)
	require.Zero(t, got)
	require.Equal(
		t,
		fmt.Sprintf("invalid timeout: %s", invalidHarnessTimeout),
		capture.lastMsg,
	)
}

func TestOfferWithRetry_notConnected_timesOut(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(OfferWithRetryNotConnectedTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	payload := []byte{0x01}
	capture := &CaptureFailTB{TB: t}
	got := OfferWithRetry(capture, client, pub, payload, pollHarnessShortTimeout)
	require.True(t, capture.failed)
	require.Zero(t, got)
	wantMsg := fmt.Sprintf(
		"publication %d (stream %d session %d) offer timed out within %s; position=0; status=%d",
		pub.RegistrationID(),
		pub.StreamID(),
		pub.SessionID(),
		pollHarnessShortTimeout,
		aeron.NotConnected,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestOfferWithRetry_notConnected_thenConnect_succeeds(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(OfferWithRetryNotConnectedThenConnectStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	subReady := make(chan struct {
		sub *aeron.Subscription
		err error
	}, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		sub, err := client.AddSubscription(OfferWithRetryNotConnectedThenConnectStreamID, IPCChannel)
		subReady <- struct {
			sub *aeron.Subscription
			err error
		}{sub: sub, err: err}
	}()

	payload := []byte{0xAB}
	result := OfferWithRetry(t, client, pub, payload, ConnectedPollTimeout)

	res := <-subReady
	require.NoError(t, res.err)
	CleanupClose(t, "close subscription", res.sub.Close)
	require.Positive(t, result)
}

func TestOfferWithRetry_closedPublication_failsImmediately(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(OfferWithRetryClosedPublicationStreamID, IPCChannel)
	require.NoError(t, err)
	require.NoError(t, pub.Close())

	capture := &CaptureFailTB{TB: t}
	got := OfferWithRetry(capture, client, pub, []byte{0x01}, time.Second)
	require.True(t, capture.failed)
	require.Equal(t, aeron.Closed, got)
	wantMsg := fmt.Sprintf(
		"publication %d (stream %d session %d) offer failed: status=%d; position=%d",
		pub.RegistrationID(),
		pub.StreamID(),
		pub.SessionID(),
		aeron.Closed,
		aeron.Closed,
	)
	require.Equal(t, wantMsg, capture.lastMsg)
}

func TestPollReassembled_liveIPC_fragmented(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollReassembledLiveIPCStreamID)

	maxPayload := pub.MaxPayloadLength()
	payload := FragmentedRoundtripPayload(maxPayload, FragmentedRoundtripExtraBytes)

	pubPosBefore := pub.Position()
	wantAdvance := int64(logbuffer.ComputeFragmentedFrameLength(len(payload), maxPayload))

	result := OfferWithRetry(t, client, pub, payload, ConnectedPollTimeout)
	require.Positive(t, result)
	client.DoWork()

	got, fragments := PollReassembled(t, client, sub, 1, ConnectedPollTimeout)
	require.Len(t, got, 1)
	require.Equal(t, payload, got[0])
	require.Equal(t, 2, fragments)

	AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)
}

func TestComputeRolloverMessageCount_table(t *testing.T) {
	tests := []struct {
		name        string
		termLen     int
		payloadSize int
		want        int
	}{
		{
			name:        "four_kiB_term_256_payload",
			termLen:     4096,
			payloadSize: 256,
			want:        17,
		},
		{
			name:        "term_min_length_256_payload",
			termLen:     logbuffer.TermMinLength,
			payloadSize: 256,
			want:        230,
		},
		{
			name:        "zero_term_len",
			termLen:     0,
			payloadSize: 256,
			want:        0,
		},
		{
			name:        "zero_payload_size",
			termLen:     4096,
			payloadSize: 0,
			want:        0,
		},
		{
			name:        "negative_inputs",
			termLen:     -1,
			payloadSize: -1,
			want:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, ComputeRolloverMessageCount(tt.termLen, tt.payloadSize))
		})
	}
}

func TestOfferWithRetryWitness_cleanSuccess_returnsFalse(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, _ := connectIPCPubSub(t, client, OfferWithRetryWitnessCleanStreamID)

	pubPosBefore := pub.Position()
	payload := []byte{0x01}
	pos, witness := OfferWithRetryWitness(t, client, pub, payload, ConnectedPollTimeout)
	require.False(t, witness)
	require.Positive(t, pos)
	require.Greater(t, pos, pubPosBefore)
}

func TestOfferWithRetryWitness_backPressured_returnsTrue(t *testing.T) {
	md := SharedParallel(t)
	pubClient := ConnectClient(t, md, aeron.Context{})
	subClient := ConnectClient(t, md, aeron.Context{})

	pub, sub := ConnectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		OfferWithRetryWitnessBackPressuredStreamID,
	)

	payload := make([]byte, 256)
	sawBackpressure := false
	for range 300 {
		result := pub.Offer(payload)
		switch {
		case result > 0:
			pubClient.DoWork()
		case result == aeron.BackPressured:
			sawBackpressure = true
		case result == aeron.AdminAction:
			pubClient.DoWork()
		default:
			require.FailNowf(t, "unexpected offer status", "status=%d", result)
		}
		if sawBackpressure {
			break
		}
	}
	require.True(t, sawBackpressure, "expected to fill publication window without subscriber poll")

	pubPosBefore := pub.Position()
	transientSeen := make(chan struct{}, 1)
	drainCtx, stopDrain := context.WithCancel(context.Background())
	drainDone := make(chan struct{})
	t.Cleanup(func() {
		stopDrain()
		select {
		case <-drainDone:
		case <-time.After(time.Second):
			t.Errorf("witness backpressure drain goroutine did not exit within 1s")
		}
	})
	go func() {
		defer close(drainDone)
		select {
		case <-transientSeen:
		case <-drainCtx.Done():
			return
		}
		for drainCtx.Err() == nil {
			drainSubscriptionFragments(sub, 1000)
			subClient.DoWork()
			time.Sleep(aeron.DefaultIdleSleepDuration)
		}
	}()

	pos, witness := OfferWithRetryWitnessAfterNotify(
		t,
		pubClient,
		pub,
		payload,
		ConnectedPollTimeout,
		transientSeen,
	)
	stopDrain()
	// stopDrain cancels drainCtx; goroutine closes drainDone after at most one poll loop.
	<-drainDone
	require.True(t, witness)
	require.Positive(t, pos)
	require.Greater(t, pos, pubPosBefore)
}

// TestAssertSubscribeMetadata_liveIPC proves AssertSubscribeMetadata stream,
// session, and position checks on live aeronmd IPC pub/sub.
func TestAssertSubscribeMetadata_liveIPC(t *testing.T) {
	md := SharedParallel(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := ConnectIPCPubSubClients(t, client, client, AssertSubscribeMetadataStreamID)

	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	joinPosBefore := image.JoinPosition()

	const messageCount = 2
	const payloadSize = 32
	wantAdvance := AlignedOfferAdvance(payloadSize, messageCount)
	payloads := DistinctIPCPayloads(messageCount, payloadSize, 0xD0)

	for _, payload := range payloads {
		OfferWithRetry(t, client, pub, payload, ConnectedPollTimeout)
	}
	_ = PollDelivered(t, client, sub, messageCount, ConnectedPollTimeout)

	sessionID := pub.SessionID()
	witnessCases := []struct {
		name        string
		streamID    int32
		sessionID   int32
		wantAdvance int64
		wantFail    bool
	}{
		{
			name:        "matchingWitness",
			streamID:    AssertSubscribeMetadataStreamID,
			sessionID:   sessionID,
			wantAdvance: wantAdvance,
		},
		{
			name:        "wrongSessionID",
			streamID:    AssertSubscribeMetadataStreamID,
			sessionID:   sessionID + 1,
			wantAdvance: wantAdvance,
			wantFail:    true,
		},
		{
			name:        "wrongStreamID",
			streamID:    AssertSubscribeMetadataStreamID + 1,
			sessionID:   sessionID,
			wantAdvance: wantAdvance,
			wantFail:    true,
		},
		{
			name:        "wrongWantAdvance",
			streamID:    AssertSubscribeMetadataStreamID,
			sessionID:   sessionID,
			wantAdvance: wantAdvance + 1,
			wantFail:    true,
		},
	}
	assertWitness := func(tb testing.TB, streamID, sessionID int32, advance int64) {
		AssertSubscribeMetadata(tb, streamID, sessionID, sub, joinPosBefore, advance)
	}
	// Witness cases share parent pub/sub; parallel subtests would race on shared state.
	for _, tt := range witnessCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantFail {
				capture := NewCaptureFailTB(t)
				assertWitness(capture, tt.streamID, tt.sessionID, tt.wantAdvance)
				require.True(t, capture.Failed())
				return
			}
			assertWitness(t, tt.streamID, tt.sessionID, tt.wantAdvance)
		})
	}
}

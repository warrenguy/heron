//go:build integration

package harness

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const pollHarnessShortTimeout = 50 * time.Millisecond

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
	t.Helper()

	pub, err := client.AddPublication(streamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	sub, err := client.AddSubscription(streamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	WaitForIPCLinkConnected(t, client, pub, sub, ConnectedPollTimeout)
	return pub, sub
}

func alignedDataFrameAdvance(payload []byte) int64 {
	return int64(logbuffer.AlignFragmentChunkLength(len(payload)))
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
	return alignedDataFrameAdvance(payload)
}

func TestPollUntilConnected_timeoutMessage(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(PollUntilConnectedTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	capture := &captureFailTB{TB: t}
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
		md := Start(t)
		client := ConnectClient(t, md, aeron.Context{})

		sub, err := client.AddSubscription(PollUntilImageCountZeroActiveStreamID, IPCChannel)
		require.NoError(t, err)
		CleanupClose(t, "close subscription", sub.Close)

		capture := &captureFailTB{TB: t}
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
		md := Start(t)
		client := ConnectClient(t, md, aeron.Context{})

		pub, sub := connectIPCPubSub(t, client, PollUntilImageCountOneWantTwoStreamID)

		capture := &captureFailTB{TB: t}
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

func TestPollUntilImageCount_invalidWantFails(t *testing.T) {
	testInvalidPollWant(t, PollUntilImageCountInvalidWantStreamID, func(
		capture *captureFailTB,
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
	poll func(*captureFailTB, *aeron.Client, *aeron.Subscription, int),
) {
	t.Helper()
	for _, tt := range invalidWantCases {
		t.Run(tt.name, func(t *testing.T) {
			md := Start(t)
			client := ConnectClient(t, md, aeron.Context{})

			sub, err := client.AddSubscription(streamID, IPCChannel)
			require.NoError(t, err)
			CleanupClose(t, "close subscription", sub.Close)

			capture := &captureFailTB{TB: t}
			poll(capture, client, sub, tt.want)
			require.True(t, capture.failed)
			require.Equal(t, fmt.Sprintf(`"%d" is not positive`, tt.want), capture.lastMsg)
		})
	}
}

func TestPollDelivered_timeoutMessage(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollDeliveredTimeoutStreamID)

	capture := &captureFailTB{TB: t}
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
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollDeliveredPartialDeliveryStreamID)

	alignedAdvance := offerSingleAlignedFragment(t, client, pub)

	capture := &captureFailTB{TB: t}
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
		capture *captureFailTB,
		client *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		got := PollDelivered(capture, client, sub, want, time.Second)
		require.Empty(t, got)
	})
}

func TestPollDelivered_liveIPC(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	payload := []byte{0xAB, 0xCD, 0xEF}

	pub, sub := connectIPCPubSub(t, client, PollDeliveredLiveIPCStreamID)

	pubPosBefore := pub.Position()
	alignedAdvance := alignedDataFrameAdvance(payload)

	result := pub.Offer(payload)
	require.Positive(t, result)
	client.DoWork()

	got := PollDelivered(t, client, sub, 1, ConnectedPollTimeout)
	require.Len(t, got, 1)
	require.Equal(t, payload, got[0])

	AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, alignedAdvance)
}

func TestPollReassembled_timeoutMessage(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollReassembledTimeoutStreamID)

	capture := &captureFailTB{TB: t}
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
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	sub, err := client.AddSubscription(PollReassembledNoImageStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close subscription", sub.Close)

	capture := &captureFailTB{TB: t}
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
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, sub := connectIPCPubSub(t, client, PollReassembledPartialDeliveryStreamID)

	alignedAdvance := offerSingleAlignedFragment(t, client, pub)

	capture := &captureFailTB{TB: t}
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
		capture *captureFailTB,
		client *aeron.Client,
		sub *aeron.Subscription,
		want int,
	) {
		got, fragments := PollReassembled(capture, client, sub, want, time.Second)
		require.Empty(t, got)
		require.Zero(t, fragments)
	})
}

func TestOfferWithRetry_notConnected_timesOut(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(PollUntilConnectedTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	payload := []byte{0x01}
	capture := &captureFailTB{TB: t}
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
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(WaitForIPCLinkConnectedStreamID, IPCChannel)
	require.NoError(t, err)
	CleanupClose(t, "close publication", pub.Close)

	subReady := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		sub, err := client.AddSubscription(WaitForIPCLinkConnectedStreamID, IPCChannel)
		if err != nil {
			subReady <- err
			return
		}
		CleanupClose(t, "close subscription", sub.Close)
		subReady <- nil
	}()

	payload := []byte{0xAB}
	result := OfferWithRetry(t, client, pub, payload, ConnectedPollTimeout)
	require.NoError(t, <-subReady)
	require.Positive(t, result)
}

func TestOfferWithRetry_closedPublication_failsImmediately(t *testing.T) {
	md := Start(t)
	client := ConnectClient(t, md, aeron.Context{})

	pub, err := client.AddPublication(ConnectedWaitPublicationTimeoutStreamID, IPCChannel)
	require.NoError(t, err)
	require.NoError(t, pub.Close())

	capture := &captureFailTB{TB: t}
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
	md := Start(t)
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

//go:build integration

package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// boundPublication is *aeron.Publication or *aeron.ExclusivePublication.
type boundPublication interface {
	aeron.PublicationOfferer
	WaitUntilConnected(context.Context) error
	OfferWithRetryObserved(
		context.Context,
		[]byte,
		aeron.OfferRetriableObserver,
	) (int64, error)
}

// WaitUntilConnected is harness wrap of [aeron.Publication.WaitUntilConnected]
// (and exclusive counterpart) on live IPC.
func WaitUntilConnected(
	t testing.TB,
	pub boundPublication,
) {
	t.Helper()

	waitUntilConnectedCtx(t, t.Context(), pub)
}

func waitUntilConnectedCtx(
	t testing.TB,
	ctx context.Context,
	pub boundPublication,
) {
	t.Helper()
	failPollTimeout(t, pub.WaitUntilConnected(ctx))
}

// WaitUntilImageCount is harness wrap of [aeron.Subscription.WaitUntilImageCount].
func WaitUntilImageCount(
	t testing.TB,
	sub *aeron.Subscription,
	want int,
) {
	t.Helper()
	if !preparePollWant(t, want) {
		return
	}

	err := sub.WaitUntilImageCount(t.Context(), want)
	failPollTimeout(t, err)
}

func failDelegatedContext(
	t testing.TB,
	err error,
	verb string,
) bool {
	t.Helper()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("%s: %v", verb, err)
		t.FailNow()
		return true
	}
	return false
}

func failPollTimeout(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		return
	}

	if failDelegatedContext(t, err, "poll") {
		return
	}

	if connected, ok := errors.AsType[*aeron.WaitUntilConnectedTimeoutError](err); ok {
		t.Errorf(
			"publication %d (stream %d session %d) never connected; position=%d within %s",
			connected.PublicationRegID,
			connected.StreamID,
			connected.SessionID,
			connected.Position,
			connected.Timeout,
		)
		t.FailNow()
		return
	}

	if imageCount, ok := errors.AsType[*aeron.WaitUntilImageCountTimeoutError](err); ok {
		t.Errorf(
			"%s within %s",
			formatSubscriptionImageCountMsg(
				imageCount.RegistrationID,
				imageCount.StreamID,
				imageCount.Have,
				imageCount.Want,
				imageCount.Detail,
			),
			imageCount.Timeout,
		)
		t.FailNow()
		return
	}

	t.Errorf("unexpected poll error: %v", err)
	t.FailNow()
}

// RequireNoActiveImages fails fast if the subscription has any active image.
func RequireNoActiveImages(t testing.TB, sub *aeron.Subscription) {
	t.Helper()

	if aeron.SubscriptionActiveImageCountAtLeast(sub, 1) {
		require.FailNow(t, subscriptionImageCountFailMsg(sub, 0))
	}
}

func pollSplitClientsUntilDynamicCtx(
	t testing.TB,
	ctx context.Context,
	clientA, clientB *aeron.Client,
	bound time.Duration,
	ok func() bool,
	failMsg func() string,
) {
	t.Helper()

	pollUntilDynamicCtx(
		t,
		ctx,
		bound,
		func() {
			clientA.DoWork()
			clientB.DoWork()
		},
		ok,
		failMsg,
	)
}

// PollSplitClientsUntil polls DoWork on both clients until ok or the poll bound
// elapses (caller ctx may clamp via EffectiveRetryBound).
func PollSplitClientsUntil(
	t testing.TB,
	clientA, clientB *aeron.Client,
	ok func() bool,
	failMsg string,
) {
	t.Helper()

	pollSplitClientsUntilDynamicCtx(
		t,
		t.Context(),
		clientA,
		clientB,
		ConnectedPollTimeout,
		ok,
		func() string { return failMsg },
	)
}

// PollSplitClientIPCLink polls both clients until pub is connected and sub has active images.
func PollSplitClientIPCLink(
	t testing.TB,
	pubClient, subClient *aeron.Client,
	pub aeron.PublicationOfferer,
	sub *aeron.Subscription,
) {
	t.Helper()

	pollSplitClientIPCLinkCtx(
		t,
		t.Context(),
		pubClient,
		subClient,
		pub,
		sub,
		ConnectedPollTimeout,
	)
}

func pollSplitClientIPCLinkCtx(
	t testing.TB,
	ctx context.Context,
	pubClient, subClient *aeron.Client,
	pub aeron.PublicationOfferer,
	sub *aeron.Subscription,
	bound time.Duration,
) {
	t.Helper()

	pollSplitClientsUntilDynamicCtx(
		t,
		ctx,
		pubClient,
		subClient,
		bound,
		func() bool {
			return pub.IsConnected() && aeron.SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		func() string { return "split-client IPC link not ready" },
	)
}

type ipcPublication interface {
	*aeron.Publication | *aeron.ExclusivePublication
	boundPublication
	Close() error
}

func connectIPCPubSubClients[P ipcPublication](
	t testing.TB,
	pubClient *aeron.Client,
	subClient *aeron.Client,
	streamID int32,
	pubLabel string,
	addPublication func(int32, string) (P, error),
) (P, *aeron.Subscription) {
	t.Helper()

	if pubClient == subClient {
		pub, err := addPublication(streamID, IPCChannel)
		require.NoError(t, err)
		CleanupClose(t, pubLabel, pub.Close)

		sub, err := subClient.AddSubscription(streamID, IPCChannel)
		require.NoError(t, err)
		CleanupClose(t, "close subscription", sub.Close)

		WaitForIPCLinkConnected(t, pubClient, pub, sub)
		return pub, sub
	}

	var (
		pub    P
		sub    *aeron.Subscription
		pubErr error
		subErr error
	)

	var registerWG sync.WaitGroup
	registerWG.Add(2)

	go func() {
		defer registerWG.Done()
		pub, pubErr = addPublication(streamID, IPCChannel)
	}()

	go func() {
		defer registerWG.Done()
		sub, subErr = subClient.AddSubscription(streamID, IPCChannel)
	}()

	registerWG.Wait()

	if pub != nil {
		CleanupClose(t, pubLabel, pub.Close)
	}
	if sub != nil {
		CleanupClose(t, "close subscription", sub.Close)
	}

	require.NoError(t, pubErr)
	require.NoError(t, subErr)

	PollSplitClientIPCLink(t, pubClient, subClient, pub, sub)
	return pub, sub
}

// ConnectIPCPubSubClients registers a concurrent publication and subscription on
// distinct or shared clients and waits until the IPC link is ready on live aeronmd.
//
// Split clients register concurrently; each Add* blocks until ready or client
// DriverTimeout. PollSplitClientIPCLink then pumps both conductors for link setup.
func ConnectIPCPubSubClients(
	t testing.TB,
	pubClient *aeron.Client,
	subClient *aeron.Client,
	streamID int32,
) (*aeron.Publication, *aeron.Subscription) {
	t.Helper()

	return connectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		streamID,
		"close publication",
		pubClient.AddPublication,
	)
}

// ConnectExclusiveIPCPubSubClients registers an exclusive publication and
// subscription on distinct or shared clients and waits until the IPC link is ready.
//
// Split clients register concurrently; each Add* blocks until ready or client
// DriverTimeout. PollSplitClientIPCLink then pumps both conductors for link setup.
func ConnectExclusiveIPCPubSubClients(
	t testing.TB,
	pubClient *aeron.Client,
	subClient *aeron.Client,
	streamID int32,
) (*aeron.ExclusivePublication, *aeron.Subscription) {
	t.Helper()

	return connectIPCPubSubClients(
		t,
		pubClient,
		subClient,
		streamID,
		"close exclusive publication",
		pubClient.AddExclusivePublication,
	)
}

// PollUntilNoActiveImagesSplit polls both clients until sub has zero active images.
func PollUntilNoActiveImagesSplit(
	t testing.TB,
	pubClient, subClient *aeron.Client,
	sub *aeron.Subscription,
) {
	t.Helper()

	pollUntilNoActiveImagesSplitCtx(
		t,
		t.Context(),
		pubClient,
		subClient,
		sub,
		ConnectedPollTimeout,
	)
}

func pollUntilNoActiveImagesSplitCtx(
	t testing.TB,
	ctx context.Context,
	pubClient, subClient *aeron.Client,
	sub *aeron.Subscription,
	bound time.Duration,
) {
	t.Helper()

	pollSplitClientsUntilDynamicCtx(
		t,
		ctx,
		pubClient,
		subClient,
		bound,
		func() bool {
			return !aeron.SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		func() string {
			return subscriptionImageCountFailMsg(sub, 0)
		},
	)
}

// PollUntilNoActiveImages polls DoWork until the subscription has zero active images.
func PollUntilNoActiveImages(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
) {
	t.Helper()

	pollUntilNoActiveImagesCtx(t, t.Context(), client, sub, ConnectedPollTimeout)
}

// PollUntilNoActiveImagesCtx polls DoWork until the subscription has zero active images
// or ConnectedPollTimeout elapses. When ctx carries a deadline, the effective bound is
// the earlier of ConnectedPollTimeout and the remaining ctx deadline.
func PollUntilNoActiveImagesCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
) {
	t.Helper()

	pollUntilNoActiveImagesCtx(t, ctx, client, sub, ConnectedPollTimeout)
}

func pollUntilNoActiveImagesCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
	bound time.Duration,
) {
	t.Helper()

	pollClientUntilDynamicCtx(
		t,
		ctx,
		client,
		bound,
		func() bool {
			return !aeron.SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		func() string {
			return subscriptionImageCountFailMsg(sub, 0)
		},
	)
}

// PollDelivered collects want payload fragments via DoWork and sub.Poll.
//
// Harness-only batch oracle; apps loop Subscription.PollOneMessage instead.
func PollDelivered(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
) [][]byte {
	t.Helper()

	return pollDeliveredCtx(t, t.Context(), client, sub, want, ConnectedPollTimeout)
}

func pollDeliveredCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	bound time.Duration,
) [][]byte {
	t.Helper()
	if !preparePollWant(t, want) {
		return nil
	}

	received := make([][]byte, 0, want)

	pollClientUntilDynamicCtx(
		t,
		ctx,
		client,
		bound,
		func() bool {
			received = CollectSubscriptionFragments(sub, want, received)
			return len(received) >= want
		},
		func() string {
			return pollDeliveredFailMsg(sub, len(received), want)
		},
	)
	return received
}

// PollDeliveredSplit polls DoWork on both clients and sub.Poll until want payload
// fragments are collected.
//
// Harness-only: split-client topology oracle (distinct pub/sub conductors). Not
// promoted to public aeron API.
func PollDeliveredSplit(
	t testing.TB,
	pubClient, subClient *aeron.Client,
	sub *aeron.Subscription,
	want int,
) [][]byte {
	t.Helper()

	return pollDeliveredSplitCtx(
		t,
		t.Context(),
		pubClient,
		subClient,
		sub,
		want,
		ConnectedPollTimeout,
	)
}

func pollDeliveredSplitCtx(
	t testing.TB,
	ctx context.Context,
	pubClient, subClient *aeron.Client,
	sub *aeron.Subscription,
	want int,
	bound time.Duration,
) [][]byte {
	t.Helper()
	if !preparePollWant(t, want) {
		return nil
	}

	received := make([][]byte, 0, want)

	pollSplitClientsUntilDynamicCtx(
		t,
		ctx,
		pubClient,
		subClient,
		bound,
		func() bool {
			received = CollectSubscriptionFragments(sub, want, received)
			return len(received) >= want
		},
		func() string {
			return pollDeliveredFailMsg(sub, len(received), want)
		},
	)
	return received
}

// OfferWithRetry delegates to resource OfferWithRetryObserved and maps typed
// errors to testing.TB failures.
//
// See [aeron.Publication.OfferWithRetryObserved].
func OfferWithRetry(
	t testing.TB,
	pub boundPublication,
	payload []byte,
) int64 {
	t.Helper()

	position, _ := offerWithRetry(t, pub, payload, nil)
	return position
}

// OfferWithRetryWitness is OfferWithRetry plus a saw-BackPressured-or-AdminAction
// flag.
//
// Harness-only oracle; not public aeron API.
func OfferWithRetryWitness(
	t testing.TB,
	pub boundPublication,
	payload []byte,
) (int64, bool) {
	t.Helper()

	return offerWithRetry(t, pub, payload, nil)
}

// OfferWithRetryWitnessAfterNotify is OfferWithRetryWitness and signals notify
// once on the first BackPressured or AdminAction (non-blocking send).
//
// Harness-only; not public aeron API.
func OfferWithRetryWitnessAfterNotify(
	t testing.TB,
	pub boundPublication,
	payload []byte,
	notify chan<- struct{},
) (int64, bool) {
	t.Helper()

	return offerWithRetry(t, pub, payload, notify)
}

// OfferStallTimeout derives stall timeout from offer retry timeout.
func OfferStallTimeout(timeout time.Duration) time.Duration {
	return aeron.OfferStallTimeout(timeout)
}

func offerWithRetry(
	t testing.TB,
	pub boundPublication,
	payload []byte,
	notify chan<- struct{},
) (int64, bool) {
	t.Helper()

	var (
		sawBackpressureOrAdmin bool
		notifySent             bool
	)
	onRetriable := func(status int64) {
		sawBackpressureOrAdmin = true
		if notify == nil || notifySent {
			return
		}
		select {
		case notify <- struct{}{}:
		default:
		}
		notifySent = true
	}

	position, err := pub.OfferWithRetryObserved(t.Context(), payload, onRetriable)
	if err == nil {
		return position, sawBackpressureOrAdmin
	}
	return failOfferWithRetry(t, pub, err), sawBackpressureOrAdmin
}

func failOfferWithRetry(
	t testing.TB,
	pub aeron.PublicationOfferer,
	err error,
) int64 {
	t.Helper()

	regID := pub.RegistrationID()
	streamID := pub.StreamID()
	sessionID := pub.SessionID()

	if terminal, ok := errors.AsType[*aeron.OfferTerminalError](err); ok {
		t.Errorf(
			"publication %d (stream %d session %d) offer failed: status=%s; position=%d",
			regID,
			streamID,
			sessionID,
			aeron.PositionCodeName(terminal.Status),
			terminal.Position,
		)
		t.FailNow()
		return terminal.Status
	}

	if stall, ok := errors.AsType[*aeron.OfferStallError](err); ok {
		t.Errorf(
			"publication %d (stream %d session %d) offer stalled within %s; position=%d; status=%s",
			regID,
			streamID,
			sessionID,
			stall.StallTimeout,
			stall.Position,
			aeron.PositionCodeName(stall.Status),
		)
		t.FailNow()
		return 0
	}

	if failDelegatedContext(t, err, "offer") {
		return 0
	}

	if timeoutErr, ok := errors.AsType[*aeron.OfferTimeoutError](err); ok {
		t.Errorf(
			"publication %d (stream %d session %d) offer timed out within %s; position=%d; status=%s",
			regID,
			streamID,
			sessionID,
			timeoutErr.Timeout,
			timeoutErr.Position,
			aeron.PositionCodeName(timeoutErr.Status),
		)
		t.FailNow()
		return 0
	}

	t.Errorf("unexpected offer error: %v", err)
	t.FailNow()
	return 0
}

// pollReassembledDelegatedMaxBuf sizes the want=1 Subscription.PollReassembled
// path.
//
// Covers FragmentedRoundtripPayload (maxPayload+512) with headroom.
const pollReassembledDelegatedMaxBuf = 256 * 1024

// PollReassembled receives want logical messages on live IPC.
//
// want==1 delegates to Subscription.PollReassembled; fragment count is always
// zero on that path (not a poll-fragment oracle).
//
// want>1 uses harness-only B/E stitch (not production FragmentAssembler):
// single image, in-order sequential fragments only. Orphan middle/END fails
// fast. New BEGIN resets partial assembly.
//
// Caller must drive client DoWork on the want>1 path.
func PollReassembled(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
) ([][]byte, int) {
	t.Helper()

	return pollReassembledCtx(t, t.Context(), client, sub, want, ConnectedPollTimeout)
}

func pollReassembledCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	bound time.Duration,
) ([][]byte, int) {
	t.Helper()
	if !preparePollWant(t, want) {
		return nil, 0
	}
	if want == 1 {
		return pollReassembledSingleDelegatedCtx(t, ctx, sub, bound)
	}
	return pollReassembledHarnessOracleCtx(t, ctx, client, sub, want, bound)
}

func pollReassembledSingleDelegatedCtx(
	t testing.TB,
	ctx context.Context,
	sub *aeron.Subscription,
	bound time.Duration,
) ([][]byte, int) {
	t.Helper()

	pollCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()

	effectiveWait := aeron.EffectiveRetryBound(ctx, bound, time.Now())

	buf := make([]byte, pollReassembledDelegatedMaxBuf)
	n, err := sub.PollReassembled(pollCtx, buf)
	if err == nil {
		return [][]byte{slices.Clone(buf[:n])}, 0
	}
	failPollReassembledDelegated(t, sub, err, effectiveWait)
	return nil, 0
}

func failPollReassembledDelegated(
	t testing.TB,
	sub *aeron.Subscription,
	err error,
	effectiveWait time.Duration,
) {
	t.Helper()

	if errors.Is(err, context.DeadlineExceeded) {
		_, detail := aeron.SubscriptionImageSummary(sub)
		failReassembledSingleTimeout(t, sub, detail, effectiveWait)
		return
	}

	if failDelegatedContext(t, err, "poll") {
		return
	}

	if _, ok := errors.AsType[*aeron.PollReassembledPreconditionError](err); ok {
		_, detail := aeron.SubscriptionImageSummary(sub)
		t.Errorf(
			"subscription %d reassembled 0/1 messages; fragments=0; %s",
			sub.RegistrationID(),
			detail,
		)
		t.FailNow()
		return
	}

	if bufErr, ok := errors.AsType[*aeron.PollBufferTooSmallError](err); ok {
		t.Errorf(
			"poll buffer too small: required=%d have=%d",
			bufErr.Required,
			bufErr.Have,
		)
		t.FailNow()
		return
	}

	if timeoutErr, ok := errors.AsType[*aeron.PollReassembledTimeoutError](err); ok {
		failReassembledSingleTimeout(t, sub, timeoutErr.Detail, timeoutErr.Timeout)
		return
	}

	t.Errorf("unexpected poll error: %v", err)
	t.FailNow()
}

func failReassembledSingleTimeout(
	t testing.TB,
	sub *aeron.Subscription,
	detail string,
	within time.Duration,
) {
	t.Helper()
	t.Errorf(
		"subscription %d reassembled 0/1 messages; fragments=0; %s within %s",
		sub.RegistrationID(),
		detail,
		within,
	)
	t.FailNow()
}

func pollReassembledHarnessOracleCtx(
	t testing.TB,
	ctx context.Context,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	bound time.Duration,
) ([][]byte, int) {
	t.Helper()

	if !aeron.SubscriptionActiveImageCountAtLeast(sub, 1) {
		_, detail := aeron.SubscriptionImageSummary(sub)
		t.Errorf(
			"subscription %d reassembled 0/%d messages; fragments=0; %s",
			sub.RegistrationID(),
			want,
			detail,
		)
		t.FailNow()
		return nil, 0
	}

	received := make([][]byte, 0, want)
	var pending []byte
	fragmentsPolled := 0
	handler := func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		fragmentsPolled++
		fragment := buffer.Bytes()[offset : offset+length]
		flags := header.Flags()
		begin := flags&logbuffer.BeginFlag != 0
		end := flags&logbuffer.EndFlag != 0

		switch {
		case begin && end:
			received = append(received, slices.Clone(fragment))
			pending = pending[:0]
		case begin:
			pending = slices.Clone(fragment)
		case end:
			if len(pending) == 0 {
				t.Errorf(
					"subscription %d unexpected END fragment without BEGIN",
					sub.RegistrationID(),
				)
				t.FailNow()
				return
			}
			received = append(
				received,
				append(slices.Clone(pending), slices.Clone(fragment)...),
			)
			pending = pending[:0]
		default:
			if len(pending) == 0 {
				t.Errorf(
					"subscription %d unexpected middle fragment without BEGIN",
					sub.RegistrationID(),
				)
				t.FailNow()
				return
			}
			pending = append(pending, slices.Clone(fragment)...)
		}
	}

	pollClientUntilDynamicCtx(
		t,
		ctx,
		client,
		bound,
		func() bool {
			if failedCaptureTB(t) {
				return true
			}
			// fragmentLimit caps fragments per Poll call, not logical messages.
			pollSubscriptionFragments(sub, want, len(received), handler)
			return len(received) >= want || failedCaptureTB(t)
		},
		func() string {
			_, detail := aeron.SubscriptionImageSummary(sub)
			return fmt.Sprintf(
				"subscription %d reassembled %d/%d messages; fragments=%d; %s",
				sub.RegistrationID(),
				len(received),
				want,
				fragmentsPolled,
				detail,
			)
		},
	)
	return received, fragmentsPolled
}

func pollSubscriptionFragments(
	sub *aeron.Subscription,
	want int,
	have int,
	handler aeron.FragmentHandler,
) {
	if remaining := want - have; remaining > 0 {
		sub.Poll(handler, remaining)
	}
}

func discardSubscriptionFragment(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	_ int,
	_ *aeron.Header,
) {
}

// drainSubscriptionFragments polls up to want fragments without retaining payloads.
func drainSubscriptionFragments(sub *aeron.Subscription, want int) {
	pollSubscriptionFragments(sub, want, 0, discardSubscriptionFragment)
}

// CollectSubscriptionFragments appends up to want-len(received) payload fragments
// from a single sub.Poll tick.
func CollectSubscriptionFragments(
	sub *aeron.Subscription,
	want int,
	received [][]byte,
) [][]byte {
	pollSubscriptionFragments(sub, want, len(received), func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		_ *aeron.Header,
	) {
		received = append(
			received,
			slices.Clone(buffer.Bytes()[offset:offset+length]),
		)
	})
	return received
}

// rolloverTermCrossMargin is extra offers beyond one full term to cross PAD/rotation.
const rolloverTermCrossMargin = 2

// ComputeRolloverMessageCount returns offers needed to exceed one term partition.
//
// termLen and payloadSize must be positive; otherwise returns zero.
func ComputeRolloverMessageCount(termLen, payloadSize int) int {
	if termLen <= 0 || payloadSize <= 0 {
		return 0
	}
	frameLen := logbuffer.AlignFragmentChunkLength(payloadSize)
	return (termLen+frameLen-1)/frameLen + rolloverTermCrossMargin
}

// AlignedOfferAdvance returns aligned DATA frame bytes for offerCount fixed-size payloads.
func AlignedOfferAdvance(payloadSize, offerCount int) int64 {
	return int64(logbuffer.AlignFragmentChunkLength(payloadSize)) * int64(offerCount)
}

// PayloadsWithPrefix builds count fixed-size payloads; index i is ASCII prefix
// followed by decimal i, padded with 0x00 to size bytes.
func PayloadsWithPrefix(prefix string, count, size int) [][]byte {
	payloads := make([][]byte, count)
	for index := range count {
		payloads[index] = make([]byte, size)
		copy(payloads[index], strconv.AppendInt([]byte(prefix), int64(index), 10))
	}
	return payloads
}

// DistinctIPCPayloads builds count same-size payloads; full slices differ when
// size >= 2 and count <= 256.
//
// Byte 0 uses headerBase|index for tagging and may collide across indices.
// Bytes 1..size-1 repeat byte(index) for slice identity.
func DistinctIPCPayloads(count, size int, headerBase byte) [][]byte {
	payloads := make([][]byte, count)
	for index := range count {
		payloads[index] = make([]byte, size)
		payloads[index][0] = headerBase | byte(index)
		for offset := 1; offset < size; offset++ {
			payloads[index][offset] = byte(index)
		}
	}
	return payloads
}

// FragmentedRoundtripExtraBytes is the default extra size for FragmentedRoundtripPayload.
const FragmentedRoundtripExtraBytes = 512

// FragmentedRoundtripPayload builds a patterned payload larger than one DATA frame.
func FragmentedRoundtripPayload(maxPayload, extra int) []byte {
	payload := make([]byte, maxPayload+extra)
	payload[0] = 0xF0
	for offset := 1; offset < len(payload); offset++ {
		payload[offset] = byte(offset)
	}
	return payload
}

// AssertPubSubIPCMetadata checks publication and image positions advanced by wantAdvance
// and that stream and session IDs match across pub, sub, and image.
func AssertPubSubIPCMetadata(
	t testing.TB,
	streamID int32,
	pub aeron.PublicationOfferer,
	sub *aeron.Subscription,
	pubPosBefore int64,
	wantAdvance int64,
) {
	t.Helper()

	image := AssertPubSubPositionsAdvanced(t, pub, sub, pubPosBefore, wantAdvance)

	require.Equal(t, streamID, pub.StreamID())
	AssertSubscribeMetadata(t, streamID, pub.SessionID(), sub, image.JoinPosition(), wantAdvance)
}

// AssertSubscribeMetadata checks subscriber-only IPC metadata when Go does not publish.
//
// Verifies stream ID on sub and image, session ID on image, and image.Position() equals
// joinPosBefore+wantAdvance.
func AssertSubscribeMetadata(
	t testing.TB,
	streamID int32,
	sessionID int32,
	sub *aeron.Subscription,
	joinPosBefore int64,
	wantAdvance int64,
) {
	t.Helper()

	require.Equal(t, streamID, sub.StreamID())

	image := RequireFirstImage(t, sub)
	require.Equal(t, streamID, image.StreamID())
	require.Equal(t, sessionID, image.SessionID())
	require.Equal(t, joinPosBefore+wantAdvance, image.Position())
}

// AssertPubSubPositionsAdvanced checks publication and image positions advanced by advance.
func AssertPubSubPositionsAdvanced(
	t testing.TB,
	pub aeron.PublicationOfferer,
	sub *aeron.Subscription,
	pubPosBefore int64,
	advance int64,
) *aeron.Image {
	t.Helper()

	require.Equal(t, pubPosBefore+advance, pub.Position())

	image := RequireFirstImage(t, sub)
	require.Equal(t, image.JoinPosition()+advance, image.Position())
	return image
}

// RequireFirstImage returns ImageAt(0) or fails.
//
// Use after ConnectExclusiveIPCPubSubClients or similar has already connected
// the subscription; this helper does not wait for an image.
func RequireFirstImage(t testing.TB, sub *aeron.Subscription) *aeron.Image {
	t.Helper()

	image, ok := sub.ImageAt(0)
	require.True(t, ok, "subscription has no image at index 0")
	return image
}

func preparePollWant(t testing.TB, want int) bool {
	t.Helper()
	if want <= 0 {
		t.Errorf(`"%d" is not positive`, want)
		t.FailNow()
		return false
	}
	return !failedCaptureTB(t)
}

func pollDeliveredFailMsg(sub *aeron.Subscription, received, want int) string {
	_, detail := aeron.SubscriptionImageSummary(sub)
	return fmt.Sprintf(
		"subscription %d delivered %d/%d fragments; %s",
		sub.RegistrationID(),
		received,
		want,
		detail,
	)
}

func formatSubscriptionImageCountMsg(
	regID int64,
	streamID int32,
	active, want int,
	detail string,
) string {
	return fmt.Sprintf(
		"subscription %d (stream %d) active image count=%d want=%d; %s",
		regID,
		streamID,
		active,
		want,
		detail,
	)
}

func subscriptionImageCountFailMsg(sub *aeron.Subscription, want int) string {
	active, detail := aeron.SubscriptionImageSummary(sub)
	return formatSubscriptionImageCountMsg(
		sub.RegistrationID(),
		sub.StreamID(),
		active,
		want,
		detail,
	)
}

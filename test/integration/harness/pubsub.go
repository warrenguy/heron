//go:build integration

package harness

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// PollUntilConnected polls DoWork until pub.IsConnected on live IPC.
func PollUntilConnected(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	timeout time.Duration,
) {
	t.Helper()

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		pub.IsConnected,
		func() string {
			return fmt.Sprintf(
				"publication %d (stream %d session %d) never connected; position=%d",
				pub.RegistrationID(),
				pub.StreamID(),
				pub.SessionID(),
				pub.Position(),
			)
		},
	)
}

// PollUntilImageCount polls DoWork until the subscription has want non-closed images.
func PollUntilImageCount(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	timeout time.Duration,
) {
	t.Helper()
	if !preparePollWant(t, want) {
		return
	}

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		func() bool {
			return SubscriptionActiveImageCountAtLeast(sub, want)
		},
		func() string {
			return subscriptionImageCountFailMsg(sub, want)
		},
	)
}

// RequireNoActiveImages fails fast if the subscription has any active image.
func RequireNoActiveImages(t testing.TB, sub *aeron.Subscription) {
	t.Helper()

	if SubscriptionActiveImageCountAtLeast(sub, 1) {
		require.FailNow(t, subscriptionImageCountFailMsg(sub, 0))
	}
}

// SubscriptionActiveImageCountAtLeast reports whether sub has at least want active images.
func SubscriptionActiveImageCountAtLeast(sub *aeron.Subscription, want int) bool {
	active := 0
	for index := range sub.ImageCount() {
		if _, ok := sub.ImageAt(index); ok {
			active++
			if active >= want {
				return true
			}
		}
	}
	return false
}

// PollSplitClientsUntil polls DoWork on both clients until ok or timeout.
func PollSplitClientsUntil(
	t testing.TB,
	clientA, clientB *aeron.Client,
	timeout time.Duration,
	ok func() bool,
	failMsg string,
) {
	t.Helper()

	timeout = connectedPollTimeoutOr(timeout)
	if !pollUntilBool(
		timeout,
		aeron.DefaultIdleSleepDuration,
		func() bool {
			clientA.DoWork()
			clientB.DoWork()
			return ok()
		},
	) {
		t.Errorf("%s within %s", failMsg, timeout)
		t.FailNow()
	}
}

// PollSplitClientIPCLink polls both clients until pub is connected and sub has active images.
func PollSplitClientIPCLink(
	t testing.TB,
	pubClient, subClient *aeron.Client,
	pub *aeron.Publication,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()

	PollSplitClientsUntil(
		t,
		pubClient,
		subClient,
		timeout,
		func() bool {
			return pub.IsConnected() && SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		"split-client IPC link not ready",
	)
}

// PollUntilNoActiveImagesSplit polls both clients until sub has zero active images.
func PollUntilNoActiveImagesSplit(
	t testing.TB,
	pubClient, subClient *aeron.Client,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()

	PollSplitClientsUntil(
		t,
		pubClient,
		subClient,
		timeout,
		func() bool {
			return !SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		subscriptionImageCountFailMsg(sub, 0),
	)
}

// PollUntilNoActiveImages polls DoWork until the subscription has zero active images.
func PollUntilNoActiveImages(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	timeout time.Duration,
) {
	t.Helper()

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		func() bool {
			return !SubscriptionActiveImageCountAtLeast(sub, 1)
		},
		func() string {
			return subscriptionImageCountFailMsg(sub, 0)
		},
	)
}

// PollDelivered polls DoWork and sub.Poll until want payload fragments are collected.
func PollDelivered(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	timeout time.Duration,
) [][]byte {
	t.Helper()
	if !preparePollWant(t, want) {
		return nil
	}

	received := make([][]byte, 0, want)

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		func() bool {
			received = CollectSubscriptionFragments(sub, want, received)
			return len(received) >= want
		},
		func() string {
			_, detail := subscriptionImageSummary(sub)
			return fmt.Sprintf(
				"subscription %d delivered %d/%d fragments; %s",
				sub.RegistrationID(),
				len(received),
				want,
				detail,
			)
		},
	)
	return received
}

// Retries BackPressured and NotConnected with DoWork and idle sleep. AdminAction
// retries with DoWork only; Aeron requires retrying AdminAction, and a mid-fragment
// AdminAction may leave a padding-terminated partial in the log before a successful
// full retry.
//
// Terminal codes (Closed, MaxPositionExceeded) fail immediately.
func OfferWithRetry(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	payload []byte,
	timeout time.Duration,
) int64 {
	t.Helper()

	position, _ := offerWithRetry(t, client, pub, payload, timeout, nil)
	return position
}

// OfferWithRetryWitness is OfferWithRetry and reports whether any transient
// BackPressured or AdminAction occurred before a successful offer.
//
// Returns the successful publication position and sawBackpressureOrAdmin.
func OfferWithRetryWitness(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	payload []byte,
	timeout time.Duration,
) (int64, bool) {
	t.Helper()

	return offerWithRetry(t, client, pub, payload, timeout, nil)
}

// OfferWithRetryWitnessAfterNotify is OfferWithRetryWitness and sends one
// non-blocking signal on notify when the first transient BackPressured or
// AdminAction is observed, so callers can gate work (e.g. subscriber drain)
// until the witness has seen backpressure.
func OfferWithRetryWitnessAfterNotify(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	payload []byte,
	timeout time.Duration,
	notify chan<- struct{},
) (int64, bool) {
	t.Helper()

	return offerWithRetry(t, client, pub, payload, timeout, notify)
}

func offerStallTimeout(timeout time.Duration) time.Duration {
	stall := timeout / 5
	if stall < time.Second && timeout >= 5*time.Second {
		return time.Second
	}
	return stall
}

func offerWithRetry(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	payload []byte,
	timeout time.Duration,
	notify chan<- struct{},
) (int64, bool) {
	timeout = connectedPollTimeoutOr(timeout)
	stallTimeout := offerStallTimeout(timeout)
	deadline := time.Now().Add(timeout)
	var (
		lastStatus             int64
		sawBackpressureOrAdmin bool
		retryPosition          = pub.Position()
		stallMarked            time.Time
	)
	for time.Now().Before(deadline) {
		result := pub.Offer(payload)
		if result > 0 {
			return result, sawBackpressureOrAdmin
		}
		lastStatus = result
		position := pub.Position()
		if position > retryPosition {
			retryPosition = position
			stallMarked = time.Time{}
		} else if result == aeron.BackPressured || result == aeron.AdminAction {
			if stallMarked.IsZero() {
				stallMarked = time.Now()
			} else if time.Since(stallMarked) >= stallTimeout {
				t.Errorf(
					"publication %d (stream %d session %d) offer stalled within %s; position=%d; status=%d",
					pub.RegistrationID(),
					pub.StreamID(),
					pub.SessionID(),
					stallTimeout,
					position,
					lastStatus,
				)
				t.FailNow()
				return 0, sawBackpressureOrAdmin
			}
		} else if result == aeron.NotConnected {
			stallMarked = time.Time{}
		}
		switch result {
		case aeron.AdminAction, aeron.BackPressured:
			if notify != nil {
				select {
				case notify <- struct{}{}:
				default:
				}
				notify = nil
			}
			sawBackpressureOrAdmin = true
			client.DoWork()
			if result == aeron.BackPressured {
				time.Sleep(aeron.DefaultIdleSleepDuration)
			}
		case aeron.NotConnected:
			client.DoWork()
			time.Sleep(aeron.DefaultIdleSleepDuration)
		default:
			t.Errorf(
				"publication %d (stream %d session %d) offer failed: status=%d; position=%d",
				pub.RegistrationID(),
				pub.StreamID(),
				pub.SessionID(),
				result,
				pub.Position(),
			)
			t.FailNow()
			return result, sawBackpressureOrAdmin
		}
	}

	t.Errorf(
		"publication %d (stream %d session %d) offer timed out within %s; position=%d; status=%d",
		pub.RegistrationID(),
		pub.StreamID(),
		pub.SessionID(),
		timeout,
		pub.Position(),
		lastStatus,
	)
	t.FailNow()
	return 0, sawBackpressureOrAdmin
}

// Harness-only reassembly via Begin/Middle/End flags; not a production FragmentAssembler.
//
// Assumes a single assembly context, a single publisher/image, and in-order sequential
// fragmented messages only; not safe for multi-image or interleaved fragmentation.
//
// Orphan middle or END fragments (no prior BEGIN in pending) fail fast.
//
// A new BEGIN resets in-progress assembly (expected after AdminAction partial + retry).
//
// Returns reassembled payloads and the number of poll fragments seen.
func PollReassembled(
	t testing.TB,
	client *aeron.Client,
	sub *aeron.Subscription,
	want int,
	timeout time.Duration,
) ([][]byte, int) {
	t.Helper()
	if !preparePollWant(t, want) {
		return nil, 0
	}

	if !SubscriptionActiveImageCountAtLeast(sub, 1) {
		_, detail := subscriptionImageSummary(sub)
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
		buffer atomic.AtomicBuffer,
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
			// OfferWithRetry mid-fragment AdminAction can commit an orphan BEGIN before
			// padding and a full retry; see OfferWithRetry godoc and
			// TestPublication_FragmentedOffer_midFragmentTermIDChangeAdminAction.
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

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		func() bool {
			if failedCaptureTB(t) {
				return true
			}
			// fragmentLimit caps fragments per Poll call, not logical messages.
			pollSubscriptionFragments(sub, want, len(received), handler)
			return len(received) >= want || failedCaptureTB(t)
		},
		func() string {
			_, detail := subscriptionImageSummary(sub)
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
	_ atomic.AtomicBuffer,
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
		buffer atomic.AtomicBuffer,
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

// AssertPubSubPositionsAdvanced checks publication and image positions advanced by advance.
func AssertPubSubPositionsAdvanced(
	t testing.TB,
	pub *aeron.Publication,
	sub *aeron.Subscription,
	pubPosBefore int64,
	advance int64,
) {
	t.Helper()

	require.Equal(t, pubPosBefore+advance, pub.Position())

	image, ok := sub.ImageAt(0)
	require.True(t, ok)
	require.Equal(t, image.JoinPosition()+advance, image.Position())
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

func subscriptionImageCountFailMsg(sub *aeron.Subscription, want int) string {
	active, detail := subscriptionImageSummary(sub)
	return fmt.Sprintf(
		"subscription %d (stream %d) active image count=%d want=%d; %s",
		sub.RegistrationID(),
		sub.StreamID(),
		active,
		want,
		detail,
	)
}

func subscriptionImageSummary(sub *aeron.Subscription) (active int, detail string) {
	imageCount := sub.ImageCount()
	if imageCount == 0 {
		return 0, "no images"
	}

	var positions strings.Builder
	for index := range imageCount {
		image, ok := sub.ImageAt(index)
		if !ok {
			continue
		}
		active++
		fmt.Fprintf(
			&positions,
			" [%d stream=%d session=%d position=%d]",
			index,
			image.StreamID(),
			image.SessionID(),
			image.Position(),
		)
	}
	if active == 0 {
		return 0, fmt.Sprintf("active=0 slice=%d", imageCount)
	}
	return active, fmt.Sprintf("active=%d%s", active, positions.String())
}

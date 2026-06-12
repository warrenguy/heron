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
			return subscriptionActiveImageCountAtLeast(sub, want)
		},
		func() string {
			active, detail := subscriptionImageSummary(sub)
			return fmt.Sprintf(
				"subscription %d (stream %d) active image count=%d want=%d; %s",
				sub.RegistrationID(),
				sub.StreamID(),
				active,
				want,
				detail,
			)
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
	handler := func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		_ *aeron.Header,
	) {
		received = append(
			received,
			slices.Clone(buffer.Bytes()[offset:offset+length]),
		)
	}

	pollClientUntilDynamic(
		t,
		client,
		connectedPollTimeoutOr(timeout),
		func() bool {
			pollSubscriptionFragments(sub, want, len(received), handler)
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

// OfferWithRetry loops Offer until success or deadline.
//
// Retries BackPressured and NotConnected with DoWork and idle sleep. AdminAction
// retries with DoWork only; Aeron requires retrying AdminAction, and a mid-fragment
// AdminAction may leave a padding-terminated partial in the log before a successful
// full retry.
// Terminal codes (Closed, MaxPositionExceeded) fail immediately.
func OfferWithRetry(
	t testing.TB,
	client *aeron.Client,
	pub *aeron.Publication,
	payload []byte,
	timeout time.Duration,
) int64 {
	t.Helper()

	timeout = connectedPollTimeoutOr(timeout)
	deadline := time.Now().Add(timeout)
	var lastStatus int64
	for time.Now().Before(deadline) {
		result := pub.Offer(payload)
		if result > 0 {
			return result
		}
		lastStatus = result
		switch result {
		case aeron.AdminAction:
			client.DoWork()
		case aeron.BackPressured, aeron.NotConnected:
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
			return result
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
	return 0
}

// PollReassembled polls DoWork and sub.Poll until want logical messages are reassembled.
//
// Harness-only reassembly via Begin/Middle/End flags; not a production FragmentAssembler.
// Assumes a single assembly context, a single publisher/image, and in-order sequential
// fragmented messages only; not safe for multi-image or interleaved fragmentation.
// Orphan middle or END fragments (no prior BEGIN in pending) fail fast.
// - A new BEGIN resets in-progress assembly (expected after AdminAction partial + retry).
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

	if !subscriptionActiveImageCountAtLeast(sub, 1) {
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

func subscriptionActiveImageCountAtLeast(sub *aeron.Subscription, want int) bool {
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

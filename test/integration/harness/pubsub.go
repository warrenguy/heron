//go:build integration

package harness

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/atomic"
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
			remaining := want - len(received)
			if remaining > 0 {
				sub.Poll(handler, remaining)
			}
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

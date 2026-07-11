package aeron

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// EffectiveRetryBound returns the earlier of effective and the remaining ctx deadline
// measured from now. Callers normalize timeout (zero default, negative policy) first.
func EffectiveRetryBound(
	ctx context.Context,
	effective time.Duration,
	now time.Time,
) time.Duration {
	ctxDeadline, ok := ctx.Deadline()
	if !ok {
		return effective
	}
	cfgDeadline := now.Add(effective)
	if !ctxDeadline.Before(cfgDeadline) {
		return effective
	}
	return max(ctxDeadline.Sub(now), 0)
}

// RetryDeadlineNs returns a retry-loop expiry deadline on the caller's NanoClock
// epoch along with the effective bound. bound clamps timeout to ctx.Deadline() via
// EffectiveRetryBound using wall-clock time.Now(); startNs is the loop NanoClock
// reading at loop entry and feeds only the returned deadlineNs expiry check inside
// the loop.
func RetryDeadlineNs(
	ctx context.Context,
	startNs int64,
	timeout time.Duration,
) (deadlineNs int64, bound time.Duration) {
	bound = EffectiveRetryBound(ctx, timeout, time.Now())
	return startNs + bound.Nanoseconds(), bound
}

func retryDeadline(
	ctx context.Context,
	timeout time.Duration,
) (time.Time, time.Duration, error) {
	now := time.Now()
	effective := EffectiveRetryBound(ctx, timeout, now)
	deadline := now.Add(effective)
	if err := ctx.Err(); err != nil {
		return time.Time{}, 0, err
	}
	return deadline, effective, nil
}

// PreferContextError returns ctx.Err() when the context is canceled or its deadline
// has passed; otherwise returns err. Use on retry-loop exits so typed timeout or
// terminal errors apply only while the context remains active.
func PreferContextError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	ctxDeadline, ok := ctx.Deadline()
	if ok && !time.Now().Before(ctxDeadline) {
		return context.DeadlineExceeded
	}
	return err
}

// pollWorkLoop drives client.DoWork until ready is true, ctx is canceled, or deadline passes.
func pollWorkLoop(
	ctx context.Context,
	client *Client,
	deadline time.Time,
	ready func() bool,
) (bool, error) {
	for {
		if ready() {
			return true, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		client.DoWork()
		if ready() {
			return true, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		time.Sleep(DefaultIdleSleepDuration)
	}
}

// SubscriptionActiveImageCountAtLeast reports whether sub has at least want active images.
//
// want must be positive; non-positive want returns false.
//
// Iterates ImageAt, not raw ImageCount.
func SubscriptionActiveImageCountAtLeast(sub *Subscription, want int) bool {
	if want <= 0 {
		return false
	}
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

// SubscriptionImageSummary reports active image count and a diagnostic detail string.
func SubscriptionImageSummary(sub *Subscription) (active int, detail string) {
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

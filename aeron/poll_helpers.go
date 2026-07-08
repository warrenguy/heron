package aeron

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/warrenguy/heron/agrona/atomic"
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

// PollUntilConnected polls until pub.IsConnected or Context.PollTimeout elapses.
//
// When ctx carries a deadline, the effective bound is the earlier of PollTimeout and
// ctx.Deadline().
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates for error strings. Not zero-alloc.
func PollUntilConnected(
	ctx context.Context,
	client *Client,
	pub PublicationOfferer,
) error {
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return err
	}

	ready := func() bool { return pub.IsConnected() }
	if ok, err := pollWorkLoop(ctx, client, deadline, ready); err != nil {
		return err
	} else if ok {
		return nil
	}
	return PreferContextError(ctx, &PollUntilConnectedTimeoutError{
		PublicationRegID: pub.RegistrationID(),
		StreamID:         pub.StreamID(),
		SessionID:        pub.SessionID(),
		Timeout:          effective,
		Position:         pub.Position(),
	})
}

// PollUntilImageCount polls until the subscription has want active images or
// Context.PollTimeout elapses.
//
// When ctx carries a deadline, the effective bound is the earlier of PollTimeout and
// ctx.Deadline().
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates for error strings. Not zero-alloc.
func PollUntilImageCount(
	ctx context.Context,
	client *Client,
	sub *Subscription,
	want int,
) error {
	if want <= 0 {
		return ErrInvalidWant
	}
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return err
	}

	ready := func() bool { return SubscriptionActiveImageCountAtLeast(sub, want) }
	if ok, err := pollWorkLoop(ctx, client, deadline, ready); err != nil {
		return err
	} else if ok {
		return nil
	}
	active, detail := SubscriptionImageSummary(sub)
	return PreferContextError(ctx, &PollUntilImageCountTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Want:           want,
		Have:           active,
		Timeout:        effective,
		Detail:         detail,
	})
}

// PollOneMessage receives one unfragmented payload. Does not use FragmentAssembler.
//
// Delivers the first fragment from the first sub.Poll tick that returns work; ignores
// additional handler invocations in the same tick (fragmentLimit is 1).
// Clones that fragment into a newly allocated slice.
//
// Callers should wait for subscriber readiness first, e.g.
// PollUntilImageCount(ctx, client, sub, 1), because this helper does not fail fast on
// zero active images. For fragmented payloads use PollReassembled.
//
// When ctx carries a deadline, the effective bound is the earlier of PollTimeout and
// ctx.Deadline().
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates cloned payload and error strings. Not zero-alloc.
func PollOneMessage(
	ctx context.Context,
	client *Client,
	sub *Subscription,
) ([]byte, error) {
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return nil, err
	}

	var (
		msg      []byte
		received bool
	)
	handler := func(buf atomic.AtomicBuffer, off, length int, _ *Header) {
		if received {
			return
		}
		msg = slices.Clone(buf.Bytes()[off : off+length])
		received = true
	}
	ready := func() bool { return sub.Poll(handler, 1) > 0 && received }

	if ok, err := pollWorkLoop(ctx, client, deadline, ready); err != nil {
		return nil, err
	} else if ok {
		return msg, nil
	}
	_, detail := SubscriptionImageSummary(sub)
	return nil, PreferContextError(ctx, &PollOneMessageTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Timeout:        effective,
		Detail:         detail,
	})
}

// PollReassembled receives one logical message using FragmentAssembler.
//
// When sub has zero active images, returns PollReassembledPreconditionError
// immediately. Prefer PollUntilImageCount before PollReassembled to avoid this path.
//
// If buf is nil or cap(buf) < payload length, returns PollBufferTooSmallError.
// The logical message is consumed from the subscription on that error; retrying
// with a larger buffer will not re-read the same message.
// On success returns (n, nil) where n is payload length copied into buf[:n].
//
// When ctx carries a deadline, the effective bound is the earlier of PollTimeout and
// ctx.Deadline().
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: heap-allocates a FragmentAssembler per call. Not zero-alloc.
func PollReassembled(
	ctx context.Context,
	client *Client,
	sub *Subscription,
	buf []byte,
) (int, error) {
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return 0, err
	}
	if !SubscriptionActiveImageCountAtLeast(sub, 1) {
		return 0, &PollReassembledPreconditionError{
			RegistrationID: sub.RegistrationID(),
			StreamID:       sub.StreamID(),
			Detail:         "no active images",
		}
	}

	var (
		ready      bool
		copied     bool
		payloadLen int
	)
	assembler := NewFragmentAssembler(func(termBuf atomic.AtomicBuffer, off, length int, _ *Header) {
		if ready {
			return
		}
		payloadLen = length
		ready = true
		if cap(buf) >= length {
			copy(buf[:length], termBuf.Bytes()[off:off+length])
			copied = true
		}
	})

	finish := func() (int, error) {
		if !copied {
			return 0, &PollBufferTooSmallError{Required: payloadLen, Have: cap(buf)}
		}
		return payloadLen, nil
	}
	handler := assembler.Handler()
	tryPoll := func() bool {
		return sub.Poll(handler, 1) > 0 && ready
	}

	if ok, err := pollWorkLoop(ctx, client, deadline, tryPoll); err != nil {
		return 0, err
	} else if ok {
		return finish()
	}
	_, detail := SubscriptionImageSummary(sub)
	return 0, PreferContextError(ctx, &PollReassembledTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Timeout:        effective,
		Detail:         detail,
	})
}

// RunDuringClientWork runs fn on a goroutine and drives client.DoWork until fn returns,
// ctx is canceled, or timeout elapses.
//
// Timeout or cancellation returns promptly; fn continues running in the background until
// it finishes on its own; this helper does not wait for fn to exit after returning.
//
// When ctx carries a deadline, the effective bound is the earlier of PollTimeout and
// ctx.Deadline().
//
// Does not install an ErrorHandler; callers should wrap client.Context.ErrorHandler
// if transport errors must fail the wait.
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: goroutine bookkeeping and error strings. Not zero-alloc.
func RunDuringClientWork(
	ctx context.Context,
	client *Client,
	fn func() error,
) error {
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	for {
		select {
		case fnErr := <-done:
			return fnErr
		default:
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return PreferContextError(ctx, &RunDuringClientWorkTimeoutError{
				Timeout: effective,
			})
		}
		client.DoWork()
		if err := ctx.Err(); err != nil {
			return err
		}
		time.Sleep(DefaultIdleSleepDuration)
	}
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

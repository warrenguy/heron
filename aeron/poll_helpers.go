package aeron

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/warrenguy/heron/agrona/atomic"
)

// DefaultConnectedPollTimeout matches harness ConnectedPollTimeout (10s).
const DefaultConnectedPollTimeout = 10 * time.Second

func pollDeadline(ctx context.Context, timeout time.Duration) (time.Time, time.Duration, error) {
	if timeout < 0 {
		return time.Time{}, 0, ErrInvalidTimeout
	}
	effective := timeout
	if effective == 0 {
		effective = DefaultConnectedPollTimeout
	}
	deadline := time.Now().Add(effective)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, 0, err
	}
	return deadline, effective, nil
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

// PollUntilConnected polls until pub.IsConnected or timeout.
//
// Zero timeout maps to DefaultConnectedPollTimeout.
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates for error strings. Not zero-alloc.
func PollUntilConnected(
	ctx context.Context,
	client *Client,
	pub PublicationOfferer,
	timeout time.Duration,
) error {
	deadline, effective, err := pollDeadline(ctx, timeout)
	if err != nil {
		return err
	}

	ready := func() bool { return pub.IsConnected() }
	if ok, err := pollWorkLoop(ctx, client, deadline, ready); err != nil {
		return err
	} else if ok {
		return nil
	}
	return &PollUntilConnectedTimeoutError{
		PublicationRegID: pub.RegistrationID(),
		StreamID:         pub.StreamID(),
		SessionID:        pub.SessionID(),
		Timeout:          effective,
		Position:         pub.Position(),
	}
}

// PollUntilImageCount polls until the subscription has want active images or timeout.
//
// Zero timeout maps to DefaultConnectedPollTimeout.
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates for error strings. Not zero-alloc.
func PollUntilImageCount(
	ctx context.Context,
	client *Client,
	sub *Subscription,
	want int,
	timeout time.Duration,
) error {
	if want <= 0 {
		return ErrInvalidWant
	}
	deadline, effective, err := pollDeadline(ctx, timeout)
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
	return &PollUntilImageCountTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Want:           want,
		Have:           active,
		Timeout:        effective,
		Detail:         detail,
	}
}

// PollOneMessage receives one unfragmented payload. Does not use FragmentAssembler.
//
// Delivers the first fragment from the first sub.Poll tick that returns work; ignores
// additional handler invocations in the same tick (fragmentLimit is 1).
// Clones that fragment into a newly allocated slice.
//
// Callers should wait for subscriber readiness first, e.g.
// PollUntilImageCount(ctx, client, sub, 1, timeout), because this helper does not
// fail fast on zero active images. For fragmented payloads use PollReassembled.
//
// Zero timeout maps to DefaultConnectedPollTimeout.
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: allocates cloned payload and error strings. Not zero-alloc.
func PollOneMessage(
	ctx context.Context,
	client *Client,
	sub *Subscription,
	timeout time.Duration,
) ([]byte, error) {
	deadline, effective, err := pollDeadline(ctx, timeout)
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
	return nil, &PollOneMessageTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Timeout:        effective,
		Detail:         detail,
	}
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
// Zero timeout maps to DefaultConnectedPollTimeout.
//
// This helper drives client.DoWork() inside its wait loop; the caller need not
// invoke DoWork separately while blocked here.
// Cold-path integration ergonomics: heap-allocates a FragmentAssembler per call. Not zero-alloc.
func PollReassembled(
	ctx context.Context,
	client *Client,
	sub *Subscription,
	timeout time.Duration,
	buf []byte,
) (int, error) {
	deadline, effective, err := pollDeadline(ctx, timeout)
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
	return 0, &PollReassembledTimeoutError{
		RegistrationID: sub.RegistrationID(),
		StreamID:       sub.StreamID(),
		Timeout:        effective,
		Detail:         detail,
	}
}

// RunDuringClientWork runs fn on a goroutine and drives client.DoWork until fn returns,
// ctx is canceled, or timeout elapses.
//
// Timeout or cancellation returns promptly; fn continues running in the background until
// it finishes on its own; this helper does not wait for fn to exit after returning.
//
// Zero timeout maps to DefaultConnectedPollTimeout.
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
	timeout time.Duration,
	fn func() error,
) error {
	deadline, effective, err := pollDeadline(ctx, timeout)
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
			return &RunDuringClientWorkTimeoutError{
				Timeout: effective,
			}
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

package aeron

import (
	"context"
	"time"
)

// PollUntilConnected polls until pub.IsConnected or [Config.PollTimeout] elapses.
//
// See [Client] for poll wait timeout and DoWork contract.
func (client *Client) PollUntilConnected(
	ctx context.Context,
	pub PublicationOfferer,
) error {
	return client.pollUntil(ctx, pub.IsConnected, func(timeout time.Duration) error {
		regID, streamID, sessionID := publicationIdentity(pub)
		return &PollUntilConnectedTimeoutError{
			PublicationRegID: regID,
			StreamID:         streamID,
			SessionID:        sessionID,
			Timeout:          timeout,
			Position:         pub.Position(),
		}
	})
}

// PollUntilImageCount polls until the subscription has want active images or
// [Config.PollTimeout] elapses.
//
// Non-positive want returns [ErrInvalidWant].
//
// See [Client] for poll wait timeout and DoWork contract.
func (client *Client) PollUntilImageCount(
	ctx context.Context,
	sub *Subscription,
	want int,
) error {
	if want <= 0 {
		return ErrInvalidWant
	}

	ready := func() bool { return SubscriptionActiveImageCountAtLeast(sub, want) }
	return client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		active, detail := SubscriptionImageSummary(sub)
		return &PollUntilImageCountTimeoutError{
			RegistrationID: sub.RegistrationID(),
			StreamID:       sub.StreamID(),
			Want:           want,
			Have:           active,
			Timeout:        timeout,
			Detail:         detail,
		}
	})
}

// PollOneMessage receives one unfragmented payload. Does not use [FragmentAssembler].
//
// Delivers the first fragment from the first sub.Poll tick that returns work; ignores
// additional handler invocations in the same tick (fragmentLimit is 1).
//
// Clones non-empty fragments into a newly allocated slice.
//
// Zero-length payloads return nil, not an empty non-nil slice; see [FragmentHandlerBytes].
//
// Callers should wait for subscriber readiness first, e.g.
// [Client.PollUntilImageCount](ctx, sub, 1), because this method does not fail fast on
// zero active images. For fragmented payloads use [Client.PollReassembled](ctx, sub, buf).
//
// See [Client] for poll wait timeout and DoWork contract.
func (client *Client) PollOneMessage(
	ctx context.Context,
	sub *Subscription,
) ([]byte, error) {
	var (
		msg      []byte
		received bool
	)
	handler := AdaptFragmentHandlerBytesRetain(func(payload []byte, _ *Header) {
		if received {
			return
		}
		msg = payload
		received = true
	})
	ready := func() bool { return sub.Poll(handler, 1) > 0 && received }

	err := client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		_, detail := SubscriptionImageSummary(sub)
		return &PollOneMessageTimeoutError{
			RegistrationID: sub.RegistrationID(),
			StreamID:       sub.StreamID(),
			Timeout:        timeout,
			Detail:         detail,
		}
	})
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// PollReassembled receives one logical message using [FragmentAssembler].
//
// When sub has zero active images, returns [PollReassembledPreconditionError]
// immediately. Prefer [Client.PollUntilImageCount] before [Client.PollReassembled] to avoid
// this path.
//
// If buf is nil or cap(buf) < payload length, returns [PollBufferTooSmallError].
// The logical message is consumed from the subscription on that error; retrying
// with a larger buffer will not re-read the same message.
// On success returns (n, nil) where n is payload length copied into buf[:n].
//
// See [Client] for poll wait timeout and DoWork contract.
func (client *Client) PollReassembled(
	ctx context.Context,
	sub *Subscription,
	buf []byte,
) (int, error) {
	// Canceled ctx wins over the precondition error, matching the other poll methods.
	if err := ctx.Err(); err != nil {
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
		assembled  bool
		copied     bool
		payloadLen int
	)
	assembler := NewFragmentAssembler(AdaptFragmentHandlerBytes(
		func(payload []byte, _ *Header) {
			if assembled {
				return
			}
			payloadLen = len(payload)
			assembled = true
			if cap(buf) >= payloadLen {
				copy(buf[:payloadLen], payload)
				copied = true
			}
		},
	))

	handler := assembler.Handler()
	ready := func() bool {
		return sub.Poll(handler, 1) > 0 && assembled
	}

	err := client.pollUntil(ctx, ready, func(timeout time.Duration) error {
		_, detail := SubscriptionImageSummary(sub)
		return &PollReassembledTimeoutError{
			RegistrationID: sub.RegistrationID(),
			StreamID:       sub.StreamID(),
			Timeout:        timeout,
			Detail:         detail,
		}
	})
	if err != nil {
		return 0, err
	}
	if !copied {
		return 0, &PollBufferTooSmallError{Required: payloadLen, Have: cap(buf)}
	}
	return payloadLen, nil
}

// RunDuringClientWork runs fn on a goroutine and drives DoWork until fn returns,
// ctx is canceled, or [Config.PollTimeout] elapses.
//
// Timeout or cancellation returns promptly; fn continues running in the background until
// it finishes on its own; this method does not wait for fn to exit after returning.
//
// Does not install an ErrorHandler; callers should wrap [Config.ErrorHandler]
// if transport errors must fail the wait.
//
// See [Client] for poll wait timeout and DoWork contract.
func (client *Client) RunDuringClientWork(
	ctx context.Context,
	fn func() error,
) error {
	// Pre-canceled ctx returns before fn is spawned.
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	var fnErr error
	finished := func() bool {
		select {
		case fnErr = <-done:
			return true
		default:
			return false
		}
	}
	err := client.pollUntil(ctx, finished, func(timeout time.Duration) error {
		return &RunDuringClientWorkTimeoutError{Timeout: timeout}
	})
	if err != nil {
		return err
	}
	return fnErr
}

// pollUntil drives pollWorkLoop under [Config.PollTimeout] until ready is true.
//
// Expiry maps to [PreferContextError] over timeoutErr called with the effective bound.
//
// See [Client] for the poll wait timeout and DoWork contract shared by PollUntil*,
// [Client.PollOneMessage], [Client.PollReassembled], and [Client.RunDuringClientWork].
func (client *Client) pollUntil(
	ctx context.Context,
	ready func() bool,
	timeoutErr func(timeout time.Duration) error,
) error {
	deadline, effective, err := retryDeadline(ctx, client.pollTimeout)
	if err != nil {
		return err
	}
	ok, err := pollWorkLoop(ctx, client, deadline, ready)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return PreferContextError(ctx, timeoutErr(effective))
}

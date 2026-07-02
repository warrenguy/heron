package client

import (
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
)

// TryMatchControlResponse reports whether poller already holds a ControlResponse
// matching controlSessionID and correlationID from its most recent poll. It does not
// poll and does not call DoWork.
//
// Returns (fields, nil, true) on an OK match, (zero, err, true) when the matched
// response carries a non-OK code, and (zero, nil, false) when no matching response is
// captured. Error codes map through MapControlResponse.
func TryMatchControlResponse(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	if poller == nil {
		return codecs.ControlResponseFields{}, nil, false
	}
	fields, ok := poller.Response()
	if !ok {
		return codecs.ControlResponseFields{}, nil, false
	}
	return matchControlResponseFields(fields, controlSessionID, correlationID, true)
}

// AwaitArchiveControlResponse loops DoWork then pollFn until TryMatchControlResponse
// reports a match for controlSessionID and correlationID, or responseTimeout elapses.
//
// It drives DoWork on aeronClient for the duration of the await window only. The idle
// strategy paces empty polls and is reset on every exit path; a nil ctx.IdleStrategy
// uses NewDefaultIdleStrategy.
//
// A non-positive responseTimeout defaults to DefaultControlOfferTimeout, mirroring
// ControlOfferUntilPositive.
//
// Returns the matched fields with a nil error, the mapped error for a non-OK response,
// or a timeout error naming correlation_id when the deadline passes without a match.
func AwaitArchiveControlResponse(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
	responseTimeout time.Duration,
) (codecs.ControlResponseFields, error) {
	return awaitMatchedControlResponse(
		aeronClient,
		ctx,
		pollFn,
		responseTimeout,
		func() (codecs.ControlResponseFields, error, bool) {
			return TryMatchControlResponse(poller, controlSessionID, correlationID)
		},
		func() error {
			return fmt.Errorf(
				"archive client: control response timeout: correlation_id=%d",
				correlationID,
			)
		},
	)
}

// awaitMatchedControlResponse loops DoWork then pollFn until tryMatch reports a captured
// response, or responseTimeout elapses, returning the matched fields with their mapped
// error status.
func awaitMatchedControlResponse(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	responseTimeout time.Duration,
	tryMatch func() (codecs.ControlResponseFields, error, bool),
	timeoutFn func() error,
) (codecs.ControlResponseFields, error) {
	var matched codecs.ControlResponseFields
	var matchErr error
	// A response buffered during the offer phase must be read before the first pollFn:
	// ControlResponsePoller.Poll resets the capture once pollComplete is set, which would
	// otherwise discard it. runArchiveControlAwaitLoop matches once before polling.
	err := runArchiveControlAwaitLoop(
		aeronClient,
		ctx,
		pollFn,
		responseTimeout,
		func() (bool, error) {
			fields, err, ok := tryMatch()
			if !ok {
				return false, nil
			}
			matched = fields
			matchErr = err
			return true, nil
		},
		timeoutFn,
	)
	if err != nil {
		return codecs.ControlResponseFields{}, err
	}
	return matched, matchErr
}

func runArchiveControlAwaitLoop(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	responseTimeout time.Duration,
	matchFn func() (done bool, err error),
	timeoutFn func() error,
) error {
	clock := resolveNanoClock(ctx.AeronContext.NanoClock)
	idle := ctx.IdleStrategy
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	defer idle.Reset()

	timeout := responseTimeout
	if timeout <= 0 {
		timeout = DefaultControlOfferTimeout
	}
	deadlineNs := clock() + timeout.Nanoseconds()
	if done, err := matchFn(); done {
		return err
	}
	for clock() < deadlineNs {
		aeronClient.DoWork()
		work := pollFn()
		if done, err := matchFn(); done {
			return err
		}
		idle.Idle(work)
	}
	return timeoutFn()
}

// offerAndAwaitArchiveControlRequest offers a control request until the publication
// accepts it, then awaits the matching ControlResponse.
//
// The offer retry uses ctx.MessageTimeout as its budget. Once the offer succeeds, the
// response await starts a fresh ctx.MessageTimeout window so a slow offer cannot starve
// the response wait.
func offerAndAwaitArchiveControlRequest(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	poller *ControlResponsePoller,
	offerFn func() (int64, bool),
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	if poller != nil {
		poller.ResetCapture()
	}
	if _, err := offerArchiveControlRequest(aeronClient, ctx, pollFn, offerFn); err != nil {
		return codecs.ControlResponseFields{}, err
	}

	return AwaitArchiveControlResponse(
		aeronClient,
		ctx,
		pollFn,
		poller,
		controlSessionID,
		correlationID,
		ctx.MessageTimeout,
	)
}

func offerArchiveControlRequest(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	offerFn func() (int64, bool),
) (int64, error) {
	pos := ControlOfferUntilPositive(aeronClient, ControlOfferConfig{
		Timeout:      ctx.MessageTimeout,
		IdleStrategy: ctx.IdleStrategy,
		NanoClock:    ctx.AeronContext.NanoClock,
		PollFn:       pollFn,
	}, func() int64 {
		// Pass the raw position through: a retriable negative code (BackPressured,
		// AdminAction) must reach ControlOfferUntilPositive to trigger a retry.
		// Collapsing !ok to 0 would make retriable failures look terminal.
		position, _ := offerFn()
		return position
	})
	if pos <= 0 {
		return 0, fmt.Errorf("archive client: control offer failed: status=%d", pos)
	}
	return pos, nil
}

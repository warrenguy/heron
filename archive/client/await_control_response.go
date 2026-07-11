package client

import (
	"context"
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
	return matchControlResponseFields(fields, controlSessionID, correlationID)
}

// AwaitArchiveControlResponse loops DoWork then pollFn until TryMatchControlResponse
// reports a match for controlSessionID and correlationID, or responseTimeout elapses.
//
// It drives DoWork on aeronClient for the duration of the await window only. The idle
// strategy paces empty polls and is reset on every exit path; a nil archiveCtx.IdleStrategy
// uses NewDefaultIdleStrategy.
//
// Uses ArchiveContext.ControlResponseTimeout. When ctx carries a deadline, the effective
// bound is the earlier of ControlResponseTimeout and ctx.Deadline().
//
// Returns the matched fields with a nil error, the mapped error for a non-OK response,
// ControlResponseTimeoutError on timeout, or ctx.Err() when the context is done.
func AwaitArchiveControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	archiveCtx = archiveCtx.Normalize()
	return awaitMatchedControlResponse(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		archiveCtx.ControlResponseTimeout,
		correlationID,
		ControlResponseAwaitOperation,
		func() (codecs.ControlResponseFields, error, bool) {
			return TryMatchControlResponse(poller, controlSessionID, correlationID)
		},
	)
}

// awaitMatchedControlResponse loops DoWork then pollFn until tryMatch reports a captured
// response, or responseTimeout elapses, returning the matched fields with their mapped
// error status.
func awaitMatchedControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	responseTimeout time.Duration,
	correlationID int64,
	operation string,
	tryMatch func() (codecs.ControlResponseFields, error, bool),
) (codecs.ControlResponseFields, error) {
	var matched codecs.ControlResponseFields
	var matchErr error
	// A response buffered during the offer phase must be read before the first pollFn:
	// ControlResponsePoller.Poll resets the capture once pollComplete is set, which would
	// otherwise discard it. runArchiveControlAwaitLoop matches once before polling.
	err := runArchiveControlAwaitLoop(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		responseTimeout,
		correlationID,
		operation,
		func() (bool, error) {
			fields, err, ok := tryMatch()
			if !ok {
				return false, nil
			}
			matched = fields
			matchErr = err
			return true, nil
		},
	)
	if err != nil {
		return codecs.ControlResponseFields{}, err
	}
	return matched, matchErr
}

// runArchiveControlAwaitLoop requires a normalized archiveCtx: it reads IdleStrategy
// without a nil fallback.
func runArchiveControlAwaitLoop(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	responseTimeout time.Duration,
	correlationID int64,
	operation string,
	matchFn func() (done bool, err error),
) error {
	if aeronClient == nil {
		return errNilControlAeronClient
	}
	clock := resolveNanoClock(archiveCtx.AeronContext.NanoClock)
	idle := archiveCtx.IdleStrategy
	defer idle.Reset()

	startNs := clock()
	deadlineNs, bound := aeron.RetryDeadlineNs(ctx, startNs, responseTimeout)
	if done, err := matchFn(); done {
		return err
	}
	for clock() < deadlineNs {
		if err := ctx.Err(); err != nil {
			return err
		}
		aeronClient.DoWork()
		work := pollFn()
		if done, err := matchFn(); done {
			return err
		}
		idle.Idle(work)
	}
	return aeron.PreferContextError(ctx, &ControlResponseTimeoutError{
		Operation:     operation,
		CorrelationID: correlationID,
		Timeout:       bound,
	})
}

// offerAndAwaitArchiveControlRequest offers a control request until the publication
// accepts it, then awaits the matching ControlResponse.
//
// The offer retry uses ControlOfferRetryTimeout. Once the offer succeeds, the response
// await starts a fresh ControlResponseTimeout window so a slow offer cannot starve the
// response wait.
func offerAndAwaitArchiveControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	poller *ControlResponsePoller,
	offerFn func() (int64, bool),
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	if poller != nil {
		poller.ResetCapture()
	}
	if err := offerArchiveControlRequest(
		ctx, aeronClient, archiveCtx, pollFn, offerFn,
	); err != nil {
		return codecs.ControlResponseFields{}, err
	}

	return AwaitArchiveControlResponse(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		poller,
		controlSessionID,
		correlationID,
	)
}

func offerArchiveControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	offerFn func() (int64, bool),
) error {
	_, err := controlOfferUntilPositive(
		ctx,
		aeronClient,
		archiveCtx,
		ControlOfferConfig{PollFn: pollFn},
		func() int64 {
			// Pass the raw position through: a retriable negative code (BackPressured,
			// AdminAction) must reach controlOfferUntilPositive to trigger a retry.
			// Collapsing !ok to 0 would make retriable failures look terminal.
			position, _ := offerFn()
			return position
		},
	)
	return err
}

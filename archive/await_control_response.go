package archive

import (
	"context"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
)

// TryMatchControlResponse reports whether poller already holds a ControlResponse
// matching controlSessionID and correlationID from its most recent poll.
// Reads the last latched response only. Does not poll. Does not call DoWork.
//
// Returns:
//   - (fields, nil, true) on an OK match
//   - (zero, err, true) when the matched response has a non-OK code
//   - (zero, nil, false) when no matching response is captured
//
// Error codes map through MapControlResponse.
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

// tryMatchControlResponseDiscardStale matches like TryMatchControlResponse and
// clears a latched terminal that is not the awaited ControlResponse (wrong kind
// or wrong IDs).
//
// The session runs one outstanding request at a time, so a non-matching terminal
// is stale. Left latched, that terminal holds the awaited response in the image.
// After the clear, the next poll latches the awaited response.
//
// Prefer ResetAwait before offer so offer-retry polls drop non-matching
// terminals. This clear is a fallback when demux is unarmed, the terminal is
// Challenge, or the control session id does not match.
func tryMatchControlResponseDiscardStale(
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	fields, err, ok := TryMatchControlResponse(poller, controlSessionID, correlationID)
	if !ok && poller != nil && poller.IsPollComplete() {
		poller.ResetCapture()
	}
	return fields, err, ok
}

// AwaitControlResponse loops DoWork then pollFn until a ControlResponse matching
// controlSessionID and correlationID is captured, or the response timeout elapses.
//
// Arms the poller for correlationID so demux drops non-matching terminals without
// latching. Session offer paths should call ResetAwait before offer so offer-retry
// polls share that filter. Re-arm is safe when already armed. AwaitControlResponse
// clears a latched terminal that is not the awaited ControlResponse. Left latched,
// that terminal holds the awaited response in the image.
//
// Drives DoWork on aeronClient only for the await duration. Idle strategy paces
// empty polls and resets on every exit path. Nil cfg.IdleStrategy uses
// NewDefaultIdleStrategy.
//
// Uses Config.ControlResponseTimeout. When ctx carries a deadline, the effective
// bound is the earlier of ControlResponseTimeout and ctx.Deadline().
//
// Returns:
//   - matched fields with a nil error
//   - mapped error for a non-OK response
//   - ControlResponseTimeoutError on timeout
//   - ctx.Err() when the context is done
func AwaitControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	poller *ControlResponsePoller,
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	cfg = cfg.Normalize()
	if poller != nil {
		poller.ArmAwait(correlationID)
		defer poller.ArmAwait(0)
	}
	return awaitMatchedControlResponse(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		cfg.ControlResponseTimeout,
		correlationID,
		ControlResponseAwaitOperation,
		func() (codecs.ControlResponseFields, error, bool) {
			return tryMatchControlResponseDiscardStale(poller, controlSessionID, correlationID)
		},
	)
}

// awaitMatchedControlResponse loops DoWork then pollFn until tryMatch reports a captured
// response, or responseTimeout elapses, returning the matched fields with their mapped
// error status.
func awaitMatchedControlResponse(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
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
	// otherwise discard it. runControlAwaitLoop matches once before polling.
	err := runControlAwaitLoop(
		ctx,
		aeronClient,
		cfg,
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

// runControlAwaitLoop requires a normalized cfg: it reads IdleStrategy
// without a nil fallback.
func runControlAwaitLoop(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	responseTimeout time.Duration,
	correlationID int64,
	operation string,
	matchFn func() (done bool, err error),
) error {
	if aeronClient == nil {
		return errNilControlAeronClient
	}
	clock := resolveNanoClock(cfg.AeronConfig.NanoClock)
	idle := cfg.IdleStrategy
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

// offerAndAwaitControlRequest offers a control request until the publication
// accepts it, then awaits the matching ControlResponse.
//
// ResetAwait arms the poller before offer so offer-retry polls filter by correlationID.
// The offer retry uses ControlOfferRetryTimeout. Once the offer succeeds, the response
// await starts a fresh ControlResponseTimeout window so a slow offer cannot starve the
// response wait.
func offerAndAwaitControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	poller *ControlResponsePoller,
	offerFn func() (int64, bool),
	controlSessionID int64,
	correlationID int64,
) (codecs.ControlResponseFields, error) {
	if poller != nil {
		poller.ResetAwait(correlationID)
		defer poller.ArmAwait(0)
	}
	if err := offerControlRequest(
		ctx, aeronClient, cfg, pollFn, offerFn,
	); err != nil {
		return codecs.ControlResponseFields{}, err
	}

	return AwaitControlResponse(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		poller,
		controlSessionID,
		correlationID,
	)
}

func offerControlRequest(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	offerFn func() (int64, bool),
) error {
	_, err := controlOfferUntilPositive(
		ctx,
		aeronClient,
		cfg,
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

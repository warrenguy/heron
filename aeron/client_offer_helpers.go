package aeron

import (
	"context"
	"time"
)

// OfferWithRetry offers payload until success, timeout, stall, terminal code, or ctx
// cancellation. See [Client.OfferWithRetryObserved].
func (client *Client) OfferWithRetry(
	ctx context.Context,
	pub PublicationOfferer,
	payload []byte,
) (int64, error) {
	return client.OfferWithRetryObserved(ctx, pub, payload, nil)
}

// OfferWithRetryObserved offers payload until success (position > 0), timeout, stall,
// or terminal code.
//
// Retries while [IsOfferRetriable](result): BackPressured, AdminAction, NotConnected.
// Sleeps [DefaultIdleSleepDuration] on BackPressured and NotConnected.
//
// Stall detection: if BackPressured or AdminAction persists without position advance for
// [OfferStallTimeout] of the effective bound (the earlier of [Context.PublicationOfferTimeout] and
// ctx.Deadline()), maps to [PreferContextError] over [OfferStallError].
//
// Terminal codes (Closed, MaxPositionExceeded, UnsupportedMessageLength, unknown negatives)
// map to [PreferContextError] over [OfferTerminalError] with [PositionCodeName] in Error().
//
// Expiry maps to [PreferContextError] over [OfferTimeoutError] called with the effective bound.
//
// Do not use in cluster ingress or archive control offer loops; those paths require
// ingress retriability helpers ([IsIngressRetriable]), not [IsOfferRetriable].
//
// On concurrent *[Publication], fragmented Offer under retried AdminAction may commit an
// orphan BEGIN plus padding before a successful full retry. Safe for [ExclusivePublication]
// and single-frame offers; avoid concurrent fragmented [Publication] unless subscribers use
// [FragmentAssembler] (reset-on-BEGIN tolerates the orphan).
//
// Invokes onRetriable on each BackPressured or AdminAction before retrying.
//
// See [Client] for offer retry timeout and DoWork contract.
func (client *Client) OfferWithRetryObserved(
	ctx context.Context,
	pub PublicationOfferer,
	payload []byte,
	onRetriable OfferRetriableObserver,
) (int64, error) {
	deadline, effective, err := retryDeadline(ctx, client.publicationOfferTimeout)
	if err != nil {
		return 0, err
	}
	stallTimeout := OfferStallTimeout(effective)

	var (
		lastStatus    int64
		retryPosition = pub.Position()
		stallMarked   time.Time
	)

	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		result := pub.Offer(payload)
		if result > 0 {
			return result, nil
		}
		lastStatus = result

		position := pub.Position()
		if position > retryPosition {
			retryPosition = position
			stallMarked = time.Time{}
		} else if isOfferStallCandidate(result) {
			if stallMarked.IsZero() {
				stallMarked = time.Now()
			} else if time.Since(stallMarked) >= stallTimeout {
				return 0, PreferContextError(ctx, offerStallError(pub, lastStatus, position, stallTimeout))
			}
		} else if result == NotConnected {
			stallMarked = time.Time{}
		}

		if !IsOfferRetriable(result) {
			return 0, PreferContextError(ctx, offerTerminalError(pub, result, position))
		}

		switch result {
		case BackPressured, AdminAction:
			if onRetriable != nil {
				onRetriable(result)
			}
			client.DoWork()
			if result == BackPressured {
				time.Sleep(DefaultIdleSleepDuration)
			}
		case NotConnected:
			client.DoWork()
			time.Sleep(DefaultIdleSleepDuration)
		}
	}

	return 0, PreferContextError(ctx, offerTimeoutError(pub, lastStatus, effective))
}

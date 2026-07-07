package aeron

import (
	"context"
	"time"
)

// PublicationOfferer is implemented by Publication and ExclusivePublication.
// OfferWithRetry operates on the offer surface only.
type PublicationOfferer interface {
	IsConnected() bool
	RegistrationID() int64
	StreamID() int32
	SessionID() int32
	Position() int64
	Offer([]byte) int64
}

// isOfferStallCandidate reports persistent BackPressured or AdminAction without
// position advance. NotConnected is excluded; transport wait uses OfferTimeoutError.
func isOfferStallCandidate(status int64) bool {
	return status == BackPressured || status == AdminAction
}

// OfferStallTimeout derives stall detection window from overall retry timeout.
// Matches harness OfferStallTimeout.
func OfferStallTimeout(timeout time.Duration) time.Duration {
	stall := timeout / 5
	if stall < time.Second && timeout >= 5*time.Second {
		return time.Second
	}
	return stall
}

func publicationIdentity(pub PublicationOfferer) (regID int64, streamID, sessionID int32) {
	return pub.RegistrationID(), pub.StreamID(), pub.SessionID()
}

func offerTerminalError(pub PublicationOfferer, status, position int64) *OfferTerminalError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferTerminalError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         position,
	}
}

func offerStallError(
	pub PublicationOfferer,
	status int64,
	position int64,
	stallTimeout time.Duration,
) *OfferStallError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferStallError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         position,
		StallTimeout:     stallTimeout,
	}
}

func offerTimeoutError(
	pub PublicationOfferer,
	status int64,
	timeout time.Duration,
) *OfferTimeoutError {
	regID, streamID, sessionID := publicationIdentity(pub)
	return &OfferTimeoutError{
		Status:           status,
		PublicationRegID: regID,
		StreamID:         streamID,
		SessionID:        sessionID,
		Position:         pub.Position(),
		Timeout:          timeout,
	}
}

// OfferRetriableObserver runs when OfferWithRetryObserved observes BackPressured or
// AdminAction before retrying. Not invoked for NotConnected.
type OfferRetriableObserver func(status int64)

// OfferWithRetry offers payload until success, timeout, stall, terminal code, or ctx
// cancellation. See OfferWithRetryObserved.
func OfferWithRetry(
	ctx context.Context,
	client *Client,
	pub PublicationOfferer,
	payload []byte,
) (int64, error) {
	return OfferWithRetryObserved(ctx, client, pub, payload, nil)
}

// OfferWithRetryObserved offers payload until success (position > 0), timeout, stall,
// or terminal code.
//
// Retries while IsOfferRetriable(result): BackPressured, AdminAction, NotConnected.
// This helper drives client.DoWork() inside its retry loop; the caller need not
// invoke DoWork separately while blocked here. Sleeps DefaultIdleSleepDuration on
// BackPressured and NotConnected.
//
// Stall detection: if BackPressured or AdminAction persists without position advance for
// OfferStallTimeout of the effective bound (the earlier of PublicationOfferTimeout and
// ctx.Deadline()), returns OfferStallError.
//
// Terminal codes (Closed, MaxPositionExceeded, UnsupportedMessageLength, unknown negatives)
// return OfferTerminalError immediately with PositionCodeName in Error().
//
// Timeout: uses Context.PublicationOfferTimeout. When ctx carries a deadline, the
// effective bound is the earlier of PublicationOfferTimeout and ctx.Deadline().
//
// Returns ctx.Err() when the context is done before success (context.Canceled or
// context.DeadlineExceeded). OfferTimeoutError, OfferStallError, and OfferTerminalError
// apply only while the context remains active.
//
// Do not use in cluster ingress or archive control offer loops; those paths require
// ingress retriability helpers (IsIngressRetriable), not IsOfferRetriable.
//
// On concurrent *Publication, fragmented Offer under retried AdminAction may commit an
// orphan BEGIN plus padding before a successful full retry. Safe for ExclusivePublication
// and single-frame offers; avoid concurrent fragmented Publication unless subscribers use
// FragmentAssembler (reset-on-BEGIN tolerates the orphan).
//
// Invokes onRetriable on each BackPressured or AdminAction before retrying.
//
// Cold-path integration ergonomics: allocates for error strings. Not zero-alloc.
func OfferWithRetryObserved(
	ctx context.Context,
	client *Client,
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
		case BackPressured:
			if onRetriable != nil {
				onRetriable(result)
			}
			client.DoWork()
			time.Sleep(DefaultIdleSleepDuration)
		case AdminAction:
			if onRetriable != nil {
				onRetriable(result)
			}
			client.DoWork()
		case NotConnected:
			client.DoWork()
			time.Sleep(DefaultIdleSleepDuration)
		}
	}

	return 0, PreferContextError(ctx, offerTimeoutError(pub, lastStatus, effective))
}

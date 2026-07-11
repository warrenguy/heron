package aeron

import (
	"time"
)

// [PublicationOfferer] is implemented by [Publication] and [ExclusivePublication].
// [Client.OfferWithRetry] methods operate on the [PublicationOfferer] surface only.
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

// OfferRetriableObserver runs when [Client.OfferWithRetryObserved] observes BackPressured or
// AdminAction before retrying. Not invoked for NotConnected.
type OfferRetriableObserver func(status int64)

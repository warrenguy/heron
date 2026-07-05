package aeron

import (
	"fmt"
	"time"
)

// OfferTerminalError reports a non-retriable publication offer result.
type OfferTerminalError struct {
	Status           int64
	PublicationRegID int64
	StreamID         int32
	SessionID        int32
	Position         int64
}

func (err *OfferTerminalError) Error() string {
	return formatPublicationError(
		err.PublicationRegID,
		err.StreamID,
		err.SessionID,
		fmt.Sprintf(
			"offer failed: status=%s; position=%d",
			PositionCodeName(err.Status),
			err.Position,
		),
	)
}

// OfferTimeoutError reports an offer retry deadline expiry.
type OfferTimeoutError struct {
	Status           int64
	PublicationRegID int64
	StreamID         int32
	SessionID        int32
	Position         int64
	Timeout          time.Duration
}

func (err *OfferTimeoutError) Error() string {
	return formatPublicationError(
		err.PublicationRegID,
		err.StreamID,
		err.SessionID,
		fmt.Sprintf(
			"offer timed out within %s; position=%d; status=%s",
			err.Timeout,
			err.Position,
			PositionCodeName(err.Status),
		),
	)
}

// OfferStallError reports persistent backpressure or admin action without position advance.
type OfferStallError struct {
	Status           int64
	PublicationRegID int64
	StreamID         int32
	SessionID        int32
	Position         int64
	StallTimeout     time.Duration
}

func (err *OfferStallError) Error() string {
	return formatPublicationError(
		err.PublicationRegID,
		err.StreamID,
		err.SessionID,
		fmt.Sprintf(
			"offer stalled within %s; position=%d; status=%s",
			err.StallTimeout,
			err.Position,
			PositionCodeName(err.Status),
		),
	)
}

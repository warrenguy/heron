package aeron

import (
	"errors"
	"fmt"
	"time"
)

// ErrPollBufferTooSmall reports that PollReassembled buf is nil or too small.
// Use errors.As into PollBufferTooSmallError for Required length.
var ErrPollBufferTooSmall = errors.New("aeron: poll buffer too small")

// WaitUntilConnectedTimeoutError reports WaitUntilConnected deadline expiry.
type WaitUntilConnectedTimeoutError struct {
	PublicationRegID int64
	StreamID         int32
	SessionID        int32
	Timeout          time.Duration
	Position         int64
}

func (err *WaitUntilConnectedTimeoutError) Error() string {
	return formatPublicationError(
		err.PublicationRegID,
		err.StreamID,
		err.SessionID,
		fmt.Sprintf("not connected within %s; position=%d", err.Timeout, err.Position),
	)
}

// WaitUntilImageCountTimeoutError reports WaitUntilImageCount deadline expiry.
type WaitUntilImageCountTimeoutError struct {
	RegistrationID int64
	StreamID       int32
	Want           int
	Have           int
	Timeout        time.Duration
	Detail         string
}

func (err *WaitUntilImageCountTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: subscription %d (stream %d) active image count=%d want=%d; %s within %s",
		err.RegistrationID,
		err.StreamID,
		err.Have,
		err.Want,
		err.Detail,
		err.Timeout,
	)
}

// PollOneMessageTimeoutError reports PollOneMessage deadline expiry.
type PollOneMessageTimeoutError struct {
	RegistrationID int64
	StreamID       int32
	Timeout        time.Duration
	Detail         string
}

func (err *PollOneMessageTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: subscription %d (stream %d) received no message within %s; %s",
		err.RegistrationID,
		err.StreamID,
		err.Timeout,
		err.Detail,
	)
}

// PollReassembledPreconditionError reports PollReassembled called with zero active images.
type PollReassembledPreconditionError struct {
	RegistrationID int64
	StreamID       int32
	Detail         string
}

func (err *PollReassembledPreconditionError) Error() string {
	return fmt.Sprintf(
		"aeron: subscription %d reassembled 0/1 messages; fragments=0; %s",
		err.RegistrationID,
		err.Detail,
	)
}

// PollReassembledTimeoutError reports PollReassembled deadline expiry.
type PollReassembledTimeoutError struct {
	RegistrationID int64
	StreamID       int32
	Timeout        time.Duration
	Detail         string
}

func (err *PollReassembledTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: subscription %d reassembled 0/1 messages; fragments=0; %s within %s",
		err.RegistrationID,
		err.Detail,
		err.Timeout,
	)
}

// RunDuringClientWorkTimeoutError reports RunDuringClientWork deadline expiry.
type RunDuringClientWorkTimeoutError struct {
	Timeout time.Duration
}

func (err *RunDuringClientWorkTimeoutError) Error() string {
	return fmt.Sprintf(
		"aeron: RunDuringClientWork timed out within %s",
		err.Timeout,
	)
}

// PollBufferTooSmallError reports that PollReassembled buf capacity is insufficient.
type PollBufferTooSmallError struct {
	Required int
	Have     int
}

func (err *PollBufferTooSmallError) Error() string {
	return fmt.Sprintf(
		"aeron: poll buffer too small: required=%d have=%d",
		err.Required,
		err.Have,
	)
}

func (err *PollBufferTooSmallError) Is(target error) bool {
	return target == ErrPollBufferTooSmall
}

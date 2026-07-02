package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
)

var errNilDescriptorPoller = errors.New(
	"archive client: list recording: descriptor poller must be non-nil",
)

// TryMatchDescriptorControlResponse reports whether poller holds a ControlResponse
// matching correlationID from its most recent poll. It does not poll and does not call
// DoWork.
//
// Returns (fields, nil, true) on an OK match, (zero, err, true) when the matched
// response carries a non-OK code, and (zero, nil, false) when no matching response is
// captured. Error codes map through MapControlResponse.
//
// Unlike TryMatchControlResponse it filters on correlation_id only: each client allocates
// globally unique correlation ids, so a control-session filter is redundant here.
func TryMatchDescriptorControlResponse(
	poller *RecordingDescriptorPoller,
	correlationID int64,
) (codecs.ControlResponseFields, error, bool) {
	if poller == nil {
		return codecs.ControlResponseFields{}, nil, false
	}
	fields, ok := poller.ControlResponse()
	if !ok {
		return codecs.ControlResponseFields{}, nil, false
	}
	return matchControlResponseFields(fields, 0, correlationID, false)
}

// AwaitDescriptorControlResponse loops DoWork then pollFn until
// TryMatchDescriptorControlResponse reports a match for correlationID, or responseTimeout
// elapses. It is the first list-recording phase: await the OK ControlResponse.
//
// descriptorPoller must be non-nil: it carries the awaited correlation id and captured
// response state.
//
// A non-positive responseTimeout defaults to DefaultControlOfferTimeout.
func AwaitDescriptorControlResponse(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
	correlationID int64,
	responseTimeout time.Duration,
) (codecs.ControlResponseFields, error) {
	if descriptorPoller == nil {
		return codecs.ControlResponseFields{}, errNilDescriptorPoller
	}
	return awaitMatchedControlResponse(
		aeronClient,
		ctx,
		pollFn,
		responseTimeout,
		func() (codecs.ControlResponseFields, error, bool) {
			return TryMatchDescriptorControlResponse(descriptorPoller, correlationID)
		},
		func() error {
			return fmt.Errorf(
				"archive client: list recording response timeout: correlation_id=%d",
				correlationID,
			)
		},
	)
}

// AwaitArchiveListRecording loops DoWork then pollFn until descriptorPoller reports
// completion, or responseTimeout elapses. It is the second list-recording phase after
// the OK ControlResponse: await the matching RecordingDescriptor.
//
// descriptorPoller must be non-nil: it carries the awaited correlation id and captured
// descriptor state.
//
// A non-positive responseTimeout defaults to DefaultControlOfferTimeout.
func AwaitArchiveListRecording(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
	responseTimeout time.Duration,
) error {
	if descriptorPoller == nil {
		return errNilDescriptorPoller
	}
	correlationID := descriptorPoller.awaitCorrelationID
	return runArchiveControlAwaitLoop(
		aeronClient,
		ctx,
		pollFn,
		responseTimeout,
		func() (bool, error) {
			return descriptorPoller.IsPollComplete(), nil
		},
		func() error {
			return fmt.Errorf(
				"archive client: list recording descriptor timeout: correlation_id=%d",
				correlationID,
			)
		},
	)
}

// offerAndAwaitListRecording offers a list-recording request until the publication
// accepts it, then awaits the OK ControlResponse and the RecordingDescriptor.
//
// The offer retry uses ctx.MessageTimeout as its budget. Each await phase starts a fresh
// ctx.MessageTimeout window so a slow offer cannot starve the response wait. Var-ascii
// fields are cloned at descriptor latch time for retention past later polls.
func offerAndAwaitListRecording(
	aeronClient *aeron.Client,
	ctx ArchiveContext,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
	offerFn func() (int64, bool),
	correlationID int64,
) (codecs.RecordingDescriptorFields, error) {
	if _, err := offerArchiveControlRequest(aeronClient, ctx, pollFn, offerFn); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	if _, err := AwaitDescriptorControlResponse(
		aeronClient,
		ctx,
		pollFn,
		descriptorPoller,
		correlationID,
		ctx.MessageTimeout,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	if err := AwaitArchiveListRecording(
		aeronClient,
		ctx,
		pollFn,
		descriptorPoller,
		ctx.MessageTimeout,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	return descriptorPoller.Descriptor(), nil
}

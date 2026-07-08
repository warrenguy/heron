package client

import (
	"context"
	"errors"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
)

var errNilDescriptorPoller = errors.New(
	"archive client: list recording: descriptor poller must be non-nil",
)

// AwaitArchiveListRecording loops DoWork then pollFn until descriptorPoller reports
// completion, or responseTimeout elapses. Awaits the matching RecordingDescriptor or a
// non-OK ControlResponse; Java list-by-id may send only the descriptor with no preceding
// OK response.
//
// descriptorPoller must be non-nil: it carries the awaited correlation id and captured
// descriptor state.
//
// Uses ArchiveContext.ControlResponseTimeout. When ctx carries a deadline, the effective
// bound is the earlier of ControlResponseTimeout and ctx.Deadline().
func AwaitArchiveListRecording(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
) error {
	if descriptorPoller == nil {
		return errNilDescriptorPoller
	}
	archiveCtx = archiveCtx.Normalize()
	correlationID := descriptorPoller.awaitCorrelationID
	return runArchiveControlAwaitLoop(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		archiveCtx.ControlResponseTimeout,
		correlationID,
		ListRecordingDescriptorAwaitOperation,
		func() (bool, error) {
			return descriptorPoller.IsPollComplete(), nil
		},
	)
}

// offerAndAwaitListRecording offers a list-recording request until the publication
// accepts it, then awaits the matching RecordingDescriptor or a non-OK ControlResponse.
//
// Java Aeron Archive list-by-id typically sends only RecordingDescriptor; some flows may
// precede it with an OK ControlResponse. The offer retry uses ControlOfferRetryTimeout.
// The descriptor await starts a fresh ControlResponseTimeout window so a slow offer
// cannot starve the response wait. Var-ascii fields are cloned at descriptor latch time
// for retention past later polls.
func offerAndAwaitListRecording(
	ctx context.Context,
	aeronClient *aeron.Client,
	archiveCtx ArchiveContext,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
	offerFn func() (int64, bool),
) (codecs.RecordingDescriptorFields, error) {
	if err := offerArchiveControlRequest(
		ctx, aeronClient, archiveCtx, pollFn, offerFn,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	if err := AwaitArchiveListRecording(
		ctx,
		aeronClient,
		archiveCtx,
		pollFn,
		descriptorPoller,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	if resp, ok := descriptorPoller.ControlResponse(); ok {
		if err := MapControlResponse(resp); err != nil {
			return codecs.RecordingDescriptorFields{}, err
		}
	}

	return descriptorPoller.Descriptor(), nil
}

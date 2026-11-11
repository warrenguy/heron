package archive

import (
	"context"
	"errors"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/archive/codecs"
)

var errNilDescriptorPoller = errors.New(
	"archive: list recording: descriptor poller must be non-nil",
)

// AwaitListRecording loops DoWork, then the fault check, then pollFn, until
// descriptorPoller reports completion or the response timeout elapses.
//
// The loop accepts a matching RecordingDescriptor or a non-OK
// ControlResponse. A list-by-id reply may carry only the descriptor.
//
// descriptorPoller must be non-nil. It holds the awaited correlation id and
// the captured descriptor.
//
// The response timeout is Config.ControlResponseTimeout, or ctx.Deadline()
// when that comes first.
//
// Returns ErrTransportTerminating when aeronClient reports termination.
//
// The function does not check the subscription connection. Client methods do.
func AwaitListRecording(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	descriptorPoller *RecordingDescriptorPoller,
) error {
	return awaitListRecording(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		awaitChecks{},
		descriptorPoller,
	)
}

func awaitListRecording(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	descriptorPoller *RecordingDescriptorPoller,
) error {
	if descriptorPoller == nil {
		return errNilDescriptorPoller
	}
	cfg = cfg.Normalize()
	correlationID := descriptorPoller.awaitCorrelationID
	return runControlAwaitLoop(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		checks,
		cfg.ControlResponseTimeout,
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
// The offer retry uses ControlOfferRetryTimeout. The descriptor await starts a
// fresh ControlResponseTimeout window so a slow offer does not shorten the
// response wait.
func offerAndAwaitListRecording(
	ctx context.Context,
	aeronClient *aeron.Client,
	cfg Config,
	pollFn func() int,
	checks awaitChecks,
	descriptorPoller *RecordingDescriptorPoller,
	offerFn func() (int64, bool),
) (codecs.RecordingDescriptorFields, error) {
	if err := offerControlRequest(
		ctx, aeronClient, cfg, pollFn, checks.transportFault, offerFn,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	if err := awaitListRecording(
		ctx,
		aeronClient,
		cfg,
		pollFn,
		checks,
		descriptorPoller,
	); err != nil {
		return codecs.RecordingDescriptorFields{}, err
	}

	return descriptorPollerOutcome(descriptorPoller)
}

// descriptorPollerOutcome maps a completed descriptor poller to its result. A
// latched non-OK ControlResponse maps through MapControlResponse. Otherwise the
// captured descriptor is the result.
func descriptorPollerOutcome(
	descriptorPoller *RecordingDescriptorPoller,
) (codecs.RecordingDescriptorFields, error) {
	if resp, ok := descriptorPoller.ControlResponse(); ok {
		if err := MapControlResponse(resp); err != nil {
			return codecs.RecordingDescriptorFields{}, err
		}
	}
	return descriptorPoller.Descriptor(), nil
}

package cluster

import (
	"context"
	"fmt"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/idlestrategy"
)

// termRecordingStopArchive is the archive surface termRecordingStop uses.
type termRecordingStopArchive interface {
	StopRecordingByIdentity(
		ctx context.Context,
		recordingID int64,
	) (
		stopped bool,
		err error,
	)
	ListRecording(
		ctx context.Context,
		recordingID int64,
	) (codecs.RecordingDescriptorFields, error)
}

var (
	_ termRecordingStopArchive = (*archive.Client)(nil)
	_ TermRecordingStopper     = (*termRecordingStop)(nil)
)

// termRecordingStop stops the term recording at open. It then waits up to
// waitNs for the catalog stop position.
//
// Concurrency: invoker-only.
//
// Ownership: does not own the archive client.
type termRecordingStop struct {
	ctx       context.Context
	client    termRecordingStopArchive
	idle      idlestrategy.IdleStrategy
	nanoClock func() int64
	waitNs    int64
}

// newTermRecordingStop builds a termRecordingStop with its own idle strategy.
// The archive client resets its strategy on every blocking call, so a shared
// one would never back off.
func newTermRecordingStop(
	ctx context.Context,
	client termRecordingStopArchive,
	waitNs int64,
	nanoClock func() int64,
) *termRecordingStop {
	return &termRecordingStop{
		ctx:       ctx,
		client:    client,
		idle:      idlestrategy.NewDefaultBackoff(),
		nanoClock: nanoClock,
		waitNs:    waitNs,
	}
}

// StopTermRecording stops recordingID by identity. It then lists the
// recording until the catalog holds a stop position or waitNs expires.
//
// Behaviour:
//   - the wait also runs after a not-active reply. The archive can still be
//     closing the session after an earlier stop, so the catalog stop can
//     still be null
//   - waitNs expiry returns nil. The prepare path stops and reads the
//     recording later
//   - the deadline check runs between lists only. Each list runs its own
//     offer and response bounds
//
// Concurrency: invoker-only. Blocks the invoker.
//
// Returns an error when:
//   - the stop fails. ErrorUnknownRecording maps to
//     recordinglog.ErrUnknownRecordingID
//   - a catalog read fails, mapped the same way
//   - ctx is done before a reply
func (s *termRecordingStop) StopTermRecording(recordingID int64) error {
	if _, err := s.client.StopRecordingByIdentity(s.ctx, recordingID); err != nil {
		return archive.MapListRecordingError(err)
	}
	defer s.idle.Reset()
	deadlineNs := s.nanoClock() + s.waitNs
	for {
		descriptor, err := s.client.ListRecording(s.ctx, recordingID)
		if err != nil {
			return fmt.Errorf("read catalog stop: %w", archive.MapListRecordingError(err))
		}
		if descriptor.StopPosition != recordinglog.NullPosition {
			return nil
		}
		if s.nanoClock() >= deadlineNs {
			return nil
		}
		s.idle.Idle(0)
	}
}

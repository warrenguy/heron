package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/idlestrategy"
)

// moduleSnapshotLoadFragmentLimit matches upstream ConsensusModuleAgent
// FRAGMENT_LIMIT for a module snapshot replay.
const moduleSnapshotLoadFragmentLimit = 10

// Poll drains the control-response subscription while the replay runs.
type snapshotLoadControl interface {
	replayControl
	Poll() int
}

var _ snapshotLoadControl = (*archive.Client)(nil)

// The replay-image wait and each fragment gap stop after timeoutNs on
// nanoClock. Both waits also stop when ctx ends.
type moduleSnapshotLoad struct {
	ctx       context.Context
	control   snapshotLoadControl
	adder     SubscriptionAdder
	pump      func() int
	nanoClock func() int64
	channel   string
	streamID  int32
	timeoutNs int64
}

// run stops the replay and closes the subscription on every path that
// started them.
func (l moduleSnapshotLoad) run(
	agent *consensus.Agent,
	snapshot recordinglog.Snapshot,
) error {
	replaySessionID, err := l.control.StartReplay(
		l.ctx,
		snapshot.RecordingID,
		0,
		archive.NullPosition,
		l.streamID,
		[]byte(l.channel),
	)
	if err != nil {
		return moduleSnapshotLoadError(snapshot.RecordingID, fmt.Errorf("start replay: %w", err))
	}

	// Archive returns a composite session id. The subscription image carries
	// only the low 32 bits.
	//nolint:gosec // G115: the image carries the low 32 bits of the session id
	channel := sessionScopedLogChannel(l.channel, int32(uint32(replaySessionID)))
	sub, err := addSubscriptionChecked(l.adder, l.streamID, channel, "replay subscription")
	if err != nil {
		return errors.Join(
			moduleSnapshotLoadError(
				snapshot.RecordingID,
				fmt.Errorf("add replay subscription: %w", err),
			),
			l.stopReplay(snapshot.RecordingID, replaySessionID),
		)
	}

	err = l.load(agent, sub, replaySessionID, snapshot)
	return errors.Join(err, l.finish(snapshot.RecordingID, replaySessionID, sub))
}

func (l moduleSnapshotLoad) load(
	agent *consensus.Agent,
	sub *aeron.Subscription,
	replaySessionID int64,
	snapshot recordinglog.Snapshot,
) error {
	image, err := l.awaitImage(sub, replaySessionID, snapshot.RecordingID)
	if err != nil {
		return err
	}

	loader := agent.NewSnapshotLoader(snapshot.LogPosition)
	handler := loader.Handler()
	idle := idlestrategy.NewDefaultBackoff()
	deadline := l.nanoClock() + l.timeoutNs
	for {
		if ctxErr := l.ctx.Err(); ctxErr != nil {
			return moduleSnapshotLoadError(snapshot.RecordingID, ctxErr)
		}
		l.pump()
		l.control.Poll()
		n := image.ControlledPoll(handler, moduleSnapshotLoadFragmentLimit)
		if loadErr := loader.Err(); loadErr != nil {
			return moduleSnapshotLoadError(snapshot.RecordingID, loadErr)
		}
		if loader.IsDone() {
			return nil
		}
		if n > 0 {
			deadline = l.nanoClock() + l.timeoutNs
			idle.Idle(n)
			continue
		}
		if image.IsClosed() || image.IsEndOfStream() {
			return moduleSnapshotLoadError(
				snapshot.RecordingID,
				errors.New("replay ended before the end marker"),
			)
		}
		if l.nanoClock() >= deadline {
			return moduleSnapshotLoadError(
				snapshot.RecordingID,
				errors.New("no replay fragment within one leader heartbeat"),
			)
		}
		idle.Idle(0)
	}
}

func (l moduleSnapshotLoad) awaitImage(
	sub *aeron.Subscription,
	replaySessionID int64,
	recordingID int64,
) (*aeron.Image, error) {
	deadline := l.nanoClock() + l.timeoutNs
	idle := idlestrategy.NewDefaultBackoff()
	for {
		if err := l.ctx.Err(); err != nil {
			return nil, moduleSnapshotLoadError(recordingID, err)
		}
		image, found, err := archive.FindReplayImage(sub, replaySessionID)
		if err != nil {
			return nil, moduleSnapshotLoadError(recordingID, err)
		}
		if found {
			return image, nil
		}
		worked := l.pump() + l.control.Poll()
		if l.nanoClock() >= deadline {
			return nil, moduleSnapshotLoadError(
				recordingID,
				errors.New("replay image did not appear within one leader heartbeat"),
			)
		}
		idle.Idle(worked)
	}
}

func (l moduleSnapshotLoad) finish(
	recordingID int64,
	replaySessionID int64,
	sub *aeron.Subscription,
) error {
	err := l.stopReplay(recordingID, replaySessionID)
	if closeErr := sub.Close(); closeErr != nil {
		err = errors.Join(err, moduleSnapshotLoadError(
			recordingID,
			fmt.Errorf("close replay subscription: %w", closeErr),
		))
	}
	return err
}

func (l moduleSnapshotLoad) stopReplay(recordingID, replaySessionID int64) error {
	if err := l.control.StopReplay(moduleControlContext(l.ctx), replaySessionID); err != nil {
		return moduleSnapshotLoadError(recordingID, fmt.Errorf("stop replay: %w", err))
	}
	return nil
}

func moduleSnapshotLoadError(recordingID int64, cause error) error {
	return fmt.Errorf("cluster: load module snapshot recording %d: %w", recordingID, cause)
}

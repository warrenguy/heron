//go:build testhooks

package service

import (
	"testing"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	snapshotTakeFakeControlSessionID int64 = 77

	// snapshotTakeFakePreProofID is the id RecordingID returns before START.
	snapshotTakeFakePreProofID int64 = 9_999

	// snapshotTakeFakeFirstRecordingID is the default id of the first take.
	// Each later start build adds one.
	snapshotTakeFakeFirstRecordingID int64 = 500
)

// snapshotTakeFakeStep is one scripted outcome of TryOffer or PollSignal.
type snapshotTakeFakeStep struct {
	done bool
	err  error
}

// snapshotTakeFakeAddPoll is one scripted outcome of
// PollAddExclusivePublication. A done entry with no publication and no error
// returns a new hook publication.
type snapshotTakeFakeAddPoll struct {
	publication *aeron.ExclusivePublication
	done        bool
	err         error
}

// SnapshotTakeFake stands in for the archive client, the start request, the
// snapshot publication adder, and the row offers of one agent.
//
// Behaviour: each step completes on its first call unless a script queue
// holds an entry for it. The first Poll after the take closes its
// publication passes STOP to the session handler at the close position.
//
// Concurrency: invoker-only.
type SnapshotTakeFake struct {
	tb    testing.TB
	agent *Agent

	offerRetryTimeout time.Duration
	responseTimeout   time.Duration

	handler    archive.RecordingSignalHandler
	pending    []codecs.RecordingSignalEventFields
	stopNever  bool
	stopEarly  bool
	stopDelta  int64
	stopDelay  int
	stopTake   *snapshotTake
	stopWaited int
	stopDone   bool
	stopSent   []codecs.RecordingSignalEventFields

	// closePositions holds the take close position at each scripted STOP.
	closePositions []int64

	addOffers         []error
	addPolls          []snapshotTakeFakeAddPoll
	addStreamIDs      []int32
	addChannels       []string
	nextCorrelationID int64
	cancelled         []int64
	publications      []*aeron.ExclusivePublication

	startOffers     []snapshotTakeFakeStep
	startSignals    []snapshotTakeFakeStep
	startStreamIDs  []int32
	startChannels   []string
	startBuilds     int
	startCloses     int
	startClosed     bool
	startProven     bool
	recordingIDs    []int64
	nextRecordingID int64
	recordingID     int64

	rowOffers []int64
}

var (
	_ snapshotArchiveControl   = (*SnapshotTakeFake)(nil)
	_ snapshotStartRequest     = (*SnapshotTakeFake)(nil)
	_ SnapshotPublicationAdder = (*SnapshotTakeFake)(nil)
)

// InstallSnapshotTakeFakeForTest replaces the archive client, the start
// request factory, the snapshot publication adder, and the row offer on
// agent with one fake. Call it after NewAgent and before the first snapshot
// action.
func InstallSnapshotTakeFakeForTest(tb testing.TB, agent *Agent) *SnapshotTakeFake {
	tb.Helper()
	f := &SnapshotTakeFake{
		tb:                tb,
		agent:             agent,
		nextCorrelationID: 1,
		nextRecordingID:   snapshotTakeFakeFirstRecordingID,
		recordingID:       snapshotTakeFakePreProofID,
	}
	agent.archive = f
	agent.snapshotPublications = f
	agent.newSnapshotStart = f.newStart
	agent.snapshotRowOffer = f.offerRow
	return f
}

// RecordingID returns the recording id of the last start request after its
// START. Before START it returns snapshotTakeFakePreProofID. An ack must not
// carry that id.
func (f *SnapshotTakeFake) RecordingID() int64 {
	if !f.startProven {
		return snapshotTakeFakePreProofID
	}
	return f.recordingID
}

// LastPublication returns the last publication the fake added, or nil.
func (f *SnapshotTakeFake) LastPublication() *aeron.ExclusivePublication {
	if len(f.publications) == 0 {
		return nil
	}
	return f.publications[len(f.publications)-1]
}

// Poll passes queued signals, then a scripted STOP, to the session handler.
func (f *SnapshotTakeFake) Poll() int {
	work := 0
	pending := f.pending
	f.pending = nil
	for _, event := range pending {
		f.signal(event)
		work++
	}

	take := f.agent.take
	if take == nil || f.stopNever {
		return work
	}
	if take != f.stopTake {
		f.stopTake = take
		f.stopWaited = 0
		f.stopDone = false
	}
	if f.stopDone {
		return work
	}
	if take.state == takeWriting && f.stopEarly {
		f.sendStop(0)
		return work + 1
	}
	if take.state != takeAwaitingStop {
		return work
	}
	if f.stopWaited < f.stopDelay {
		f.stopWaited++
		return work
	}
	f.closePositions = append(f.closePositions, take.closePosition)
	f.sendStop(take.closePosition + f.stopDelta)
	return work + 1
}

func (f *SnapshotTakeFake) sendStop(position int64) {
	event := codecs.RecordingSignalEventFields{
		ControlSessionID: snapshotTakeFakeControlSessionID,
		RecordingID:      f.recordingID,
		Position:         position,
		Signal:           archivewire.RecordingSignal_STOP,
	}
	f.stopSent = append(f.stopSent, event)
	f.signal(event)
	f.stopDone = true
}

func (f *SnapshotTakeFake) signal(event codecs.RecordingSignalEventFields) {
	if f.handler != nil {
		f.handler.RecordingSignal(event)
	}
}

func (*SnapshotTakeFake) ControlSessionID() int64 {
	return snapshotTakeFakeControlSessionID
}

// Config returns the normalized config with the scripted timeouts.
func (f *SnapshotTakeFake) Config() archive.Config {
	return archive.Config{
		ControlOfferRetryTimeout: f.offerRetryTimeout,
		ControlResponseTimeout:   f.responseTimeout,
	}.Normalize()
}

func (f *SnapshotTakeFake) SetRecordingSignalHandler(handler archive.RecordingSignalHandler) {
	f.handler = handler
}

// OfferAddExclusivePublication records the add. It returns the first
// scripted error, or a new correlation id.
func (f *SnapshotTakeFake) OfferAddExclusivePublication(
	streamID int32,
	channel string,
) (int64, error) {
	f.addStreamIDs = append(f.addStreamIDs, streamID)
	f.addChannels = append(f.addChannels, channel)
	if len(f.addOffers) > 0 {
		err := f.addOffers[0]
		f.addOffers = f.addOffers[1:]
		if err != nil {
			return 0, err
		}
	}
	id := f.nextCorrelationID
	f.nextCorrelationID++
	return id, nil
}

// PollAddExclusivePublication returns the first scripted outcome, or a new
// hook publication.
func (f *SnapshotTakeFake) PollAddExclusivePublication(
	int64,
) (*aeron.ExclusivePublication, bool, error) {
	step := snapshotTakeFakeAddPoll{done: true}
	if len(f.addPolls) > 0 {
		step = f.addPolls[0]
		f.addPolls = f.addPolls[1:]
	}
	if !step.done || step.err != nil {
		return nil, step.done, step.err
	}
	publication := step.publication
	if publication == nil {
		publication = aeron.NewTestExclusivePublicationForHooks(f.tb)
	}
	f.publications = append(f.publications, publication)
	return publication, true, nil
}

func (f *SnapshotTakeFake) CancelPending(correlationID int64) {
	f.cancelled = append(f.cancelled, correlationID)
}

func (f *SnapshotTakeFake) newStart(streamID int32, channel []byte) snapshotStartRequest {
	f.startStreamIDs = append(f.startStreamIDs, streamID)
	f.startChannels = append(f.startChannels, string(channel))
	f.startBuilds++
	f.startClosed = false
	f.startProven = false
	if len(f.recordingIDs) > 0 {
		f.recordingID = f.recordingIDs[0]
		f.recordingIDs = f.recordingIDs[1:]
	} else {
		f.recordingID = f.nextRecordingID
		f.nextRecordingID++
	}
	return f
}

// TryOffer returns the first scripted outcome, or true, nil.
func (f *SnapshotTakeFake) TryOffer() (bool, error) {
	if f.startClosed {
		return false, nil
	}
	return popSnapshotTakeFakeStep(&f.startOffers)
}

// PollSignal returns the first scripted outcome, or true, nil. A true, nil
// outcome proves the START.
func (f *SnapshotTakeFake) PollSignal() (bool, error) {
	if f.startClosed {
		return false, nil
	}
	done, err := popSnapshotTakeFakeStep(&f.startSignals)
	if done && err == nil {
		f.startProven = true
	}
	return done, err
}

func (f *SnapshotTakeFake) Close() {
	f.startClosed = true
	f.startCloses++
}

// offerRow pops the first scripted code, when one is queued. A negative code
// returns without an offer. Otherwise it offers row on publication.
func (f *SnapshotTakeFake) offerRow(publication *aeron.ExclusivePublication, row []byte) int64 {
	if len(f.rowOffers) > 0 {
		code := f.rowOffers[0]
		f.rowOffers = f.rowOffers[1:]
		if code < 0 {
			return code
		}
	}
	return publication.Offer(row)
}

func popSnapshotTakeFakeStep(queue *[]snapshotTakeFakeStep) (bool, error) {
	if len(*queue) == 0 {
		return true, nil
	}
	step := (*queue)[0]
	*queue = (*queue)[1:]
	return step.done, step.err
}

//go:build rapid

package cluster

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// branchModel is the reference model for the start-versus-extend branch.
type branchModel struct {
	startSucceeded bool
	lastReturned   int64
	sawWriteTerm   bool
}

func TestLogRecording_startVsExtendBranch_rapid(t *testing.T) {
	t.Parallel()

	const (
		logChannel = "aeron:udp?endpoint=239.255.12.1:9004"
		streamID   = int32(100)
		sessionID  = int32(9)
	)

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		dir := t.TempDir()
		termLog, err := recordinglog.Open(dir, true)
		require.NoError(rt, err)

		fake := &recordingArchiveStub{}
		r := &logRecording{
			ctx:         context.Background(),
			client:      fake,
			terms:       termLog,
			logChannel:  logChannel,
			logStreamID: streamID,
		}

		model := branchModel{}
		n := rapid.IntRange(1, 30).Draw(rt, "n")
		ops := []string{
			"startRecording",
			"writeTerm",
			"stopRecording",
			"imageStop",
			"reopenLog",
		}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))

			switch op {
			case "startRecording":
				if model.startSucceeded {
					// Idempotent re-start first drains; keep model in step by
					// stopping through the control first.
					require.NoError(rt, r.StopLogRecording())
					model.startSucceeded = false
					require.False(rt, r.IsActive())
				}

				wantStart := termLog.TermRecordingID() == recordinglog.NullRecordingID
				// After any writeTerm, no later step takes start.
				if model.sawWriteTerm {
					require.False(rt, wantStart)
				}

				fake.lastBranch = ""
				fake.startID = rapid.Int64Range(0, 1<<20).Draw(rt, "startID"+strconv.Itoa(i))
				got, startErr := r.StartLogRecording(
					archivewire.SourceLocation_REMOTE,
					sessionID,
					"",
				)
				require.NoError(rt, startErr)

				// Branch is a total function of the shared TERM id.
				if wantStart {
					require.Equal(rt, "start", fake.lastBranch)
					require.Equal(rt, fake.startID, got)
					require.True(rt, fake.lastStartAutoStop)
				} else {
					require.Equal(rt, "extend", fake.lastBranch)
					require.Equal(rt, termLog.TermRecordingID(), fake.lastExtendID)
					require.True(rt, fake.lastExtendAutoStop)
					// Extend carries the established id forward.
					require.Equal(rt, termLog.TermRecordingID(), got)
				}

				// Control stores the id it returned.
				require.Equal(rt, got, r.recordingID)
				// Successful start or extend leaves control active.
				require.True(rt, r.IsActive())

				model.lastReturned = got
				model.startSucceeded = true

			case "writeTerm":
				if model.lastReturned < 0 || model.lastReturned == recordinglog.NullRecordingID {
					continue
				}
				if !model.startSucceeded {
					continue
				}
				// Pick a free leadership term id.
				leadershipTermID := rapid.Int64Range(0, 40).Draw(rt, "term"+strconv.Itoa(i))
				if termLog.FindTermEntry(leadershipTermID) != nil {
					continue
				}
				err := termLog.AppendTerm(model.lastReturned, leadershipTermID, 0, int64(i+1))
				// Term write after the branch never mismatches.
				require.NoError(rt, err)
				model.sawWriteTerm = true
				require.Equal(rt, model.lastReturned, termLog.TermRecordingID())

			case "stopRecording":
				if !model.startSucceeded {
					continue
				}
				require.NoError(rt, r.StopLogRecording())
				model.startSucceeded = false
				// Inactive after stop.
				require.False(rt, r.IsActive())
				// A blocking stop sets no STOP mark.
				_, stopped := r.ConsumeStopSignal()
				require.False(rt, stopped)

			case "imageStop":
				if !model.startSucceeded {
					continue
				}
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "stopPos"+strconv.Itoa(i))
				stopCalls := fake.stopCalls
				stopAwaitCalls := fake.stopAwaitCalls
				r.handleRecordingSignal(codecs.RecordingSignalEventFields{
					RecordingID: r.recordingID,
					Position:    pos,
					Signal:      archivewire.RecordingSignal_STOP,
				})
				require.Equal(rt, stopCalls, fake.stopCalls)
				require.Equal(rt, stopAwaitCalls, fake.stopAwaitCalls)
				require.False(rt, r.IsActive())
				got, posErr := r.StopPositionFor(r.latchedRecordingID)
				require.NoError(rt, posErr)
				require.Equal(rt, pos, got)
				// The STOP mark reports the signal position once.
				markPosition, stopped := r.ConsumeStopSignal()
				require.True(rt, stopped)
				require.Equal(rt, pos, markPosition)
				_, stopped = r.ConsumeStopSignal()
				require.False(rt, stopped)
				model.startSucceeded = false

			case "reopenLog":
				// Reopen preserves the branch decision.
				before := termLog.TermRecordingID()
				require.NoError(rt, termLog.Close())

				reopened, openErr := recordinglog.Open(dir, false)
				require.NoError(rt, openErr)
				termLog = reopened
				require.Equal(rt, before, termLog.TermRecordingID())

				// Restart control over the reopened log; clear start state.
				r = &logRecording{
					ctx:         context.Background(),
					client:      fake,
					terms:       termLog,
					logChannel:  logChannel,
					logStreamID: streamID,
				}
				model.startSucceeded = false
				model.lastReturned = recordinglog.NullRecordingID
				// Inactive after reopen.
				require.False(rt, r.IsActive())

				// Next startRecording (if drawn) must take the same branch
				// pre-reopen state would have taken.

				wantStart := before == recordinglog.NullRecordingID
				if model.sawWriteTerm {
					require.False(rt, wantStart)
				}
			}
		}

		require.NoError(rt, termLog.Close())
	})
}

// truncateStoreModel is the reference model for the truncate stop position
// store.
type truncateStoreModel struct {
	latchedID int64
	stopPos   int64
}

// TestLogRecording_truncateStopPosition_rapid asserts StopPositionFor reports
// the last successful truncate position across failed truncates and dirty
// drains, with no catalog read once nothing is pending.
func TestLogRecording_truncateStopPosition_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		fake := &recordingArchiveStub{}
		r := &logRecording{
			ctx:                context.Background(),
			client:             fake,
			logChannel:         "aeron:udp?endpoint=239.255.12.1:9004",
			logStreamID:        100,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		model := truncateStoreModel{latchedID: recordingIDUnknown}
		recordingID := rapid.Int64Range(0, 1<<20).Draw(rt, "recordingID")

		n := rapid.IntRange(1, 30).Draw(rt, "n")
		ops := []string{"truncateOK", "truncateFail", "armDirtyThenDrain"}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))

			switch op {
			case "truncateOK":
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "pos"+strconv.Itoa(i))
				fake.truncateErr = nil
				_, err := r.TruncateLogRecording(recordingID, pos)
				require.NoError(rt, err)
				model.latchedID = recordingID
				model.stopPos = pos

			case "truncateFail":
				fake.truncateErr = errors.New("archive refused")
				_, err := r.TruncateLogRecording(recordingID, 64)
				require.Error(rt, err)

			case "armDirtyThenDrain":
				// A dirty start keeps the channel. The drain keeps the stored stop.
				// markStopped with an unknown catalog id changes nothing.
				r.startChannel = []byte("aeron:udp?endpoint=239.255.12.1:9004")
				r.recordingID = recordingIDUnknown
				require.NoError(rt, r.StopLogRecording())
			}

			require.Equal(rt, model.latchedID, r.latchedRecordingID)
			require.Equal(rt, model.stopPos, r.stopPosition)
			if model.latchedID >= 0 {
				listBefore := fake.listCalls
				got, err := r.StopPositionFor(model.latchedID)
				require.NoError(rt, err)
				require.Equal(rt, model.stopPos, got)
				require.Equal(rt, listBefore, fake.listCalls)
			}
		}
	})
}

// pollStopModel is the reference model for PollStopLogRecording.
type pollStopModel struct {
	needsStop   bool
	recordingID int64
	latchedID   int64
	stopPos     int64
	pending     bool
	// listing is true during the catalog read.
	listing   bool
	offered   bool
	deadline  int64
	inFlight  int
	nowNs     int64
	proofHeld bool
	// knownStop is the last known-id stop position, from an adopted STOP or
	// a catalog stop. hasKnownStop is false until one lands. A blocking stop
	// clears it.
	knownStop    int64
	hasKnownStop bool
	// termID is the term recording id the lookup reports.
	termID int64
	// identity is true while the pending attempt stops by identity.
	identity bool
	// stopMarked mirrors the STOP mark. Only an adopted STOP sets it.
	stopMarked bool
}

// pollStopScript is the harness state one poll reads.
type pollStopScript struct {
	offerOK          bool
	offerErr         error
	responded        bool
	responseErr      error
	listOfferOK      bool
	listOfferErr     error
	listDelivered    bool
	listStopPosition int64
	listReplyErr     error
}

func scriptOf(h *stopRequestHarness) pollStopScript {
	return pollStopScript{
		offerOK:          h.offerOK,
		offerErr:         h.offerErr,
		responded:        h.responded,
		responseErr:      h.responseErr,
		listOfferOK:      h.listOfferOK,
		listOfferErr:     h.listOfferErr,
		listDelivered:    h.listDelivered,
		listStopPosition: h.listStopPosition,
		listReplyErr:     h.listReplyErr,
	}
}

func (m *pollStopModel) isActive() bool {
	return m.needsStop && m.recordingID >= 0
}

func (m *pollStopModel) clearPending() {
	if m.pending {
		m.inFlight--
	}
	m.pending = false
	m.listing = false
	m.offered = false
	m.identity = false
}

// identityTarget mirrors identityStopTarget.
func (m *pollStopModel) identityTarget() int64 {
	if m.needsStop || m.termID == recordinglog.NullRecordingID || m.termID == m.latchedID {
		return recordinglog.NullRecordingID
	}
	return m.termID
}

// catalogID is the recording id the catalog read lists.
func (m *pollStopModel) catalogID() int64 {
	if m.identity {
		return m.termID
	}
	return m.recordingID
}

func (m *pollStopModel) markStopped(pos int64) {
	if m.recordingID >= 0 {
		m.latchedID = m.recordingID
	}
	m.stopPos = pos
	m.needsStop = false
	m.proofHeld = true
	m.clearPending()
}

func (m *pollStopModel) markKnownStopped(pos int64) {
	m.markStopped(pos)
	m.knownStop = pos
	m.hasKnownStop = true
}

// offerStep mirrors offerPendingStep.
func (m *pollStopModel) offerStep(offerOK bool, offerErr error) (bool, error) {
	if m.offered {
		return true, nil
	}
	if offerErr != nil {
		m.clearPending()
		return false, offerErr
	}
	if !offerOK {
		if m.nowNs >= m.deadline {
			m.clearPending()
			return false, errStopOfferTimeout
		}
		return false, nil
	}
	m.offered = true
	m.deadline = m.nowNs + pollStopResponseTimeoutNs
	return true, nil
}

func (m *pollStopModel) poll(script pollStopScript) (done bool, err error) {
	if !m.needsStop {
		if m.identityTarget() == recordinglog.NullRecordingID {
			m.clearPending()
			return true, nil
		}
		return m.pollIdentity(script)
	}
	if !m.pending {
		m.pending = true
		m.offered = false
		m.deadline = m.nowNs + pollStopOfferTimeoutNs
		m.inFlight++
	}
	if m.listing {
		return m.pollCatalog(script)
	}
	offered, err := m.offerStep(script.offerOK, script.offerErr)
	if err != nil || !offered {
		return false, err
	}
	dirty := m.recordingID < 0
	unknownSubscription := isUnknownSubscription(script.responseErr)
	switch {
	case dirty && unknownSubscription:
		m.markStopped(m.stopPos)
		return true, nil
	case unknownSubscription:
		// inFlight holds: the list handle replaces the stop handle.
		m.listing = true
		m.offered = false
		m.deadline = m.nowNs + pollStopOfferTimeoutNs
		return m.pollCatalog(script)
	case script.responseErr != nil:
		m.clearPending()
		return false, script.responseErr
	case script.responded && dirty:
		m.markStopped(m.stopPos)
		return true, nil
	case script.responded:
		return false, nil
	}
	if m.nowNs >= m.deadline {
		m.clearPending()
		return false, errStopResponseTimeout
	}
	return false, nil
}

// pollIdentity mirrors pollIdentityStop.
func (m *pollStopModel) pollIdentity(script pollStopScript) (done bool, err error) {
	if !m.pending {
		m.pending = true
		m.identity = true
		m.offered = false
		m.deadline = m.nowNs + pollStopOfferTimeoutNs
		m.inFlight++
	}
	if m.listing {
		return m.pollCatalog(script)
	}
	offered, err := m.offerStep(script.offerOK, script.offerErr)
	if err != nil || !offered {
		return false, err
	}
	switch {
	case script.responseErr != nil:
		m.clearPending()
		return false, script.responseErr
	case script.responded:
		// Stopped and not-active replies both enter the catalog read.
		m.listing = true
		m.offered = false
		m.deadline = m.nowNs + pollStopOfferTimeoutNs
		return m.pollCatalog(script)
	}
	if m.nowNs >= m.deadline {
		m.clearPending()
		return false, errStopResponseTimeout
	}
	return false, nil
}

func (m *pollStopModel) pollCatalog(script pollStopScript) (done bool, err error) {
	offered, err := m.offerStep(script.listOfferOK, script.listOfferErr)
	if err != nil || !offered {
		return false, err
	}
	switch {
	case script.listReplyErr != nil:
		m.clearPending()
		return false, script.listReplyErr
	case script.listDelivered && script.listStopPosition == recordinglog.NullPosition:
		m.clearPending()
		return false, errNoCatalogStopPosition
	case script.listDelivered && m.identity:
		m.latchedID = m.termID
		m.stopPos = script.listStopPosition
		m.knownStop = script.listStopPosition
		m.hasKnownStop = true
		m.clearPending()
		return true, nil
	case script.listDelivered:
		m.markKnownStopped(script.listStopPosition)
		return true, nil
	}
	if m.nowNs >= m.deadline {
		m.clearPending()
		return false, errStopResponseTimeout
	}
	return false, nil
}

func requirePollStopInvariants(
	rt *rapid.T,
	r *logRecording,
	model *pollStopModel,
	h *stopRequestHarness,
	done bool,
	pollErr error,
) {
	rt.Helper()

	require.Equal(rt, model.needsStop, r.needsStop())
	require.Equal(rt, model.isActive(), r.IsActive())
	require.Equal(rt, model.stopPos, r.stopPosition)
	require.Equal(rt, model.latchedID, r.latchedRecordingID)
	inFlight := 0
	if r.pendingStop != nil {
		inFlight = 1
	}
	require.Equal(rt, inFlight, model.inFlight)
	require.Equal(rt, inFlight, h.inFlight())
	require.LessOrEqual(rt, h.inFlight(), 1)
	listing := r.pendingStop != nil && r.pendingStop.list != nil
	require.Equal(rt, model.listing, listing)
	if listing {
		require.True(rt, h.last().closed)
		require.True(rt, h.lastList().stopClosedAtBuild)
		require.False(rt, h.lastList().closed)
		require.Equal(rt, model.catalogID(), h.lastList().recordingID)
	}
	identity := r.pendingStop != nil && r.pendingStop.isIdentity()
	require.Equal(rt, model.identity, identity)
	if identity {
		require.False(rt, r.needsStop())
		require.NotEqual(rt, recordinglog.NullRecordingID, model.termID)
		require.NotEqual(rt, model.termID, r.latchedRecordingID)
		require.Equal(rt, model.termID, r.pendingStop.identityRecordingID)
	}
	require.Equal(rt, model.stopMarked, r.stopSignalled)
	require.Equal(rt, 0, r.client.(*recordingArchiveStub).listCalls)
	if done {
		require.NoError(rt, pollErr)
		require.False(rt, r.needsStop())
		if model.recordingID >= 0 && model.hasKnownStop {
			require.Equal(rt, model.knownStop, r.stopPosition)
		}
	}
	if pollErr != nil {
		require.False(rt, done)
		require.Nil(rt, r.pendingStop)
	}
}

// errPollStopOffer is the terminal offer error the stop and list ops script.
var errPollStopOffer = errors.New("publication closed")

// TestLogRecording_pollStop_rapid draws poll, offer, response, list, signal,
// clock, sync-stop, and term-stop seed ops against pollStopModel. A drawn term
// id with no stored stop selects the identity attempt.
func TestLogRecording_pollStop_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()

		needsStop := rapid.Bool().Draw(rt, "needsStop")
		dirty := rapid.Bool().Draw(rt, "dirty")
		priorPos := rapid.Int64Range(0, 1<<20).Draw(rt, "priorPos")
		recordingID := pollStopRecordingID
		if dirty {
			recordingID = recordingIDUnknown
		}
		termID := recordinglog.NullRecordingID
		if rapid.Bool().Draw(rt, "termID") {
			termID = pollStopRecordingID
		}
		storedStop := termID != recordinglog.NullRecordingID &&
			rapid.Bool().Draw(rt, "storedStop")
		latchedID := recordingIDUnknown
		if storedStop {
			latchedID = termID
		}

		h := &stopRequestHarness{
			offerOK:     rapid.Bool().Draw(rt, "offerOK"),
			listOfferOK: rapid.Bool().Draw(rt, "listOfferOK"),
		}
		// Starting on an unknown subscription reply reaches the catalog
		// read in fewer ops.
		if rapid.Bool().Draw(rt, "firstUnknownSubscription") {
			h.responseErr = &archive.Exception{
				ErrorCode: archive.ErrorUnknownSubscription,
			}
		}
		r := newPollStopRecording(recordingID, priorPos, h)
		if !needsStop {
			r.startChannel = nil
		}
		r.terms = fixedTermLookup(termID)
		r.newIdentityStopRequest = h.newIdentityRequest
		r.latchedRecordingID = latchedID

		model := pollStopModel{
			needsStop:   needsStop,
			recordingID: recordingID,
			latchedID:   latchedID,
			stopPos:     priorPos,
			proofHeld:   !needsStop,
			termID:      termID,
		}

		n := rapid.IntRange(1, 40).Draw(rt, "n")
		ops := []string{
			"poll",
			"offerAccepted",
			"deliverUnknownSubscription",
			"deliverDescriptor",
			"listOfferAccepted",
			"offerBackPressure",
			"offerError",
			"deliverOK",
			"deliverError",
			"deliverStopSignal",
			"deliverTermStopSignal",
			"listBackPressure",
			"listOfferError",
			"deliverListError",
			"advanceClock",
			"syncStop",
			"seedTermStop",
		}

		for i := range n {
			op := rapid.SampledFrom(ops).Draw(rt, "op"+strconv.Itoa(i))
			var (
				done bool
				err  error
			)

			switch op {
			case "offerAccepted":
				h.offerOK = true
				h.offerErr = nil
			case "offerBackPressure":
				h.offerOK = false
				h.offerErr = nil
			case "offerError":
				h.offerOK = false
				h.offerErr = errPollStopOffer
			case "deliverOK":
				h.responded = true
				h.responseErr = nil
			case "deliverUnknownSubscription":
				h.responded = false
				h.responseErr = &archive.Exception{
					ErrorCode: archive.ErrorUnknownSubscription,
				}
			case "deliverError":
				h.responded = false
				h.responseErr = &archive.Exception{ErrorCode: archive.ErrorGeneric}
			case "deliverStopSignal":
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "stopPos"+strconv.Itoa(i))
				r.handleRecordingSignal(codecs.RecordingSignalEventFields{
					RecordingID: r.recordingID,
					Position:    pos,
					Signal:      archivewire.RecordingSignal_STOP,
				})
				if model.isActive() {
					model.markKnownStopped(pos)
					model.stopMarked = true
				}
			case "deliverTermStopSignal":
				// An identity attempt runs inactive, so the control ignores this
				// STOP. An active control with the term id as catalog id adopts it.
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "termStopPos"+strconv.Itoa(i))
				r.handleRecordingSignal(codecs.RecordingSignalEventFields{
					RecordingID: termID,
					Position:    pos,
					Signal:      archivewire.RecordingSignal_STOP,
				})
				if model.isActive() && termID == model.recordingID {
					model.markKnownStopped(pos)
					model.stopMarked = true
				}
			case "listOfferAccepted":
				h.listOfferOK = true
				h.listOfferErr = nil
			case "listBackPressure":
				h.listOfferOK = false
				h.listOfferErr = nil
			case "listOfferError":
				h.listOfferOK = false
				h.listOfferErr = errPollStopOffer
			case "deliverDescriptor":
				stop := recordinglog.NullPosition
				if !rapid.Bool().Draw(rt, "nullStop"+strconv.Itoa(i)) {
					stop = rapid.Int64Range(0, 1<<20).Draw(rt, "catalogStop"+strconv.Itoa(i))
				}
				h.listReplyErr = nil
				h.deliverDescriptor(stop)
			case "deliverListError":
				h.listDelivered = false
				h.listReplyErr = &archive.Exception{ErrorCode: archive.ErrorUnknownRecording}
			case "advanceClock":
				delta := rapid.Int64Range(1, 1500).Draw(rt, "delta"+strconv.Itoa(i))
				model.nowNs += delta
			case "syncStop":
				require.NoError(rt, r.StopLogRecording())
				if model.needsStop {
					pos := int64(0)
					if model.recordingID < 0 {
						pos = model.stopPos
					}
					model.markStopped(pos)
					model.hasKnownStop = false
				} else {
					model.clearPending()
				}
			case "seedTermStop":
				// A seed or truncate stores the term stop. The next poll ends any
				// identity attempt.
				if model.needsStop || termID == recordinglog.NullRecordingID {
					break
				}
				pos := rapid.Int64Range(0, 1<<20).Draw(rt, "seedPos"+strconv.Itoa(i))
				r.seedStopPosition(termID, pos)
				model.latchedID = termID
				model.stopPos = pos
				model.hasKnownStop = false
				done, err = requirePollStopStep(rt, r, &model, h)
			case "poll":
				done, err = requirePollStopStep(rt, r, &model, h)
			}

			requirePollStopInvariants(rt, r, &model, h, done, err)
			// A poll after a scripting op reaches the catalog read in fewer
			// ops.
			if op != "poll" && rapid.Bool().Draw(rt, "pollAfter"+strconv.Itoa(i)) {
				done, err = requirePollStopStep(rt, r, &model, h)
				requirePollStopInvariants(rt, r, &model, h, done, err)
			}
		}
	})
}

// requirePollStopStep polls the recording and the model on one harness
// snapshot, then compares the outcomes.
func requirePollStopStep(
	rt *rapid.T,
	r *logRecording,
	model *pollStopModel,
	h *stopRequestHarness,
) (
	done bool,
	err error,
) {
	rt.Helper()

	needsStopBefore := r.needsStop()
	identityBefore := model.identityTarget() != recordinglog.NullRecordingID
	pendingBefore := r.pendingStop != nil
	builtBefore := len(h.built)
	script := scriptOf(h)
	wantDone, wantErr := model.poll(script)
	done, err = r.PollStopLogRecording(model.nowNs)
	if wantErr != nil {
		require.ErrorIs(rt, err, wantErr)
		require.False(rt, done)
	} else {
		require.NoError(rt, err)
		require.Equal(rt, wantDone, done)
	}
	switch {
	case needsStopBefore:
	case identityBefore && !pendingBefore:
		require.Len(rt, h.built, builtBefore+1)
		require.Equal(rt, model.termID, h.identityBuilt[len(h.identityBuilt)-1])
	default:
		require.Len(rt, h.built, builtBefore)
	}
	if done && identityBefore {
		require.True(rt, script.listDelivered)
		require.Equal(rt, model.termID, r.latchedRecordingID)
		require.Equal(rt, script.listStopPosition, r.stopPosition)
		require.NotEqual(rt, recordinglog.NullPosition, r.stopPosition)
	}
	if err != nil && needsStopBefore {
		// A failed channel attempt keeps the start channel for a fresh attempt.
		require.True(rt, r.needsStop())
		require.NotEmpty(rt, r.startChannel)
	}
	if done && needsStopBefore {
		require.True(rt, model.proofHeld)
	}
	if done && needsStopBefore && model.recordingID >= 0 {
		require.True(rt, script.listDelivered)
		require.Equal(rt, script.listStopPosition, r.stopPosition)
		require.NotEqual(rt, recordinglog.NullPosition, r.stopPosition)
	}
	return done, err
}

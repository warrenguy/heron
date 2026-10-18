//go:build rapid && testhooks

package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Handler outcomes the take model draws.
const (
	modelHandlerOK = iota
	modelHandlerError
	modelHandlerClose
)

// modelTimeoutNs is the offer retry and response timeout.
const modelTimeoutNs int64 = 1_000

// modelSnapshotService pops one handler outcome per TakeSnapshot call.
type modelSnapshotService struct {
	recordingService
	outcomes []int
	calls    int
	err      error
}

func (s *modelSnapshotService) TakeSnapshot(publication *aeron.ExclusivePublication) error {
	s.calls++
	outcome := s.outcomes[0]
	s.outcomes = s.outcomes[1:]
	switch outcome {
	case modelHandlerError:
		return s.err
	case modelHandlerClose:
		return publication.Close()
	}
	return nil
}

// takeScript is the drawn outcome of every step of one take.
type takeScript struct {
	addOffer     error
	addPolls     []snapshotTakeFakeAddPoll
	startOffers  []snapshotTakeFakeStep
	startSignals []snapshotTakeFakeStep
	rowOffers    []int64
	handler      int
	recordingID  int64
	stopDelta    int64
	stopDelay    int
	stopNever    bool
	clockStepsNs []int64
}

func drawTakeScript(rt *rapid.T, label string, err error) takeScript {
	rt.Helper()
	var s takeScript
	if rapid.IntRange(0, 9).Draw(rt, label+"addOfferFails") == 0 {
		s.addOffer = err
	}
	for range rapid.IntRange(0, 3).Draw(rt, label+"addWaits") {
		s.addPolls = append(s.addPolls, snapshotTakeFakeAddPoll{})
	}
	switch rapid.IntRange(0, 9).Draw(rt, label+"addOutcome") {
	case 0:
		s.addPolls = append(s.addPolls, snapshotTakeFakeAddPoll{err: err})
	case 1:
		s.addPolls = append(s.addPolls, snapshotTakeFakeAddPoll{
			done: true,
			err:  &aeron.RegistrationTimeoutError{Kind: "publication"},
		})
	}
	for range rapid.IntRange(0, 3).Draw(rt, label+"startOfferWaits") {
		s.startOffers = append(s.startOffers, snapshotTakeFakeStep{})
	}
	if rapid.IntRange(0, 9).Draw(rt, label+"startOfferFails") == 0 {
		s.startOffers = append(s.startOffers, snapshotTakeFakeStep{err: err})
	}
	for range rapid.IntRange(0, 3).Draw(rt, label+"startSignalWaits") {
		s.startSignals = append(s.startSignals, snapshotTakeFakeStep{})
	}
	if rapid.IntRange(0, 9).Draw(rt, label+"startSignalFails") == 0 {
		s.startSignals = append(s.startSignals, snapshotTakeFakeStep{done: true, err: err})
	}
	codes := []int64{
		0,
		aeron.BackPressured,
		aeron.AdminAction,
		aeron.NotConnected,
		aeron.MaxPositionExceeded,
	}
	for range rapid.IntRange(0, 6).Draw(rt, label+"rowOffers") {
		code := rapid.SampledFrom(codes).Draw(rt, label+"rowCode")
		if code == aeron.MaxPositionExceeded && rapid.IntRange(0, 3).Draw(rt, label+"rowTerminal") != 0 {
			code = 0
		}
		s.rowOffers = append(s.rowOffers, code)
	}
	s.handler = rapid.IntRange(0, 5).Draw(rt, label+"handler")
	if s.handler > modelHandlerClose {
		s.handler = modelHandlerOK
	}
	s.recordingID = rapid.Int64Range(0, 5_000).Draw(rt, label+"recordingID")
	s.stopDelta = rapid.SampledFrom([]int64{0, 0, 32, -1}).Draw(rt, label+"stopDelta")
	s.stopDelay = rapid.IntRange(0, 2).Draw(rt, label+"stopDelay")
	s.stopNever = rapid.IntRange(0, 9).Draw(rt, label+"stopNever") == 0
	for range 24 {
		step := rapid.SampledFrom([]int64{0, 0, modelTimeoutNs / 2, modelTimeoutNs}).
			Draw(rt, label+"clockStep")
		s.clockStepsNs = append(s.clockStepsNs, step)
	}
	return s
}

// takeModel mirrors one take over its script, one tick at a time.
type takeModel struct {
	script     takeScript
	rows       int
	state      snapshotTakeState
	deadlineNs int64
	addPending bool
	nextRow    int
	stopWaited int
	stopSeen   bool
	hookCalls  int
	nextAddID  *int64
	cancelled  *[]int64
	addID      int64
}

// tick mirrors one DoWork call: the fake Poll, then the take work. Returns
// done and the cause once the take resolves. A nil cause on done is a success.
func (m *takeModel) tick(nowNs int64) (bool, error) {
	if m.state == takeAwaitingStop && !m.script.stopNever && !m.stopSeen {
		if m.stopWaited < m.script.stopDelay {
			m.stopWaited++
		} else {
			m.stopSeen = true
		}
	}
	for {
		switch m.state {
		case takeAddingPublication:
			if !m.addPending {
				if err := m.script.addOffer; err != nil {
					m.script.addOffer = nil
					return true, errSnapshotPublicationAdd
				}
				m.addID = *m.nextAddID
				*m.nextAddID++
				m.addPending = true
			}
			poll := snapshotTakeFakeAddPoll{done: true}
			if len(m.script.addPolls) > 0 {
				poll = m.script.addPolls[0]
				m.script.addPolls = m.script.addPolls[1:]
			}
			if poll.done {
				m.addPending = false
			}
			if poll.err != nil {
				return m.fail(errSnapshotPublicationAdd)
			}
			if !poll.done {
				return false, nil
			}
			m.deadlineNs = nowNs + modelTimeoutNs
			m.state = takeOfferingStart

		case takeOfferingStart:
			done, err := popSnapshotTakeFakeStep(&m.script.startOffers)
			switch {
			case err != nil:
				return m.fail(errSnapshotRecordingStart)
			case done:
				m.deadlineNs = nowNs + modelTimeoutNs
				m.state = takeAwaitingStart
			case nowNs >= m.deadlineNs:
				return m.fail(errSnapshotRecordingStart)
			default:
				return false, nil
			}

		case takeAwaitingStart:
			done, err := popSnapshotTakeFakeStep(&m.script.startSignals)
			switch {
			case err != nil:
				return m.fail(errSnapshotRecordingStart)
			case done:
				m.deadlineNs = nowNs + modelTimeoutNs
				m.state = takeWriting
			case nowNs >= m.deadlineNs:
				return m.fail(errSnapshotRecordingStart)
			default:
				return false, nil
			}

		case takeWriting:
			for m.nextRow < m.rows {
				code := int64(0)
				if len(m.script.rowOffers) > 0 {
					code = m.script.rowOffers[0]
					m.script.rowOffers = m.script.rowOffers[1:]
				}
				switch {
				case code >= 0:
					m.nextRow++
					m.deadlineNs = nowNs + modelTimeoutNs
					continue
				case !aeron.IsOfferRetriable(code):
					return m.fail(errSnapshotWrite)
				case nowNs >= m.deadlineNs:
					return m.fail(errSnapshotWrite)
				}
				return false, nil
			}
			m.hookCalls++
			switch m.script.handler {
			case modelHandlerError:
				return m.fail(errTakeSnapshot)
			case modelHandlerClose:
				return m.fail(errSnapshotWrite)
			}
			m.deadlineNs = nowNs + modelTimeoutNs
			m.state = takeAwaitingStop

		default:
			switch {
			case m.stopSeen && m.script.stopDelta >= 0:
				return true, nil
			case m.stopSeen:
				return m.fail(errSnapshotRecordingStop)
			case nowNs >= m.deadlineNs:
				return m.fail(errSnapshotRecordingStop)
			}
			return false, nil
		}
	}
}

// fail appends a pending add id to cancelled, then resolves with cause.
func (m *takeModel) fail(cause error) (bool, error) {
	if m.addPending {
		*m.cancelled = append(*m.cancelled, m.addID)
		m.addPending = false
	}
	return true, cause
}

// Asserts the snapshot take against a reference model of its steps. Each
// take draws the add, the start offer, the START signal, the row offers, the
// handler, the STOP, the recording id, and clock steps across each deadline.
// The proxy accepts every ack.
//
// Invariants:
//   - at most one ack per action, exactly one once the take resolves
//   - a success ack carries the START recording id, after a STOP at or above
//     the close position
//   - no ack that carries the pre-proof id
//   - a failure ack of -1, with the mark on the same tick as its send
//   - with no term event, a failure before any publication add
//   - no log frame past the action while the take runs
//   - after resolution: no pending add, a closed start request, a closed
//     publication, and no session handler
func TestSnapshotTake_model_rapid(t *testing.T) {
	t.Parallel()
	termPayload := termEventPayload(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	sessionPayloads := make([][]byte, 3)
	for id := range sessionPayloads {
		sessionPayloads[id] = sessionOpenPayload(t, int64(id+1), "aeron:ipc", "")
	}
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()
		drawErr := errors.New("rapid take failure")
		svc := &modelSnapshotService{err: drawErr}
		proxy, pub := boundModuleProxy(t)
		agent, image, committed := bindImage(t, AgentConfig{
			ServiceID:   1,
			Service:     svc,
			ModuleProxy: proxy,
		}, 0)
		fake := InstallSnapshotTakeFakeForTest(t, agent)
		fake.offerRetryTimeout = time.Duration(modelTimeoutNs)
		fake.responseTimeout = time.Duration(modelTimeoutNs)
		log := &takeLog{image: image, committed: committed}

		termEvent := rapid.IntRange(0, 5).Draw(rt, "termEvent") != 0
		if termEvent {
			log.write(rt, termPayload)
		}
		sessions := rapid.IntRange(0, len(sessionPayloads)).Draw(rt, "sessions")
		for id := range sessions {
			log.write(rt, sessionPayloads[id])
		}
		takes := 1
		if termEvent {
			takes = rapid.IntRange(1, 3).Draw(rt, "takes")
		}

		nextAddID := int64(1)
		var modelCancelled []int64
		var wantAcks []int64
		var actionEnds []int64
		nowNs := int64(0)
		timers := 0

		for take := range takes {
			script := drawTakeScript(rt, "take", drawErr)
			fake.addOffers = []error{script.addOffer}
			fake.addPolls = script.addPolls
			fake.startOffers = script.startOffers
			fake.startSignals = script.startSignals
			fake.rowOffers = script.rowOffers
			fake.recordingIDs = []int64{script.recordingID}
			fake.stopDelta = script.stopDelta
			fake.stopDelay = script.stopDelay
			fake.stopNever = script.stopNever
			svc.outcomes = append(svc.outcomes, script.handler)

			end := log.snapshot(rt, int64(take+1))
			actionEnds = append(actionEnds, end)
			log.timer(rt, int64(take+1))

			model := &takeModel{
				script:    script,
				rows:      sessions + 2,
				nextAddID: &nextAddID,
				cancelled: &modelCancelled,
			}
			addsBefore := len(fake.addStreamIDs)
			hookCallsBefore := svc.calls
			resolved := false
			var cause error

			// Consume tick: the action stores the take, or fails it.
			agent.DoWork(nowNs)
			require.Equal(rt, end, agent.LogPosition(), "consume position")
			if !termEvent {
				require.NoError(rt, agent.ServiceFailure(), "mark before the ack")
				require.Nil(rt, agent.take, "take without a time unit")
				resolved, cause = true, errSnapshotTimeUnitUnknown
				nowNs++
				agent.DoWork(nowNs)
			}

			for tick, step := range script.clockStepsNs {
				if resolved {
					break
				}
				if tick >= 16 {
					step = modelTimeoutNs
				}
				nowNs += step
				resolved, cause = model.tick(nowNs)
				agent.DoWork(nowNs)
				require.Len(rt, decodeServiceAcksFromPub(rt, pub), len(wantAcks)+boolInt(resolved),
					"tick %d acks", tick)
				if !resolved {
					require.NoError(rt, agent.ServiceFailure(), "tick %d mark", tick)
					require.NotNil(rt, agent.take, "tick %d take", tick)
					require.Equal(rt, model.state, agent.take.state, "tick %d state", tick)
					require.Equal(rt, end, agent.LogPosition(), "tick %d hold", tick)
					require.Len(rt, svc.timers, timers, "tick %d timers", tick)
				}
			}
			require.True(rt, resolved, "take %d resolved", take)

			want := nullValue
			if cause == nil {
				want = script.recordingID
				require.NotEmpty(rt, fake.stopSent, "STOP before success")
				stop := fake.stopSent[len(fake.stopSent)-1]
				closePosition := fake.closePositions[len(fake.closePositions)-1]
				require.GreaterOrEqual(rt, stop.Position, closePosition, "STOP at close")
				require.Equal(rt, script.recordingID, stop.RecordingID)
			}
			wantAcks = append(wantAcks, want)

			acks := decodeServiceAcksFromPub(rt, pub)
			require.Len(rt, acks, len(wantAcks))
			for i, ack := range acks {
				require.Equal(rt, int64(i), ack.AckID)
				require.Equal(rt, actionEnds[i], ack.LogPosition)
				require.Equal(rt, wantAcks[i], ack.RelevantID)
				require.NotEqual(rt, snapshotTakeFakePreProofID, ack.RelevantID)
			}
			require.Equal(rt, want, agent.LastSnapshotRecordingID())

			requireSnapshotTakeCleanedUp(rt, agent, fake, modelCancelled)
			require.Equal(rt, model.hookCalls, svc.calls-hookCallsBefore, "hook calls")
			if !termEvent {
				require.Len(rt, fake.addStreamIDs, addsBefore, "adds without a time unit")
			}

			if cause != nil {
				require.ErrorIs(rt, agent.ServiceFailure(), cause)
				require.Len(rt, svc.timers, timers, "timers after a failed take")
				return
			}
			require.NoError(rt, agent.ServiceFailure())
			timers++
			require.Len(rt, svc.timers, timers, "timers after the take")
		}
	})
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

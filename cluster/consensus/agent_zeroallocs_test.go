//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/aeron/cnc"
	"luas.fast/aeron/logbuffer"
	"luas.fast/atomicbuffer"
	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
	"luas.fast/internal/testutil"
)

const (
	dutyCycleWarmupCycles = 10
	dutyCycleAckRuns      = 500
	dutyCycleLeaderRuns   = 500
	dutyCycleLogRuns      = 250

	// dutyCyclePair is the cycles per run. AllocsPerRun floors its average, so one
	// cycle per run reads an allocation on every second cycle as 0. The clock
	// advances one timer wheel tick per cycle, so a pair holds one slow tick.
	dutyCyclePair = 2

	dutyCycleFollowerMemberID int32 = 1
)

// dutyCycleCount returns the cycles a measurement runs. The 1 counts the call
// AllocsPerRun makes before it measures.
func dutyCycleCount(runs int) int {
	return dutyCycleWarmupCycles + (1+runs)*dutyCyclePair
}

// measureDutyCycle requires zero allocations and a true result from every
// measured cycle. A compare does not allocate, so a cycle can check its outcome
// inside the measurement.
func measureDutyCycle(t *testing.T, runs int, cycle func() bool) {
	t.Helper()

	// A pair must span the slow tick interval, or a run can miss the slow tick.
	require.GreaterOrEqual(t, dutyCyclePair*timerWheelTickResolutionNs, slowTickIntervalNs)
	for range dutyCycleWarmupCycles {
		cycle()
	}
	misses := 0
	testutil.AssertZeroAllocs(t, "duty cycle", runs, func() {
		for range dutyCyclePair {
			if !cycle() {
				misses++
			}
		}
	})
	require.Zero(t, misses)
}

// failOnElection sets a factory that fails t. Without a factory, enterElection
// returns at once, so a cycle that leaves steady-state still reads a nil
// election.
func failOnElection(t *testing.T, a *Agent) {
	t.Helper()
	a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
		require.Fail(t, "election entered")
		return nil
	})
}

func requireSettledAgent(t *testing.T, a *Agent) {
	t.Helper()
	require.Nil(t, a.Election())
	require.NoError(t, a.TerminationError())
	require.NoError(t, a.ServiceAckFailure())
	require.NoError(t, a.TimerFailure())
	require.NoError(t, a.AppendRankFailure())
}

// rewoundStream holds committed frames at fixed term offsets. A cycle can encode
// a frame body in place. A rewind makes the next poll deliver the frames again.
type rewoundStream struct {
	subscription *aeron.Subscription
	image        *aeron.Image
	nextOffset   int
}

func newRewoundStream(t *testing.T) *rewoundStream {
	t.Helper()
	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return &rewoundStream{subscription: sub, image: image}
}

func (s *rewoundStream) addFrame(t *testing.T, payloadLength int) []byte {
	t.Helper()
	offset := s.nextOffset
	writeLogAdapterUnfragmentedFrame(t, s.image, offset, make([]byte, payloadLength))
	s.nextOffset += logbuffer.AlignFragmentChunkLength(payloadLength)
	payloadStart := offset + logbuffer.OffsetFramePayload
	term := aeron.ImageTermBufferForHooks(s.image)
	return term.Bytes()[payloadStart : payloadStart+payloadLength]
}

func (s *rewoundStream) rewind() {
	aeron.ImageSetSubscriberPositionForHooks(s.image, 0)
}

// followerStampTable keeps one position, so a stamp does not allocate.
type followerStampTable struct {
	stubPositionTable
	lastFollowerPosition int64
}

func (p *followerStampTable) UpdateMemberPosition(memberID int32, _, logPosition int64) {
	if memberID == dutyCycleFollowerMemberID {
		p.lastFollowerPosition = logPosition
	}
}

// wireRecordingAppendSource returns the recording-position counter buffer. A
// write at offset 0 stands in for the archive reaching that log position.
func wireRecordingAppendSource(t *testing.T, a *Agent) atomicbuffer.AtomicBuffer {
	t.Helper()
	const recordingID int64 = 11
	counters := countersWithRecordingPosition(t, recordingID, 0, true)

	// Set the id first. SetLogRecordingID ignores a change while a recording runs.
	a.SetLogRecordingID(recordingID)
	a.SetLogRecordingControl(&fakeLogRecordingControl{active: true})
	a.SetAppendSource(NewRecordingPosAppendPosition(a, counters, cnc.NullValue))
	return counters.CounterValuesBuffer()
}

func TestAgent_DoWork_ZeroAllocs(t *testing.T) {
	t.Run("service ack wait", testServiceAckWaitZeroAllocs)
	t.Run("leader", testLeaderZeroAllocs)
	t.Run("follower", testFollowerZeroAllocs)
}

func testServiceAckWaitZeroAllocs(t *testing.T) {
	a, _, _ := agentWithServiceControl(t, 2)
	failOnElection(t, a)
	control := newRewoundStream(t)
	a.SetServiceIngress(NewSubscriptionConsensusIngress(control.subscription))
	acks := [...][]byte{
		control.addFrame(t, codecs.ServiceAckEncodedLen()),
		control.addFrame(t, codecs.ServiceAckEncodedLen()),
	}

	var (
		ackID int64
		nowNs int64
	)
	cycle := func() bool {
		position := (ackID + 1) * 64
		ready := a.RequestServiceAck(position) == nil
		for i, ack := range acks {
			_, ok := codecs.EncodeServiceAck(
				ack,
				0,
				clusterwire.ServiceAckSbeSchemaVersion,
				codecs.ServiceAckFields{
					LogPosition: position,
					AckID:       ackID,
					RelevantID:  ackID,
					ServiceID:   int32(i),
				},
			)
			ready = ready && ok
		}
		control.rewind()
		nowNs += timerWheelTickResolutionNs
		a.DoWork(nowNs)
		ackID++
		return ready &&
			a.PendingServiceAckWaits() == 0 &&
			a.ExpectedServiceAckID() == ackID
	}

	measureDutyCycle(t, dutyCycleAckRuns, cycle)

	require.Equal(t, int64(dutyCycleCount(dutyCycleAckRuns)), a.ExpectedServiceAckID())
	requireSettledAgent(t, a)
}

// testLeaderZeroAllocs has each cycle fire the timer the previous cycle scheduled.
func testLeaderZeroAllocs(t *testing.T) {
	const (
		leadershipTermID int64 = 3
		clusterSessionID int64 = 1
		appPayloadLength       = 8
	)
	logPublisher, _ := boundLogPublisher(t)

	// The preset capacity keeps the double's append out of the measurement.
	egress := &recordingEgress{
		commits: make([]commitPublishCall, 0, dutyCycleCount(dutyCycleLeaderRuns)),
	}
	positions := &followerStampTable{}
	a := NewAgent(AgentConfig{
		Role:             RoleLeader,
		Commit:           new(atomic.Int64),
		LeadershipTermID: leadershipTermID,
		ThisMemberID:     0,
		ServiceCount:     1,
		LogPublisher:     logPublisher,
		Positions:        positions,
		Egress:           egress,
	})
	failOnElection(t, a)
	a.SetClusterControlToggle(newFakeToggle(int64(ControlToggleNeutral)))
	counterValues := wireRecordingAppendSource(t, a)

	wireImmediateEgress(t, a)
	a.acceptSessionConnect(validConnectFields(1), 0)
	require.Equal(t, 2, a.sessionWork(0))
	session, ok := a.sessions.sessionByID(clusterSessionID)
	require.True(t, ok)
	require.Equal(t, sessionOpen, session.state)

	clientStream := newRewoundStream(t)
	a.SetClientIngress(NewSubscriptionClientIngress(clientStream.subscription))
	sessionMessage := encodeSessionMessageWire(
		t,
		leadershipTermID,
		clusterSessionID,
		0,
		make([]byte, appPayloadLength),
	)
	copy(clientStream.addFrame(t, len(sessionMessage)), sessionMessage)

	consensusStream := newRewoundStream(t)
	a.SetIngress(NewSubscriptionConsensusIngress(consensusStream.subscription))
	appendPositionLength, ok := codecs.ClusterWireMessageLen(clusterwire.AppendPositionSbeBlockLength)
	require.True(t, ok)
	appendPosition := consensusStream.addFrame(t, appendPositionLength)

	controlStream := newRewoundStream(t)
	a.SetServiceIngress(NewSubscriptionConsensusIngress(controlStream.subscription))
	scheduleTimer := controlStream.addFrame(t, codecs.ScheduleTimerEncodedLen())

	minimumGrowth := int64(
		logbuffer.AlignFragmentChunkLength(len(sessionMessage)) +
			logbuffer.AlignFragmentChunkLength(codecs.TimerEventEncodedLen()),
	)

	var (
		correlationID int64
		nowNs         int64
	)
	cycle := func() bool {
		startPosition := logPublisher.Position()
		counterValues.PutInt64Release(0, startPosition)
		nowNs += timerWheelTickResolutionNs
		correlationID++

		_, appendEncoded := codecs.EncodeAppendPosition(
			appendPosition,
			0,
			clusterwire.AppendPositionSbeSchemaVersion,
			codecs.AppendPositionFields{
				LeadershipTermID: leadershipTermID,
				LogPosition:      startPosition,
				FollowerMemberID: dutyCycleFollowerMemberID,
			},
		)
		_, timerEncoded := codecs.EncodeScheduleTimer(
			scheduleTimer,
			0,
			clusterwire.ScheduleTimerSbeSchemaVersion,
			codecs.ScheduleTimerFields{CorrelationID: correlationID, Deadline: nowNs},
		)
		clientStream.rewind()
		consensusStream.rewind()
		controlStream.rewind()

		a.DoWork(nowNs)

		return appendEncoded && timerEncoded &&
			positions.lastFollowerPosition == startPosition &&
			logPublisher.Position()-startPosition >= minimumGrowth &&
			a.CommitPosition() == startPosition &&
			a.ScheduledTimerCount() <= 1 &&
			len(a.uncommittedTimers) <= 1
	}

	measureDutyCycle(t, dutyCycleLeaderRuns, cycle)

	require.Greater(t, logPublisher.Position(), int64(2*logbuffer.TermMinLength))
	require.Equal(t, sessionOpen, session.state)
	require.Len(t, egress.commits, dutyCycleCount(dutyCycleLeaderRuns))
	requireSettledAgent(t, a)
}

func testFollowerZeroAllocs(t *testing.T) {
	const (
		leadershipTermID int64 = 1
		leaderMemberID   int32 = 2
	)
	cycleCount := dutyCycleCount(dutyCycleLogRuns)
	adapter, logImage := boundLogAdapter(t)
	frameEnds := make([]int64, cycleCount)
	var written int64
	for i := range frameEnds {
		if i%2 == 0 {
			written = writeSessionMessageFrame(t, logImage, int(written), 7)
		} else {
			written = writeTimerEventFrame(t, logImage, int(written), int64(i/2+1))
		}

		// A log image never rewinds, so every cycle must fit one term.
		require.LessOrEqual(t, written, int64(logbuffer.TermMinLength))
		frameEnds[i] = written
	}

	egress := &recordingEgress{appends: make([]appendPublishCall, 0, cycleCount)}
	config := followerLogPollConfig(0, adapter)
	config.Egress = egress
	a := NewAgent(config)
	failOnElection(t, a)
	counterValues := wireRecordingAppendSource(t, a)

	consensusStream := newRewoundStream(t)
	a.SetIngress(NewSubscriptionConsensusIngress(consensusStream.subscription))
	commitPositionLength, ok := codecs.ClusterWireMessageLen(clusterwire.CommitPositionSbeBlockLength)
	require.True(t, ok)
	commitPosition := consensusStream.addFrame(t, commitPositionLength)

	controlStream := newRewoundStream(t)
	a.SetServiceIngress(NewSubscriptionConsensusIngress(controlStream.subscription))
	scheduleTimer := controlStream.addFrame(t, codecs.ScheduleTimerEncodedLen())

	var (
		cursor int
		nowNs  int64
	)
	cycle := func() bool {
		frameEnd := frameEnds[cursor]
		nowNs += timerWheelTickResolutionNs
		_, commitEncoded := codecs.EncodeCommitPosition(
			commitPosition,
			0,
			clusterwire.CommitPositionSbeSchemaVersion,
			codecs.CommitPositionFields{
				LeadershipTermID: leadershipTermID,
				LogPosition:      frameEnd,
				LeaderMemberID:   leaderMemberID,
			},
		)

		// Service control runs before the log poll, so the local timer exists when
		// its TimerEvent arrives. The TimerEvent cancels the timer.
		_, timerEncoded := codecs.EncodeScheduleTimer(
			scheduleTimer,
			0,
			clusterwire.ScheduleTimerSbeSchemaVersion,
			codecs.ScheduleTimerFields{CorrelationID: int64(cursor/2 + 1), Deadline: nowNs},
		)
		counterValues.PutInt64Release(0, frameEnd)
		consensusStream.rewind()
		controlStream.rewind()

		a.DoWork(nowNs)

		cursor++
		return commitEncoded && timerEncoded &&
			adapter.Position() == frameEnd &&
			a.CommitPosition() == frameEnd &&
			a.NotifiedCommitPosition() == frameEnd &&
			a.ScheduledTimerCount() <= 1 &&
			a.ExpiredTimerCount() == 0
	}

	measureDutyCycle(t, dutyCycleLogRuns, cycle)

	require.Len(t, egress.appends, cycleCount)
	requireSettledAgent(t, a)
}

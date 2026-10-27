//go:build testhooks

package consensus

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func encodeScheduleTimerWire(t testing.TB, fields codecs.ScheduleTimerFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.ScheduleTimerSbeSchemaVersion)
	buf := make([]byte, codecs.ScheduleTimerEncodedLen())
	end, ok := codecs.EncodeScheduleTimer(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func encodeCancelTimerWire(t testing.TB, fields codecs.CancelTimerFields) []byte {
	t.Helper()
	actingVersion := uint16(clusterwire.CancelTimerSbeSchemaVersion)
	buf := make([]byte, codecs.CancelTimerEncodedLen())
	end, ok := codecs.EncodeCancelTimer(buf, 0, actingVersion, fields)
	require.True(t, ok)
	return buf[:end]
}

func enqueueScheduleTimer(
	t testing.TB,
	q *QueueConsensusIngress,
	correlationID, deadline int64,
) {
	t.Helper()
	q.Enqueue(encodeScheduleTimerWire(t, codecs.ScheduleTimerFields{
		CorrelationID: correlationID,
		Deadline:      deadline,
	}))
}

func enqueueCancelTimer(t testing.TB, q *QueueConsensusIngress, correlationID int64) {
	t.Helper()
	q.Enqueue(encodeCancelTimerWire(t, codecs.CancelTimerFields{
		CorrelationID: correlationID,
	}))
}

func scheduledDeadline(t fragmentRequireT, svc *timerService, correlationID int64) int64 {
	t.Helper()
	timerID, ok := svc.timerIDByCorrelationID[correlationID]
	require.True(t, ok, "id %d missing from maps", correlationID)
	deadline, ok := svc.wheel.Deadline(timerID)
	require.True(t, ok, "id %d timer %d missing from wheel", correlationID, timerID)
	return deadline
}

func requireScheduled(
	t testing.TB,
	a *Agent,
	q *QueueConsensusIngress,
	correlationID, deadline int64,
	wantCount int,
) {
	t.Helper()
	enqueueScheduleTimer(t, q, correlationID, deadline)
	require.Equal(t, 1, a.pollServiceControl())
	requireTimerCount(t, a.timers, wantCount)
	require.Equal(t, deadline, scheduledDeadline(t, a.timers, correlationID))
}

func requireTimerEventOnLog(
	t *testing.T,
	pub *aeron.ExclusivePublication,
	leadershipTermID, correlationID, timestamp int64,
) {
	t.Helper()
	got, ok := codecs.TryDecodeTimerEventAt(termStartWire(t, pub), 0)
	require.True(t, ok)
	require.Equal(
		t,
		codecs.TimerEventFields{
			LeadershipTermID: leadershipTermID,
			CorrelationID:    correlationID,
			Timestamp:        timestamp,
		},
		got,
	)
}

func leaderWithTimerIngress(
	t testing.TB,
	cfg AgentConfig,
) (*Agent, *QueueConsensusIngress, *aeron.ExclusivePublication) {
	t.Helper()
	p, pub := boundLogPublisher(t)
	cfg.Role = RoleLeader
	cfg.LogPublisher = p
	a, q := agentWithServiceIngressConfig(cfg)
	return a, q, pub
}

func TestHandleScheduleTimer_queueIngress(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(0)
	const (
		correlationID int64 = 11
		deadline      int64 = 42
	)
	requireScheduled(t, a, q, correlationID, deadline, 1)
	require.NoError(t, a.TimerFailure())
}

func TestHandleScheduleTimer_rescheduleAndCancel(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(0)
	const correlationID int64 = 7
	oldDeadline := timerWheelTickResolutionNs
	newDeadline := 8 * timerWheelTickResolutionNs

	requireScheduled(t, a, q, correlationID, oldDeadline, 1)
	requireScheduled(t, a, q, correlationID, newDeadline, 1)

	enqueueCancelTimer(t, q, correlationID)
	require.Equal(t, 1, a.pollServiceControl())
	requireTimerCount(t, a.timers, 0)
	require.NoError(t, a.TimerFailure())
}

func TestHandleCancelTimer_missSilent(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(0)
	enqueueCancelTimer(t, q, 99)
	require.NotPanics(t, func() {
		require.Equal(t, 1, a.pollServiceControl())
	})
	requireTimerCount(t, a.timers, 0)
	require.NoError(t, a.TimerFailure())
}

func TestHandleServiceControlFragment_undecodableTimer(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(0)

	// Short buffer. Template peek fails.
	q.Enqueue([]byte{0x01, 0x02})

	// Wrong schema id. Template peek rejects.
	q.Enqueue(encodeLogAdapterMessageHeader(
		t,
		16,
		clusterwire.ScheduleTimerSbeTemplateID,
		0,
		0,
	))

	// Valid cluster schema and timer templates. Truncated body makes
	// TryDecode*At return false after the template match.
	q.Enqueue(encodeLogAdapterMessageHeader(
		t,
		clusterwire.ScheduleTimerSbeBlockLength,
		clusterwire.ScheduleTimerSbeTemplateID,
		clusterwire.MessageHeaderSbeSchemaID,
		clusterwire.ScheduleTimerSbeSchemaVersion,
	))
	q.Enqueue(encodeLogAdapterMessageHeader(
		t,
		clusterwire.CancelTimerSbeBlockLength,
		clusterwire.CancelTimerSbeTemplateID,
		clusterwire.MessageHeaderSbeSchemaID,
		clusterwire.CancelTimerSbeSchemaVersion,
	))

	require.NotPanics(t, func() {
		require.Equal(t, 4, a.pollServiceControl())
	})
	requireTimerCount(t, a.timers, 0)
	require.NoError(t, a.TimerFailure())
}

func TestTimerWork_leaderExpiry(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 10
		correlationID    int64 = 20
		nowNs            int64 = 30
	)
	a, q, pub := leaderWithTimerIngress(t, AgentConfig{
		LeadershipTermID: leadershipTermID,
	})
	requireScheduled(t, a, q, correlationID, nowNs, 1)

	require.Equal(t, 1, a.timerWork(nowNs))
	requireTimerCount(t, a.timers, 0)
	requireTimerEventOnLog(t, pub, leadershipTermID, correlationID, nowNs)
}

func TestTimerWork_prepareRestoresUncommittedTimer(t *testing.T) {
	t.Parallel()

	const (
		correlationID int64 = 20
		nowNs         int64 = 30
	)
	tests := []struct {
		name          string
		commitFired   bool
		wantScheduled int64
	}{
		{name: "below", wantScheduled: 1},
		{name: "committed", commitFired: true, wantScheduled: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, q, pub := leaderWithTimerIngress(t, AgentConfig{
				LeadershipTermID: 10,
				Commit:           new(atomic.Int64),
			})
			requireScheduled(t, a, q, correlationID, nowNs, 1)
			require.Equal(t, 1, a.timerWork(nowNs))
			requireTimerCount(t, a.timers, 0)
			firedEnd := pub.Position()

			prepareAt := int64(0)
			if tt.commitFired {
				require.Equal(t, 1, a.UpdateLeaderPosition(nowNs, firedEnd))
				prepareAt = firedEnd
			}
			_, prepared, err := a.PrepareForNewLeadership(prepareAt, nowNs)
			require.NoError(t, err)
			require.True(t, prepared)

			require.Equal(t, tt.wantScheduled, a.ScheduledTimerCount())
			require.Zero(t, a.ExpiredTimerCount())
			require.Empty(t, a.uncommittedTimers)
		})
	}
}

func TestTimerWork_followerAndElectionLive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  AgentConfig
	}{
		{name: "follower", cfg: AgentConfig{Role: RoleFollower}},
		{
			name: "electionLive",
			cfg:  AgentConfig{Role: RoleLeader, Election: &orderRunner{n: 0}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, _ := boundLogPublisher(t)
			tt.cfg.LogPublisher = p
			a, q := agentWithServiceIngressConfig(tt.cfg)
			enqueueScheduleTimer(t, q, 1, 0)
			require.Equal(t, 1, a.DoWork(0))
			requireTimerCount(t, a.timers, 1)
			require.Equal(t, 0, a.timerWork(0))
			requireTimerCount(t, a.timers, 1)
		})
	}
}

func TestTimerWork_appendFailureThenBind(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 4
		correlationID    int64 = 5
		nowNs            int64 = 0
	)
	a, q := agentWithServiceIngressConfig(AgentConfig{
		Role:             RoleLeader,
		LeadershipTermID: leadershipTermID,
	})
	requireScheduled(t, a, q, correlationID, nowNs, 1)

	// Unbound publisher: timerWork returns 0. Timer stays.
	require.Equal(t, 0, a.timerWork(nowNs))
	requireTimerCount(t, a.timers, 1)

	// Unconnected publication: append fails. Timer stays.
	a.SetLogPublisher(boundLogPublisherWithLimit(t, 0, false))
	require.Equal(t, 0, a.timerWork(nowNs))
	requireTimerCount(t, a.timers, 1)

	// Connected bind: next poll appends and consumes.
	p, pub := boundLogPublisher(t)
	a.SetLogPublisher(p)
	require.Equal(t, 1, a.DoWork(nowNs))
	requireTimerCount(t, a.timers, 0)
	requireTimerEventOnLog(t, pub, leadershipTermID, correlationID, nowNs)
}

func TestHandleScheduleTimer_capacityRefusal(t *testing.T) {
	t.Parallel()

	a, q := agentWithServiceIngress(0)
	a.timers = newSmallTimerService(testWheelCapacity)

	// Fill the current spoke (alloc 2). The next same-spoke schedule refuses
	// growth and marks once.
	enqueueScheduleTimer(t, q, 1, 0)
	enqueueScheduleTimer(t, q, 2, 0)
	require.Equal(t, 2, a.pollServiceControl())
	requireTimerCount(t, a.timers, 2)
	require.NoError(t, a.TimerFailure())

	enqueueScheduleTimer(t, q, 3, 0)
	require.Equal(t, 1, a.pollServiceControl())
	first := a.TimerFailure()
	require.ErrorIs(t, first, errTimerCapacity)
	requireTimerCount(t, a.timers, 2)
	require.NotContains(t, a.timers.timerIDByCorrelationID, int64(3))

	// Later schedule still dispatches. Mark stays the first cause.
	requireScheduled(t, a, q, 4, testWheelResolution*2, 3)
	require.Same(t, first, a.TimerFailure())

	enqueueScheduleTimer(t, q, 5, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Same(t, first, a.TimerFailure())
	requireTimerCount(t, a.timers, 3)
}

func TestElectionComplete_stampsWheelTime(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 6
		correlationID    int64 = 8
	)
	a, q, pub := leaderWithTimerIngress(t, AgentConfig{
		Election:         &orderRunner{n: 0},
		LeadershipTermID: leadershipTermID,
	})

	// Mid-election schedule. timerWork does not run while election is live.
	enqueueScheduleTimer(t, q, correlationID, 0)
	require.Equal(t, 1, a.DoWork(0))
	requireTimerCount(t, a.timers, 1)

	// Land on spoke 10 so the first post-complete poll does not scan spoke 0.
	completeNs := 10 * timerWheelTickResolutionNs
	a.ElectionComplete(completeNs)
	require.Nil(t, a.election)
	require.Equal(t, 0, a.timerWork(completeNs))
	requireTimerCount(t, a.timers, 1)

	nowNs := completeNs
	expiredAt := int64(0)
	for range timerWheelTicksPerWheel {
		nowNs += timerWheelTickResolutionNs
		if n := a.timerWork(nowNs); n != 0 {
			require.Equal(t, 1, n)
			expiredAt = nowNs
			break
		}
	}
	require.NotZero(t, expiredAt)
	requireTimerCount(t, a.timers, 0)
	requireTimerEventOnLog(t, pub, leadershipTermID, correlationID, expiredAt)
}

// timerEventTermID is the leadership term of the TimerEvent frames these tests
// write.
const timerEventTermID int64 = 1

// writeTimerEventFrame returns where the written frame ends.
func writeTimerEventFrame(
	t testing.TB,
	image *aeron.Image,
	frameOffset int,
	correlationID int64,
) int64 {
	t.Helper()
	return writeTimerEventFrameInTerm(t, image, frameOffset, correlationID, timerEventTermID)
}

// writeTimerEventFrameInTerm writes a TimerEvent of leadershipTermID and
// returns where the written frame ends.
func writeTimerEventFrameInTerm(
	t testing.TB,
	image *aeron.Image,
	frameOffset int,
	correlationID, leadershipTermID int64,
) int64 {
	t.Helper()
	payload := encodeLogAdapterTimerEventPayload(t, codecs.TimerEventFields{
		LeadershipTermID: leadershipTermID,
		CorrelationID:    correlationID,
		Timestamp:        int64(frameOffset),
	})
	writeLogAdapterUnfragmentedFrame(t, image, frameOffset, payload)
	return int64(frameOffset + logbuffer.AlignFragmentChunkLength(len(payload)))
}

func scheduleTimer(a *Agent, correlationID, deadline int64) {
	a.handleScheduleTimer(codecs.ScheduleTimerFields{
		CorrelationID: correlationID,
		Deadline:      deadline,
	})
}

func requireAgentTimers(
	t fragmentRequireT,
	a *Agent,
	held map[int64]int64,
	counts map[int64]int64,
) {
	t.Helper()
	requireTimerCount(t, a.timers, len(held))
	for correlationID, deadline := range held {
		require.Equal(t, deadline, scheduledDeadline(t, a.timers, correlationID),
			"id %d deadline", correlationID)
	}
	require.Equal(t, len(counts), a.ExpiredTimerCount())
	for correlationID, count := range counts {
		require.Positive(t, count, "want id %d holds a zero count", correlationID)
		require.Equal(t, count, a.expiredTimerCountByCorrelationID[correlationID],
			"id %d count", correlationID)
	}
}

type timerOrderKind byte

const (
	orderSchedule timerOrderKind = 'S'
	orderCancel   timerOrderKind = 'C'
	orderEvent    timerOrderKind = 'F'
)

type timerOrderStep struct {
	kind          timerOrderKind
	correlationID int64
	deadline      int64
}

func scheduleStep(correlationID, deadline int64) timerOrderStep {
	return timerOrderStep{kind: orderSchedule, correlationID: correlationID, deadline: deadline}
}

func cancelStep(correlationID int64) timerOrderStep {
	return timerOrderStep{kind: orderCancel, correlationID: correlationID}
}

func eventStep(correlationID int64) timerOrderStep {
	return timerOrderStep{kind: orderEvent, correlationID: correlationID}
}

func consumeTimerEventAboveCommit(a *Agent, correlationID int64) {
	logPosition := a.CommitPosition() + 64
	a.handleTimerEvent(correlationID, logPosition)
	a.ProposeMaxCommit(logPosition)
}

func applyTimerOrderSteps(t fragmentRequireT, a *Agent, steps []timerOrderStep) {
	t.Helper()
	for _, step := range steps {
		switch step.kind {
		case orderSchedule:
			scheduleTimer(a, step.correlationID, step.deadline)
		case orderCancel:
			a.handleCancelTimer(codecs.CancelTimerFields{CorrelationID: step.correlationID})
		case orderEvent:
			consumeTimerEventAboveCommit(a, step.correlationID)
		default:
			require.FailNow(t, "unknown timer order step kind", "%q", step.kind)
		}
	}
}

func TestHandleTimerEvent_orders(t *testing.T) {
	t.Parallel()

	const id int64 = 7
	d1 := 2 * timerWheelTickResolutionNs
	d2 := 4 * timerWheelTickResolutionNs
	d3 := 6 * timerWheelTickResolutionNs

	const uniqueIDs = 1000
	noService := make([]timerOrderStep, 0, uniqueIDs)
	noServiceCounts := make(map[int64]int64, uniqueIDs)
	for i := range int64(uniqueIDs) {
		noService = append(noService, eventStep(100+i))
		noServiceCounts[100+i] = 1
	}

	tests := []struct {
		name   string
		steps  []timerOrderStep
		held   map[int64]int64
		counts map[int64]int64
	}{
		{
			name:  "in class: schedule then event",
			steps: []timerOrderStep{scheduleStep(id, d1), eventStep(id)},
		},
		{
			name:  "in class: event then schedule",
			steps: []timerOrderStep{eventStep(id), scheduleStep(id, d1)},
		},
		{
			name: "in class: two events then three schedules",
			steps: []timerOrderStep{
				eventStep(id),
				eventStep(id),
				scheduleStep(id, d1),
				scheduleStep(id, d2),
				scheduleStep(id, d3),
			},
			held: map[int64]int64{id: d3},
		},
		{
			name: "in class: two events then two schedules",
			steps: []timerOrderStep{
				eventStep(id),
				eventStep(id),
				scheduleStep(id, d1),
				scheduleStep(id, d2),
			},
		},
		{
			name: "in class: periodic reschedule after a late schedule",
			steps: []timerOrderStep{
				eventStep(id),
				scheduleStep(id, d1),
				scheduleStep(id, d2),
			},
			held: map[int64]int64{id: d2},
		},
		{
			name: "in class: schedule, event, cancel with the event first",
			steps: []timerOrderStep{
				eventStep(id),
				scheduleStep(id, d1),
				cancelStep(id),
			},
		},
		{
			name:  "in class: reschedule",
			steps: []timerOrderStep{scheduleStep(id, d1), scheduleStep(id, d2)},
			held:  map[int64]int64{id: d2},
		},
		{
			name: "race: S S F with the event first keeps a stale timer",
			steps: []timerOrderStep{
				eventStep(id),
				scheduleStep(id, d1),
				scheduleStep(id, d2),
			},
			held: map[int64]int64{id: d2},
		},
		{
			name: "race: S S F with the event between keeps a stale timer",
			steps: []timerOrderStep{
				scheduleStep(id, d1),
				eventStep(id),
				scheduleStep(id, d2),
			},
			held: map[int64]int64{id: d2},
		},
		{
			name: "race: S C S F with the event before the cancel keeps a stale timer",
			steps: []timerOrderStep{
				scheduleStep(id, d1),
				eventStep(id),
				cancelStep(id),
				scheduleStep(id, d2),
			},
			held: map[int64]int64{id: d2},
		},
		{
			name: "race: S C F with a fast service keeps count 1",
			steps: []timerOrderStep{
				scheduleStep(id, d1),
				cancelStep(id),
				eventStep(id),
			},
			counts: map[int64]int64{id: 1},
		},
		{
			name: "race: S S F F with a fast service keeps count 1",
			steps: []timerOrderStep{
				scheduleStep(id, d1),
				scheduleStep(id, d2),
				eventStep(id),
				eventStep(id),
			},
			counts: map[int64]int64{id: 1},
		},
		{
			name: "race: S C F S with a fast service holds no timer",
			steps: []timerOrderStep{
				scheduleStep(id, d1),
				cancelStep(id),
				eventStep(id),
				scheduleStep(id, d2),
			},
		},
		{
			name:   "no service: unique ids count every event",
			steps:  noService,
			counts: noServiceCounts,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := newFollowerLogPollAgent(0, nil)
			applyTimerOrderSteps(t, a, tt.steps)
			requireAgentTimers(t, a, tt.held, tt.counts)
		})
	}
}

func TestTimerEventConsume_consumePaths(t *testing.T) {
	t.Parallel()

	const (
		correlationID int64 = 31
		otherID       int64 = 32
		nowNs         int64 = 5
	)
	d1 := 2 * timerWheelTickResolutionNs
	d2 := 4 * timerWheelTickResolutionNs

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "catchup poll consumes before timer work",
			run: func(t *testing.T) {
				adapter, image := boundLogAdapter(t)
				end := writeTimerEventFrame(t, image, 0, correlationID)
				a := NewAgent(AgentConfig{
					Role:       RoleFollower,
					Commit:     new(atomic.Int64),
					LogAdapter: adapter,
				})
				scheduleTimer(a, correlationID, d1)

				require.Positive(t, catchupPoll(t, a, end+32, nowNs))
				require.Equal(t, end, a.CommitPosition())
				requireAgentTimers(t, a, nil, nil)
			},
		},
		{
			name: "bound re-join skips the tail and consumes above commit",
			run: func(t *testing.T) {
				const sessionID int32 = 10
				appendV := new(atomic.Int64)
				a, image, adapter := agentWithLogJoin(t, sessionID, 0, AgentConfig{
					Role:                      RoleFollower,
					Commit:                    new(atomic.Int64),
					Egress:                    &recordingEgress{},
					AppendSource:              &AtomicAppendPosition{V: appendV},
					LeadershipTermID:          1,
					LeaderHeartbeatIntervalNs: 1_000_000_000,
					LeaderMemberID:            2,
				})
				adapter.SetImage(image)
				tailEnd := writeTimerEventFrame(t, image, 0, correlationID)
				otherEnd := writeTimerEventFrame(t, image, int(tailEnd), otherID)
				appendV.Store(otherEnd + 100)
				a.SetElectionFactory((&recordingElectionFactory{}).factory())
				scheduleTimer(a, correlationID, d1)
				scheduleTimer(a, otherID, d1)

				// enterElection reads no log.
				a.notifiedCommitPosition = 0
				require.Equal(t, 1, a.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false))
				require.Equal(t, int64(0), adapter.Position())

				_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
				require.NoError(t, err)
				require.True(t, prepared)

				// Replay raises the commit past the first event. The service
				// reschedules.
				a.ProposeMaxCommit(tailEnd)
				scheduleTimer(a, correlationID, d2)

				joined, err := a.JoinLogAsFollower(FollowerLogJoinParams{
					LogSessionID:  sessionID,
					IsBoundRejoin: true,
				}, nowNs)
				require.NoError(t, err)
				require.True(t, joined)
				a.ElectionComplete(nowNs)

				a.notifiedCommitPosition = otherEnd
				require.Positive(t, a.followerConsensusWork(nowNs))
				require.Equal(t, otherEnd, adapter.Position())
				require.Equal(t, otherEnd, a.CommitPosition())
				requireAgentTimers(t, a, map[int64]int64{correlationID: d2}, nil)
			},
		},
		{
			name: "direct call at commit changes nothing and above commit cancels",
			run: func(t *testing.T) {
				const commitPosition int64 = 64
				a := newFollowerLogPollAgent(0, nil)
				a.ProposeMaxCommit(commitPosition)
				scheduleTimer(a, correlationID, d1)

				a.handleTimerEvent(correlationID, commitPosition)
				a.handleTimerEvent(otherID, commitPosition)
				requireAgentTimers(t, a, map[int64]int64{correlationID: d1}, nil)

				a.handleTimerEvent(correlationID, commitPosition+1)
				requireAgentTimers(t, a, nil, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestTimerWork_promotedFollowerSkipsConsumedTimer(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 3
		consumedID       int64 = 1
		liveID           int64 = 2
		lateID           int64 = 3
	)
	consumedDeadline := 2 * timerWheelTickResolutionNs
	lateDeadline := 3 * timerWheelTickResolutionNs
	liveDeadline := 5 * timerWheelTickResolutionNs

	a, q, pub := leaderWithTimerIngress(t, AgentConfig{
		LeadershipTermID: leadershipTermID,
		Commit:           new(atomic.Int64),
	})
	a.RoleSet(RoleFollower)

	// As follower: schedule the consumed and live timers. The late timer's
	// event arrives before its schedule.
	requireScheduled(t, a, q, consumedID, consumedDeadline, 1)
	requireScheduled(t, a, q, liveID, liveDeadline, 2)
	a.handleTimerEvent(consumedID, 64)
	a.handleTimerEvent(lateID, 128)
	requireAgentTimers(t, a, map[int64]int64{liveID: liveDeadline}, map[int64]int64{lateID: 1})

	// Election: prepare, promote, then complete before every deadline.
	_, prepared, err := a.PrepareForNewLeadership(0, 0)
	require.NoError(t, err)
	require.True(t, prepared)
	a.RoleSet(RoleLeader)
	a.ElectionComplete(0)

	// As leader: the late schedule is ignored.
	enqueueScheduleTimer(t, q, lateID, lateDeadline)
	require.Equal(t, 1, a.pollServiceControl())
	requireAgentTimers(t, a, map[int64]int64{liveID: liveDeadline}, nil)

	expired := 0
	firedAt := int64(0)
	for tick := int64(1); tick <= 8; tick++ {
		nowNs := tick * timerWheelTickResolutionNs
		if n := a.timerWork(nowNs); n != 0 {
			expired += n
			firedAt = nowNs
		}
	}
	require.Equal(t, 1, expired)
	requireTimerCount(t, a.timers, 0)
	requireTimerEventOnLog(t, pub, leadershipTermID, liveID, firedAt)
}

// replayTimerEvents binds a replay image that holds one TimerEvent of
// leadershipTermID per id from position 0. It replays the image to its end,
// then unbinds it.
func replayTimerEvents(
	t *testing.T,
	a *Agent,
	leadershipTermID int64,
	correlationIDs ...int64,
) {
	t.Helper()
	image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
	end := int64(0)
	for _, correlationID := range correlationIDs {
		end = writeTimerEventFrameInTerm(t, image, int(end), correlationID, leadershipTermID)
	}
	a.BindReplayLog(image)
	require.Equal(t, len(correlationIDs), a.ReplayLogPoll(end, 0))
	require.Equal(t, end, a.LogAdapter().Position())
	a.UnbindReplayLog(image)
}

func TestTimerWork_promotedFollowerFiresNoReplayedTimer(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 3
		replayedID       int64 = 1
		otherID          int64 = 2
	)
	replayedDeadline := 2 * timerWheelTickResolutionNs
	otherDeadline := 3 * timerWheelTickResolutionNs

	a, q, pub := leaderWithTimerIngress(t, AgentConfig{
		LeadershipTermID: leadershipTermID,
		Commit:           new(atomic.Int64),
		LogAdapter:       NewLogAdapter(0),
	})
	a.RoleSet(RoleFollower)
	requireScheduled(t, a, q, replayedID, replayedDeadline, 1)
	requireScheduled(t, a, q, otherID, otherDeadline, 2)

	// Election replay: the replayed TimerEvent cancels its timer.
	replayTimerEvents(t, a, leadershipTermID, replayedID)
	requireAgentTimers(t, a, map[int64]int64{otherID: otherDeadline}, nil)

	// Promote, then run timer work past both deadlines.
	require.NoError(t, a.JoinLogAsLeader(leadershipTermID+1, a.CommitPosition(), 0, false))
	a.ElectionComplete(0)
	expired := 0
	firedAt := int64(0)
	for tick := int64(1); tick <= 8; tick++ {
		nowNs := tick * timerWheelTickResolutionNs
		if n := a.timerWork(nowNs); n != 0 {
			expired += n
			firedAt = nowNs
		}
	}
	require.Equal(t, 1, expired)
	requireTimerCount(t, a.timers, 0)
	requireTimerEventOnLog(t, pub, leadershipTermID+1, otherID, firedAt)
}

func TestTimerEventConsume_formerLeaderReplay(t *testing.T) {
	t.Parallel()

	const (
		leadershipTermID int64 = 4
		correlationID    int64 = 21
	)
	deadline := 2 * timerWheelTickResolutionNs
	rescheduleDeadline := 6 * timerWheelTickResolutionNs

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "former leader replays its own TimerEvent",
			run: func(t *testing.T) {
				a, q, _ := leaderWithTimerIngress(t, AgentConfig{
					Commit:     new(atomic.Int64),
					LogAdapter: NewLogAdapter(0),
				})
				require.NoError(t, a.JoinLogAsLeader(leadershipTermID, 0, 0, false))
				requireScheduled(t, a, q, correlationID, deadline, 1)
				require.Equal(t, 1, a.timerWork(4*timerWheelTickResolutionNs))
				require.Equal(t, int64(0), a.CommitPosition())
				requireAgentTimers(t, a, nil, nil)

				// Prepare restores the fired timer. The replayed TimerEvent
				// cancels it and adds no count.
				_, prepared, err := a.PrepareForNewLeadership(0, 0)
				require.NoError(t, err)
				require.True(t, prepared)
				require.Equal(t, int64(1), a.ScheduledTimerCount())
				replayTimerEvents(t, a, leadershipTermID, correlationID)
				requireAgentTimers(t, a, nil, nil)

				// The service reschedules the id after it reads the TimerEvent.
				requireScheduled(t, a, q, correlationID, rescheduleDeadline, 1)
			},
		},
		{
			name: "restart counts the id",
			run: func(t *testing.T) {
				a, q, _ := leaderWithTimerIngress(t, AgentConfig{
					Commit:     new(atomic.Int64),
					LogAdapter: NewLogAdapter(0),
				})
				a.RoleSet(RoleFollower)
				replayTimerEvents(t, a, leadershipTermID, correlationID)
				requireAgentTimers(t, a, nil, map[int64]int64{correlationID: 1})

				// The replayed schedule spends the count. A later reschedule
				// schedules a timer.
				enqueueScheduleTimer(t, q, correlationID, deadline)
				require.Equal(t, 1, a.pollServiceControl())
				requireAgentTimers(t, a, nil, nil)
				requireScheduled(t, a, q, correlationID, rescheduleDeadline, 1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
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

func scheduledDeadline(t testing.TB, svc *timerService, correlationID int64) int64 {
	t.Helper()
	timerID, ok := svc.timerIDByCorrelationID[correlationID]
	require.True(t, ok)
	deadline, ok := svc.wheel.Deadline(timerID)
	require.True(t, ok)
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

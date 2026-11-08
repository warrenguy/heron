//go:build testhooks

package consensus

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const terminationTestTimeoutNs int64 = 1_000

type terminationCommitCall struct {
	term int64
	pos  int64
}

type terminationCommitLog struct {
	calls []terminationCommitCall
	err   error
}

func (l *terminationCommitLog) CommitLogPosition(term, pos int64) error {
	l.calls = append(l.calls, terminationCommitCall{term: term, pos: pos})
	return l.err
}

func requireServiceTerminationFrame(
	t fragmentRequireT,
	pub *aeron.Publication,
	pos int64,
	ack bool,
) {
	t.Helper()
	wantAck := clusterwire.BooleanType_FALSE
	if ack {
		wantAck = clusterwire.BooleanType_TRUE
	}

	require.Equal(t, []codecs.ServiceTerminationPositionFields{{
		LogPosition: pos,
		AckRequired: wantAck,
	}}, decodeTermFrames(t, pub, codecs.TryDecodeServiceTerminationPositionAt))
}

func clusterActionCount(t fragmentRequireT, pub *aeron.ExclusivePublication) int {
	t.Helper()
	return len(decodeTermFrames(t, pub, codecs.TryDecodeClusterActionRequestAt))
}

func mustConsensus(t testing.TB, encode func([]byte) (int, bool)) []byte {
	t.Helper()
	buf := make([]byte, 512)
	end, ok := encode(buf)
	require.True(t, ok)
	return buf[:end]
}

func terminationMarks(a *Agent) []bool {
	if a.clusterTermination == nil {
		return nil
	}
	return append([]bool(nil), a.clusterTermination.terminated...)
}

func setAdapterPosition(t testing.TB, a *Agent, pos int64) {
	t.Helper()
	adapter, image := boundLogAdapter(t)
	aeron.ImageSetSubscriberPositionForHooks(image, pos)
	a.SetLogAdapter(adapter)
}

func ackTermination(a *Agent, term, position int64, memberID int32) {
	a.handleTerminationAck(codecs.TerminationAckFields{
		LeadershipTermID: term,
		LogPosition:      position,
		MemberID:         memberID,
	})
}

// terminationFollower returns a term 6 follower with service control. The
// follower holds position from a same-term TerminationPosition.
func terminationFollower(
	t testing.TB,
	serviceCount int,
	position int64,
) (*Agent, *QueueConsensusIngress, *aeron.Publication) {
	t.Helper()
	a, q, pub := agentWithServiceControlConfig(t, AgentConfig{
		Role:             RoleFollower,
		ServiceCount:     serviceCount,
		LeadershipTermID: 6,
		Commit:           new(atomic.Int64),
	})
	a.handleTerminationPosition(codecs.TerminationPositionFields{
		LeadershipTermID: 6,
		LogPosition:      position,
	}, 0)
	require.Equal(t, position, a.TerminationPosition())
	return a, q, pub
}

func appendTermEvent(t testing.TB, a *Agent) {
	t.Helper()
	require.True(t, a.logPublisher.AppendNewLeadershipTermEvent(
		toggleTestTermID, 1, 0, 0, 1, clusterwire.ClusterTimeUnit_NANOS, 1,
	))
}

func bindTerminationEgress(a *Agent, peers []int32) *recordingEgress {
	eg := &recordingEgress{peerIDs: peers}
	a.SetEgress(eg)
	return eg
}

func requireSnapshotThenTerminating(t *testing.T, states []ModuleState) {
	t.Helper()

	for i := range len(states) - 1 {
		if states[i] != ModuleStateSnapshot {
			continue
		}

		require.Equal(t, ModuleStateTerminating, states[i+1])
		return
	}

	t.Fatal("module state counter did not store SNAPSHOT")
}

// startShutdownTake sets the SHUTDOWN toggle on a leader. It returns the stored
// termination position.
func startShutdownTake(
	t *testing.T,
	f *takeFixture,
	peers []int32,
	nowNs int64,
) (int64, *recordingEgress, *aeron.Publication, *recordingModuleStateCounter) {
	t.Helper()
	f.agent.terminationTimeoutNs = terminationTestTimeoutNs
	f.agent.SetLeadershipTermID(toggleTestTermID)
	counter := &recordingModuleStateCounter{}
	f.agent.SetModuleStateCounter(counter)
	var svcPub *aeron.Publication
	if len(f.agent.serviceAcks.queues) > 0 {
		proxy, pub := boundServiceProxy(t)
		f.agent.SetServiceProxy(proxy)
		svcPub = pub
	}

	eg := bindTerminationEgress(f.agent, peers)
	toggle := newFakeToggle(int64(ControlToggleNeutral))
	require.True(t, ControlToggleShutdown.Toggle(toggle))
	f.agent.SetClusterControlToggle(toggle)

	require.Equal(t, 1, f.agent.clusterControlToggleWork(nowNs))
	position := f.agent.TerminationPosition()
	require.Equal(t, position, f.agent.ExpectedAckPosition())
	require.Equal(t, toggleTestTermID, f.agent.terminationLeadershipTermID)
	require.Equal(t, ModuleStateSnapshot, f.agent.ModuleState())
	require.Zero(t, f.agent.clusterTermination.deadlineNs)
	f.commit.Store(position)
	return position, eg, svcPub, counter
}

func TestClusterTermination_leader(t *testing.T) {
	t.Parallel()

	t.Run("shutdown sends one position per peer", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		eg := bindTerminationEgress(l.agent, []int32{2, 4})

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.pub.Position()
		require.Equal(t, []terminationPublish{
			{memberID: 2, term: toggleTestTermID, position: position},
			{memberID: 4, term: toggleTestTermID, position: position},
		}, eg.terminationPositions)
	})

	t.Run("failed send reports the member", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		eg := bindTerminationEgress(l.agent, []int32{2, 4})
		eg.refuseTerminationFor = map[int32]bool{4: true}
		var reported []error
		l.agent.errorHandler = func(err error) { reported = append(reported, err) }

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, []terminationPublish{{
			memberID: 2,
			term:     toggleTestTermID,
			position: l.pub.Position(),
		}}, eg.terminationPositions)
		require.Len(t, reported, 1)
		require.ErrorIs(t, reported[0], errTerminationPositionSend)
		require.ErrorContains(t, reported[0], "member 4")
	})

	t.Run("take resolve stops the services and resets the deadline", func(t *testing.T) {
		t.Parallel()
		const startNs int64 = 100
		f := newTakeLeader(t, 1)
		position, _, svcPub, counter := startShutdownTake(t, f, nil, startNs)
		f.ack(t, position, 0, 77, 0)

		result, nowNs := f.tickUntilResolved(t, 5_000, 1)
		require.NoError(t, result.Err)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, serviceTerminationWaitQueued, f.agent.serviceTermination)
		require.Equal(t, []int64{position}, f.agent.serviceAckWaits)
		requireServiceTerminationFrame(t, svcPub, position, true)
		require.Equal(t, nowNs+terminationTestTimeoutNs, f.agent.clusterTermination.deadlineNs)
		require.NotEqual(t, startNs+terminationTestTimeoutNs, f.agent.clusterTermination.deadlineNs)
		requireSnapshotThenTerminating(t, counter.states)
		require.Empty(t, f.reported)
	})

	t.Run("failed take still terminates", func(t *testing.T) {
		t.Parallel()
		const startNs int64 = 100
		f := newTakeLeader(t, 1)
		position, _, svcPub, counter := startShutdownTake(t, f, nil, startNs)
		f.ack(t, position, 0, nullValue, 0)

		result, nowNs := f.tickUntilResolved(t, 5_000, 1)
		require.ErrorIs(t, result.Err, errSnapshotServiceFailed)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, serviceTerminationWaitQueued, f.agent.serviceTermination)
		requireServiceTerminationFrame(t, svcPub, position, true)
		require.Equal(t, nowNs+terminationTestTimeoutNs, f.agent.clusterTermination.deadlineNs)
		requireSnapshotThenTerminating(t, counter.states)
		require.NotEmpty(t, f.reported)
		require.ErrorIs(t, f.reported[0].err, errSnapshotServiceFailed)
	})

	t.Run("service ack and peer acks close on the next slow tick", func(t *testing.T) {
		t.Parallel()
		f := newTakeLeader(t, 1)
		position, _, _, counter := startShutdownTake(t, f, []int32{2, 4}, 100)
		f.ack(t, position, 0, 77, 0)
		_, _ = f.tickUntilResolved(t, 5_000, 1)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		reports := len(f.reported)

		log := &terminationCommitLog{}
		f.agent.SetTerminationRecordingLog(log)
		f.ack(t, position, 1, 1, 0)
		require.Equal(t, 1, f.agent.pollServiceControl())
		require.Equal(t, 1, f.agent.serviceControlWork())
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.False(t, f.agent.clusterTermination.awaitingServices)
		require.NoError(t, f.agent.TerminationError())

		ackTermination(f.agent, toggleTestTermID, position, 2)
		ackTermination(f.agent, toggleTestTermID, position, 4)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())

		require.Equal(t, 1, f.agent.slowTickWork(f.agent.clusterTermination.deadlineNs-1))
		require.Equal(t, []terminationCommitCall{{
			term: toggleTestTermID,
			pos:  position,
		}}, log.calls)
		requireTerminated(t, f.agent, ErrClusterTerminated, true)
		require.ErrorIs(t, f.agent.TerminationError(), ErrClusterTerminated)
		require.Len(t, f.reported, reports)
		require.Equal(t, ModuleStateTerminating, counter.states[len(counter.states)-2])
		require.Equal(t, ModuleStateClosed, counter.states[len(counter.states)-1])
		require.Equal(t, int32(5), ModuleStateTerminating.Code())
		require.Equal(t, int32(6), ModuleStateClosed.Code())
	})

	t.Run("peer ack past P marks and closes before the deadline", func(t *testing.T) {
		t.Parallel()
		f := newTakeLeader(t, 1)
		position, _, _, _ := startShutdownTake(t, f, []int32{2, 4}, 100)
		f.ack(t, position, 0, 77, 0)
		_, _ = f.tickUntilResolved(t, 5_000, 1)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, []bool{false, false}, terminationMarks(f.agent))

		ackTermination(f.agent, toggleTestTermID, position+40, 2)
		require.Equal(t, []bool{true, false}, terminationMarks(f.agent))
		deadline := f.agent.clusterTermination.deadlineNs
		require.Equal(t, 0, f.agent.slowTickWork(deadline-1))
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())

		log := &terminationCommitLog{}
		f.agent.SetTerminationRecordingLog(log)
		f.ack(t, position, 1, 1, 0)
		require.Equal(t, 1, f.agent.pollServiceControl())
		require.Equal(t, 1, f.agent.serviceControlWork())
		require.False(t, f.agent.clusterTermination.awaitingServices)
		ackTermination(f.agent, toggleTestTermID, position, 4)
		require.Equal(t, []bool{true, true}, terminationMarks(f.agent))
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())

		require.Equal(t, 1, f.agent.slowTickWork(deadline-1))
		require.Equal(t, []terminationCommitCall{{
			term: toggleTestTermID,
			pos:  position,
		}}, log.calls)
		requireTerminated(t, f.agent, ErrClusterTerminated, true)
	})

	t.Run("no peer ack stays open until the deadline", func(t *testing.T) {
		t.Parallel()
		f := newTakeLeader(t, 1)
		position, _, _, _ := startShutdownTake(t, f, []int32{2}, 100)
		f.ack(t, position, 0, 77, 0)
		_, _ = f.tickUntilResolved(t, 5_000, 1)
		f.ack(t, position, 1, 1, 0)
		require.Equal(t, 1, f.agent.pollServiceControl())
		require.Equal(t, 1, f.agent.serviceControlWork())

		deadline := f.agent.clusterTermination.deadlineNs
		require.Equal(t, 0, f.agent.slowTickWork(deadline-1))
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, 1, f.agent.slowTickWork(deadline))
		requireTerminated(t, f.agent, ErrClusterTerminated, true)
	})

	t.Run("peer acks without a service ack stay open", func(t *testing.T) {
		t.Parallel()
		f := newTakeLeader(t, 1)
		position, _, _, _ := startShutdownTake(t, f, []int32{2, 4}, 100)
		f.ack(t, position, 0, 77, 0)
		_, _ = f.tickUntilResolved(t, 5_000, 1)
		deadline := f.agent.clusterTermination.deadlineNs

		for _, id := range []int32{2, 4} {
			ackTermination(f.agent, toggleTestTermID, position, id)
		}

		require.Equal(t, 0, f.agent.slowTickWork(deadline))
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.True(t, f.agent.clusterTermination.awaitingServices)
		require.NoError(t, f.agent.TerminationError())
	})

	t.Run("zero services publish nothing and close on the peer acks", func(t *testing.T) {
		t.Parallel()
		f := newTakeLeader(t, 0)
		position, _, svcPub, _ := startShutdownTake(t, f, []int32{2}, 100)
		require.Nil(t, svcPub)
		_, _ = f.tickUntilResolved(t, 5_000, 1)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, serviceTerminationIdle, f.agent.serviceTermination)
		require.Equal(t, 0, f.agent.PendingServiceAckWaits())
		require.False(t, f.agent.clusterTermination.awaitingServices)
		require.Equal(t, 0, f.agent.slowTickWork(f.agent.clusterTermination.deadlineNs-1))
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())

		ackTermination(f.agent, toggleTestTermID, position, 2)
		require.Equal(t, ModuleStateTerminating, f.agent.ModuleState())
		require.Equal(t, 1, f.agent.slowTickWork(100))
		requireTerminated(t, f.agent, ErrClusterTerminated, true)
	})

	t.Run("abort sends the publisher position above commit", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		appendTermEvent(t, l.agent)
		before := clusterActionCount(t, l.pub)
		position := l.pub.Position()
		require.Greater(t, position, l.agent.CommitPosition())
		eg := bindTerminationEgress(l.agent, []int32{3, 8})

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, before, clusterActionCount(t, l.pub))
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.Equal(t, position, l.agent.TerminationPosition())
		require.Equal(t, []terminationPublish{
			{memberID: 3, term: toggleTestTermID, position: position},
			{memberID: 8, term: toggleTestTermID, position: position},
		}, eg.terminationPositions)
		requireServiceTerminationFrame(t, l.servicePub, position, true)

		require.Equal(t, 1, l.agent.UpdateLeaderPosition(toggleTestNowNs, position-1))
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.Equal(t, []commitPublishCall{{
			pos:  position - 1,
			term: toggleTestTermID,
		}}, eg.commits)
	})

	t.Run("one member closes on the slow tick after the service ack", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		log := &terminationCommitLog{}
		l.agent.SetTerminationRecordingLog(log)

		require.Equal(t, 1, l.agent.slowTickWork(toggleTestNowNs))
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.NoError(t, l.agent.TerminationError())
		position := l.agent.TerminationPosition()

		enqueueServiceAck(t, l.queue, position, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.NoError(t, l.agent.TerminationError())

		require.Equal(t, 1, l.agent.slowTickWork(toggleTestNowNs+1))
		require.Equal(t, []terminationCommitCall{{
			term: toggleTestTermID,
			pos:  position,
		}}, log.calls)
		requireTerminated(t, l.agent, ErrClusterTerminated, true)
	})

	t.Run("max timeout saturates the deadline", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 0)
		l.agent.terminationTimeoutNs = math.MaxInt64
		bindTerminationEgress(l.agent, []int32{2})

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, int64(math.MaxInt64), l.agent.clusterTermination.deadlineNs)
		require.False(t, l.agent.clusterTermination.awaitingServices)
		require.Equal(t, 0, l.agent.terminationTick(toggleTestNowNs+1))
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.NoError(t, l.agent.TerminationError())
	})

	t.Run("abort does not stop the services after a send fault ends the member", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		eg := bindTerminationEgress(l.agent, []int32{4})
		eg.refuseTerminationFor = map[int32]bool{4: true}
		var reported error
		l.agent.errorHandler = func(err error) {
			reported = err
			l.agent.TerminateOnError(err)
		}

		before := l.servicePub.Position()

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.ErrorIs(t, reported, errTerminationPositionSend)
		requireTerminated(t, l.agent, reported, false)
		require.Equal(t, before, l.servicePub.Position())
		require.Equal(t, serviceTerminationIdle, l.agent.serviceTermination)
		require.Equal(t, 0, l.agent.PendingServiceAckWaits())
	})
}

func TestClusterTermination_wait(t *testing.T) {
	t.Parallel()

	t.Run("request at P pops before the termination", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleNeutral), 1)
		appendTermEvent(t, l.agent)
		position := l.pub.Position()
		require.NoError(t, l.agent.RequestServiceAck(position))
		l.toggle.Store(int64(ControlToggleAbort))
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, []int64{position, position}, l.agent.serviceAckWaits)
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())

		enqueueServiceAck(t, l.queue, position, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.Equal(t, []int64{position}, l.agent.serviceAckWaits)
		require.True(t, l.agent.clusterTermination.awaitingServices)
		require.NoError(t, l.agent.TerminationError())

		enqueueServiceAck(t, l.queue, position, 1, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.False(t, l.agent.clusterTermination.awaitingServices)
		require.NoError(t, l.agent.ServiceAckFailure())
	})

	t.Run("request above P pops then the termination", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleNeutral), 1)
		appendTermEvent(t, l.agent)
		position := l.pub.Position()
		above := position + 64
		require.NoError(t, l.agent.RequestServiceAck(above))
		l.toggle.Store(int64(ControlToggleAbort))
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, []int64{above, position}, l.agent.serviceAckWaits)

		enqueueServiceAck(t, l.queue, above, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, []int64{position}, l.agent.serviceAckWaits)

		enqueueServiceAck(t, l.queue, position, 1, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, 0, l.agent.PendingServiceAckWaits())
		require.NoError(t, l.agent.ServiceAckFailure())
		require.False(t, l.agent.clusterTermination.awaitingServices)
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
	})

	t.Run("request in TERMINATING is refused", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		before := l.servicePub.Position()
		waits := l.agent.PendingServiceAckWaits()

		err := l.agent.RequestServiceAck(l.agent.TerminationPosition())
		require.ErrorIs(t, err, errTerminationInProgress)
		require.Equal(t, before, l.servicePub.Position())
		require.Equal(t, waits, l.agent.PendingServiceAckWaits())
	})

	t.Run("request after the member ends is refused", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		require.Equal(t, 1, l.agent.slowTickWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		enqueueServiceAck(t, l.queue, position, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Equal(t, 1, l.agent.slowTickWork(toggleTestNowNs+1))
		requireTerminated(t, l.agent, ErrClusterTerminated, true)
		before := l.servicePub.Position()

		err := l.agent.RequestServiceAck(position + 64)
		require.ErrorIs(t, err, errTerminationInProgress)
		require.Equal(t, before, l.servicePub.Position())
		require.Equal(t, 0, l.agent.PendingServiceAckWaits())
	})

	t.Run("pending join publishes before the service stop", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		pending := copyJoinLogFields(sampleJoinLogFields())
		pending.LogPosition = 40
		l.agent.pendingJoinLog = pending

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, serviceTerminationPublishPending, l.agent.serviceTermination)
		require.Equal(t, 0, l.agent.PendingServiceAckWaits())
		require.NotNil(t, l.agent.pendingJoinLog)

		l.agent.DoWork(toggleTestNowNs + 1)
		require.Nil(t, l.agent.pendingJoinLog)
		require.Equal(t, serviceTerminationWaitQueued, l.agent.serviceTermination)
		require.Equal(t, []int64{40, l.agent.TerminationPosition()}, l.agent.serviceAckWaits)
	})

	t.Run("not connected service stop retries", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		l.agent.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.Equal(t, serviceTerminationPublishPending, l.agent.serviceTermination)
		require.Equal(t, 0, l.agent.PendingServiceAckWaits())

		l.agent.DoWork(toggleTestNowNs + 1)
		require.Equal(t, serviceTerminationPublishPending, l.agent.serviceTermination)

		proxy, pub := boundServiceProxy(t)
		l.agent.SetServiceProxy(proxy)
		l.agent.DoWork(toggleTestNowNs + 2)
		require.Equal(t, serviceTerminationWaitQueued, l.agent.serviceTermination)
		requireServiceTerminationFrame(t, pub, l.agent.TerminationPosition(), true)
	})

	t.Run("closed service stop ends the member", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleAbort), 1)
		proxy, pub := boundServiceProxy(t)
		require.NoError(t, pub.Close())
		l.agent.SetServiceProxy(proxy)
		var reported []error
		l.agent.errorHandler = func(err error) { reported = append(reported, err) }

		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		require.ErrorIs(t, l.agent.ServicePublishFailure(), ErrServicePublish)
		requireTerminated(t, l.agent, l.agent.ServicePublishFailure(), false)
		require.Len(t, reported, 1)
		require.ErrorIs(t, reported[0], ErrServicePublish)
	})

	t.Run("ack below P marks out of sequence", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleNeutral), 1)
		appendTermEvent(t, l.agent)
		l.toggle.Store(int64(ControlToggleAbort))
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		below := position - 1
		enqueueServiceAck(t, l.queue, below, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.NoError(t, l.agent.ServiceAckFailure())
		require.Equal(t, 0, l.agent.serviceControlWork())

		failure := l.agent.ServiceAckFailure()
		require.ErrorIs(t, failure, ErrServiceAckOutOfSequence)
		require.ErrorContains(t, failure, "termination ack at")
		require.ErrorContains(t, failure, "below termination position")
	})

	t.Run("divergent overshoot marks out of sequence", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleNeutral), 2)
		appendTermEvent(t, l.agent)
		l.toggle.Store(int64(ControlToggleAbort))
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		enqueueServiceAck(t, l.queue, position+1, 0, 0)
		enqueueServiceAck(t, l.queue, position+2, 0, 1)
		require.Equal(t, 2, l.agent.pollServiceControl())
		require.ErrorIs(t, l.agent.ServiceAckFailure(), ErrServiceAckOutOfSequence)
	})
}

func TestClusterTermination_ackDrops(t *testing.T) {
	t.Parallel()

	l := newToggleLeader(t, int64(ControlToggleAbort), 0)
	eg := bindTerminationEgress(l.agent, []int32{2, 4})
	require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
	position := l.agent.TerminationPosition()
	require.True(t, l.agent.clusterTermination.markTerminated(2))
	kept := terminationMarks(l.agent)

	rows := []struct {
		name  string
		apply func()
	}{
		{
			name: "wrong term",
			apply: func() {
				ackTermination(l.agent, toggleTestTermID+1, position, 4)
			},
		},
		{
			name: "position below P",
			apply: func() {
				ackTermination(l.agent, toggleTestTermID, position-1, 4)
			},
		},
		{
			name: "follower role",
			apply: func() {
				l.agent.RoleSet(RoleFollower)
				ackTermination(l.agent, toggleTestTermID, position, 4)
				l.agent.RoleSet(RoleLeader)
			},
		},
		{
			name: "no termination",
			apply: func() {
				saved := l.agent.clusterTermination
				l.agent.clusterTermination = nil
				ackTermination(l.agent, toggleTestTermID, position, 4)
				l.agent.clusterTermination = saved
			},
		},
		{
			name: "unknown member",
			apply: func() {
				ackTermination(l.agent, toggleTestTermID, position, 99)
			},
		},
		{
			name: "repeat ack",
			apply: func() {
				ackTermination(l.agent, toggleTestTermID, position, 2)
			},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			row.apply()
			require.Equal(t, kept, terminationMarks(l.agent))
			require.Empty(t, eg.terminationAcks)
		})
	}
}

func TestClusterTermination_follower(t *testing.T) {
	t.Parallel()

	t.Run("same term stores the position", func(t *testing.T) {
		t.Parallel()
		a := NewAgent(AgentConfig{Role: RoleFollower, LeadershipTermID: 6})
		a.handleTerminationPosition(codecs.TerminationPositionFields{
			LeadershipTermID: 6,
			LogPosition:      80,
		}, 900)
		require.Equal(t, int64(80), a.TerminationPosition())
		require.Equal(t, int64(6), a.terminationLeadershipTermID)
		require.Equal(t, int64(900), a.timeOfLastLogUpdateNs)

		a.handleTerminationPosition(codecs.TerminationPositionFields{
			LeadershipTermID: 7,
			LogPosition:      10,
		}, 901)
		require.Equal(t, int64(80), a.TerminationPosition())
		require.Equal(t, int64(900), a.timeOfLastLogUpdateNs)

		a.RoleSet(RoleLeader)
		a.handleTerminationPosition(codecs.TerminationPositionFields{
			LeadershipTermID: 6,
			LogPosition:      11,
		}, 902)
		require.Equal(t, int64(80), a.TerminationPosition())
		require.Equal(t, int64(900), a.timeOfLastLogUpdateNs)
	})

	t.Run("election does not see the frame", func(t *testing.T) {
		t.Parallel()
		a := NewAgent(AgentConfig{Role: RoleFollower, LeadershipTermID: 6})
		runner := &recordingRunner{}
		a.election = runner
		wire := mustConsensus(t, func(buf []byte) (int, bool) {
			return codecs.EncodeTerminationPosition(
				buf,
				0,
				clusterwire.TerminationPositionSbeSchemaVersion,
				codecs.TerminationPositionFields{LeadershipTermID: 6, LogPosition: 44},
			)
		})
		a.handleConsensusFragment(wire, 0, 50)
		require.Empty(t, runner.handles)
		require.Equal(t, int64(44), a.TerminationPosition())
		require.Equal(t, int64(50), a.timeOfLastLogUpdateNs)

		ack := mustConsensus(t, func(buf []byte) (int, bool) {
			return codecs.EncodeTerminationAck(
				buf,
				0,
				clusterwire.TerminationAckSbeSchemaVersion,
				codecs.TerminationAckFields{
					LeadershipTermID: 6,
					LogPosition:      44,
					MemberID:         1,
				},
			)
		})
		a.handleConsensusFragment(ack, 0, 51)
		require.Empty(t, runner.handles)
	})

	for _, state := range []ModuleState{ModuleStateActive, ModuleStateSuspended} {
		t.Run("reach in "+state.String(), func(t *testing.T) {
			t.Parallel()
			const position int64 = 100
			a, _, pub := terminationFollower(t, 1, position)
			setAdapterPosition(t, a, position)

			if state != ModuleStateActive {
				a.setModuleState(state, "test")
			}

			require.Equal(t, 1, a.followerConsensusWork(1))
			require.Equal(t, ModuleStateTerminating, a.ModuleState())
			require.Equal(t, int64(0), a.commit.Load())
			requireServiceTerminationFrame(t, pub, position, true)
			require.Equal(t, serviceTerminationWaitQueued, a.serviceTermination)
		})
	}

	t.Run("snapshot defers the reach", func(t *testing.T) {
		t.Parallel()
		const position int64 = 100
		a, q, pub := terminationFollower(t, 1, position)
		setAdapterPosition(t, a, position)
		a.queueSnapshotWait(40, 6, 1)
		require.Equal(t, 0, a.followerConsensusWork(1))
		require.Equal(t, ModuleStateSnapshot, a.ModuleState())
		require.Equal(t, int64(0), pub.Position())

		enqueueServiceAckRelevant(t, q, 40, 0, 77, 0)
		require.Equal(t, 1, a.pollServiceControl())
		require.Equal(t, 1, a.serviceControlWork())
		require.Equal(t, 1, a.snapshotTakeWork(2))
		require.Equal(t, ModuleStateActive, a.ModuleState())
		require.Equal(t, int64(0), pub.Position())

		require.Positive(t, a.followerConsensusWork(3))
		require.Equal(t, ModuleStateTerminating, a.ModuleState())
		requireServiceTerminationFrame(t, pub, position, true)
	})

	t.Run("termination pop acks and closes", func(t *testing.T) {
		t.Parallel()
		const position int64 = 100
		a, q, _ := terminationFollower(t, 1, position)
		eg := bindTerminationEgress(a, nil)
		log := &terminationCommitLog{}
		a.SetTerminationRecordingLog(log)
		setAdapterPosition(t, a, position)
		require.Positive(t, a.followerConsensusWork(1))

		enqueueServiceAck(t, q, position, 0, 0)
		require.Equal(t, 1, a.pollServiceControl())
		require.Equal(t, 1, a.serviceControlWork())
		require.Equal(t, []terminationPublish{{
			term:     6,
			position: position,
		}}, eg.terminationAcks)
		require.Equal(t, []terminationCommitCall{{term: 6, pos: position}}, log.calls)
		requireTerminated(t, a, ErrClusterTerminated, true)
	})

	t.Run("zero services ack the adapter position", func(t *testing.T) {
		t.Parallel()
		a := NewAgent(AgentConfig{
			Role:             RoleFollower,
			LeadershipTermID: 6,
			Commit:           new(atomic.Int64),
		})
		eg := bindTerminationEgress(a, nil)
		log := &terminationCommitLog{}
		a.SetTerminationRecordingLog(log)
		a.handleTerminationPosition(codecs.TerminationPositionFields{
			LeadershipTermID: 6,
			LogPosition:      400,
		}, 0)
		require.Equal(t, int64(400), a.TerminationPosition())
		setAdapterPosition(t, a, 500)

		require.Positive(t, a.followerConsensusWork(1))
		require.Equal(t, []terminationPublish{{term: 6, position: 500}}, eg.terminationAcks)
		require.Equal(t, []terminationCommitCall{{term: 6, pos: 500}}, log.calls)
		requireTerminated(t, a, ErrClusterTerminated, true)
	})

	t.Run("changed term reports and does not ack", func(t *testing.T) {
		t.Parallel()
		a, reported := agentWithReports(AgentConfig{
			Role:             RoleFollower,
			LeadershipTermID: 8,
		})
		eg := bindTerminationEgress(a, nil)
		a.terminationPosition = 400
		a.terminationLeadershipTermID = 6
		setAdapterPosition(t, a, 500)

		require.Positive(t, a.followerConsensusWork(1))
		require.Empty(t, eg.terminationAcks)
		require.Len(t, *reported, 1)
		require.ErrorIs(t, (*reported)[0], errTerminationAckTermChanged)
		requireTerminated(t, a, ErrClusterTerminated, true)
	})

	t.Run("overshoot acks and commits L", func(t *testing.T) {
		t.Parallel()
		const (
			position int64 = 100
			reach    int64 = 140
		)
		a, q, _ := terminationFollower(t, 2, position)
		eg := bindTerminationEgress(a, nil)
		log := &terminationCommitLog{}
		a.SetTerminationRecordingLog(log)
		a.terminateFollower()
		require.Equal(t, ModuleStateTerminating, a.ModuleState())

		enqueueServiceAck(t, q, reach, 0, 0)
		enqueueServiceAck(t, q, reach, 0, 1)
		require.Equal(t, 2, a.pollServiceControl())
		require.Equal(t, 1, a.serviceControlWork())
		require.NoError(t, a.ServiceAckFailure())
		require.Equal(t, []terminationPublish{{term: 6, position: reach}}, eg.terminationAcks)
		require.Equal(t, []terminationCommitCall{{term: 6, pos: reach}}, log.calls)
		requireTerminated(t, a, ErrClusterTerminated, true)
	})
}

func TestClusterTermination_elections(t *testing.T) {
	t.Parallel()

	t.Run("TERMINATING refuses a higher term", func(t *testing.T) {
		t.Parallel()
		a := NewAgent(AgentConfig{
			Role:             RoleFollower,
			LeadershipTermID: 3,
			ServiceCount:     1,
		})
		a.setModuleState(ModuleStateTerminating, "test")
		a.SetElectionFactory(func(ElectionSeed) ElectionRunner {
			t.Fatal("election factory called")
			return nil
		})
		frames := [][]byte{
			encodeRequestVoteWire(t, codecs.RequestVoteFields{CandidateTermID: 9}),
			encodeNewLeadershipTermWire(t, codecs.NewLeadershipTermFields{
				LeadershipTermID: 9,
				LeaderMemberID:   1,
			}),
			codecs.EncodeCommitPositionForTest(t, codecs.CommitPositionFields{
				LeadershipTermID: 9,
				LogPosition:      20,
				LeaderMemberID:   1,
			}),
		}

		for _, frame := range frames {
			a.handleConsensusFragment(frame, 0, 1)
			require.Equal(t, int64(0), a.ElectionCount())
			require.Nil(t, a.Election())
			require.Equal(t, ModuleStateTerminating, a.ModuleState())
		}
	})

	t.Run("prepare in SNAPSHOT keeps the position", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		_, prepared, err := l.agent.PrepareForNewLeadership(0, 50)
		require.NoError(t, err)
		require.True(t, prepared)
		require.Equal(t, ModuleStateActive, l.agent.ModuleState())
		require.Equal(t, RoleFollower, l.agent.Role())
		require.Equal(t, int64(ControlToggleInactive), l.toggle.Load())
		require.Nil(t, l.agent.clusterTermination)
		require.Equal(t, position, l.agent.TerminationPosition())
		require.Equal(t, toggleTestTermID, l.agent.terminationLeadershipTermID)
	})

	t.Run("former leader closes in the new term", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		_, prepared, err := l.agent.PrepareForNewLeadership(0, 50)
		require.NoError(t, err)
		require.True(t, prepared)

		l.agent.SetLeadershipTermID(toggleTestTermID + 1)
		setAdapterPosition(t, l.agent, position)
		eg := bindTerminationEgress(l.agent, nil)
		var reported []error
		l.agent.errorHandler = func(err error) { reported = append(reported, err) }
		require.Positive(t, l.agent.followerConsensusWork(60))
		require.Equal(t, ModuleStateTerminating, l.agent.ModuleState())
		require.Empty(t, eg.terminationAcks)
		require.Empty(t, reported)

		enqueueServiceAck(t, l.queue, position, 0, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 0, l.agent.serviceControlWork())
		enqueueServiceAck(t, l.queue, position, 1, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.Empty(t, eg.terminationAcks)
		require.Len(t, reported, 1)
		require.ErrorIs(t, reported[0], errTerminationAckTermChanged)
		requireTerminated(t, l.agent, ErrClusterTerminated, true)
	})

	t.Run("former leader that wins clears the position and keeps running", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		_, prepared, err := l.agent.PrepareForNewLeadership(0, 50)
		require.NoError(t, err)
		require.True(t, prepared)

		// Election win.
		l.agent.RoleSet(RoleLeader)
		l.agent.ElectionComplete(80)
		require.Equal(t, int64(ControlToggleNeutral), l.toggle.Load())
		require.Equal(t, int64(nullValue), l.agent.TerminationPosition())
		require.Equal(t, int64(nullValue), l.agent.terminationLeadershipTermID)
		setAdapterPosition(t, l.agent, position)
		eg := bindTerminationEgress(l.agent, nil)
		before := l.servicePub.Position()
		l.agent.DoWork(position)
		require.Equal(t, ModuleStateActive, l.agent.ModuleState())
		require.Equal(t, before, l.servicePub.Position())
		require.Empty(t, eg.terminationAcks)
		require.NoError(t, l.agent.TerminationError())

		// Later demotion.
		l.agent.RoleSet(RoleFollower)
		require.False(t, l.agent.hasReachedTerminationPosition())
		require.Equal(t, ModuleStateActive, l.agent.ModuleState())
	})

	t.Run("take after enterElection ends in ACTIVE", func(t *testing.T) {
		t.Parallel()
		l := newToggleLeader(t, int64(ControlToggleShutdown), 1)
		require.Equal(t, 1, l.agent.clusterControlToggleWork(toggleTestNowNs))
		position := l.agent.TerminationPosition()
		enqueueServiceAckRelevant(t, l.queue, position, 0, 77, 0)
		require.Equal(t, 1, l.agent.pollServiceControl())
		require.Equal(t, 1, l.agent.serviceControlWork())
		require.NotNil(t, l.agent.snapshotTake)
		require.Equal(t, ModuleStateSnapshot, l.agent.ModuleState())

		fac := &recordingElectionFactory{}
		l.agent.SetElectionFactory(fac.factory())
		require.Equal(t, 1, l.agent.enterElection(ElectionTriggerVoteRequest, false))
		require.Equal(t, RoleFollower, l.agent.Role())
		require.Equal(t, ModuleStateSnapshot, l.agent.ModuleState())
		require.Equal(t, 1, l.agent.snapshotTakeWork(1))
		require.Equal(t, ModuleStateActive, l.agent.ModuleState())
		require.NotNil(t, l.agent.clusterTermination)
		require.Equal(t, position, l.agent.TerminationPosition())
	})
}

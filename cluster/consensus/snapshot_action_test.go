//go:build testhooks

package consensus

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func bindLeaderLog(t testing.TB, a *Agent) *aeron.ExclusivePublication {
	t.Helper()
	lp, pub := boundLogPublisher(t)
	a.SetLogPublisher(lp)
	a.RoleSet(RoleLeader)
	return pub
}

func settledLeaderWithLog(
	t testing.TB,
	serviceCount int,
) (*Agent, *aeron.ExclusivePublication) {
	t.Helper()
	a := NewAgent(AgentConfig{ServiceCount: serviceCount})
	return a, bindLeaderLog(t, a)
}

func settledLeaderWithControl(
	t testing.TB,
	serviceCount int,
) (*Agent, *QueueConsensusIngress, *aeron.ExclusivePublication) {
	t.Helper()
	a, q, _ := agentWithServiceControl(t, serviceCount)
	return a, q, bindLeaderLog(t, a)
}

func decodeClusterActionAtTermStart(
	t *testing.T,
	pub *aeron.ExclusivePublication,
) codecs.ClusterActionRequestFields {
	t.Helper()
	fields, decoded := codecs.TryDecodeClusterActionRequestAt(termStartWire(t, pub), 0)
	require.True(t, decoded)
	return fields
}

func TestRequestSnapshot_success(t *testing.T) {
	t.Parallel()

	const (
		termID int64 = 11
		nowNs  int64 = 5000
	)
	a, pub := settledLeaderWithLog(t, 1)
	a.SetLeadershipTermID(termID)

	require.NoError(t, a.RequestSnapshot(nowNs))
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	waitPos := a.ExpectedAckPosition()
	require.Equal(t, a.snapshotWaitPosition, waitPos)
	require.Positive(t, waitPos)

	fields := decodeClusterActionAtTermStart(t, pub)
	require.Equal(t, termID, fields.LeadershipTermID)
	require.Equal(t, waitPos, fields.LogPosition)
	require.Equal(t, nowNs, fields.Timestamp)
	require.Equal(t, clusterwire.ClusterAction_SNAPSHOT, fields.Action)
	require.Equal(t, int32(0), fields.Flags)
}

func TestRequestSnapshot_notSettledLeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*Agent)
	}{
		{
			name: "follower",
			setup: func(a *Agent) {
				a.RoleSet(RoleFollower)
			},
		},
		{
			name: "leader with election",
			setup: func(a *Agent) {
				a.election = &orderRunner{n: 1}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, pub := settledLeaderWithLog(t, 1)
			tt.setup(a)
			pre := pub.Position()

			err := a.RequestSnapshot(1000)
			require.ErrorIs(t, err, errSnapshotNotLeader)
			require.False(t, a.HasPendingSnapshotAction())
			require.Equal(t, 0, a.PendingServiceAckWaits())
			require.Equal(t, pre, pub.Position())
		})
	}
}

func TestRequestSnapshot_outstandingCoordination(t *testing.T) {
	t.Parallel()

	t.Run("queued wait", func(t *testing.T) {
		t.Parallel()

		a, _, pub := settledLeaderWithControl(t, 1)
		require.NoError(t, a.RequestServiceAck(42))
		require.Equal(t, 1, a.PendingServiceAckWaits())
		pre := pub.Position()

		err := a.RequestSnapshot(1000)
		require.ErrorIs(t, err, errCoordinationOutstanding)
		require.False(t, a.HasPendingSnapshotAction())
		require.Equal(t, 1, a.PendingServiceAckWaits())
		require.Equal(t, int64(42), a.ExpectedAckPosition())
		require.Equal(t, pre, pub.Position())
	})

	t.Run("pending JoinLog", func(t *testing.T) {
		t.Parallel()

		a, pub := settledLeaderWithLog(t, 1)
		a.pendingJoinLog = &codecs.JoinLogFields{}
		pre := pub.Position()

		err := a.RequestSnapshot(1000)
		require.ErrorIs(t, err, errCoordinationOutstanding)
		require.False(t, a.HasPendingSnapshotAction())
		require.Equal(t, 0, a.PendingServiceAckWaits())
		require.Equal(t, pre, pub.Position())
	})
}

func TestRequestServiceAck_refusesPendingSnapshot(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	require.True(t, a.HasPendingSnapshotAction())
	waitPos := a.ExpectedAckPosition()

	svcPub := a.ServiceProxy().Publication()
	require.NotNil(t, svcPub)
	preSvc := svcPub.Position()

	err := a.RequestServiceAck(99)
	require.ErrorIs(t, err, errSnapshotActionPending)
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, waitPos, a.ExpectedAckPosition())
	require.Equal(t, preSvc, svcPub.Position())

	enqueueServiceAck(t, q, waitPos, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, 0, a.PendingServiceAckWaits())

	require.NoError(t, a.RequestServiceAck(99))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(99), a.ExpectedAckPosition())
}

func TestRequestSnapshot_appendFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*Agent)
	}{
		{
			name: "nil publisher",
			setup: func(a *Agent) {
				a.SetLogPublisher(nil)
			},
		},
		{
			name: "unbound publisher",
			setup: func(a *Agent) {
				a.SetLogPublisher(NewLogPublisher())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := NewAgent(AgentConfig{ServiceCount: 1, Role: RoleLeader})
			tt.setup(a)

			err := a.RequestSnapshot(1000)
			require.ErrorIs(t, err, errSnapshotAppend)
			require.False(t, a.HasPendingSnapshotAction())
			require.Equal(t, 0, a.PendingServiceAckWaits())
		})
	}
}

func TestRequestSnapshot_completesWait(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, int64(0), a.ExpectedServiceAckID())

	enqueueServiceAck(t, q, waitPos, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, int64(0), a.snapshotWaitPosition)
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
}

func TestRequestSnapshot_prepareClearsMarker(t *testing.T) {
	t.Parallel()

	a, _, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Positive(t, waitPos)

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, int64(0), a.snapshotWaitPosition)
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(nullValue), a.ExpectedAckPosition())

	require.NoError(t, a.RequestServiceAck(99))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, int64(99), a.ExpectedAckPosition())
}

func TestRequestSnapshot_completeAfterPrepare(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()
	require.Equal(t, int64(0), a.ExpectedServiceAckID())

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, int64(0), a.snapshotWaitPosition)
	require.Equal(t, 0, a.PendingServiceAckWaits())

	enqueueServiceAck(t, q, waitPos, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 0, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
	require.Equal(t, int64(0), a.snapshotWaitPosition)
	require.False(t, a.HasPendingSnapshotAction())
}

func TestRequestSnapshot_prepareThenControlWait(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 0, a.PendingServiceAckWaits())

	const controlPos int64 = 99
	require.NoError(t, a.RequestServiceAck(controlPos))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, controlPos, a.ExpectedAckPosition())

	enqueueServiceAck(t, q, controlPos, 0, 0)
	enqueueServiceAck(t, q, waitPos, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
}

func TestRequestSnapshot_prepareThenWaitAtSnapshotPosition(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 0, a.PendingServiceAckWaits())

	require.NoError(t, a.RequestServiceAck(waitPos))
	require.Equal(t, 1, a.PendingServiceAckWaits())
	require.Equal(t, waitPos, a.ExpectedAckPosition())

	enqueueServiceAck(t, q, waitPos, 0, 0)
	enqueueServiceAck(t, q, waitPos, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())

	const laterPos int64 = 99
	require.NoError(t, a.RequestServiceAck(laterPos))
	enqueueServiceAck(t, q, laterPos, 2, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(3), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
}

func TestHandleServiceAck_owedRetiredAck(t *testing.T) {
	t.Parallel()

	// retiredLeader returns a leader whose snapshot wait at the returned
	// position retired. Each service in ackedServices sends ack id 0 before
	// the retire, so the retire drops it. Every other service owes an ack.
	retiredLeader := func(
		t *testing.T,
		serviceCount int,
		ackedServices ...int32,
	) (*Agent, *QueueConsensusIngress, int64) {
		t.Helper()
		a, q, _ := settledLeaderWithControl(t, serviceCount)
		require.NoError(t, a.RequestSnapshot(1000))
		waitPos := a.ExpectedAckPosition()
		for _, serviceID := range ackedServices {
			enqueueServiceAck(t, q, waitPos, 0, serviceID)
			require.Equal(t, 1, a.pollServiceControl())
		}
		_, prepared, err := a.PrepareForNewLeadership(0, 1)
		require.NoError(t, err)
		require.True(t, prepared)
		require.Equal(t, 0, a.PendingServiceAckWaits())
		return a, q, waitPos
	}
	// requireOwedAckDrops queues a wait at waitPos, then asserts that ack id 0
	// from service 0 drops and the wait stays.
	requireOwedAckDrops := func(
		t *testing.T,
		a *Agent,
		q *QueueConsensusIngress,
		waitPos int64,
	) {
		t.Helper()
		require.NoError(t, a.RequestServiceAck(waitPos))
		enqueueServiceAck(t, q, waitPos, 0, 0)
		require.Equal(t, 1, a.pollServiceControl())
		require.Equal(t, 0, a.serviceControlWork())
		require.Equal(t, 1, a.PendingServiceAckWaits())
		require.Equal(t, 0, a.ServiceAckQueueDepth(0))
		require.Equal(t, int64(1), a.ExpectedServiceAckID())
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "owed ack with a wait at the position",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
				requireOwedAckDrops(t, a, q, waitPos)

				// The next ack at the position pops the wait.
				ackAndPopWait(t, a, q, waitPos, 1)
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "ack dropped at retire",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1, 0)
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// No owed flag. The next ack at the position pops the wait.
				require.NoError(t, a.RequestServiceAck(waitPos))
				ackAndPopWait(t, a, q, waitPos, 1)
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "wait at the position second in the FIFO",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
				const headWaitPos int64 = 4096
				require.NoError(t, a.RequestServiceAck(headWaitPos))
				require.NoError(t, a.RequestServiceAck(waitPos))

				// Phase 1: the owed ack drops behind the head wait.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 0, a.ServiceAckQueueDepth(0))
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// Phase 2: both acks arrive in one poll. Both enter the
				// tracker.
				enqueueServiceAck(t, q, headWaitPos, 1, 0)
				enqueueServiceAck(t, q, waitPos, 2, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 2, a.ServiceAckQueueDepth(0))
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(3), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "no wait at the position and no owed flag",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)

				// Phase 1: the owed ack drops and clears the flag.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// Phase 2: a later ack at the position with no wait drops.
				enqueueServiceAck(t, q, waitPos, 1, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 0, a.ServiceAckQueueDepth(0))
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "two services with one owed ack",
			run: func(t *testing.T) {
				// Service 0's ack drops at retire. Service 1 owes.
				a, q, waitPos := retiredLeader(t, 2, 0)
				require.Equal(t, int64(0), a.ExpectedServiceAckID())
				require.NoError(t, a.RequestServiceAck(waitPos))

				// Service 1's owed ack drops. Every service has dropped id 0.
				enqueueServiceAck(t, q, waitPos, 0, 1)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 0, a.ServiceAckQueueDepth(1))
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// The acks for the wait pop it.
				enqueueServiceAck(t, q, waitPos, 1, 0)
				enqueueServiceAck(t, q, waitPos, 1, 1)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "truncate below the position clears the owed flag",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
				a.SetLogTruncateControl(&fakeLogTruncateControl{})
				a.SetLogRecordingID(77)
				require.NoError(t, a.TruncateLogEntry(0, 0))
				require.NotNil(t, a.abandonedSnapshotWaitAt(waitPos))

				// Phase 1: a new wait at the position pops on its first ack.
				require.NoError(t, a.RequestServiceAck(waitPos))
				ackAndPopWait(t, a, q, waitPos, 0)
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// Phase 2: an ack at the retired position with no wait
				// drops.
				enqueueServiceAck(t, q, waitPos, 1, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 0, a.ServiceAckQueueDepth(0))
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "refused truncate below the position keeps the owed flag",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
				a.SetLogTruncateControl(&fakeLogTruncateControl{err: errors.New("refused")})
				a.SetLogRecordingID(77)
				require.Error(t, a.TruncateLogEntry(0, 0))
				requireOwedAckDrops(t, a, q, waitPos)
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "truncate at the position keeps the owed flag",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
				a.SetLogTruncateControl(&fakeLogTruncateControl{})
				a.SetLogRecordingID(77)
				require.NoError(t, a.TruncateLogEntry(0, waitPos))
				require.NotNil(t, a.abandonedSnapshotWaitAt(waitPos))
				requireOwedAckDrops(t, a, q, waitPos)
				require.NoError(t, a.ServiceAckFailure())
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

func TestRequestSnapshot_prepareDropsQueuedSnapshotAck(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	waitPos := a.ExpectedAckPosition()

	enqueueServiceAck(t, q, waitPos, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.ServiceAckQueueDepth(0))

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())

	require.NoError(t, a.RequestServiceAck(99))
	enqueueServiceAck(t, q, 99, 1, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
}

func TestRequestSnapshot_secondPrepareKeepsFirstRetiredPosition(t *testing.T) {
	t.Parallel()

	a, q, _ := settledLeaderWithControl(t, 1)
	require.NoError(t, a.RequestSnapshot(1000))
	firstPos := a.ExpectedAckPosition()

	_, prepared, err := a.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)

	a.RoleSet(RoleLeader)
	require.NoError(t, a.RequestSnapshot(2000))
	secondPos := a.ExpectedAckPosition()
	require.NotEqual(t, firstPos, secondPos)

	_, prepared, err = a.PrepareForNewLeadership(0, 2)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, 0, a.PendingServiceAckWaits())

	// Late ServiceAcks at both retired positions drop.
	enqueueServiceAck(t, q, firstPos, 0, 0)
	enqueueServiceAck(t, q, secondPos, 1, 0)
	require.Equal(t, 2, a.pollServiceControl())
	require.Equal(t, 0, a.ServiceAckQueueDepth(0))
	require.Equal(t, int64(2), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())

	const laterPos int64 = 99
	require.NoError(t, a.RequestServiceAck(laterPos))
	enqueueServiceAck(t, q, laterPos, 2, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(3), a.ExpectedServiceAckID())
	require.NoError(t, a.ServiceAckFailure())
}

func TestRequestSnapshot_zeroServicesCompletes(t *testing.T) {
	t.Parallel()

	a, _ := settledLeaderWithLog(t, 0)
	require.NoError(t, a.RequestSnapshot(1000))
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, 1, a.PendingServiceAckWaits())

	// With zero services, the wait completes without a ServiceAck.
	require.Equal(t, 1, a.serviceControlWork())
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
}

// snapshotActionFields returns SNAPSHOT action fields at logPosition with
// flags 0.
func snapshotActionFields(logPosition int64) codecs.ClusterActionRequestFields {
	return codecs.ClusterActionRequestFields{
		LeadershipTermID: 1,
		LogPosition:      logPosition,
		Timestamp:        5,
		Action:           clusterwire.ClusterAction_SNAPSHOT,
	}
}

func TestHandleClusterAction_rules(t *testing.T) {
	t.Parallel()

	const (
		consumePos int64 = 4096
		otherPos   int64 = 8192
	)
	type fieldsAt func(int64) codecs.ClusterActionRequestFields
	withAction := func(action clusterwire.ClusterAction) fieldsAt {
		return func(p int64) codecs.ClusterActionRequestFields {
			fields := snapshotActionFields(p)
			fields.Action = action
			return fields
		}
	}
	atConsume := func(*testing.T, *Agent) int64 { return consumePos }
	leaderSnapshot := func(t *testing.T, a *Agent) int64 {
		bindLeaderLog(t, a)
		require.NoError(t, a.RequestSnapshot(1000))
		return a.ExpectedAckPosition()
	}
	tests := []struct {
		name         string
		zeroServices bool
		// setup returns the consume position.
		setup  func(t *testing.T, a *Agent) int64
		fields func(p int64) codecs.ClusterActionRequestFields
		// wantWaits returns the expected serviceAckWaits and snapshotWaitPosition.
		wantWaits   func(p int64) ([]int64, int64)
		wantFailure error
	}{
		{
			name:      "follower snapshot flags 0",
			setup:     atConsume,
			wantWaits: func(p int64) ([]int64, int64) { return []int64{p}, p },
		},
		{
			name:   "suspend",
			setup:  atConsume,
			fields: withAction(clusterwire.ClusterAction_SUSPEND),
		},
		{
			name:   "resume",
			setup:  atConsume,
			fields: withAction(clusterwire.ClusterAction_RESUME),
		},
		{
			name:   "null action",
			setup:  atConsume,
			fields: withAction(clusterwire.ClusterAction_NULL_VALUE),
		},
		{
			name:   "unknown action",
			setup:  atConsume,
			fields: withAction(clusterwire.ClusterAction(9)),
		},
		{
			name:  "snapshot flags 1",
			setup: atConsume,
			fields: func(p int64) codecs.ClusterActionRequestFields {
				fields := snapshotActionFields(p)
				fields.Flags = 1
				return fields
			},
		},
		{
			name:  "wire position differs from frame end",
			setup: atConsume,
			fields: func(p int64) codecs.ClusterActionRequestFields {
				return snapshotActionFields(p + 32)
			},
		},
		{
			name:      "leader consumes its own appended action",
			setup:     leaderSnapshot,
			wantWaits: func(p int64) ([]int64, int64) { return []int64{p}, p },
		},
		{
			name: "leader with no wait",
			setup: func(_ *testing.T, a *Agent) int64 {
				a.RoleSet(RoleLeader)
				return consumePos
			},
		},
		{
			name: "follower consumes the pending position again",
			setup: func(_ *testing.T, a *Agent) int64 {
				a.handleClusterAction(snapshotActionFields(consumePos), consumePos)
				return consumePos
			},
			wantWaits: func(p int64) ([]int64, int64) { return []int64{p}, p },
		},
		{
			name: "follower with a snapshot wait at another position",
			setup: func(_ *testing.T, a *Agent) int64 {
				a.handleClusterAction(snapshotActionFields(otherPos), otherPos)
				return consumePos
			},
			wantWaits: func(int64) ([]int64, int64) { return []int64{otherPos}, otherPos },
		},
		{
			name: "retired position",
			setup: func(t *testing.T, a *Agent) int64 {
				p := leaderSnapshot(t, a)
				_, prepared, err := a.PrepareForNewLeadership(0, 1)
				require.NoError(t, err)
				require.True(t, prepared)
				require.Equal(t, RoleFollower, a.Role())
				return p
			},
		},
		{
			name: "service ack failure marked",
			setup: func(_ *testing.T, a *Agent) int64 {
				a.serviceAckFailure = ErrServiceAckOutOfSequence
				return consumePos
			},
			wantFailure: ErrServiceAckOutOfSequence,
		},
		{
			name:         "zero services",
			zeroServices: true,
			setup:        atConsume,
			wantWaits:    func(p int64) ([]int64, int64) { return []int64{p}, p },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			serviceCount := 1
			if tt.zeroServices {
				serviceCount = 0
			}
			a := NewAgent(AgentConfig{ServiceCount: serviceCount, Role: RoleFollower})
			p := tt.setup(t, a)
			fields := snapshotActionFields(p)
			if tt.fields != nil {
				fields = tt.fields(p)
			}

			a.handleClusterAction(fields, p)

			wantFIFO, wantSnapshot := []int64(nil), int64(0)
			if tt.wantWaits != nil {
				wantFIFO, wantSnapshot = tt.wantWaits(p)
			}
			require.Equal(t, len(wantFIFO), a.PendingServiceAckWaits())
			if len(wantFIFO) > 0 {
				require.Equal(t, wantFIFO, a.serviceAckWaits)
			}
			require.Equal(t, wantSnapshot, a.snapshotWaitPosition)
			if tt.wantFailure != nil {
				require.ErrorIs(t, a.ServiceAckFailure(), tt.wantFailure)
			} else {
				require.NoError(t, a.ServiceAckFailure())
			}
			if tt.zeroServices {
				require.Equal(t, 1, a.serviceControlWork())
				require.False(t, a.HasPendingSnapshotAction())
				require.Equal(t, 0, a.PendingServiceAckWaits())
			}
		})
	}
}

func TestHandleClusterAction_queuesBehindJoinLogWait(t *testing.T) {
	t.Parallel()

	const (
		joinPos     int64 = 1024
		snapshotPos int64 = 2048
	)
	a, q, _ := agentWithServiceControl(t, 1)
	a.RoleSet(RoleFollower)
	require.NoError(t, a.RequestServiceAck(joinPos))

	a.handleClusterAction(snapshotActionFields(snapshotPos), snapshotPos)
	require.Equal(t, []int64{joinPos, snapshotPos}, a.serviceAckWaits)
	require.Equal(t, snapshotPos, a.snapshotWaitPosition)

	enqueueServiceAck(t, q, joinPos, 0, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, []int64{snapshotPos}, a.serviceAckWaits)
	require.Equal(t, snapshotPos, a.snapshotWaitPosition)

	enqueueServiceAck(t, q, snapshotPos, 1, 0)
	require.Equal(t, 1, a.pollServiceControl())
	require.Equal(t, 1, a.serviceControlWork())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(0), a.snapshotWaitPosition)
	require.NoError(t, a.ServiceAckFailure())
}

func TestSnapshotActionConsume_paths(t *testing.T) {
	t.Parallel()

	const nowNs int64 = 5

	// holdingFollower is a one-service follower. Its log holds a snapshot
	// action at 0 then a session message. Its notified commit lies past both
	// frames.
	type holdingFollower struct {
		agent      *Agent
		queue      *QueueConsensusIngress
		adapter    *LogAdapter
		handler    *logAdapterHandlerSpy
		actionEnd  int64
		messageEnd int64
	}
	newHoldingFollower := func(t *testing.T) holdingFollower {
		adapter, image, handler := boundLogAdapterWithHandler(t)
		actionEnd, messageEnd := writeSnapshotActionThenMessage(t, image, 0)
		a, q := newFollowerActionAgent(messageEnd+100, adapter, 1)
		a.notifiedCommitPosition = messageEnd + 32
		return holdingFollower{
			agent:      a,
			queue:      q,
			adapter:    adapter,
			handler:    handler,
			actionEnd:  actionEnd,
			messageEnd: messageEnd,
		}
	}
	requireHeld := func(t *testing.T, f holdingFollower) {
		t.Helper()
		require.Equal(t, f.actionEnd, f.adapter.Position())
		require.Equal(t, f.actionEnd, f.agent.CommitPosition())
		require.True(t, f.agent.HasPendingSnapshotAction())
		require.Equal(t, f.actionEnd, f.agent.ExpectedAckPosition())
		requireHandlerIdle(t, f.handler)
	}
	ackAndPop := func(t *testing.T, f holdingFollower, ackID int64) {
		t.Helper()
		enqueueServiceAck(t, f.queue, f.actionEnd, ackID, 0)
		require.Equal(t, 1, f.agent.pollServiceControl())
		require.Equal(t, 1, f.agent.serviceControlWork())
		require.False(t, f.agent.HasPendingSnapshotAction())
	}
	requireMessageRead := func(t *testing.T, f holdingFollower) {
		t.Helper()
		require.Equal(t, 1, f.handler.sessionMessages)
		require.Equal(t, f.messageEnd, f.adapter.Position())
		require.Equal(t, f.messageEnd, f.agent.CommitPosition())
	}
	prepare := func(t *testing.T, a *Agent) {
		t.Helper()
		_, prepared, err := a.PrepareForNewLeadership(0, nowNs)
		require.NoError(t, err)
		require.True(t, prepared)
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "catchup poll stops at the action",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.CatchupPoll(f.messageEnd+32, nowNs)
				requireHeld(t, f)
			},
		},
		{
			name: "catchup hold below the limit",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				limit := f.messageEnd + 32
				for range 3 {
					f.agent.CatchupPoll(limit, nowNs)
					requireHeld(t, f)
					require.Less(t, f.agent.CommitPosition(), limit)
					require.Nil(t, f.agent.abandonedSnapshotWaitAt(f.actionEnd))
				}

				ackAndPop(t, f, 0)
				f.agent.CatchupPoll(limit, nowNs)
				requireMessageRead(t, f)
			},
		},
		{
			name: "catchup hold at the limit",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				for range 2 {
					f.agent.CatchupPoll(f.actionEnd, nowNs)
					requireHeld(t, f)
				}

				f.agent.ElectionComplete(nowNs)
				require.True(t, f.agent.HasPendingSnapshotAction())
				require.Nil(t, f.agent.abandonedSnapshotWaitAt(f.actionEnd))

				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)

				ackAndPop(t, f, 0)
				f.agent.followerConsensusWork(nowNs)
				requireMessageRead(t, f)
			},
		},
		{
			name: "enter election drain stops at the action",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.SetElectionFactory((&recordingElectionFactory{}).factory())

				require.Equal(t, 1, f.agent.enterElection(ElectionTriggerLeaderHeartbeatTimeout, false, nowNs))
				require.Equal(t, f.actionEnd, f.adapter.Position())
				require.True(t, f.agent.HasPendingSnapshotAction())
				require.Equal(t, f.actionEnd, f.agent.ExpectedAckPosition())
				requireHandlerIdle(t, f.handler)
			},
		},
		{
			name: "leader election complete drain reads past the action",
			run: func(t *testing.T) {
				adapter, image, handler := boundLogAdapterWithHandler(t)
				_, messageEnd := writeSnapshotActionThenMessage(t, image, 0)
				commit := new(atomic.Int64)
				commit.Store(messageEnd)
				a := NewAgent(AgentConfig{
					Role:         RoleLeader,
					Commit:       commit,
					LogAdapter:   adapter,
					ServiceCount: 1,
				})

				a.ElectionComplete(nowNs)
				require.Equal(t, messageEnd, adapter.Position())
				require.Equal(t, 1, handler.sessionMessages)
				require.False(t, a.HasPendingSnapshotAction())
				require.Equal(t, 0, a.PendingServiceAckWaits())
			},
		},
		{
			name: "retire during a hold",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)

				prepare(t, f.agent)
				require.Equal(t, int64(0), f.agent.snapshotWaitPosition)
				require.NotNil(t, f.agent.abandonedSnapshotWaitAt(f.actionEnd))

				f.agent.CatchupPoll(f.messageEnd+32, nowNs)
				requireMessageRead(t, f)

				enqueueServiceAck(t, f.queue, f.actionEnd, 0, 0)
				require.Equal(t, 1, f.agent.pollServiceControl())
				require.Equal(t, 0, f.agent.ServiceAckQueueDepth(0))
				require.Equal(t, int64(1), f.agent.ExpectedServiceAckID())
				require.NoError(t, f.agent.ServiceAckFailure())
			},
		},
		{
			name: "late snapshot ack at a JoinLog wait at the action drops",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				proxy, _ := boundServiceProxy(t)
				f.agent.SetServiceProxy(proxy)
				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)

				prepare(t, f.agent)
				f.agent.publishFollowerJoinLog(FollowerLogJoinParams{LogJoinPosition: f.actionEnd})
				require.Equal(t, []int64{f.actionEnd}, f.agent.serviceAckWaits)
				startID := f.agent.ExpectedServiceAckID()

				// The owed snapshot ack drops. The JoinLog wait stays.
				enqueueServiceAck(t, f.queue, f.actionEnd, startID, 0)
				require.Equal(t, 1, f.agent.pollServiceControl())
				require.Equal(t, 0, f.agent.serviceControlWork())
				require.Equal(t, 0, f.agent.ServiceAckQueueDepth(0))
				require.Equal(t, []int64{f.actionEnd}, f.agent.serviceAckWaits)

				// The JoinLog ack pops the JoinLog wait.
				ackAndPopWait(t, f.agent, f.queue, f.actionEnd, startID+1)
				require.Equal(t, 0, f.agent.PendingServiceAckWaits())
				require.Equal(t, startID+2, f.agent.ExpectedServiceAckID())
				require.NoError(t, f.agent.ServiceAckFailure())
			},
		},
		{
			name: "service ack failure during a hold keeps the hold",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)

				enqueueServiceAck(t, f.queue, f.messageEnd, 0, 0)
				require.Equal(t, 1, f.agent.pollServiceControl())
				require.Equal(t, 0, f.agent.serviceControlWork())
				require.ErrorIs(t, f.agent.ServiceAckFailure(), ErrServiceAckOutOfSequence)

				f.agent.followerConsensusWork(nowNs)
				require.Equal(t, f.actionEnd, f.adapter.Position())
				require.Equal(t, f.actionEnd, f.agent.CommitPosition())
				require.True(t, f.agent.HasPendingSnapshotAction())
				requireHandlerIdle(t, f.handler)
			},
		},
		{
			name: "pending JoinLog holds the action until the JoinLog wait",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.SetServiceProxy(boundServiceProxyWithLimit(t, 0, false))
				f.agent.publishFollowerJoinLog(FollowerLogJoinParams{LogJoinPosition: 0})
				require.True(t, f.agent.HasPendingJoinLog())

				f.agent.followerConsensusWork(nowNs)
				f.agent.CatchupPoll(f.messageEnd+32, nowNs)
				require.Equal(t, int64(0), f.adapter.Position())
				require.False(t, f.agent.HasPendingSnapshotAction())
				require.Equal(t, 0, f.agent.PendingServiceAckWaits())

				proxy, _ := boundServiceProxy(t)
				f.agent.SetServiceProxy(proxy)
				require.Equal(t, 1, f.agent.retryPendingJoinLog())
				f.agent.followerConsensusWork(nowNs)
				require.Equal(t, f.actionEnd, f.adapter.Position())
				require.Equal(t, f.actionEnd, f.agent.snapshotWaitPosition)
				require.Equal(t, []int64{0, f.actionEnd}, f.agent.serviceAckWaits)
				requireHandlerIdle(t, f.handler)

				// The service acks the JoinLog first, then the action.
				enqueueServiceAck(t, f.queue, 0, 0, 0)
				require.Equal(t, 1, f.agent.pollServiceControl())
				require.Equal(t, 1, f.agent.serviceControlWork())
				ackAndPop(t, f, 1)
				require.NoError(t, f.agent.ServiceAckFailure())

				f.agent.followerConsensusWork(nowNs)
				requireMessageRead(t, f)
			},
		},
		{
			name: "redelivery at a retired position",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)
				prepare(t, f.agent)

				sub := aeron.NewTestSubscriptionForHooks(t)
				image := aeron.NewTestImageForHooks(t, sub, 0)
				end := writeClusterActionFrame(t, image, 0, clusterwire.ClusterAction_SNAPSHOT, 0)
				require.Equal(t, f.actionEnd, end)
				f.adapter.SetImage(image)
				require.Equal(t, int64(0), f.adapter.Position())

				f.agent.followerConsensusWork(nowNs)
				require.Equal(t, f.actionEnd, f.adapter.Position())
				require.False(t, f.agent.HasPendingSnapshotAction())
				require.Equal(t, 0, f.agent.PendingServiceAckWaits())
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

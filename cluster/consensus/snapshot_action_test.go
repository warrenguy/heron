//go:build testhooks

package consensus

import (
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

	// Late ServiceAcks at both retired positions drop before the offer.
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

	// With zero services, the first wait completes without a ServiceAck.
	require.Equal(t, 1, a.serviceControlWork())
	require.False(t, a.HasPendingSnapshotAction())
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.Equal(t, int64(1), a.ExpectedServiceAckID())
}

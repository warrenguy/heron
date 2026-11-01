//go:build testhooks

package consensus

import (
	"errors"
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

// retiredLeader returns a leader whose snapshot wait at the returned position
// retired in PrepareForNewLeadership. Each service in ackedServices sends ack
// id 0 before the retire, so the retire drops it. Every other service owes an
// ack at the position.
func retiredLeader(
	t testing.TB,
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

// truncatedRetiredLeader calls retiredLeader. TruncateLogEntry then
// truncates the log to position 0. TruncateLogEntry removes the retired
// entry at the returned position.
func truncatedRetiredLeader(
	t testing.TB,
	serviceCount int,
	ackedServices ...int32,
) (*Agent, *QueueConsensusIngress, int64) {
	t.Helper()
	a, q, waitPos := retiredLeader(t, serviceCount, ackedServices...)
	a.SetLogTruncateControl(&fakeLogTruncateControl{})
	a.SetLogRecordingID(77)
	require.NoError(t, a.TruncateLogEntry(0, 0))
	return a, q, waitPos
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

// requireTakeNotConfigured runs one snapshotTakeWork call on a take with no
// recording control wired. The take resolves with errSnapshotNotConfigured,
// and the pending report clears.
func requireTakeNotConfigured(t testing.TB, a *Agent) {
	t.Helper()
	require.True(t, a.HasPendingSnapshotAction())
	require.Equal(t, 1, a.snapshotTakeWork(0))
	result, ok := a.LastSnapshotTake()
	require.True(t, ok)
	require.ErrorIs(t, result.Err, errSnapshotNotConfigured)
	require.Equal(t, int64(nullValue), result.RecordingID)
	require.False(t, a.HasPendingSnapshotAction())
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

func TestRequestSnapshot_moduleNotActive(t *testing.T) {
	t.Parallel()

	for _, state := range allModuleStates {
		if state == ModuleStateActive {
			continue
		}
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()

			a, pub := settledLeaderWithLog(t, 1)
			a.setModuleState(state, "test")
			pre := pub.Position()

			err := a.RequestSnapshot(1000)
			require.ErrorIs(t, err, errSnapshotModuleNotActive)
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
	require.Equal(t, 0, a.PendingServiceAckWaits())
	require.ErrorIs(t, a.RequestServiceAck(99), errSnapshotActionPending)
	requireTakeNotConfigured(t, a)

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
	requireTakeNotConfigured(t, a)
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

	const controlPos int64 = 99

	// retiredControlLeader is retiredLeader with a control wait queued at
	// controlPos.
	retiredControlLeader := func(
		t *testing.T,
		serviceCount int,
	) (*Agent, *QueueConsensusIngress, int64) {
		t.Helper()
		a, q, waitPos := retiredLeader(t, serviceCount)
		require.NoError(t, a.RequestServiceAck(controlPos))
		require.Equal(t, 1, a.PendingServiceAckWaits())
		require.Equal(t, controlPos, a.ExpectedAckPosition())
		return a, q, waitPos
	}
	// requireDrained asserts every wait popped, every queue is empty, and the
	// failure never marked.
	requireDrained := func(t *testing.T, a *Agent, serviceCount int, wantAckID int64) {
		t.Helper()
		require.Equal(t, 0, a.PendingServiceAckWaits())
		for serviceID := range int32(serviceCount) {
			require.Equal(t, 0, a.ServiceAckQueueDepth(serviceID))
		}
		require.Equal(t, wantAckID, a.ExpectedServiceAckID())
		require.NoError(t, a.ServiceAckFailure())
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "one service",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 1)
				enqueueServiceAck(t, q, controlPos, 0, 0)
				enqueueServiceAck(t, q, waitPos, 1, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				requireDrained(t, a, 1, 2)
			},
		},
		{
			name: "two services, service 0 drops first",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)

				// Phase 1: service 0 ahead.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 1, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.NoError(t, a.ServiceAckFailure())
				require.Equal(t, 1, a.ServiceAckQueueDepth(0))
				require.Equal(t, int64(0), a.ExpectedServiceAckID())
				require.Equal(t, 0, a.serviceControlWork())
				require.NoError(t, a.ServiceAckFailure())

				// Phase 2: service 1 catches up.
				enqueueServiceAck(t, q, waitPos, 0, 1)
				enqueueServiceAck(t, q, controlPos, 1, 1)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				requireDrained(t, a, 2, 2)
			},
		},
		{
			name: "two services, service 1 drops first",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)

				// Phase 1: service 1 ahead. The walk stops at empty queue 0.
				enqueueServiceAck(t, q, waitPos, 0, 1)
				enqueueServiceAck(t, q, controlPos, 1, 1)
				require.Equal(t, 2, a.pollServiceControl())
				require.NoError(t, a.ServiceAckFailure())
				require.Equal(t, 1, a.ServiceAckQueueDepth(1))
				require.Equal(t, int64(0), a.ExpectedServiceAckID())
				require.Equal(t, 0, a.serviceControlWork())
				require.NoError(t, a.ServiceAckFailure())

				// Phase 2: service 0 catches up.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 1, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				requireDrained(t, a, 2, 2)
			},
		},
		{
			name: "two services, opposite delivery orders",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)

				// Phase 1: first pop. Service 1 sees the control request
				// before the retired snapshot action.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 1, 0)
				enqueueServiceAck(t, q, controlPos, 0, 1)
				require.Equal(t, 3, a.pollServiceControl())
				require.NoError(t, a.ServiceAckFailure())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, int64(1), a.ExpectedServiceAckID())

				// Phase 2: late drop. Service 1 acks the retired action.
				enqueueServiceAck(t, q, waitPos, 1, 1)
				require.Equal(t, 1, a.pollServiceControl())
				requireDrained(t, a, 2, 2)
			},
		},
		{
			name: "two services, skipped id marks",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 2, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			},
		},
		{
			name: "two services, repeated id marks",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)

				// The second ack repeats the id that service 0 dropped.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 0, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			},
		},
		{
			name: "two services, slow service skipped id marks",
			run: func(t *testing.T) {
				a, q, waitPos := retiredControlLeader(t, 2)

				// Phase 1: service 0 drops and acks in step.
				enqueueServiceAck(t, q, waitPos, 0, 0)
				enqueueServiceAck(t, q, controlPos, 1, 0)
				require.Equal(t, 2, a.pollServiceControl())
				require.NoError(t, a.ServiceAckFailure())

				// Phase 2: service 1 skips its retired ack. Queue 1 expects
				// id 0.
				enqueueServiceAck(t, q, controlPos, 1, 1)
				require.Equal(t, 1, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
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

func TestRequestSnapshot_prepareThenWaitAtSnapshotPosition(t *testing.T) {
	t.Parallel()

	const laterPos int64 = 200

	// twoServicesPoppedAtRetired returns a two-service leader whose snapshot
	// wait retired after service 0 acked it. A wait at the retired position
	// then popped.
	twoServicesPoppedAtRetired := func(t *testing.T) (*Agent, *QueueConsensusIngress) {
		t.Helper()
		// Phase 1: service 0 acks before prepare. Prepare drops the ack.
		// Service 1 owes an ack at the position.
		a, q, waitPos := retiredLeader(t, 2, 0)
		require.Equal(t, 0, a.ServiceAckQueueDepth(0))
		require.Equal(t, int64(0), a.ExpectedServiceAckID())

		// Phase 2: a wait at the retired position. Service 0 acks it while
		// service 1 still owes its snapshot ack.
		require.NoError(t, a.RequestServiceAck(waitPos))
		enqueueServiceAck(t, q, waitPos, 1, 0)
		require.Equal(t, 1, a.pollServiceControl())
		require.NoError(t, a.ServiceAckFailure())
		require.Equal(t, 1, a.ServiceAckQueueDepth(0))

		// Phase 3: service 1's owed snapshot ack drops. Its ack for the
		// wait pops the wait.
		enqueueServiceAck(t, q, waitPos, 0, 1)
		require.Equal(t, 1, a.pollServiceControl())
		require.Equal(t, 0, a.ServiceAckQueueDepth(1))
		require.Equal(t, int64(1), a.ExpectedServiceAckID())
		require.Equal(t, 0, a.serviceControlWork())
		enqueueServiceAck(t, q, waitPos, 1, 1)
		require.Equal(t, 1, a.pollServiceControl())
		require.Equal(t, 1, a.serviceControlWork())
		require.Equal(t, 0, a.PendingServiceAckWaits())
		require.Equal(t, int64(2), a.ExpectedServiceAckID())
		require.NoError(t, a.ServiceAckFailure())
		return a, q
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "one service",
			run: func(t *testing.T) {
				a, q, waitPos := retiredLeader(t, 1)
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

				require.NoError(t, a.RequestServiceAck(laterPos))
				enqueueServiceAck(t, q, laterPos, 2, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(3), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "two services, wait at the retired position",
			run: func(t *testing.T) {
				a, q := twoServicesPoppedAtRetired(t)

				// Phase 4: both services ack a later wait in step.
				require.NoError(t, a.RequestServiceAck(laterPos))
				enqueueServiceAck(t, q, laterPos, 2, 0)
				enqueueServiceAck(t, q, laterPos, 2, 1)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(3), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "two services, stale id marks",
			run: func(t *testing.T) {
				a, q := twoServicesPoppedAtRetired(t)

				// Phase 4: service 0 acks with an id the pop consumed.
				require.NoError(t, a.RequestServiceAck(laterPos))
				enqueueServiceAck(t, q, laterPos, 1, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
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

func TestHandleServiceAck_owedRetiredAck(t *testing.T) {
	t.Parallel()

	// twoServicesPoppedAfterTruncate returns a two-service leader whose
	// snapshot wait retired after service 0 acked it, then truncated below
	// the position. A wait at the position then popped with ack id 1 from
	// service 0 and ack id 0 from service 1.
	twoServicesPoppedAfterTruncate := func(t *testing.T) (*Agent, *QueueConsensusIngress) {
		t.Helper()
		// The retire drops service 0's ack. TruncateLogEntry removes the action.
		// Service 1 never acks the removed action.
		a, q, waitPos := truncatedRetiredLeader(t, 2, 0)
		require.NoError(t, a.RequestServiceAck(waitPos))
		enqueueServiceAck(t, q, waitPos, 1, 0)
		enqueueServiceAck(t, q, waitPos, 0, 1)
		require.Equal(t, 2, a.pollServiceControl())
		require.NoError(t, a.ServiceAckFailure())
		require.Equal(t, 1, a.serviceControlWork())
		require.Equal(t, 0, a.PendingServiceAckWaits())
		require.Equal(t, []int64{2, 1}, a.serviceAcks.nextAckIDs)
		require.Equal(t, int64(1), a.ExpectedServiceAckID())
		return a, q
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
			name: "truncate below the position removes the retired entry",
			run: func(t *testing.T) {
				a, q, waitPos := truncatedRetiredLeader(t, 1)
				require.Nil(t, a.abandonedSnapshotWaitAt(waitPos))

				require.NoError(t, a.RequestServiceAck(waitPos))
				ackAndPopWait(t, a, q, waitPos, 0)
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(1), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "two services, wait at the freed position after a truncate",
			run: func(t *testing.T) {
				a, q := twoServicesPoppedAfterTruncate(t)

				// Both services ack a later wait out of step.
				const laterPos int64 = 4096
				require.NoError(t, a.RequestServiceAck(laterPos))
				enqueueServiceAck(t, q, laterPos, 2, 0)
				enqueueServiceAck(t, q, laterPos, 1, 1)
				require.Equal(t, 2, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, []int64{3, 2}, a.serviceAcks.nextAckIDs)
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
			},
		},
		{
			name: "two services, stale id after a truncate marks",
			run: func(t *testing.T) {
				a, q := twoServicesPoppedAfterTruncate(t)

				// Service 0 acks with an id the pop consumed.
				const laterPos int64 = 4096
				require.NoError(t, a.RequestServiceAck(laterPos))
				enqueueServiceAck(t, q, laterPos, 1, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.ErrorIs(t, a.ServiceAckFailure(), ErrServiceAckOutOfSequence)
			},
		},
		{
			name: "snapshot action at the position after a truncate queues a wait",
			run: func(t *testing.T) {
				a, q, waitPos := truncatedRetiredLeader(t, 1)

				a.handleClusterAction(snapshotActionFields(waitPos), waitPos)
				require.Equal(t, []int64{waitPos}, a.serviceAckWaits)
				require.Equal(t, waitPos, a.snapshotWaitPosition)

				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, 0, a.PendingServiceAckWaits())
				require.Equal(t, int64(1), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
				requireTakeNotConfigured(t, a)
			},
		},
		{
			name: "truncate below a cancelled take removes the retired entry",
			run: func(t *testing.T) {
				// Phase 1: the wait pops and the take cancels.
				a, q, _ := settledLeaderWithControl(t, 1)
				require.NoError(t, a.RequestSnapshot(1000))
				waitPos := a.ExpectedAckPosition()
				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.True(t, a.HasPendingSnapshotAction())
				a.CloseSnapshotTake()
				require.NotNil(t, a.abandonedSnapshotWaitAt(waitPos))

				// Phase 2: TruncateLogEntry removes the cancelled take.
				a.SetLogTruncateControl(&fakeLogTruncateControl{})
				a.SetLogRecordingID(77)
				require.NoError(t, a.TruncateLogEntry(0, 0))
				require.Nil(t, a.abandonedSnapshotWaitAt(waitPos))

				// Phase 3: a snapshot action at the position queues and pops.
				a.handleClusterAction(snapshotActionFields(waitPos), waitPos)
				require.Equal(t, waitPos, a.snapshotWaitPosition)
				enqueueServiceAck(t, q, waitPos, 1, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.Equal(t, int64(2), a.ExpectedServiceAckID())
				require.NoError(t, a.ServiceAckFailure())
				requireTakeNotConfigured(t, a)
			},
		},
		{
			name: "truncate below an open take leaves the take for a later cancel",
			run: func(t *testing.T) {
				// Phase 1: the wait pops and the take stays open.
				a, q, _ := settledLeaderWithControl(t, 1)
				require.NoError(t, a.RequestSnapshot(1000))
				waitPos := a.ExpectedAckPosition()
				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 1, a.serviceControlWork())
				require.True(t, a.HasPendingSnapshotAction())

				// Phase 2: TruncateLogEntry does not cancel the open take.
				a.SetLogTruncateControl(&fakeLogTruncateControl{})
				a.SetLogRecordingID(77)
				require.NoError(t, a.TruncateLogEntry(0, 0))
				require.Nil(t, a.abandonedSnapshotWaitAt(waitPos))
				require.True(t, a.HasPendingSnapshotAction())

				// Phase 3: the cancel retires the take position.
				a.CloseSnapshotTake()
				require.NotNil(t, a.abandonedSnapshotWaitAt(waitPos))
				require.False(t, a.HasPendingSnapshotAction())
			},
		},
		{
			name: "second retire at the position after a truncate makes one entry",
			run: func(t *testing.T) {
				// Phase 1: both services owe an ack after the first retire.
				// TruncateLogEntry removes the retired action.
				a, q, waitPos := truncatedRetiredLeader(t, 2)

				// Phase 2: service 0 acks the new wait before the second retire.
				a.handleClusterAction(snapshotActionFields(waitPos), waitPos)
				require.Equal(t, waitPos, a.snapshotWaitPosition)
				enqueueServiceAck(t, q, waitPos, 0, 0)
				require.Equal(t, 1, a.pollServiceControl())
				_, prepared, err := a.PrepareForNewLeadership(0, 2)
				require.NoError(t, err)
				require.True(t, prepared)

				require.Equal(t, []abandonedSnapshotWait{
					{position: waitPos, owedAcks: []bool{false, true}},
				}, a.abandonedSnapshotWaits)

				enqueueServiceAck(t, q, waitPos, 0, 1)
				require.Equal(t, 1, a.pollServiceControl())
				require.Equal(t, 0, a.ServiceAckQueueDepth(1))
				require.Equal(t, int64(1), a.ExpectedServiceAckID())
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
	require.Equal(t, int64(2), a.serviceAcks.nextAckIDs[0])
	require.Empty(t, a.serviceAcks.droppedAckIDs[0])
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
	requireTakeNotConfigured(t, a)
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
			wantWaits: func(p int64) ([]int64, int64) { return []int64{p}, p },
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
			name: "leader replays its own retired action",
			setup: func(t *testing.T, a *Agent) int64 {
				p := leaderSnapshot(t, a)
				_, prepared, err := a.PrepareForNewLeadership(0, 1)
				require.NoError(t, err)
				require.True(t, prepared)
				a.RoleSet(RoleLeader)
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
				requireTakeNotConfigured(t, a)
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
	// ackAndPop pops the wait, then resolves its module take. The fixture
	// does not wire a recording control.
	ackAndPop := func(t *testing.T, f holdingFollower, ackID int64) {
		t.Helper()
		enqueueServiceAck(t, f.queue, f.actionEnd, ackID, 0)
		require.Equal(t, 1, f.agent.pollServiceControl())
		require.Equal(t, 1, f.agent.serviceControlWork())
		require.Equal(t, f.actionEnd, f.agent.holdLogPollBound(f.messageEnd+32))
		requireTakeNotConfigured(t, f.agent)
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
				catchupPoll(t, f.agent, f.messageEnd+32, nowNs)
				requireHeld(t, f)
			},
		},
		{
			name: "catchup hold below the limit",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				limit := f.messageEnd + 32
				for range 3 {
					catchupPoll(t, f.agent, limit, nowNs)
					requireHeld(t, f)
					require.Less(t, f.agent.CommitPosition(), limit)
					require.Nil(t, f.agent.abandonedSnapshotWaitAt(f.actionEnd))
				}

				ackAndPop(t, f, 0)
				catchupPoll(t, f.agent, limit, nowNs)
				requireMessageRead(t, f)
			},
		},
		{
			name: "catchup hold at the limit",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				for range 2 {
					catchupPoll(t, f.agent, f.actionEnd, nowNs)
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
			name: "retire during a hold",
			run: func(t *testing.T) {
				f := newHoldingFollower(t)
				f.agent.followerConsensusWork(nowNs)
				requireHeld(t, f)

				prepare(t, f.agent)
				require.Equal(t, int64(0), f.agent.snapshotWaitPosition)
				require.NotNil(t, f.agent.abandonedSnapshotWaitAt(f.actionEnd))

				catchupPoll(t, f.agent, f.messageEnd+32, nowNs)
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
				catchupPoll(t, f.agent, f.messageEnd+32, nowNs)
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

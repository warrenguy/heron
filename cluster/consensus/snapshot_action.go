package consensus

import (
	"errors"
	"slices"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

var _ clusterActionSink = (*Agent)(nil)

var (
	errSnapshotNotLeader       = errors.New("cluster: snapshot action requires a settled leader")
	errCoordinationOutstanding = errors.New("cluster: coordination point outstanding")
	errSnapshotActionPending   = errors.New("cluster: snapshot action pending")
	errSnapshotAppend          = errors.New("cluster: snapshot action append failed")
)

// RequestSnapshot appends one ClusterActionRequest (action SNAPSHOT, flags 0)
// to the cluster log. nowNs is the wire Timestamp. The log publisher assigns
// LogPosition. The method queues one coordination wait at LogPosition. The
// wait stores the current leadership term and nowNs for the module snapshot
// take.
//
// The leader snapshot hold starts with the wait. See [Agent.DoWork].
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the role is not leader
//   - an election is live
//   - serviceAckFailure is marked (returns that failure)
//   - a coordination wait is outstanding
//   - a pending JoinLog publish is outstanding
//   - a module snapshot take is in flight ([errSnapshotActionPending])
//   - the log publisher is nil or unbound
//   - the append fails
func (a *Agent) RequestSnapshot(nowNs int64) error {
	if a.role != RoleLeader || a.election != nil {
		return errSnapshotNotLeader
	}
	if a.serviceAckFailure != nil {
		return a.serviceAckFailure
	}
	if len(a.serviceAckWaits) > 0 || a.pendingJoinLog != nil {
		return errCoordinationOutstanding
	}
	if a.snapshotTake != nil {
		return errSnapshotActionPending
	}
	pos, ok := a.logPublisher.AppendClusterAction(
		a.leadershipTermID,
		nowNs,
		clusterwire.ClusterAction_SNAPSHOT,
		0,
	)
	if !ok {
		return errSnapshotAppend
	}
	a.queueSnapshotWait(pos, a.leadershipTermID, nowNs)
	return nil
}

// HasPendingSnapshotAction reports whether a snapshot action wait is queued,
// or whether the module snapshot take of a popped wait is unresolved.
//
// Behaviour:
//   - The leader queues the wait at append.
//   - Any role queues the wait when its log adapter consumes the action. A
//     leader consumes only in its election replay. The action may come from
//     an earlier leadership term.
//   - The pop of the wait builds the module snapshot take. The report stays
//     true until the take resolves.
//
// Concurrency: invoker-only.
//
// Returns false once the take resolves. PrepareForNewLeadership retires the
// wait, cancels the take and clears the pending report. An election join
// and [Agent.CloseSnapshotTake] cancel the take too.
func (a *Agent) HasPendingSnapshotAction() bool {
	return a.snapshotHoldPosition() != 0
}

// handleClusterAction queues a snapshot wait for a committed
// ClusterActionRequest. logPosition is the frame end.
//
// Behaviour: the method queues one wait at logPosition. Any skip below
// changes nothing:
//   - Action is not SNAPSHOT, or Flags is not 0.
//   - Wire LogPosition differs from logPosition. No service acks such a frame.
//   - A snapshot wait pends. One field holds one snapshot wait.
//   - logPosition is a retired snapshot position. PrepareForNewLeadership
//     retires the wait. A cancel after the pop retires the take.
//     TruncateLogEntry at a lower position removes the entry.
//   - serviceAckFailure is marked. No wait completes after the mark.
//
// No role check. A leader reads its adapter only in its election replay. Its
// services ack each action they replay, so the leader needs the wait too. A
// leader's own action either popped its wait at or below commit, where no
// replay reads it, or PrepareForNewLeadership retired the wait.
//
// No leadership term check. Services ack every SNAPSHOT action with flags 0,
// whatever its term.
//
// Concurrency: invoker-only. Same goroutine as [LogAdapter.Poll].
func (a *Agent) handleClusterAction(
	fields codecs.ClusterActionRequestFields,
	logPosition int64,
) {
	switch {
	case fields.Action != clusterwire.ClusterAction_SNAPSHOT || fields.Flags != 0:
	case fields.LogPosition != logPosition:
	case a.snapshotWaitPosition != 0:
	case a.abandonedSnapshotWaitAt(logPosition) != nil:
	case a.serviceAckFailure != nil:
	default:
		a.queueSnapshotWait(logPosition, fields.LeadershipTermID, fields.Timestamp)
	}
}

// queueSnapshotWait queues one snapshot wait at logPosition. The wait stores
// the action's leadership term and wire timestamp.
func (a *Agent) queueSnapshotWait(logPosition, leadershipTermID, timestamp int64) {
	a.serviceAckWaits = append(a.serviceAckWaits, logPosition)
	a.snapshotWaitPosition = logPosition
	a.snapshotWaitLeadershipTermID = leadershipTermID
	a.snapshotWaitTimestamp = timestamp
}

// holdLogPollBound returns boundPosition lowered by each pending hold. Every
// module log poll bound passes through this method.
//
// Behaviour:
//   - A pending snapshot wait lowers the bound to its log position. After
//     the pop, the module snapshot take lowers the bound to the same
//     position until the take resolves.
//   - On a non-leader, a pending JoinLog publish lowers the bound to the
//     JoinLog position. The poll consumes no frame past that position until
//     the publish succeeds, so follower commit stays there.
//
// The JoinLog hold keeps a snapshot wait from queueing ahead of the JoinLog
// wait. The service acks the JoinLog before any later snapshot action.
func (a *Agent) holdLogPollBound(boundPosition int64) int64 {
	if hold := a.snapshotHoldPosition(); hold != 0 {
		boundPosition = min(boundPosition, hold)
	}
	pending := a.pendingJoinLog
	if pending != nil && a.role != RoleLeader {
		boundPosition = min(boundPosition, pending.LogPosition)
	}
	return boundPosition
}

// abandonedSnapshotWait is one snapshot wait retired by
// PrepareForNewLeadership, or one module snapshot take cancelled after its
// pop.
type abandonedSnapshotWait struct {
	position int64

	// owedAcks has one flag per service. A set flag means the service still
	// owes an ack at position. retireSnapshotWait sets the flags. The owed
	// ack clears its flag.
	owedAcks []bool
}

// abandonedSnapshotWaitAt returns the retired wait at logPosition, or nil.
func (a *Agent) abandonedSnapshotWaitAt(logPosition int64) *abandonedSnapshotWait {
	for i := range a.abandonedSnapshotWaits {
		wait := &a.abandonedSnapshotWaits[i]
		if wait.position == logPosition {
			return wait
		}
	}
	return nil
}

// abandonSnapshotPosition returns the retired wait at logPosition. When the
// position is not retired, it appends an entry that does not owe an ack.
func (a *Agent) abandonSnapshotPosition(logPosition int64) *abandonedSnapshotWait {
	if retired := a.abandonedSnapshotWaitAt(logPosition); retired != nil {
		return retired
	}
	a.abandonedSnapshotWaits = append(a.abandonedSnapshotWaits, abandonedSnapshotWait{
		position: logPosition,
		owedAcks: make([]bool, len(a.serviceAcks.queues)),
	})
	return &a.abandonedSnapshotWaits[len(a.abandonedSnapshotWaits)-1]
}

// retireSnapshotWait drops a leftover snapshot wait from the FIFO. The wait
// joins abandonedSnapshotWaits.
//
// Behaviour:
//   - clears snapshotWaitPosition
//   - drops each tracker entry at the position through
//     [serviceAckTracker.dropAt], which notes each dropped ack id
//   - marks each service with no dropped entry as owing one ack at the
//     position
func (a *Agent) retireSnapshotWait() {
	pos := a.snapshotWaitPosition
	a.snapshotWaitPosition = 0
	if pos == 0 {
		return
	}
	retired := a.abandonSnapshotPosition(pos)
	a.serviceAckWaits = slices.DeleteFunc(a.serviceAckWaits, func(wait int64) bool {
		return wait == pos
	})
	owed := retired.owedAcks
	for sid := range owed {
		owed[sid] = true
	}
	a.serviceAcks.dropAt(pos, func(serviceID int32) {
		owed[serviceID] = false
	})
}

// dropAbandonedSnapshotAck reports whether handleServiceAck drops an ack from
// serviceID at logPosition. Reports false at a position that is not retired.
// At a retired position the ack drops when either holds:
//   - serviceID owes an ack there. The call clears that owed flag. A queued
//     wait at the position does not keep the ack.
//   - No queued wait uses the position.
func (a *Agent) dropAbandonedSnapshotAck(serviceID int32, logPosition int64) bool {
	retired := a.abandonedSnapshotWaitAt(logPosition)
	if retired == nil {
		return false
	}
	if a.serviceAcks.hasService(serviceID) && retired.owedAcks[serviceID] {
		retired.owedAcks[serviceID] = false
		return true
	}
	return !slices.Contains(a.serviceAckWaits, logPosition)
}

// removeAbandonedSnapshotWaitsAbove removes retired waits and cancelled takes
// above logPosition. TruncateLogEntry calls removeAbandonedSnapshotWaitsAbove
// after the archive drops the actions above logPosition. A later snapshot
// action at a removed position queues a wait.
//
// An election truncate never drops a committed frame. A service reads only
// committed frames, so no service acks a removed action. handleServiceAck
// offers an ack at a removed position and does not drop it.
func (a *Agent) removeAbandonedSnapshotWaitsAbove(logPosition int64) {
	a.abandonedSnapshotWaits = slices.DeleteFunc(
		a.abandonedSnapshotWaits,
		func(retired abandonedSnapshotWait) bool {
			return retired.position > logPosition
		},
	)
}

// dropAbandonedSnapshotLeftovers removes leftover ServiceAck tracker entries
// at a consumed abandoned snapshot position. [serviceAckTracker.dropAt] notes
// each dropped ack id.
//
// A remaining wait at that position keeps the entries.
func (a *Agent) dropAbandonedSnapshotLeftovers(consumed int64) {
	if a.abandonedSnapshotWaitAt(consumed) == nil {
		return
	}
	if slices.Contains(a.serviceAckWaits, consumed) {
		return
	}
	a.serviceAcks.dropAt(consumed, nil)
}

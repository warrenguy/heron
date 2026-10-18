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
// LogPosition. The method queues one coordination wait at LogPosition.
//
// Concurrency: invoker-only.
//
// Returns an error when:
//   - the role is not leader
//   - an election is live
//   - a coordination wait is outstanding
//   - a pending JoinLog publish is outstanding
//   - the log publisher is nil or unbound
//   - the append fails
func (a *Agent) RequestSnapshot(nowNs int64) error {
	if a.role != RoleLeader || a.election != nil {
		return errSnapshotNotLeader
	}
	if len(a.serviceAckWaits) > 0 || a.pendingJoinLog != nil {
		return errCoordinationOutstanding
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
	a.queueSnapshotWait(pos)
	return nil
}

// HasPendingSnapshotAction reports whether a snapshot action wait is queued.
//
// Behaviour:
//   - The leader queues the wait at append.
//   - A follower queues the wait when its log adapter consumes the action.
//     The action may come from an earlier leadership term.
//
// Concurrency: invoker-only.
//
// Returns false after the wait completes. PrepareForNewLeadership retires the
// wait and clears the pending report.
func (a *Agent) HasPendingSnapshotAction() bool {
	return a.snapshotWaitPosition != 0
}

// handleClusterAction queues a follower snapshot wait for a committed
// ClusterActionRequest. logPosition is the frame end.
//
// Behaviour: the method queues one wait at logPosition. Any skip below
// changes nothing:
//   - Action is not SNAPSHOT, or Flags is not 0.
//   - Wire LogPosition differs from logPosition. No service acks such a frame.
//   - The role is leader. The leader queues its wait at append.
//   - A snapshot wait pends. One field holds one snapshot wait.
//   - logPosition is a retired snapshot position. PrepareForNewLeadership
//     retired that wait.
//   - serviceAckFailure is marked. No wait completes after the mark.
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
	case a.role == RoleLeader:
	case a.snapshotWaitPosition != 0:
	case slices.Contains(a.abandonedSnapshotPositions, logPosition):
	case a.serviceAckFailure != nil:
	default:
		a.queueSnapshotWait(logPosition)
	}
}

func (a *Agent) queueSnapshotWait(logPosition int64) {
	a.serviceAckWaits = append(a.serviceAckWaits, logPosition)
	a.snapshotWaitPosition = logPosition
}

// holdLogPollBound returns boundPosition lowered by each pending hold. Every
// module log poll bound passes through this method.
//
// Behaviour:
//   - A pending snapshot wait lowers the bound to its log position.
//   - On a non-leader, a pending JoinLog publish lowers the bound to the
//     JoinLog position. The poll consumes no frame past that position until
//     the publish succeeds, so follower commit stays there.
//
// The JoinLog hold keeps a snapshot wait from queueing ahead of the JoinLog
// wait. The service acks the JoinLog before any later snapshot action.
func (a *Agent) holdLogPollBound(boundPosition int64) int64 {
	if a.snapshotWaitPosition != 0 {
		boundPosition = min(boundPosition, a.snapshotWaitPosition)
	}
	pending := a.pendingJoinLog
	if pending != nil && a.role != RoleLeader {
		boundPosition = min(boundPosition, pending.LogPosition)
	}
	return boundPosition
}

// retireSnapshotWait drops a leftover snapshot wait from the FIFO. The method
// adds the wait log position to the retired positions. Earlier retired
// positions stay. A later ServiceAck at any retired position is dropped
// unless a new wait uses the same position.
//
// Already queued tracker entries at the retired position are dropped. Dropped
// ack ids are noted. serviceAckID advances when every service has dropped the
// current expected id.
func (a *Agent) retireSnapshotWait() {
	pos := a.snapshotWaitPosition
	a.snapshotWaitPosition = 0
	if pos == 0 {
		return
	}
	if !slices.Contains(a.abandonedSnapshotPositions, pos) {
		a.abandonedSnapshotPositions = append(a.abandonedSnapshotPositions, pos)
	}
	a.serviceAckWaits = slices.DeleteFunc(a.serviceAckWaits, func(wait int64) bool {
		return wait == pos
	})
	a.serviceAcks.dropAt(pos, func(serviceID int32, ack serviceAck) {
		a.noteAbandonedSnapshotAckID(serviceID, ack.ackID)
	})
	a.advancePastAbandonedSnapshotAcks()
}

func (a *Agent) isAbandonedSnapshotAck(logPosition int64) bool {
	if !slices.Contains(a.abandonedSnapshotPositions, logPosition) {
		return false
	}
	if len(a.serviceAckWaits) > 0 && a.serviceAckWaits[0] == logPosition {
		return false
	}
	return true
}

// dropAbandonedSnapshotLeftovers removes leftover ServiceAck tracker entries
// at a consumed abandoned snapshot position. Dropped ack ids are noted.
// serviceAckID advances when every service has dropped the current expected id.
//
// A remaining wait at that position keeps the entries. The first queued wait
// at the retired position must still accept one ack.
func (a *Agent) dropAbandonedSnapshotLeftovers(consumed int64) {
	if !slices.Contains(a.abandonedSnapshotPositions, consumed) {
		return
	}
	if slices.Contains(a.serviceAckWaits, consumed) {
		return
	}
	a.serviceAcks.dropAt(consumed, func(serviceID int32, ack serviceAck) {
		a.noteAbandonedSnapshotAckID(serviceID, ack.ackID)
	})
	a.advancePastAbandonedSnapshotAcks()
}

func (a *Agent) noteAbandonedSnapshotAckID(serviceID int32, ackID int64) {
	if !a.serviceAcks.hasService(serviceID) {
		return
	}
	count := len(a.serviceAcks.queues)
	if len(a.abandonedSnapshotAckIDs) != count {
		a.abandonedSnapshotAckIDs = make([][]int64, count)
	}
	sid := int(serviceID)
	ids := a.abandonedSnapshotAckIDs[sid]
	if slices.Contains(ids, ackID) {
		return
	}
	a.abandonedSnapshotAckIDs[sid] = append(ids, ackID)
}

func (a *Agent) advancePastAbandonedSnapshotAcks() {
	for a.everyServiceAbandonedAckID(a.serviceAckID) {
		a.serviceAckID++
	}
}

func (a *Agent) everyServiceAbandonedAckID(ackID int64) bool {
	count := len(a.serviceAcks.queues)
	if count == 0 || len(a.abandonedSnapshotAckIDs) != count {
		return false
	}
	for sid := range count {
		if !slices.Contains(a.abandonedSnapshotAckIDs[sid], ackID) {
			return false
		}
	}
	return true
}

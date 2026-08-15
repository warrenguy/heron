package consensus

import (
	"errors"
	"slices"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

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
	a.serviceAckWaits = append(a.serviceAckWaits, pos)
	a.snapshotWaitPosition = pos
	return nil
}

// HasPendingSnapshotAction reports whether a snapshot action wait is queued
// in the current leadership term.
//
// Concurrency: invoker-only.
//
// Returns false after the wait completes. PrepareForNewLeadership retires the
// wait and clears the pending report.
func (a *Agent) HasPendingSnapshotAction() bool {
	return a.snapshotWaitPosition != 0
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

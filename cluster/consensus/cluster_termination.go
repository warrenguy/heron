package consensus

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// DefaultTerminationTimeoutNs caps the leader's wait for follower termination
// acks from TERMINATING entry. 10s matches upstream.
const DefaultTerminationTimeoutNs int64 = 10_000_000_000

// ErrClusterTerminated is the cause of a planned termination: a SHUTDOWN or
// ABORT toggle on this member or on its leader.
var ErrClusterTerminated = errors.New("consensus: cluster terminated")

var (
	errTerminationPositionSend   = errors.New("consensus: send termination position")
	errServiceTerminationSend    = errors.New("consensus: send service termination position")
	errTerminationAckTermChanged = errors.New(
		"consensus: termination ack not sent: leadership term changed",
	)
	errTerminationCommit     = errors.New("consensus: commit termination log position")
	errTerminationInProgress = errors.New("consensus: termination in progress")
)

// TerminationRecordingLog commits the log position of a term at a planned
// termination.
type TerminationRecordingLog interface {
	CommitLogPosition(leadershipTermID, logPosition int64) error
}

var _ TerminationRecordingLog = (*recordinglog.Log)(nil)

// serviceTerminationStep tracks the ServiceTerminationPosition publish and
// its ack wait.
type serviceTerminationStep uint8

const (
	serviceTerminationIdle serviceTerminationStep = iota
	serviceTerminationPublishPending
	serviceTerminationWaitQueued
)

// clusterTermination collects the follower termination acks on a leader.
type clusterTermination struct {
	deadlineNs       int64
	awaitingServices bool
	peerIDs          []int32
	terminated       []bool
}

func newClusterTermination(peerIDs []int32) *clusterTermination {
	return &clusterTermination{
		awaitingServices: true,
		peerIDs:          peerIDs,
		terminated:       make([]bool, len(peerIDs)),
	}
}

// markTerminated records memberID. Returns false when memberID is not a peer.
func (t *clusterTermination) markTerminated(memberID int32) bool {
	i := slices.Index(t.peerIDs, memberID)
	if i < 0 {
		return false
	}

	t.terminated[i] = true
	return true
}

func (t *clusterTermination) servicesTerminated() {
	t.awaitingServices = false
}

func (t *clusterTermination) canTerminate(nowNs int64) bool {
	if t == nil || t.awaitingServices {
		return false
	}

	return !slices.Contains(t.terminated, false) || nowNs >= t.deadlineNs
}

// SetTerminationRecordingLog sets the log that commits the termination
// position. Nil skips the commit.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines. Do not call while DoWork runs.
func (a *Agent) SetTerminationRecordingLog(log TerminationRecordingLog) {
	a.terminationRecordingLog = log
}

// TerminationPosition returns the stored termination position, or -1.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines.
func (a *Agent) TerminationPosition() int64 {
	return a.terminationPosition
}

// StopServicesAtCommit publishes ServiceTerminationPosition at the commit
// position. The call does not queue a wait. It reports a failed send unless
// the publication is closed.
//
// Concurrency: call from the goroutine that runs [Agent.DoWork]. Not safe from
// other goroutines. Do not call while DoWork runs.
func (a *Agent) StopServicesAtCommit() {
	if len(a.serviceAcks.queues) == 0 || a.serviceProxy == nil {
		return
	}

	pos := a.serviceProxy.PublishServiceTerminationPosition(a.CommitPosition(), false)
	if pos <= 0 && pos != aeron.Closed {
		a.reportError(fmt.Errorf("%w: code=%d", errServiceTerminationSend, pos))
	}
}

// startClusterTermination stores the publisher position and sends it to each
// peer. Returns false when the log publication is unbound or closed.
func (a *Agent) startClusterTermination() bool {
	pub := a.logPublisher
	if pub == nil || pub.Publication() == nil {
		return false
	}

	position := pub.Position()
	if position < 0 {
		return false
	}

	a.terminationPosition = position
	a.terminationLeadershipTermID = a.leadershipTermID

	var peers []int32
	if a.egress != nil {
		peers = a.egress.PeerMemberIDs()
	}

	a.clusterTermination = newClusterTermination(peers)

	for _, id := range peers {
		if !a.egress.PublishTerminationPosition(id, a.leadershipTermID, position) {
			a.reportError(fmt.Errorf("%w: member %d", errTerminationPositionSend, id))
		}
	}

	return true
}

// terminateServices enters TERMINATING on a leader. It starts the follower ack
// deadline.
func (a *Agent) terminateServices(reason string, nowNs int64) {
	a.setModuleState(ModuleStateTerminating, reason)

	deadline := nowNs + a.terminationTimeoutNs
	if deadline < nowNs {
		deadline = math.MaxInt64
	}

	a.clusterTermination.deadlineNs = deadline

	if len(a.serviceAcks.queues) == 0 {
		a.clusterTermination.servicesTerminated()
		return
	}

	a.beginServiceTermination()
}

func (a *Agent) beginServiceTermination() {
	a.serviceTermination = serviceTerminationPublishPending
	a.publishServiceTermination()
}

// retryServiceTermination offers a pending ServiceTerminationPosition.
func (a *Agent) retryServiceTermination() int {
	if a.serviceTermination != serviceTerminationPublishPending {
		return 0
	}

	return a.publishServiceTermination()
}

// publishServiceTermination offers ServiceTerminationPosition once no JoinLog
// publish is pending. The caller sets the publish step to pending.
//
// Returns 1 when the offer completes, else 0.
func (a *Agent) publishServiceTermination() int {
	if a.pendingJoinLog != nil || a.serviceProxy == nil {
		return 0
	}

	pos := a.serviceProxy.PublishServiceTerminationPosition(a.terminationPosition, true)
	if pos > 0 {
		a.serviceAckWaits = append(a.serviceAckWaits, a.terminationPosition)
		a.serviceTermination = serviceTerminationWaitQueued
		return 1
	}

	if aeron.IsOfferRetriable(pos) {
		return 0
	}

	a.serviceTermination = serviceTerminationIdle
	a.markServicePublishFailure(pos)
	return 0
}

func (a *Agent) markServicePublishFailure(code int64) {
	if a.servicePublishFailure != nil {
		return
	}

	a.servicePublishFailure = fmt.Errorf("%w: code=%d", ErrServicePublish, code)
	a.failMember(a.servicePublishFailure)
}

// endSnapshotTake ends SNAPSHOT after a take, failed or not. On SHUTDOWN, a
// leader that has not terminated enters TERMINATING instead.
func (a *Agent) endSnapshotTake(nowNs int64) {
	if a.clusterTermination == nil || a.role != RoleLeader || a.termination != nil {
		a.endSnapshotState(moduleStateReasonSnapshotComplete)
		return
	}

	a.terminateServices(moduleStateReasonTerminationAfterSnapshot, nowNs)
}

func (a *Agent) terminationTick(nowNs int64) int {
	if a.moduleState != ModuleStateTerminating || !a.clusterTermination.canTerminate(nowNs) {
		return 0
	}

	a.finishTermination(a.terminationPosition)
	return 1
}

func (a *Agent) finishTermination(logPosition int64) {
	if a.moduleState != ModuleStateTerminating {
		return
	}

	if a.terminationRecordingLog != nil {
		err := a.terminationRecordingLog.CommitLogPosition(a.leadershipTermID, logPosition)
		if err != nil {
			a.reportError(fmt.Errorf("%w: %w", errTerminationCommit, err))
		}
	}

	a.terminate(ErrClusterTerminated, true, moduleStateReasonExpectedTermination)
}

func (a *Agent) handleServicesTerminated(reach int64) {
	if a.clusterTermination != nil {
		a.clusterTermination.servicesTerminated()
		return
	}

	a.followerDoTermination(reach)
}

func (a *Agent) followerDoTermination(logPosition int64) {
	switch {
	case a.terminationLeadershipTermID != a.leadershipTermID:
		a.reportError(errTerminationAckTermChanged)

	case a.egress != nil:
		_ = a.egress.PublishTerminationAck(a.leadershipTermID, logPosition)
	}

	a.finishTermination(logPosition)
}

func (a *Agent) hasReachedTerminationPosition() bool {
	return a.terminationPosition != nullValue &&
		a.logAdapter != nil &&
		a.logAdapter.Position() >= a.terminationPosition
}

func (a *Agent) terminateFollower() {
	a.setModuleState(ModuleStateTerminating, moduleStateReasonTerminationPosition)

	if len(a.serviceAcks.queues) == 0 {
		a.followerDoTermination(a.logAdapter.Position())
		return
	}

	a.beginServiceTermination()
}

func (a *Agent) handleTerminationPosition(fields codecs.TerminationPositionFields, nowNs int64) {
	if a.role != RoleFollower || fields.LeadershipTermID != a.leadershipTermID {
		return
	}

	a.terminationPosition = fields.LogPosition
	a.terminationLeadershipTermID = fields.LeadershipTermID
	a.timeOfLastLogUpdateNs = nowNs
}

func (a *Agent) handleTerminationAck(fields codecs.TerminationAckFields) {
	if a.role != RoleLeader || a.clusterTermination == nil {
		return
	}

	if fields.LeadershipTermID != a.leadershipTermID || fields.LogPosition < a.terminationPosition {
		return
	}

	a.clusterTermination.markTerminated(fields.MemberID)
}

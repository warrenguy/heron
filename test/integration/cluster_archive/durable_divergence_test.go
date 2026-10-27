//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// termBaseLogPosition returns the term-base log position for leadershipTermID
// from m's recording.log, or recordinglog.NullPosition when no entry matches.
func termBaseLogPosition(m *cluster.ConsensusModule, leadershipTermID int64) int64 {
	if m == nil || m.RecordingLog() == nil {
		return recordinglog.NullPosition
	}
	e := m.RecordingLog().FindTermEntry(leadershipTermID)
	if e == nil {
		return recordinglog.NullPosition
	}
	return e.TermBaseLogPosition
}

// lastTermLogPosition returns LogPosition of the highest valid TERM entry, or
// recordinglog.NullPosition when the log holds none.
//
// Entries are comparator-sorted by leadershipTermID ascending. Reverse scan
// hits the highest term first.
func lastTermLogPosition(m *cluster.ConsensusModule) int64 {
	if m == nil || m.RecordingLog() == nil {
		return recordinglog.NullPosition
	}
	entries := m.RecordingLog().Entries()
	for _, e := range slices.Backward(entries) {
		if e.Valid && e.Type == recordinglog.EntryTypeTerm {
			return e.LogPosition
		}
	}
	return recordinglog.NullPosition
}

// followersRecordingAtMost reports every live non-skipIndex member's type-100
// counter is at most floor.
func followersRecordingAtMost(
	set *harness.ClusterMemberSet,
	skipIndex int,
	floor int64,
) bool {
	for i, m := range set.Modules() {
		if m == nil || i == skipIndex {
			continue
		}
		if harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()) > floor {
			return false
		}
	}
	return true
}

// tickMemberStreams runs transport and archive polls over m only. Omits
// Agent.DoWork, so no consensus duty runs.
//
// Use to advance one member's recording while peers stay frozen. A settled
// leader that runs Agent.DoWork against frozen peers loses its active quorum
// after LeaderHeartbeatTimeoutNs and re-enters election. The recording
// high-water is an archive fact and needs no duty cycle to advance.
func tickMemberStreams(m *cluster.ConsensusModule) {
	m.Client().DoWork()
	m.ArchiveClient().Poll()
}

// pumpLiveTransport runs Client.DoWork on every live member (conductor
// service). Omits Agent.DoWork, archive Poll, and log-adapter Poll.
//
// Does not advance peer image positions. Call drainPeerLogImages to reopen
// the leader publication limit without Agent.DoWork on peers.
func pumpLiveTransport(set *harness.ClusterMemberSet) {
	for _, m := range set.Modules() {
		if m == nil {
			continue
		}
		m.Client().DoWork()
	}
}

// peerLogDrainBound is the stream-position upper bound for gap-build peer image
// BoundedControlledPoll. Each call also stops at
// consensus.DefaultLogFragmentLimit. The large bound is not an unlimited single
// poll.
const peerLogDrainBound int64 = 1 << 30

// discardLogFragment is a ControlledFragmentHandler that returns
// ControlledContinue with no decode. Subscriber position still advances.
//
// Matches nil LogAdapterHandler on LogAdapter.Poll: position advances without
// decode.
func discardLogFragment(
	_ atomicbuffer.AtomicBuffer,
	_ int,
	_ int,
	_ *aeron.Header,
) aeron.ControlledAction {
	return aeron.ControlledContinue
}

// unbindPeerLogImages clamps each non-skipIndex log adapter at floor and
// returns the Images those adapters held.
//
// DisconnectAt clears the adapter binding and lowers the reported position.
// DisconnectAt does not own the Image or its Subscription. Both stay open so
// drainPeerLogImages can advance the driver subscriber position and reopen the
// leader publication limit.
//
// Call before the gap-build loop. Adapter position then stays at floor.
// ProposeMaxCommit on a later survivor Agent.DoWork must not raise commit into
// the election seed from a drained consumer position.
func unbindPeerLogImages(
	set *harness.ClusterMemberSet,
	skipIndex int,
	floor int64,
) []*aeron.Image {
	var images []*aeron.Image
	for i, m := range set.Modules() {
		if m == nil || i == skipIndex {
			continue
		}
		adapter := m.LogAdapter()
		if adapter == nil {
			continue
		}
		if image := adapter.Image(); image != nil {
			images = append(images, image)
		}
		adapter.DisconnectAt(floor)
	}
	return images
}

// drainPeerLogImages polls each unbound peer Image. Image positions advance so
// the leader publication limit can reopen.
//
// Omits Agent.DoWork. Does not re-elect, emit heartbeats, or run consensus duty.
// Follower archive recordings stay stopped. Only image consume moves. Adapter
// positions stay at floor because no adapter holds these Images.
func drainPeerLogImages(images []*aeron.Image) {
	for _, image := range images {
		image.BoundedControlledPoll(
			discardLogFragment,
			peerLogDrainBound,
			consensus.DefaultLogFragmentLimit,
		)
	}
}

// exLeaderMinTail is the leader-only tail the re-election test builds past
// the follower floor. One post-election append plus its header must land
// below the old tail, so the re-elected ex-leader extends above the append
// position.
const exLeaderMinTail int64 = 16 << 10

// logJoinPositionMismatch is the election error for a log image that joined
// off the leader's term base.
const logJoinPositionMismatch = "election: log join position mismatch"

// divergedLeader holds the outcome of divergeAndReElect.
type divergedLeader struct {
	// index is the slot of the divergent ex-leader.
	index int

	// newTerm is the survivors' leadership term.
	newTerm int64

	// newTermBase is the survivors' term base and the ex-leader's truncate
	// position.
	newTermBase int64
}

// truncatedExLeader holds the outcome of truncateRestartedLeader.
type truncatedExLeader struct {
	divergedLeader

	// planAppended is the ex-leader's catalog stop before the truncate. The
	// first restart opens the ex-leader log publication there.
	planAppended int64
}

// divergeAndReElect stops the leader after it builds a leader-only log tail.
// The ex-leader slot stays empty.
//
// Behaviour:
//   - the leader records at least minTail bytes past the follower floor
//   - the survivors re-elect under a term base below that tail
func divergeAndReElect(
	t *testing.T,
	set *harness.ClusterMemberSet,
	minTail int64,
) divergedLeader {
	t.Helper()

	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()
	require.NotEqual(
		t,
		int64(-1),
		leader.Agent().LocalLogRecordingID(),
		"leader LocalLogRecordingID:\n%s",
		set.State(),
	)

	// Drive a shared commit floor before the divergent high-water.
	floorPos := mustAppend(t, set, leader, "pre-divergence-floor")

	requireAwaitTick(t, set, "pre-divergence commit floor", durableWorkBound, func() bool {
		return membersReachCommit(set, floorPos)
	})

	// Stop follower log recordings so further leader appends raise only the
	// leader's catalog high-water. Survivors re-elect after the leader stops.
	// The new term base sits at the shared floor, below the stopped leader's
	// recorded high-water.
	followerFloor := floorPos
	for i, m := range set.Modules() {
		if m == nil || i == leaderIndex {
			continue
		}
		followerFloor = max(
			followerFloor,
			harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()),
		)
		// PrepareForNewLeadership stops the recording and returns the catalog
		// stop. Leaves settled election state alone. Survivors re-enter an
		// election from the STOP mark after the leader stops. Pump through
		// stream ticks until the stop proof is held.
		deadline := time.Now().Add(durableWorkBound)
		var stopPos int64
		for {
			pos, prepared, prepErr := m.Agent().PrepareForNewLeadership(
				m.CommitPosition(),
				m.NanoClock()(),
			)
			if prepErr != nil {
				// The if keeps set.State() off the passing path.
				require.NoError(t, prepErr, "follower %d PrepareForNewLeadership:\n%s", i, set.State())
			}
			if prepared {
				stopPos = pos
				break
			}
			if !time.Now().Before(deadline) {
				require.FailNow(
					t,
					"follower prepare stop pending",
					"follower %d after %s:\n%s",
					i,
					durableWorkBound,
					set.State(),
				)
			}
			tickMemberStreams(m)
		}
		require.False(
			t,
			m.Agent().IsRecordingActive(),
			"follower %d still recording after prepare:\n%s",
			i,
			set.State(),
		)
		followerFloor = max(followerFloor, stopPos)
	}

	// Unbind peer log adapters at floor before the drain loop. Adapter position
	// cannot rise from a drained consumer position. No survivor Agent.DoWork can
	// raise commit into the election seed. Images stay open for drain. Truncate
	// premise stays newTermBase < divergentHighWater.
	peerImages := unbindPeerLogImages(set, leaderIndex, followerFloor)

	divergentHighWater := buildDivergentTail(
		t,
		set,
		leaderIndex,
		followerFloor,
		minTail,
		peerImages,
	)
	t.Logf(
		"divergent high-water=%d followerFloor=%d preTerm=%d",
		divergentHighWater,
		followerFloor,
		preTerm,
	)

	// Stop the divergent leader. Survivors re-elect under a lower term base.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))

	requireAwaitTick(
		t, set, "survivors re-elect after leader stop", durableReElectBound,
		func() bool {
			ls := set.Leaders()
			if len(ls) != 1 {
				return false
			}
			nl := ls[0]
			return nl.ThisMemberID() != int32(leaderIndex) &&
				nl.LeadershipTermID() > preTerm &&
				nl.ElectionComplete()
		},
	)

	newLeader := requireSingleLeader(t, set, "after re-elect")
	newTerm := newLeader.LeadershipTermID()
	require.Greater(t, newTerm, preTerm, "new term after re-elect:\n%s", set.State())
	require.NotEqual(
		t,
		int32(leaderIndex),
		newLeader.ThisMemberID(),
		"new leader must be a survivor:\n%s",
		set.State(),
	)

	newTermBase := termBaseLogPosition(newLeader, newTerm)
	require.NotEqual(
		t,
		recordinglog.NullPosition,
		newTermBase,
		"new leader term base missing for term %d:\n%s",
		newTerm,
		set.State(),
	)
	require.Less(
		t,
		newTermBase,
		divergentHighWater,
		"new term base=%d must be below divergent high-water=%d:\n%s",
		newTermBase,
		divergentHighWater,
		set.State(),
	)
	return divergedLeader{
		index:       leaderIndex,
		newTerm:     newTerm,
		newTermBase: newTermBase,
	}
}

// restartDivergedLeader restarts the ex-leader once. Returns its catalog stop.
// Fails the test unless that stop sits above the new term base.
func restartDivergedLeader(
	t *testing.T,
	set *harness.ClusterMemberSet,
	diverged divergedLeader,
) int64 {
	t.Helper()

	leaderIndex := diverged.index
	newTermBase := diverged.newTermBase
	require.NoError(t, set.RestartMember(leaderIndex))
	returning := set.Module(leaderIndex)
	require.NotNil(t, returning, "RestartMember left slot empty:\n%s", set.State())
	require.False(t, returning.IsColdStart(), "returning cold start:\n%s", set.State())

	recordingID := returning.RecordingLog().TermRecordingID()
	require.NotEqual(
		t,
		int64(-1),
		recordingID,
		"returning TermRecordingID:\n%s",
		set.State(),
	)
	desc, err := returning.ArchiveClient().ListRecording(
		context.Background(),
		recordingID,
	)
	require.NoError(t, err, "ListRecording before truncate:\n%s", set.State())
	planAppended := desc.StopPosition
	require.Greater(
		t,
		planAppended,
		newTermBase,
		"plan/recorded append position must be above new term base before truncate; "+
			"planAppended=%d newTermBase=%d recordingID=%d:\n%s",
		planAppended,
		newTermBase,
		recordingID,
		set.State(),
	)
	t.Logf(
		"pre-truncate divergence planAppended=%d newTermBase=%d recordingID=%d",
		planAppended,
		newTermBase,
		recordingID,
	)
	return planAppended
}

// truncateRestartedLeader restarts the ex-leader once. Returns after the
// ex-leader truncates to the new term base in process.
func truncateRestartedLeader(
	t *testing.T,
	set *harness.ClusterMemberSet,
	diverged divergedLeader,
) truncatedExLeader {
	t.Helper()

	leaderIndex := diverged.index
	newTermBase := diverged.newTermBase
	planAppended := restartDivergedLeader(t, set, diverged)

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	// Wait until truncate lands three independent ways: agent stop position,
	// archive catalog stop, and last term LogPosition.
	requireAwaitTick(t, set, "truncate to new term base", durableWorkBound, func() bool {
		m := set.Module(leaderIndex)
		if m == nil {
			return false
		}
		if m.LogRecordingStopPosition() != newTermBase {
			return false
		}
		id := m.Agent().LocalLogRecordingID()
		if id < 0 {
			return false
		}
		d, listErr := m.ArchiveClient().ListRecording(
			context.Background(),
			id,
		)
		if listErr != nil || d.StopPosition != newTermBase {
			return false
		}
		return lastTermLogPosition(m) == newTermBase
	})

	return truncatedExLeader{
		divergedLeader: diverged,
		planAppended:   planAppended,
	}
}

// buildDivergentTail appends on the leader at leaderIndex until its type-100
// recording reaches followerFloor+minTail. Returns the first sampled recording
// position at or above that target.
//
// Behaviour:
//   - follower catalogs must stay at followerFloor
//   - drains peerImages each tick so the publication limit reopens
//   - fails the test when the leader loses its role or its publication
//   - fails the test with a position sample when the bound elapses
func buildDivergentTail(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leaderIndex int,
	followerFloor int64,
	minTail int64,
	peerImages []*aeron.Image,
) int64 {
	t.Helper()

	// Peers omit Agent.DoWork so they do not trip leader heartbeat loss. Term
	// length is 64k. The publication limit is half that (32768), so the leader
	// hits back-pressure well before the deadline. Draining peer log images
	// each tick reopens the limit without peer Agent.DoWork.
	target := followerFloor + minTail
	deadline := time.Now().Add(durableWorkBound)
	var (
		leader        *cluster.ConsensusModule
		lastLeaderRec int64
		maxLeaderRec  int64
	)
	for time.Now().Before(deadline) {
		leader = set.Module(leaderIndex)
		require.NotNil(t, leader)
		if !leader.IsLeader() {
			require.FailNow(t, "leader lost role while building gap", "%s", set.State())
		}

		pumpLiveTransport(set)
		tickMemberStreams(leader)
		drainPeerLogImages(peerImages)

		// Follower recording is stopped, so follower catalogs must stay at
		// the floor.
		leaderRec := harness.RecordingCounterPosition(
			leader, leader.Agent().LocalLogRecordingID(),
		)
		lastLeaderRec = leaderRec
		if leaderRec > maxLeaderRec {
			maxLeaderRec = leaderRec
		}
		if leaderRec >= target &&
			followersRecordingAtMost(set, leaderIndex, followerFloor) {
			return leaderRec
		}

		if leader.LogPublisher().Publication() == nil {
			require.FailNow(t, "leader log publication unbound", "%s", set.State())
		}

		// Pace on retriable offer codes. tryAppend fails the test on terminal
		// codes. Bytes already offered may still raise the recording position.
		if tryAppend(t, set, leader, "divergent-tail") <= 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Sample positions for triage. Lag: leaderRec passed the floor but not the
	// target. Stall: leaderRec stayed at the floor while logPubPos advanced.
	require.NotNil(t, leader, "gap build ran no tick:\n%s", set.State())
	leaderRec := harness.RecordingCounterPosition(
		leader, leader.Agent().LocalLogRecordingID(),
	)
	logPubPos := leader.LogPublisher().Position()

	// Nil publication: report Closed for the AvailableWindow sample.
	// AppendMessage maps a nil publication to the same code.
	window := aeron.Closed
	if pub := leader.LogPublisher().Publication(); pub != nil {
		window = pub.AvailableWindow()
		logPubPos = pub.Position()
	}
	// A settled leader binds no log-adapter image. Sample the adapter and image
	// positions anyway for triage.
	leaderAdapterPos := int64(-1)
	leaderImagePos := int64(-1)
	if adapter := leader.LogAdapter(); adapter != nil {
		leaderAdapterPos = adapter.Position()
		if img := adapter.Image(); img != nil {
			leaderImagePos = img.Position()
		}
	}
	require.FailNow(
		t,
		"leader recording high-water never reached the gap target",
		"target=%d lastLeaderRec=%d maxLeaderRec=%d leaderRec=%d "+
			"logPubPos=%d window=%d followerFloor=%d leaderRecPastFloor=%v "+
			"leaderRecReachedTarget=%v leaderAdapterPos=%d leaderImagePos=%d "+
			"bound=%s:\n%s",
		target,
		lastLeaderRec,
		maxLeaderRec,
		leaderRec,
		logPubPos,
		window,
		followerFloor,
		maxLeaderRec > followerFloor,
		maxLeaderRec >= target,
		leaderAdapterPos,
		leaderImagePos,
		durableWorkBound,
		set.State(),
	)
	return 0
}

// failIfLogJoinMismatch fails the test when m's election last recorded a log
// join position mismatch. A nil m has no election to probe.
func failIfLogJoinMismatch(
	t *testing.T,
	set *harness.ClusterMemberSet,
	m *cluster.ConsensusModule,
	memberIndex int,
) {
	t.Helper()

	if m == nil {
		return
	}
	err := m.ElectionLastError()
	if err == nil || !strings.Contains(err.Error(), logJoinPositionMismatch) {
		return
	}
	require.FailNow(
		t,
		"follower reported a log join position mismatch",
		"member=%d err=%v:\n%s",
		memberIndex,
		err,
		set.State(),
	)
}

// TestDurableCluster_truncatesADivergentMemberAndCommits asserts a member that
// recorded past the survivors' new term base truncates on rejoin.
//
// Asserts:
//   - truncate lowers stop position and last term LogPosition to the new term base
//   - returning member installs the new leader's term and finishes election
//   - post-truncate append reaches every member
//
// Does not assert snapshot refuse. No fixture plants a snapshot entry.
func TestDurableCluster_truncatesADivergentMemberAndCommits(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableDivergencePortBase,
		harness.DurableDivergenceLogEndpoint,
	)

	// One frame past the follower floor is enough for a truncate.
	exLeader := truncateRestartedLeader(t, set, divergeAndReElect(t, set, 1))
	leaderIndex := exLeader.index
	newTermBase := exLeader.newTermBase

	// Reopen at the truncated term position before election completes. The
	// recording log must still hold only the truncated term entry. Short
	// harness heartbeats may re-elect survivors while the slot is empty.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	require.NoError(t, set.RestartMember(leaderIndex))
	returning := set.Module(leaderIndex)
	require.NotNil(t, returning, "second RestartMember left slot empty:\n%s", set.State())
	require.Equal(
		t,
		newTermBase,
		lastTermLogPosition(returning),
		"reopened RecordingLog must report truncated term position %d:\n%s",
		newTermBase,
		set.State(),
	)

	// Fan in peer log images with transport only, then tick until election
	// completes. Agent DoWork before fan-in can enter FOLLOWER_LOG_REPLICATION
	// without peer log images.
	awaitPostRestartReady(t, set, func() bool {
		m := set.Module(leaderIndex)
		if m == nil {
			return false
		}
		ls := set.Leaders()
		if len(ls) != 1 {
			return false
		}
		return m.ElectionComplete() &&
			m.LeadershipTermID() == ls[0].LeadershipTermID()
	})

	returning = set.Module(leaderIndex)
	require.NotNil(t, returning)
	newLeader := requireSingleLeader(t, set, "after install")

	// Post-truncate append reaches every member.
	postAppendPos := mustAppend(t, set, newLeader, "post-truncate-commit")

	requireAwaitTick(t, set, "all members reach post-truncate commit", durableWorkBound, func() bool {
		return membersReachCommit(set, postAppendPos)
	})
}

// TestDurableCluster_reElectsATruncatedExLeader asserts an ex-leader whose log
// publication sits above the new append position wins re-election and extends
// its recording.
//
// Asserts:
//   - the truncate leaves the ex-leader publication at its old catalog stop
//   - the ex-leader wins the next term with its recording id unchanged
//   - the ex-leader publishes under a new session id
//   - the second follower never reports a log join position mismatch
//   - a post-election append commits on the ex-leader and the second follower
func TestDurableCluster_reElectsATruncatedExLeader(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openDurableCluster(
		t,
		harness.DurableExLeaderReElectPortBase,
		harness.DurableExLeaderReElectLogEndpoint,
	)

	exLeader := truncateRestartedLeader(t, set, divergeAndReElect(t, set, exLeaderMinTail))
	exIndex := exLeader.index
	planAppended := exLeader.planAppended

	// The in-process truncate leaves the publication at the first restart's
	// open position. A second restart would reopen it at the truncated stop.
	completeLabel := fmt.Sprintf("ex-leader %d completes election as follower", exIndex)
	requireAwaitTick(t, set, completeLabel, durableElectBound, func() bool {
		m := set.Module(exIndex)
		ls := set.Leaders()
		if m == nil || len(ls) != 1 || ls[0] == m {
			return false
		}
		return m.ElectionComplete() &&
			m.LeadershipTermID() == ls[0].LeadershipTermID()
	})

	ex := set.Module(exIndex)
	newLeader := requireSingleLeader(t, set, "after ex-leader install")
	newLeaderIndex := int(newLeader.ThisMemberID())
	newLeaderTerm := newLeader.LeadershipTermID()
	require.Equal(
		t,
		planAppended,
		ex.LogPublisher().Position(),
		"ex-leader publication must stay at the pre-truncate stop:\n%s",
		set.State(),
	)

	followerIndex := -1
	for i := range set.Modules() {
		if i != exIndex && i != newLeaderIndex {
			followerIndex = i
			break
		}
	}
	require.NotEqual(t, -1, followerIndex, "second follower slot:\n%s", set.State())

	appendPos := mustAppend(t, set, newLeader, "new-leader-commit")

	requireAwaitTick(t, set, "followers record the new leader append", durableWorkBound, func() bool {
		return membersReachCommit(set, appendPos) &&
			localRecordingAtLeast(set.Module(exIndex), appendPos) &&
			localRecordingAtLeast(set.Module(followerIndex), appendPos)
	})

	follower := set.Module(followerIndex)
	for _, m := range []*cluster.ConsensusModule{ex, follower} {
		require.Equal(
			t,
			appendPos,
			harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()),
			"member %d recording counter:\n%s",
			m.ThisMemberID(),
			set.State(),
		)
	}
	require.Less(
		t,
		appendPos,
		planAppended,
		"append position must stay below the ex-leader's old tail:\n%s",
		set.State(),
	)
	require.NotNil(
		t,
		ex.LogPublisher().Publication(),
		"ex-leader publication unbound:\n%s",
		set.State(),
	)
	require.Equal(
		t,
		planAppended,
		ex.LogPublisher().Position(),
		"ex-leader publication moved as a follower:\n%s",
		set.State(),
	)
	oldSession := ex.LogPublisher().SessionID()
	recordingID := ex.Agent().LocalLogRecordingID()

	// No tick between the stops and the restart. The new leader's stop starts
	// the re-election. The restarted follower's startup canvass holds its
	// nomination, so the ex-leader nominates first.
	require.NoError(t, set.StopMember(followerIndex))
	require.NoError(t, set.StopMember(newLeaderIndex))
	require.NoError(t, set.RestartMember(followerIndex))

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	winLabel := fmt.Sprintf("ex-leader %d wins re-election", exIndex)
	requireAwaitTick(t, set, winLabel, durableReElectBound, func() bool {
		f := set.Module(followerIndex)
		failIfFollowerLogReplication(t, set, f, followerIndex)
		failIfLogJoinMismatch(t, set, f, followerIndex)
		m := set.Module(exIndex)
		ls := set.Leaders()
		if len(ls) != 1 {
			return false
		}
		if ls[0] == f && f.ElectionComplete() {
			require.FailNow(t, "second follower won re-election before the ex-leader", "%s", set.State())
		}
		if m == nil || ls[0] != m {
			return false
		}
		return m.LeadershipTermID() > newLeaderTerm &&
			m.ElectionComplete() &&
			m.Agent().IsRecordingActive() &&
			m.Agent().LocalLogRecordingID() == recordingID
	})

	ex = set.Module(exIndex)
	require.NotNil(
		t,
		ex.LogPublisher().Publication(),
		"re-elected ex-leader publication unbound:\n%s",
		set.State(),
	)
	require.NotEqual(
		t,
		oldSession,
		ex.LogPublisher().SessionID(),
		"ex-leader must publish under a new session id:\n%s",
		set.State(),
	)

	requireAwaitTick(t, set, "second follower completes election", durableElectBound, func() bool {
		f := set.Module(followerIndex)
		failIfFollowerLogReplication(t, set, f, followerIndex)
		failIfLogJoinMismatch(t, set, f, followerIndex)
		return f.ElectionComplete() &&
			f.LeadershipTermID() == ex.LeadershipTermID()
	})

	// membersReachCommit needs every slot live. The new leader's slot is empty,
	// so check the two live members.
	postAppendPos := mustAppend(t, set, ex, "re-elected-ex-leader-commit")

	requireAwaitTick(t, set, "ex-leader append commits", durableWorkBound, func() bool {
		f := set.Module(followerIndex)
		failIfLogJoinMismatch(t, set, f, followerIndex)
		return ex.CommitPosition() >= postAppendPos &&
			localRecordingAtLeast(ex, postAppendPos) &&
			f.CommitPosition() >= postAppendPos &&
			f.NotifiedCommitPosition() >= postAppendPos
	})
}

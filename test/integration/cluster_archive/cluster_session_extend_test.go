//go:build integration && archive_live

package cluster_archive_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	trafficWindow     = 4
	trafficPhaseFloor = 8
	sessionCloseBound = 30 * time.Second
	minOfferGap       = 20 * time.Millisecond
)

var sessionExtendPayload = []byte("session-extend-traffic")

// sessionTraffic offers bounded in-flight session messages through one open
// cluster session.
//
// Behaviour:
//   - appended stores leader log publication positions sampled at each
//     successful Offer. Offer returns ingress positions, so the markers
//     stand in for cluster log positions.
//   - pump offers the next message only while fewer than trafficWindow
//     markers sit above the current leader commit.
//   - minOfferGap leaves idle DoWork calls between offers so a follower
//     in catchup can report a publication-matched position.
//   - Floors use appendedSince counts.
//   - pump polls egress.
//   - pump sends a keepalive at 1s cadence.
//   - Retriable offer codes re-offer on a later pump call.
//   - NotConnected and Closed stop offering until rearm replaces the
//     session.
//
// Concurrency: invoker-only on the test goroutine.
type sessionTraffic struct {
	t             *testing.T
	set           *harness.ClusterMemberSet
	transport     *aeron.Client
	session       *client.AeronCluster
	appended      []int64 // leader log positions sampled at Offer, in order
	committedIdx  int     // first appended marker still above last sampled commit
	stopped       bool    // set on NotConnected or Closed
	lastKeepalive time.Time
	lastOffer     time.Time
}

// pump runs one traffic step. Call pump at the top of every TickUntil
// predicate while traffic is on. A nil leader skips the in-flight cap so
// Offer can observe NotConnected or Closed.
func (tr *sessionTraffic) pump(leader *cluster.ConsensusModule) {
	if tr.transport != nil {
		tr.transport.DoWork()
	}
	if tr.session != nil {
		tr.session.PollEgress()
		now := time.Now()
		if now.Sub(tr.lastKeepalive) >= time.Second {
			ka := tr.session.SendKeepAlive()
			tr.lastKeepalive = now
			if ka == aeron.NotConnected || ka == aeron.Closed {
				tr.stopped = true
			}
		}
	}
	if tr.stopped || tr.session == nil {
		return
	}
	if !tr.lastOffer.IsZero() && time.Since(tr.lastOffer) < minOfferGap {
		return
	}

	var commit, logPos int64
	if leader != nil {
		commit = leader.CommitPosition()
		if pub := leader.LogPublisher(); pub != nil {
			logPos = pub.Position()
		}
		for tr.committedIdx < len(tr.appended) && tr.appended[tr.committedIdx] <= commit {
			tr.committedIdx++
		}
		if len(tr.appended)-tr.committedIdx >= trafficWindow {
			return
		}
	}

	pos := tr.session.Offer(sessionExtendPayload)
	switch {
	case pos > 0:
		marker := logPos + 1
		if marker <= commit {
			marker = commit + 1
		}
		tr.appended = append(tr.appended, marker)
		tr.lastOffer = time.Now()
	case pos == aeron.NotConnected || pos == aeron.Closed:
		tr.stopped = true
	case aeron.IsOfferRetriable(pos):
		// Retriable offer. Re-offer on a later pump call.
	default:
		state := ""
		if tr.set != nil {
			state = tr.set.State()
		}
		require.FailNow(
			tr.t,
			"session offer failed",
			"code=%d (%s):\n%s",
			pos,
			aeron.PositionCodeName(pos),
			state,
		)
	}
}

// appendedSince reports appends observed after mark. Per-wait floors use the
// appendedSince count.
func (tr *sessionTraffic) appendedSince(mark int) int {
	n := len(tr.appended) - mark
	if n < 0 {
		return 0
	}
	return n
}

// lastPosition returns the most recent appended marker, or 0 when appended is
// empty.
func (tr *sessionTraffic) lastPosition() int64 {
	if len(tr.appended) == 0 {
		return 0
	}
	return tr.appended[len(tr.appended)-1]
}

// rearm replaces the transport and session after the session stops. rearm
// closes the previous session and clears stopped. Historical appended
// positions stay.
func (tr *sessionTraffic) rearm(transport *aeron.Client, session *client.AeronCluster) {
	if tr.session != nil {
		_ = tr.session.Close()
	}
	tr.transport = transport
	tr.session = session
	tr.stopped = false
	tr.lastKeepalive = time.Now()
	tr.lastOffer = time.Time{}
}

// logCounts writes append counts and the last appended marker.
func (tr *sessionTraffic) logCounts(label string, mark int) {
	tr.t.Logf(
		"%s appends=%d total=%d lastOffer=%d",
		label,
		tr.appendedSince(mark),
		len(tr.appended),
		tr.lastPosition(),
	)
}

// TestDurableClusterSession_extendsFollowerRecordingAcrossRestartUnderTraffic
// asserts a restarted follower keeps its catalog recording id and extends
// that recording while session traffic keeps the log growing.
//
// Asserts:
//   - catalog recording id is unchanged after rejoin
//   - the recording is active after rejoin
//   - type-100 advances past post-restart traffic
//   - a witness sees a session message on the restarted adapter.
func TestDurableClusterSession_extendsFollowerRecordingAcrossRestartUnderTraffic(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openThreeNodeCluster(
		t,
		harness.ClusterSessionExtendRestartPortBase,
		harness.ClusterSessionExtendRestartLogEndpoint,
		amendSessionAuth,
	)
	leader := requireSingleLeader(t, set, "")
	leaderMemberID := leader.ThisMemberID()
	followers := attachFollowerWitnesses(t, set, leader)

	tr := connectSessionTraffic(t, set, leader)
	sessionID := tr.session.ClusterSessionID()

	// Open. Baseline traffic.
	baselineMark := len(tr.appended)
	baselineTarget := awaitTrafficFloorAndCommit(
		t, set, tr, "baseline traffic commit", baselineMark,
		func(_ *cluster.ConsensusModule, target int64) bool {
			return membersReachCommit(set, target)
		},
		func() bool {
			return allSawSession(followers, sessionID)
		},
	)
	require.False(t, tr.stopped, "session stopped during baseline:\n%s", set.State())
	require.Positive(t, baselineTarget)

	followerIndex := firstNonLeaderIndex(set, leaderMemberID)
	require.GreaterOrEqual(t, followerIndex, 0, "no follower slot:\n%s", set.State())
	follower := set.Module(followerIndex)
	require.NotNil(t, follower)
	require.False(t, follower.IsLeader(), "chosen member must not be leader:\n%s", set.State())

	preRecordingID := follower.RecordingLog().TermRecordingID()
	require.NotEqual(t, int64(-1), preRecordingID, "follower TermRecordingID:\n%s", set.State())
	require.Equal(
		t,
		preRecordingID,
		follower.Agent().LocalLogRecordingID(),
		"follower LocalLogRecordingID vs TermRecordingID:\n%s",
		set.State(),
	)

	// Fault mid-traffic. No recording-current await.
	require.NoError(t, set.StopMember(followerIndex))
	require.Nil(t, set.Module(followerIndex))

	// Down window.
	downMark := len(tr.appended)
	downTarget := awaitTrafficFloorAndCommit(
		t, set, tr, "down-window traffic commit", downMark,
		func(cur *cluster.ConsensusModule, target int64) bool {
			return survivorsReachCommit(
				set,
				[]int{followerIndex},
				cur.ThisMemberID(),
				cur.CommitPosition(),
				target,
			)
		},
		nil,
	)
	require.False(t, tr.stopped, "session stopped while follower down:\n%s", set.State())
	require.Positive(t, downTarget)

	// Restart and catchup under traffic.
	require.NoError(t, set.RestartMember(followerIndex))
	requireWarmRestartOpen(t, set, followerIndex, "returning member")
	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)

	leader = requireSingleLeader(t, set, "after restart")
	catchupTarget := leader.LogPublisher().Position()
	require.Positive(t, catchupTarget, "leader publication after restart:\n%s", set.State())

	catchupMark := len(tr.appended)
	requireAwaitTick(t, set, "returner catchup under traffic", durableWorkBound, func() bool {
		failIfAnyLogReplication(t, set)
		tr.pump(currentLeader(tr.set))
		return returnerCaughtUpTo(
			t, set, set.Module(followerIndex), followerIndex, catchupTarget,
		)
	})
	tr.logCounts("returner catchup under traffic", catchupMark)

	returning := set.Module(followerIndex)
	requireRecordingHeld(t, set, returning, preRecordingID, "returning member after catchup")
	require.Empty(
		t,
		returning.LogReplayReason(),
		"LogReplayReason after catchup:\n%s",
		set.State(),
	)

	// Extend evidence: type-100 advances past post-restart traffic. A witness
	// sees a session message on the restarted adapter.
	postWitness := installLogWitness(t, set, returning)
	postMark := len(tr.appended)
	postTarget := awaitTrafficFloorAndCommit(
		t, set, tr, "post-restart recording extend", postMark,
		func(_ *cluster.ConsensusModule, target int64) bool {
			return recordingAtLeast(returning, preRecordingID, target+1)
		},
		func() bool {
			return postWitness.sawSession(sessionID)
		},
	)
	require.Positive(t, postTarget)

	// Converge and close.
	tr.stopped = true
	finalTarget := requireSingleLeader(t, set, "before close").LogPublisher().Position()
	requireAwaitTick(t, set, "final traffic commit", durableWorkBound, func() bool {
		tr.pump(currentLeader(tr.set))
		return membersReachCommit(set, finalTarget)
	})
	tr.logCounts("final traffic commit", postMark)
	drainSession(t, set, tr)
}

// TestDurableClusterSession_reElectsUnderTrafficAndExtendsRecordings stops the
// leader while a session sends. The ex-leader rejoins by extending its
// recording. Follower log replication fails the test.
//
// Asserts:
//   - the recording stop equals the publication position at the stop
//   - the recording stop sits at or above the new term base
//   - the leader stays in the post-stop term
//   - the ex-leader stop-term entry sits at or after the leader term base
//   - the ex-leader rejoins as a follower on its open-time recording
//   - follower witnesses count every accepted offer of the final session
//   - all three members commit and record the leader position after the last offer
//   - each recording counter sits at or above its commit
func TestDurableClusterSession_reElectsUnderTrafficAndExtendsRecordings(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	sc := openLeaderStopScenario(
		t,
		harness.ClusterSessionExtendReElectPortBase,
		harness.ClusterSessionExtendReElectLogEndpoint,
	)
	sc.awaitLeaderRecordingCurrent()
	stopPublication := sc.stopLeader()
	sc.reElectAndReconnect("post-re-elect traffic extend", 0)
	stopTerm := sc.settledTerm

	restartStop := sc.restartExLeader()
	rejoinReadyMark := len(sc.tr.appended)
	awaitPostRestartReady(t, sc.set, func() bool {
		failIfFurtherElection(t, sc.set, stopTerm, sc.exIndex)
		failIfAnyLogReplication(t, sc.set)
		failIfCommitPassesRecording(t, sc.set)
		sc.tr.pump(currentLeader(sc.set))
		returning := sc.set.Module(sc.exIndex)
		return returning != nil && returning.ElectionComplete()
	})
	sc.tr.logCounts("post-restart election", rejoinReadyMark)

	require.Equal(
		t,
		stopPublication,
		restartStop,
		"recording stop at the leader stop:\n%s",
		sc.set.State(),
	)
	leader := requireSingleLeader(t, sc.set, "after ex-leader rejoin")
	require.Equal(
		t,
		stopTerm,
		leader.LeadershipTermID(),
		"further election:\n%s",
		sc.set.State(),
	)
	termBase := termBaseLogPosition(leader, stopTerm)
	require.Positive(t, termBase, "term base:\n%s", sc.set.State())
	require.GreaterOrEqual(
		t,
		restartStop,
		termBase,
		"recording stop below the term base:\n%s",
		sc.set.State(),
	)
	returning := sc.set.Module(sc.exIndex)
	require.Equal(
		t,
		stopTerm,
		returning.LeadershipTermID(),
		"further election:\n%s",
		sc.set.State(),
	)
	// An extend join records the open term at the caught-up commit. The leader
	// entry stays at the term start. The ex-leader base sits at or after the
	// term start.
	require.GreaterOrEqual(
		t,
		termBaseLogPosition(returning, stopTerm),
		termBase,
		"ex-leader term base:\n%s",
		sc.set.State(),
	)
	require.Equal(
		t,
		consensus.RoleFollower,
		returning.Role(),
		"returning member Role after rejoin:\n%s",
		sc.set.State(),
	)
	requireRecordingHeld(
		t,
		sc.set,
		returning,
		sc.preIDs[sc.exIndex],
		"returning ex-leader after rejoin",
	)
	require.Empty(
		t,
		returning.LogReplayReason(),
		"LogReplayReason after ex-leader rejoin:\n%s",
		sc.set.State(),
	)
	sc.awaitPostRejoinTraffic(stopTerm, "session stopped after ex-leader rejoin")
}

// TestDurableClusterSession_replicatesAnExLeaderStoppedUnderTraffic stops the
// leader while a session sends. The ex-leader rejoins by log replication
// under a later term. An extend rejoin fails the test.
//
// Asserts:
//   - the ex-leader enters FOLLOWER_LOG_REPLICATION
//   - the recording stop sits below the rejoin term base
//   - the missed-term leader stops at the missed-term end
//   - the rejoin term base equals the missed-term end
//   - the ex-leader missed-term entry equals the missed-term base
//   - the ex-leader rejoins as a follower on its open-time recording
//   - follower witnesses count every accepted offer of the final session
//   - all three members commit and record the leader position after the last offer
//   - each recording counter sits at or above its commit
func TestDurableClusterSession_replicatesAnExLeaderStoppedUnderTraffic(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	sc := openLeaderStopScenario(
		t,
		harness.ClusterSessionReplicateExLeaderPortBase,
		harness.ClusterSessionReplicateExLeaderLogEndpoint,
	)

	exDown := []int{sc.exIndex}
	stopPublication := sc.stopLeader()
	sc.reElectAndReconnect("missed-term traffic past the old leader", stopPublication)
	missedTerm := sc.settledTerm

	missedTermEnd := sc.awaitTrafficHeld("missed-term traffic held", exDown)
	require.Greater(
		t,
		missedTermEnd,
		stopPublication,
		"missed-term end:\n%s",
		sc.set.State(),
	)

	leader, leaderStop := sc.restartSecondLeader(missedTerm)
	require.Equal(
		t,
		missedTermEnd,
		leaderStop,
		"late append on missed-term leader:\n%s",
		sc.set.State(),
	)
	rejoinTerm := leader.LeadershipTermID()
	sc.settledTerm = rejoinTerm
	require.Equal(
		t,
		missedTermEnd,
		termBaseLogPosition(leader, rejoinTerm),
		"rejoin term base:\n%s",
		sc.set.State(),
	)

	transport, session := openSession(t, sc.set, leader)
	sc.adoptSession(transport, session)
	follower := sc.installCurrentFollowerWitness(leader)
	awaitTrafficPastAndCommit(
		t, sc.set, sc.tr, "rejoin-term traffic commit", sc.sessionMark, 0,
		func() {
			failIfFurtherElection(t, sc.set, rejoinTerm, -1)
		},
		func(cur *cluster.ConsensusModule, target int64) bool {
			return sc.survivorsCommitAndRecord(exDown, cur, target)
		},
		func() bool {
			return sc.witnessSawSession(follower)
		},
	)
	require.False(t, sc.tr.stopped, "session stopped in the rejoin term:\n%s", sc.set.State())

	restartStop := sc.restartExLeader()
	leaderIndex := int(leader.ThisMemberID())
	rejoinMark := len(sc.tr.appended)
	entryStop := awaitReplicatedRejoin(t, sc.set, sc.exIndex, leaderIndex, func() {
		failIfFurtherElection(t, sc.set, rejoinTerm, sc.exIndex)
		sc.tr.pump(currentLeader(sc.set))
		failIfCommitPassesRecording(t, sc.set)
		for i, m := range sc.set.Modules() {
			if i == sc.exIndex {
				continue
			}
			failIfFollowerLogReplication(t, sc.set, m, i)
		}
	})
	sc.tr.logCounts("ex-leader rejoins through replication", rejoinMark)

	leader = requireSingleLeader(t, sc.set, "after replication rejoin")
	require.Equal(
		t,
		rejoinTerm,
		leader.LeadershipTermID(),
		"further election:\n%s",
		sc.set.State(),
	)
	rejoinBase := termBaseLogPosition(leader, rejoinTerm)
	require.Less(
		t,
		restartStop,
		rejoinBase,
		"recording stop at or above the term base:\n%s",
		sc.set.State(),
	)
	missedTermBase := termBaseLogPosition(leader, missedTerm)
	returning := sc.set.Module(sc.exIndex)
	require.Positive(t, missedTermBase, "missed-term base:\n%s", sc.set.State())
	require.Equal(
		t,
		missedTermBase,
		termBaseLogPosition(returning, missedTerm),
		"ex-leader missed-term entry:\n%s",
		sc.set.State(),
	)
	require.Equal(
		t,
		consensus.RoleFollower,
		returning.Role(),
		"returning member Role after replication:\n%s",
		sc.set.State(),
	)
	requireRecordingHeld(
		t,
		sc.set,
		returning,
		sc.preIDs[sc.exIndex],
		"returning ex-leader after replication",
	)
	require.Empty(
		t,
		returning.LogReplayReason(),
		"LogReplayReason after replication:\n%s",
		sc.set.State(),
	)
	routeName := "route 4"
	if restartStop < missedTermBase {
		routeName = "route 3"
	}
	t.Logf(
		"ex-leader replication stopPublication=%d restartStop=%d "+
			"entryStop=%d missedTermBase=%d rejoinTermBase=%d %s",
		stopPublication,
		restartStop,
		entryStop,
		missedTermBase,
		rejoinBase,
		routeName,
	)
	sc.awaitPostRejoinTraffic(rejoinTerm, "session stopped after replication rejoin")
}

// openSession starts a client transport and opens one cluster session against
// leader.
//
// Ownership: the test owns Close on the transport and the session.
func openSession(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
) (*aeron.Client, *client.AeronCluster) {
	t.Helper()
	return openSessionWithEgress(t, set, leader, &client.EgressHandlers{})
}

// openSessionWithEgress is openSession with egress as the client egress
// handler.
func openSessionWithEgress(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
	egress client.EgressHandler,
) (*aeron.Client, *client.AeronCluster) {
	t.Helper()

	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnectWithEgress(
		t, set, leader, transport, []byte(sessionSmokeCredentials), egress,
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	return transport, session
}

// connectSessionTraffic opens one client session against leader and returns a
// traffic driver.
//
// Ownership: the test owns Close on the transport and the session.
func connectSessionTraffic(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
) *sessionTraffic {
	t.Helper()

	transport, session := openSession(t, set, leader)
	return &sessionTraffic{
		t:             t,
		set:           set,
		transport:     transport,
		session:       session,
		lastKeepalive: time.Now(),
	}
}

// attachFollowerWitnesses installs a logWitness on every non-leader LogAdapter.
func attachFollowerWitnesses(
	t *testing.T,
	set *harness.ClusterMemberSet,
	leader *cluster.ConsensusModule,
) []*logWitness {
	t.Helper()

	witnesses := make([]*logWitness, 0, 2)
	for _, m := range set.Modules() {
		if m == nil || m.ThisMemberID() == leader.ThisMemberID() {
			continue
		}
		witnesses = append(witnesses, installLogWitness(t, set, m))
	}
	require.Len(t, witnesses, 2, "follower witnesses:\n%s", set.State())
	return witnesses
}

// allSawSession reports whether every witness observed sessionID.
func allSawSession(witnesses []*logWitness, sessionID int64) bool {
	for _, w := range witnesses {
		if !w.sawSession(sessionID) {
			return false
		}
	}
	return true
}

// installLogWitness requires a non-nil LogAdapter and installs a fresh witness.
func installLogWitness(
	t *testing.T,
	set *harness.ClusterMemberSet,
	m *cluster.ConsensusModule,
) *logWitness {
	t.Helper()

	require.NotNil(t, m, "module for witness:\n%s", set.State())
	adapter := m.LogAdapter()
	require.NotNil(
		t,
		adapter,
		"LogAdapter for member %d:\n%s",
		m.ThisMemberID(),
		set.State(),
	)
	w := &logWitness{}
	adapter.SetHandler(w)
	return w
}

// requireRecordingHeld asserts m holds wantID and is recording.
//
// Asserts:
//   - m is not nil
//   - LocalLogRecordingID equals wantID
//   - TermRecordingID equals wantID
//   - IsRecordingActive
//   - AppendRankFailure is nil
func requireRecordingHeld(
	t *testing.T,
	set *harness.ClusterMemberSet,
	m *cluster.ConsensusModule,
	wantID int64,
	label string,
) {
	t.Helper()

	require.NotNil(t, m, "%s missing:\n%s", label, set.State())
	require.Equal(
		t,
		wantID,
		m.Agent().LocalLogRecordingID(),
		"LocalLogRecordingID %s:\n%s",
		label,
		set.State(),
	)
	require.Equal(
		t,
		wantID,
		m.RecordingLog().TermRecordingID(),
		"TermRecordingID %s:\n%s",
		label,
		set.State(),
	)
	require.True(
		t,
		m.Agent().IsRecordingActive(),
		"IsRecordingActive %s:\n%s",
		label,
		set.State(),
	)
	require.NoError(
		t,
		m.Agent().AppendRankFailure(),
		"AppendRankFailure %s:\n%s",
		label,
		set.State(),
	)
}

// survivorsHoldRecordings reports whether every live member except exclude
// holds recIDs[i] and is recording.
//
// True when each such member has:
//   - LocalLogRecordingID equal to recIDs[i]
//   - TermRecordingID equal to recIDs[i]
//   - IsRecordingActive
func survivorsHoldRecordings(
	set *harness.ClusterMemberSet,
	exclude []int,
	recIDs []int64,
) bool {
	for i, m := range set.Modules() {
		if m == nil || slices.Contains(exclude, i) {
			continue
		}
		if i >= len(recIDs) {
			return false
		}
		if m.Agent().LocalLogRecordingID() != recIDs[i] {
			return false
		}
		if m.RecordingLog().TermRecordingID() != recIDs[i] {
			return false
		}
		if !m.Agent().IsRecordingActive() {
			return false
		}
	}
	return true
}

// failIfAnyLogReplication fails the test when:
//   - an active member is on the follower log-replication path
//   - an active member has a marked AppendRankFailure
func failIfAnyLogReplication(t *testing.T, set *harness.ClusterMemberSet) {
	t.Helper()

	for i, m := range set.Modules() {
		if m == nil {
			continue
		}
		failIfFollowerLogReplication(t, set, m, i)
		if err := m.Agent().AppendRankFailure(); err != nil {
			require.NoError(
				t,
				err,
				"AppendRankFailure member=%d:\n%s",
				i,
				set.State(),
			)
		}
	}
}

// awaitTrafficFloorAndCommit pumps traffic and waits for a publication
// snapshot to commit.
//
// Open order:
//  1. pump until appendedSince(mark) meets trafficPhaseFloor
//  2. snapshot the leader publication position
//  3. wait until commitFn reports true for that snapshot
//  4. wait until extra reports true when extra is non-nil
func awaitTrafficFloorAndCommit(
	t *testing.T,
	set *harness.ClusterMemberSet,
	tr *sessionTraffic,
	label string,
	mark int,
	commitFn func(*cluster.ConsensusModule, int64) bool,
	extra func() bool,
) int64 {
	t.Helper()
	return awaitTrafficPastAndCommit(t, set, tr, label, mark, 0, nil, commitFn, extra)
}

// awaitTrafficPastAndCommit is awaitTrafficFloorAndCommit with two additions.
// A non-nil perTick runs at the start of each tick, before the replication
// check and the leader lookup. The wait samples the leader publication
// position only after the position passes minPosition.
func awaitTrafficPastAndCommit(
	t *testing.T,
	set *harness.ClusterMemberSet,
	tr *sessionTraffic,
	label string,
	mark int,
	minPosition int64,
	perTick func(),
	commitFn func(*cluster.ConsensusModule, int64) bool,
	extra func() bool,
) int64 {
	t.Helper()

	var target int64
	haveTarget := false
	requireAwaitTick(t, set, label, durableWorkBound, func() bool {
		if perTick != nil {
			perTick()
		}
		failIfAnyLogReplication(t, set)
		leader := currentLeader(set)
		if leader == nil {
			return false
		}
		tr.pump(leader)
		if tr.appendedSince(mark) < trafficPhaseFloor {
			return false
		}
		pub := leader.LogPublisher()
		if pub == nil {
			return false
		}
		pos := pub.Position()
		if pos <= minPosition {
			return false
		}
		if !haveTarget {
			target = pos
			haveTarget = true
		}
		if !commitFn(leader, target) {
			return false
		}
		return extra == nil || extra()
	})
	tr.logCounts(label, mark)
	return target
}

// drainSession closes the session and waits for the leader registry to drain.
//
// Open order:
//  1. mark sessionTraffic stopped
//  2. Close the session
//  3. wait until the leader reports zero open, pending, and closing sessions
func drainSession(t *testing.T, set *harness.ClusterMemberSet, tr *sessionTraffic) {
	t.Helper()

	tr.stopped = true
	require.NoError(t, tr.session.Close())
	drainLeader := requireSingleLeader(t, set, "close drain")
	requireAwaitTick(t, set, "session registry drained", sessionCloseBound, func() bool {
		if tr.transport != nil {
			tr.transport.DoWork()
		}
		agent := drainLeader.Agent()
		return agent.OpenSessionCount() == 0 &&
			agent.PendingSessionCount() == 0 &&
			agent.ClosingSessionCount() == 0
	})
}

type leaderStopScenario struct {
	t           *testing.T
	set         *harness.ClusterMemberSet
	tr          *sessionTraffic
	exIndex     int
	preTerm     int64
	settledTerm int64
	preIDs      []int64
	sessionID   int64
	sessionMark int
	witnesses   []*logWitness
}

// openLeaderStopScenario opens a three-member cluster, records open-time
// recording ids, and commits a baseline session floor.
func openLeaderStopScenario(t *testing.T, portBase int, logEndpoint string) *leaderStopScenario {
	t.Helper()

	set := openThreeNodeCluster(t, portBase, logEndpoint, amendSessionAuth)
	leader := requireSingleLeader(t, set, "")
	exIndex := int(leader.ThisMemberID())

	preIDs := make([]int64, len(set.Modules()))
	for i, m := range set.Modules() {
		require.NotNil(t, m, "member %d missing at open:\n%s", i, set.State())
		id := m.RecordingLog().TermRecordingID()
		require.NotEqual(t, int64(-1), id, "member %d TermRecordingID:\n%s", i, set.State())
		require.Equal(
			t,
			id,
			m.Agent().LocalLogRecordingID(),
			"member %d LocalLogRecordingID vs TermRecordingID:\n%s",
			i,
			set.State(),
		)
		preIDs[i] = id
	}
	require.Equal(t, preIDs[0], preIDs[1], "recording ids differ at open:\n%s", set.State())
	require.Equal(t, preIDs[0], preIDs[2], "recording ids differ at open:\n%s", set.State())

	sc := &leaderStopScenario{
		t:         t,
		set:       set,
		exIndex:   exIndex,
		preTerm:   leader.LeadershipTermID(),
		preIDs:    preIDs,
		witnesses: make([]*logWitness, len(set.Modules())),
	}
	for i, m := range set.Modules() {
		if m == nil || i == exIndex {
			continue
		}
		sc.witnesses[i] = installLogWitness(t, set, m)
	}
	sc.tr = connectSessionTraffic(t, set, leader)
	sc.sessionID = sc.tr.session.ClusterSessionID()

	baselineMark := len(sc.tr.appended)
	baselineTarget := awaitTrafficFloorAndCommit(
		t, set, sc.tr, "baseline traffic commit", baselineMark,
		func(_ *cluster.ConsensusModule, target int64) bool {
			return membersReachCommit(set, target)
		},
		func() bool {
			return sc.followersSawSession()
		},
	)
	require.False(t, sc.tr.stopped, "session stopped during baseline:\n%s", set.State())
	require.Positive(t, baselineTarget)
	return sc
}

func (sc *leaderStopScenario) followersSawSession() bool {
	n := 0
	for _, w := range sc.witnesses {
		if w == nil {
			continue
		}
		if !w.sawSession(sc.sessionID) {
			return false
		}
		n++
	}
	return n > 0
}

func (sc *leaderStopScenario) witnessSawSession(idx int) bool {
	w := sc.witnesses[idx]
	return w != nil && w.sawSession(sc.sessionID)
}

// awaitLeaderRecordingCurrent pumps until the leader's local recording
// reaches its log publication. The session must accept one offer in this
// wait.
func (sc *leaderStopScenario) awaitLeaderRecordingCurrent() {
	sc.t.Helper()

	mark := len(sc.tr.appended)
	requireAwaitTick(sc.t, sc.set, "leader recording current", durableWorkBound, func() bool {
		failIfAnyLogReplication(sc.t, sc.set)
		leader := currentLeader(sc.set)
		sc.tr.pump(leader)
		if leader == nil || sc.tr.appendedSince(mark) < 1 {
			return false
		}
		pub := leader.LogPublisher()
		if pub == nil {
			return false
		}
		return localRecordingAtLeast(leader, pub.Position())
	})
	sc.tr.logCounts("leader recording current", mark)
}

// stopLeader returns the leader publication position at the stop.
//
// The caller must not tick between a recording-current wait and this call.
func (sc *leaderStopScenario) stopLeader() int64 {
	sc.t.Helper()

	leader := requireSingleLeader(sc.t, sc.set, "before stop")
	pub := leader.LogPublisher()
	require.NotNil(sc.t, pub, "leader log publication:\n%s", sc.set.State())
	stopPublication := pub.Position()
	sc.t.Logf(
		"stopping leader member=%d commit=%d publication=%d",
		leader.ThisMemberID(),
		leader.CommitPosition(),
		stopPublication,
	)
	for _, m := range sc.set.Modules() {
		if m == nil || m.IsLeader() {
			continue
		}
		sc.t.Logf(
			"survivor member=%d recording=%d",
			m.ThisMemberID(),
			harness.RecordingCounterPosition(m, m.Agent().LocalLogRecordingID()),
		)
	}
	index := int(leader.ThisMemberID())
	require.NoError(sc.t, sc.set.StopMember(index))
	require.Nil(sc.t, sc.set.Module(index))
	return stopPublication
}

// survivorsCommitAndRecord reports whether cur's commit reaches target.
// Followers outside down must notify target. Every live member outside down
// must record past target.
func (sc *leaderStopScenario) survivorsCommitAndRecord(
	down []int,
	cur *cluster.ConsensusModule,
	target int64,
) bool {
	return survivorsReachCommit(
		sc.set,
		down,
		cur.ThisMemberID(),
		cur.CommitPosition(),
		target,
	) && survivorsRecordingAtLeast(sc.set, down, sc.preIDs, target+1)
}

// reElectAndReconnect waits out the leader change, reconnects a fresh
// session, and commits a traffic floor past minPosition. label names the
// traffic-floor wait. Stores the new leader term in settledTerm.
func (sc *leaderStopScenario) reElectAndReconnect(label string, minPosition int64) {
	sc.t.Helper()

	exDown := []int{sc.exIndex}
	stopMark := len(sc.tr.appended)
	requireAwaitTick(sc.t, sc.set, "session observes leader change", durableWorkBound, func() bool {
		failIfAnyLogReplication(sc.t, sc.set)
		sc.tr.pump(nil)
		return sc.tr.stopped || sc.tr.session.LeaderMemberID() != int32(sc.exIndex)
	})
	sc.tr.logCounts("session observes leader change", stopMark)

	requireAwaitTick(sc.t, sc.set, "re-elect after leader stop", durableReElectBound, func() bool {
		failIfAnyLogReplication(sc.t, sc.set)
		sc.tr.pump(currentLeader(sc.set))
		return reElectionReady(sc.set, sc.exIndex, sc.preTerm)
	})
	sc.tr.logCounts("re-elect after leader stop", stopMark)
	requireReElectedLeader(sc.t, sc.set, sc.exIndex, sc.preTerm)

	termMark := len(sc.tr.appended)
	requireAwaitTick(sc.t, sc.set, "survivors extend under new term", durableWorkBound, func() bool {
		failIfAnyLogReplication(sc.t, sc.set)
		leader := currentLeader(sc.set)
		sc.tr.pump(leader)
		if !leaderRecordsNewTerm(leader, sc.exIndex, sc.preTerm) {
			return false
		}
		for i, m := range sc.set.Modules() {
			if m == nil || i == sc.exIndex {
				continue
			}
			if !m.ElectionComplete() {
				return false
			}
		}
		return survivorsHoldRecordings(sc.set, exDown, sc.preIDs)
	})
	sc.tr.logCounts("survivors extend under new term", termMark)

	for i, m := range sc.set.Modules() {
		if m == nil || i == sc.exIndex {
			continue
		}
		requireRecordingHeld(sc.t, sc.set, m, sc.preIDs[i], "survivor after re-elect")
	}

	newLeader := requireSingleLeader(sc.t, sc.set, "before reconnect")
	sc.settledTerm = newLeader.LeadershipTermID()
	transport, session := openSession(sc.t, sc.set, newLeader)
	sc.adoptSession(transport, session)
	follower := sc.installCurrentFollowerWitness(newLeader)

	target := awaitTrafficPastAndCommit(
		sc.t, sc.set, sc.tr, label, sc.sessionMark, minPosition,
		func() {
			failIfFurtherElection(sc.t, sc.set, sc.settledTerm, sc.exIndex)
		},
		func(cur *cluster.ConsensusModule, target int64) bool {
			return sc.survivorsCommitAndRecord(exDown, cur, target)
		},
		func() bool {
			return sc.witnessSawSession(follower)
		},
	)
	require.False(sc.t, sc.tr.stopped, "session stopped after reconnect:\n%s", sc.set.State())
	require.Greater(sc.t, target, minPosition, "traffic snapshot:\n%s", sc.set.State())
}

func (sc *leaderStopScenario) adoptSession(transport *aeron.Client, session *client.AeronCluster) {
	sc.tr.rearm(transport, session)
	sc.sessionID = session.ClusterSessionID()
	sc.sessionMark = len(sc.tr.appended)
}

// installCurrentFollowerWitness replaces every follower witness with one
// witness on the live follower of leader. Returns the follower slot.
func (sc *leaderStopScenario) installCurrentFollowerWitness(leader *cluster.ConsensusModule) int {
	sc.t.Helper()

	idx := firstNonLeaderIndex(sc.set, leader.ThisMemberID())
	require.GreaterOrEqual(sc.t, idx, 0, "no follower slot:\n%s", sc.set.State())
	clear(sc.witnesses)
	sc.witnesses[idx] = installLogWitness(sc.t, sc.set, sc.set.Module(idx))
	return idx
}

// awaitTrafficHeld stops offers. The wait then requires every live follower
// witness to count the accepted offers of the current session. The leader
// registry must hold only the current session. Every live member then
// commits and records the leader publication. down names empty slots.
// Returns the publication snapshot.
func (sc *leaderStopScenario) awaitTrafficHeld(label string, down []int) int64 {
	sc.t.Helper()

	sc.tr.stopped = true
	count := sc.tr.appendedSince(sc.sessionMark)
	require.Positive(sc.t, count, "held session accepted no offer:\n%s", sc.set.State())

	var snapshot int64
	haveSnapshot := false
	requireAwaitTick(sc.t, sc.set, label, durableWorkBound, func() bool {
		failIfFurtherElection(sc.t, sc.set, sc.settledTerm, -1)
		failIfAnyLogReplication(sc.t, sc.set)
		failIfCommitPassesRecording(sc.t, sc.set)
		sc.tr.pump(currentLeader(sc.set))
		if !sc.followersHoldSessionCount(count, down) {
			return false
		}
		leader := currentLeader(sc.set)
		if leader == nil {
			return false
		}
		agent := leader.Agent()
		if agent.OpenSessionCount() != 1 ||
			agent.PendingSessionCount() != 0 ||
			agent.ClosingSessionCount() != 0 {
			return false
		}
		pub := leader.LogPublisher()
		if pub == nil {
			return false
		}
		if !haveSnapshot {
			snapshot = pub.Position()
			haveSnapshot = true
		}
		if !liveMembersReachCommit(sc.set, down, snapshot) {
			return false
		}
		for i, m := range sc.set.Modules() {
			if m == nil || slices.Contains(down, i) {
				continue
			}
			if !localRecordingAtLeast(m, snapshot) {
				return false
			}
		}
		return true
	})
	sc.tr.logCounts(label, sc.sessionMark)
	return snapshot
}

// awaitPostRejoinTraffic commits a post-rejoin traffic floor in term. It
// then holds that session on every member. stoppedMsg is the failure text
// when the session has already stopped.
func (sc *leaderStopScenario) awaitPostRejoinTraffic(term int64, stoppedMsg string) {
	sc.t.Helper()

	mark := len(sc.tr.appended)
	awaitTrafficPastAndCommit(
		sc.t, sc.set, sc.tr, "post-rejoin traffic commit", mark, 0,
		func() {
			failIfFurtherElection(sc.t, sc.set, term, -1)
		},
		func(_ *cluster.ConsensusModule, target int64) bool {
			return membersReachCommit(sc.set, target)
		},
		func() bool {
			return sc.witnessSawSession(sc.exIndex)
		},
	)
	require.False(sc.t, sc.tr.stopped, "%s:\n%s", stoppedMsg, sc.set.State())
	sc.requireHeldTraffic()
}

func (sc *leaderStopScenario) requireHeldTraffic() {
	sc.t.Helper()

	require.GreaterOrEqual(sc.t, sc.tr.appendedSince(sc.sessionMark), 2*trafficPhaseFloor)
	sc.awaitTrafficHeld("traffic held on every member", nil)
	for i, m := range sc.set.Modules() {
		requireRecordingHeld(sc.t, sc.set, m, sc.preIDs[i], "member after held traffic")
	}
	requireRecordingCoversCommit(sc.t, sc.set)
	drainSession(sc.t, sc.set, sc.tr)
}

func (sc *leaderStopScenario) followersHoldSessionCount(count int, down []int) bool {
	checked := 0
	for i, m := range sc.set.Modules() {
		if m == nil || slices.Contains(down, i) || m.IsLeader() {
			continue
		}
		w := sc.witnesses[i]
		if w == nil || w.count(sc.sessionID) != count {
			return false
		}
		checked++
	}
	return checked > 0
}

// failIfFurtherElection fails when a live member other than skip misses
// ElectionComplete or holds a leadership term other than term. Pass
// skip -1 to check every live member.
func failIfFurtherElection(t *testing.T, set *harness.ClusterMemberSet, term int64, skip int) {
	t.Helper()

	for i, m := range set.Modules() {
		if m == nil || i == skip {
			continue
		}
		if m.ElectionComplete() && m.LeadershipTermID() == term {
			continue
		}
		require.FailNow(
			t,
			"further election",
			"member=%d ElectionState=%v:\n%s",
			i,
			m.ElectionState(),
			set.State(),
		)
	}
}

// restartExLeader restarts the stopped ex-leader. Reads the recording stop
// and installs the log witness before the next tick. Returns the recording
// stop.
func (sc *leaderStopScenario) restartExLeader() int64 {
	sc.t.Helper()

	require.NoError(sc.t, sc.set.RestartMember(sc.exIndex))
	requireWarmRestartOpen(sc.t, sc.set, sc.exIndex, "returning ex-leader")
	returning := sc.set.Module(sc.exIndex)
	restartStop := returning.LogRecordingStopPosition()
	sc.witnesses[sc.exIndex] = installLogWitness(sc.t, sc.set, returning)
	return restartStop
}

// restartSecondLeader stops the missed-term leader. Restarts the stopped
// member before the next tick. The restarted member and the other survivor
// elect one leader in a term above missedTerm. Returns the new leader and
// the stopped leader's publication position.
func (sc *leaderStopScenario) restartSecondLeader(
	missedTerm int64,
) (
	*cluster.ConsensusModule,
	int64,
) {
	sc.t.Helper()

	stopped := requireSingleLeader(sc.t, sc.set, "before second stop")
	stoppedIndex := int(stopped.ThisMemberID())
	stopPos := sc.stopLeader()
	require.NoError(sc.t, sc.set.RestartMember(stoppedIndex))
	requireWarmRestartOpen(sc.t, sc.set, stoppedIndex, "returning missed-term leader")
	requireAwait(
		sc.t,
		sc.set,
		"second leader restart images",
		durableImageBound,
		sc.set.AwaitLogImages,
	)
	requireAwaitTick(
		sc.t,
		sc.set,
		"re-elect after second leader stop",
		durableElectBound,
		func() bool {
			failIfAnyLogReplication(sc.t, sc.set)
			sc.tr.pump(currentLeader(sc.set))
			leader := currentLeader(sc.set)
			if leader == nil || leader.LeadershipTermID() <= missedTerm {
				return false
			}
			term := leader.LeadershipTermID()
			for _, m := range sc.set.Modules() {
				if m == nil {
					continue
				}
				if !m.ElectionComplete() || m.LeadershipTermID() != term {
					return false
				}
			}
			return true
		},
	)
	return requireSingleLeader(sc.t, sc.set, "after second stop"), stopPos
}

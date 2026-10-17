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

// TestDurableClusterSession_reElectsUnderTrafficAndExtendsRecordings
// asserts survivors keep catalog recording ids across a mid-traffic
// leader stop.
//
// Asserts:
//   - survivors keep the catalog recording ids
//   - the new leader records the new term
//   - the returning member rejoins as a follower on the same recording
func TestDurableClusterSession_reElectsUnderTrafficAndExtendsRecordings(t *testing.T) {
	t.Parallel()

	harness.RequireArchiveRuntime(t)

	set := openThreeNodeCluster(
		t,
		harness.ClusterSessionExtendReElectPortBase,
		harness.ClusterSessionExtendReElectLogEndpoint,
		amendSessionAuth,
	)
	leader := requireSingleLeader(t, set, "")
	leaderIndex := int(leader.ThisMemberID())
	preTerm := leader.LeadershipTermID()

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

	followers := attachFollowerWitnesses(t, set, leader)
	tr := connectSessionTraffic(t, set, leader)
	sessionID := tr.session.ClusterSessionID()

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

	// Fault mid-traffic. No recording-current await. The survivors re-elect
	// on the log stop, so the session follows the new leader. It stops instead
	// when an offer lands in the ingress gap before the reconnect.
	require.NoError(t, set.StopMember(leaderIndex))
	require.Nil(t, set.Module(leaderIndex))
	stopMark := len(tr.appended)
	requireAwaitTick(t, set, "session observes leader change", durableWorkBound, func() bool {
		failIfAnyLogReplication(t, set)
		tr.pump(nil)
		return tr.stopped || tr.session.LeaderMemberID() != int32(leaderIndex)
	})
	tr.logCounts("session observes leader change", stopMark)

	requireAwaitTick(t, set, "re-elect after leader stop", durableReElectBound, func() bool {
		failIfAnyLogReplication(t, set)
		tr.pump(currentLeader(tr.set))
		return reElectionReady(set, leaderIndex, preTerm)
	})
	tr.logCounts("re-elect after leader stop", stopMark)
	requireReElectedLeader(t, set, leaderIndex, preTerm)

	// Survivor extend evidence. Wait until both survivors record again. The new
	// leader holds a term entry for the new term.
	termMark := len(tr.appended)
	requireAwaitTick(t, set, "survivors extend under new term", durableWorkBound, func() bool {
		failIfAnyLogReplication(t, set)
		tr.pump(currentLeader(tr.set))
		if !leaderRecordsNewTerm(currentLeader(set), leaderIndex, preTerm) {
			return false
		}
		return survivorsHoldRecordings(set, []int{leaderIndex}, preIDs)
	})
	tr.logCounts("survivors extend under new term", termMark)

	for i, m := range set.Modules() {
		if m == nil || i == leaderIndex {
			continue
		}
		requireRecordingHeld(t, set, m, preIDs[i], "survivor after re-elect")
	}

	// Traffic to the new leader. Fresh transport and connect.
	newLeader := requireSingleLeader(t, set, "before reconnect")
	transport, session := openSession(t, set, newLeader)
	tr.rearm(transport, session)
	newSessionID := session.ClusterSessionID()

	survivorIndex := firstNonLeaderIndex(set, newLeader.ThisMemberID())
	require.GreaterOrEqual(
		t,
		survivorIndex,
		0,
		"no surviving follower after re-elect:\n%s",
		set.State(),
	)
	survivorWitness := installLogWitness(t, set, set.Module(survivorIndex))

	postMark := len(tr.appended)
	postTarget := awaitTrafficFloorAndCommit(
		t, set, tr, "post-re-elect traffic extend", postMark,
		func(cur *cluster.ConsensusModule, target int64) bool {
			if !survivorsReachCommit(
				set,
				[]int{leaderIndex},
				cur.ThisMemberID(),
				cur.CommitPosition(),
				target,
			) {
				return false
			}
			return survivorsRecordingAtLeast(set, []int{leaderIndex}, preIDs, target+1)
		},
		func() bool {
			return survivorWitness.sawSession(newSessionID)
		},
	)
	require.False(t, tr.stopped, "session stopped after reconnect:\n%s", set.State())
	require.Positive(t, postTarget)

	// Ex-leader rejoin. Restart promptly so a second term cannot form.
	require.NoError(t, set.RestartMember(leaderIndex))
	requireWarmRestartOpen(t, set, leaderIndex, "returning ex-leader")
	rejoinReadyMark := len(tr.appended)
	awaitPostRestartReady(t, set, func() bool {
		failIfAnyLogReplication(t, set)
		tr.pump(currentLeader(tr.set))
		returning := set.Module(leaderIndex)
		return returning != nil && returning.ElectionComplete()
	})
	tr.logCounts("post-restart election", rejoinReadyMark)

	returning := set.Module(leaderIndex)
	require.NotNil(t, returning, "returning ex-leader missing:\n%s", set.State())
	require.Equal(
		t,
		consensus.RoleFollower,
		returning.Role(),
		"returning member Role after rejoin:\n%s",
		set.State(),
	)
	requireRecordingHeld(t, set, returning, preIDs[leaderIndex], "returning ex-leader after rejoin")
	require.Empty(
		t,
		returning.LogReplayReason(),
		"LogReplayReason after ex-leader rejoin:\n%s",
		set.State(),
	)

	// Converge and close.
	rejoinWitness := installLogWitness(t, set, returning)
	rejoinMark := len(tr.appended)
	rejoinTarget := awaitTrafficFloorAndCommit(
		t, set, tr, "post-rejoin traffic commit", rejoinMark,
		func(_ *cluster.ConsensusModule, target int64) bool {
			return membersReachCommit(set, target)
		},
		func() bool {
			return rejoinWitness.sawSession(newSessionID)
		},
	)
	require.Positive(t, rejoinTarget)
	drainSession(t, set, tr)
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

	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnect(
		t, set, leader, transport, []byte(sessionSmokeCredentials),
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
//   - an active member is on the nil-factory follower log-replication path
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

	var target int64
	haveTarget := false
	requireAwaitTick(t, set, label, durableWorkBound, func() bool {
		failIfAnyLogReplication(t, set)
		leader := currentLeader(set)
		if leader == nil {
			return false
		}
		tr.pump(leader)
		if tr.appendedSince(mark) < trafficPhaseFloor {
			return false
		}
		if !haveTarget {
			target = leader.LogPublisher().Position()
			haveTarget = true
		}
		if !commitFn(leader, target) {
			return false
		}
		if extra != nil && !extra() {
			return false
		}
		return true
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

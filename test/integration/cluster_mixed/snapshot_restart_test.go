//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/heron/test/integration/harness"
)

// The Go member keeps ServiceCount 0.
//
// Callers must not call t.Parallel because the test owns the compose-fixed
// directories and the PortBase 9000 block.
func TestClusterMixedJavaLeaderSnapshotRestart(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeJavaLeaderSnapshotEnvKey,
		clusterjava.MixedComposeSkipHintJavaLeaderSnapshot,
	)

	cfg := buildMixedJavaLeaderConfig(t)
	f := openMixedGoMember(t, cfg)
	writeReady(t)

	java := awaitJavaModuleState(t, f, []int{0, 1}, clusterjava.MixedStartupBudget)
	awaitJavaLeaderElection(t, f, java, clusterjava.MixedElectionBudget)
	term := f.module.LeadershipTermID()

	requireLiveLogImage(t, f)
	require.True(t, f.module.IsColdStart())
	recordingID, _ := requireGoLogRecording(
		t,
		f,
		f.module.LogAdapter().Image().SessionID(),
	)

	capture := &mixedAdminCapture{}
	session, aeronClient, hadClientError := connectMixedSession(t, f, capture.handler())
	pump := func() { mixedSessionPump(f, aeronClient, session) }

	beforeCommit := f.module.NotifiedCommitPosition()
	_, takeOK := f.module.Agent().LastSnapshotTake()
	require.False(
		t,
		takeOK,
		"snapshot take resolved before the request:\n%s",
		dumpMixedState(f.module, java),
	)
	requireJavaSnapshotCount(t, java, 0)

	requestJavaLeaderSnapshot(
		t,
		f,
		aeronClient,
		session,
		hadClientError,
		capture,
		clusterjava.MixedCommitBudget,
	)

	take := awaitGoSnapshotTake(t, f, pump, java, clusterjava.MixedCommitBudget)
	require.NoError(t, take.Err)
	require.NoError(t, take.ForceErr)
	require.Equal(t, term, take.LeadershipTermID)
	require.Empty(t, take.ServiceRecordingIDs)
	require.Greater(t, take.LogPosition, beforeCommit)
	require.GreaterOrEqual(t, take.RecordingID, int64(0))
	require.NotEqual(t, recordingID, take.RecordingID)
	snapshotLogPosition := take.LogPosition
	awaitJavaSnapshotCounts(
		t,
		f,
		pump,
		java,
		[]int64{1, 1},
		snapshotLogPosition,
		clusterjava.MixedCommitBudget,
	)

	require.NoError(t, session.Close())
	closedCommit := awaitGoSessionClosed(t, f, func() {
		f.tick()
		aeronClient.DoWork()
	}, java, snapshotLogPosition, clusterjava.MixedCommitBudget)
	require.NoError(t, aeronClient.Close())

	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		closedCommit,
		clusterjava.MixedCommitBudget,
	)
	require.NoError(t, f.module.Close())
	requireGoSnapshotRow(t, take)

	f = reopenMixedGoMember(t, cfg)
	require.False(t, f.module.IsColdStart())
	require.Equal(t, snapshotLogPosition, f.module.CommitPosition())
	agent := f.module.Agent()
	require.NotNil(t, agent)
	require.Equal(t, 1, agent.OpenSessionCount())
	recordingLog := f.module.RecordingLog()
	require.NotNil(t, recordingLog)
	require.Equal(t, recordingID, recordingLog.TermRecordingID())

	awaitJavaLeaderElection(t, f, java, clusterjava.MixedRejoinBudget)
	require.Equal(t, term, f.module.LeadershipTermID())
	require.Equal(t, 0, agent.OpenSessionCount())
	require.Equal(t, 0, agent.ClosingSessionCount())
	require.Empty(t, f.module.LogReplayReason())

	before := max(
		f.module.NotifiedCommitPosition(),
		harness.RecordingCounterPosition(f.module, recordingID),
	)
	induceSessionMessage(t, f)
	awaitNotifiedCommitAdvance(t, f, before, clusterjava.MixedCommitBudget)
	after := f.module.NotifiedCommitPosition()
	awaitGoRecordingReaches(
		t,
		f,
		java,
		recordingID,
		after,
		clusterjava.MixedCommitBudget,
	)
	require.Equal(t, recordingID, agent.LocalLogRecordingID())

	require.NoError(t, f.module.Close())
	markMixedSmokeCompleted(t)
}

// The Go member keeps ServiceCount 0.
//
// Callers must not call t.Parallel because the test owns the compose-fixed
// directories and the PortBase 9000 block.
func TestClusterMixedGoLeaderSnapshotRestart(t *testing.T) {
	requireMixedCompose(
		t,
		clusterjava.MixedComposeGoLeaderSnapshotEnvKey,
		clusterjava.MixedComposeSkipHintGoLeaderSnapshot,
	)

	requireMixedAeronDirsPresent(t)
	java := awaitJavaModuleState(t, nil, []int{0, 1}, clusterjava.MixedStartupBudget)
	recorded := recordJavaCommitPositions(t, java)
	requireJavaSnapshotCount(t, java, 0)

	f := openMixedGoMember(t, buildMixedGoLeaderConfig(t))
	writeReady(t)

	awaitGoLeaderElection(t, f, java, clusterjava.MixedElectionBudget)
	term := f.module.LeadershipTermID()

	awaitJavaServiceSawTermStart(t, f, java, clusterjava.MixedElectionBudget)
	awaitJavaCommitAdvance(t, f, java, recorded, clusterjava.MixedCommitBudget)

	p1 := requestGoLeaderSnapshot(t, f, java, clusterjava.MixedCommitBudget).LogPosition
	awaitJavaSnapshotCounts(
		t,
		f,
		f.tick,
		java,
		[]int64{1, 1},
		p1,
		clusterjava.MixedCommitBudget,
	)

	restartJavaMember(t, f, java, p1, clusterjava.MixedStartupBudget)
	fresh := awaitJavaModuleState(t, f, []int{1}, clusterjava.MixedStartupBudget)
	java[1] = fresh[0]

	awaitJavaFollowerRejoin(t, f, java, 1, term, p1, clusterjava.MixedRejoinBudget)

	p2 := requestGoLeaderSnapshot(t, f, java, clusterjava.MixedCommitBudget).LogPosition
	require.Greater(t, p2, p1)
	awaitJavaSnapshotCounts(
		t,
		f,
		f.tick,
		java,
		[]int64{2, 1},
		p2,
		clusterjava.MixedCommitBudget,
	)

	require.True(t, f.module.IsLeader())
	require.Equal(t, term, f.module.LeadershipTermID())
	agent := f.module.Agent()
	require.NotNil(t, agent)
	require.Equal(t, int64(1), agent.ElectionCount())

	require.NoError(t, f.module.Close())
	markMixedSmokeCompleted(t)
}

func reopenMixedGoMember(t *testing.T, cfg cluster.Config) *mixedMemberFixture {
	t.Helper()

	driver := harness.JoinExternal(t, cfg.AeronDir)
	start := time.Now()
	var module *cluster.ConsensusModule
	var err error
	harness.PollUntilBool(clusterjava.MixedLogRebindBudget, 10*time.Millisecond, func() bool {
		module, err = cluster.OpenConsensusModule(context.Background(), cluster.ConsensusModuleDeps{
			Config: cfg,
		})
		return err == nil || !strings.Contains(err.Error(), "Address already in use")
	})
	require.NoError(t, err)
	member := newMixedMemberFixture(t, driver, module)
	t.Logf(
		"go member rebound elapsed=%s budget=%s",
		time.Since(start),
		clusterjava.MixedLogRebindBudget,
	)
	return member
}

// The compose media driver closes a client that skips DoWork for longer
// than the liveness timeout.
func mixedSessionPump(
	f *mixedMemberFixture,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
) {
	f.tick()
	aeronClient.DoWork()
	session.PollEgress()
}

type mixedAdminCapture struct {
	sessionID     int64
	correlationID int64
	requestType   client.AdminRequestType
	responseCode  client.AdminResponseCode
	message       []byte
	seen          bool
}

func (c *mixedAdminCapture) handler() client.EgressHandler {
	return &client.EgressHandlers{
		AdminResponseFunc: func(
			clusterSessionID int64,
			correlationID int64,
			requestType client.AdminRequestType,
			responseCode client.AdminResponseCode,
			message []byte,
			_ []byte,
		) {
			if c.seen {
				return
			}
			c.sessionID = clusterSessionID
			c.correlationID = correlationID
			c.requestType = requestType
			c.responseCode = responseCode
			c.message = append([]byte(nil), message...)
			c.seen = true
		},
	}
}

// dumpMixedSnapshotState appends the Go snapshot take to dumpMixedState.
func dumpMixedSnapshotState(f *mixedMemberFixture, java []*harness.DriverCounters) string {
	agent := f.module.Agent()
	take, ok := agent.LastSnapshotTake()
	return dumpMixedState(f.module, java) + fmt.Sprintf(
		"lastSnapshotTake ok=%v logPosition=%d leadershipTermID=%d timestamp=%d "+
			"recordingID=%d serviceRecordingIDs=%v err=%v forceErr=%v pending=%v\n",
		ok,
		take.LogPosition,
		take.LeadershipTermID,
		take.Timestamp,
		take.RecordingID,
		take.ServiceRecordingIDs,
		take.Err,
		take.ForceErr,
		agent.HasPendingSnapshotAction(),
	)
}

func requireJavaSnapshotCount(t *testing.T, java []*harness.DriverCounters, want int64) {
	t.Helper()

	for i, c := range java {
		require.NotNil(t, c)
		val, ok := c.Value(consensus.ClusterSnapshotCounterTypeID)
		require.Truef(
			t,
			ok,
			"javaMember=%d snapshot counter %d missing",
			i,
			consensus.ClusterSnapshotCounterTypeID,
		)
		require.Equal(
			t,
			want,
			val,
			"javaMember=%d snapshot counter %d",
			i,
			consensus.ClusterSnapshotCounterTypeID,
		)
	}
}

func requestJavaLeaderSnapshot(
	t *testing.T,
	f *mixedMemberFixture,
	aeronClient *aeron.Client,
	session *client.AeronCluster,
	hadClientError *atomic.Bool,
	capture *mixedAdminCapture,
	budget time.Duration,
) {
	t.Helper()

	correlationID := aeronClient.NextCorrelationID()
	offerMixedIngress(t, f, aeronClient, session, hadClientError, "snapshot", func() int64 {
		return session.SendSnapshotRequest(correlationID)
	})

	start := time.Now()
	ok := harness.PollUntilBool(budget, time.Millisecond, func() bool {
		mixedSessionPump(f, aeronClient, session)
		return capture.seen
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"admin snapshot response missing after %s (budget %s): correlationID=%d:\n%s",
			elapsed,
			budget,
			correlationID,
			dumpMixedState(f.module, nil),
		)
	}
	require.Equal(t, session.ClusterSessionID(), capture.sessionID)
	require.Equal(t, correlationID, capture.correlationID)
	require.Equal(t, client.AdminRequestTypeSnapshot, capture.requestType)
	require.Equal(
		t,
		client.AdminResponseCodeOK,
		capture.responseCode,
		"admin response message=%q",
		capture.message,
	)
	t.Logf(
		"admin snapshot response elapsed=%s budget=%s correlationID=%d",
		elapsed,
		budget,
		correlationID,
	)
}

// Call after ConsensusModule.Close releases the recording log.
func requireGoSnapshotRow(t *testing.T, take consensus.SnapshotTakeResult) {
	t.Helper()

	log, err := recordinglog.Open(clusterjava.MixedClusterDirGo, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, log.Close()) }()

	var snapshots []recordinglog.Entry
	for _, entry := range log.Entries() {
		if entry.Type == recordinglog.EntryTypeSnapshot {
			snapshots = append(snapshots, entry)
		}
	}
	require.Len(t, snapshots, 1)
	row := snapshots[0]
	term := log.FindTermEntry(take.LeadershipTermID)
	require.NotNil(t, term, "no term row for %d", take.LeadershipTermID)
	require.Equal(t, take.RecordingID, row.RecordingID)
	require.Equal(t, take.LeadershipTermID, row.LeadershipTermID)
	require.Equal(t, term.TermBaseLogPosition, row.TermBaseLogPosition)
	require.Equal(t, take.LogPosition, row.LogPosition)
	require.Equal(t, take.Timestamp, row.Timestamp)
	require.Equal(t, recordinglog.ConsensusModuleServiceID, row.ServiceID)
	require.True(t, row.Valid)
}

func awaitGoSnapshotTake(
	t *testing.T,
	f *mixedMemberFixture,
	poll func(),
	java []*harness.DriverCounters,
	budget time.Duration,
) consensus.SnapshotTakeResult {
	t.Helper()

	start := time.Now()
	var take consensus.SnapshotTakeResult
	var resolved bool
	ok := harness.PollUntilBool(budget, time.Millisecond, func() bool {
		poll()
		take, resolved = f.module.Agent().LastSnapshotTake()
		return resolved
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"go snapshot take did not resolve after %s (budget %s):\n%s",
			elapsed,
			budget,
			dumpMixedSnapshotState(f, java),
		)
	}
	t.Logf(
		"go snapshot take elapsed=%s budget=%s logPosition=%d recordingID=%d",
		elapsed,
		budget,
		take.LogPosition,
		take.RecordingID,
	)
	return take
}

func awaitJavaSnapshotCounts(
	t *testing.T,
	f *mixedMemberFixture,
	poll func(),
	java []*harness.DriverCounters,
	want []int64,
	floor int64,
	budget time.Duration,
) {
	t.Helper()

	require.Len(t, want, len(java))
	start := time.Now()
	ok := harness.PollUntilBool(budget, time.Millisecond, func() bool {
		poll()
		for i, c := range java {
			snap, snapOK := c.Value(consensus.ClusterSnapshotCounterTypeID)
			commit, commitOK := c.Value(consensus.ClusterCommitPositionCounterTypeID)
			if !snapOK || snap != want[i] || !commitOK || commit < floor {
				return false
			}
		}
		return true
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"java snapshot counts did not reach want=%v floor=%d after %s (budget %s):\n%s",
			want,
			floor,
			elapsed,
			budget,
			dumpMixedSnapshotState(f, java),
		)
	}
	t.Logf("java snapshot counts elapsed=%s budget=%s want=%v floor=%d", elapsed, budget, want, floor)
}

func awaitGoSessionClosed(
	t *testing.T,
	f *mixedMemberFixture,
	poll func(),
	java []*harness.DriverCounters,
	floor int64,
	budget time.Duration,
) int64 {
	t.Helper()

	start := time.Now()
	var commit int64
	ok := harness.PollUntilBool(budget, time.Millisecond, func() bool {
		poll()
		agent := f.module.Agent()
		if agent.OpenSessionCount() != 0 || agent.ClosingSessionCount() != 0 {
			return false
		}
		commit = f.module.NotifiedCommitPosition()
		if commit <= floor {
			return false
		}
		javaCommit, javaOK := java[0].Value(consensus.ClusterCommitPositionCounterTypeID)
		return javaOK && javaCommit == commit
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"go session did not close after %s (budget %s): floor=%d commit=%d:\n%s",
			elapsed,
			budget,
			floor,
			f.module.NotifiedCommitPosition(),
			dumpMixedState(f.module, java),
		)
	}
	t.Logf("go session closed elapsed=%s budget=%s commit=%d", elapsed, budget, commit)
	return commit
}

func requestGoLeaderSnapshot(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	budget time.Duration,
) consensus.SnapshotTakeResult {
	t.Helper()

	term := f.module.LeadershipTermID()
	nowNs := f.module.NanoClock()()
	require.NoError(t, f.module.RequestSnapshot(nowNs))
	publisher := f.module.LogPublisher()
	require.NotNil(t, publisher)
	actionPosition := publisher.Position()
	agent := f.module.Agent()
	require.NotNil(t, agent)
	require.True(
		t,
		agent.HasPendingSnapshotAction(),
		"snapshot action not queued:\n%s",
		dumpMixedSnapshotState(f, java),
	)

	start := time.Now()
	ok := f.tickUntil(budget, func() bool {
		return !agent.HasPendingSnapshotAction()
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"go leader snapshot did not resolve after %s (budget %s):\n%s",
			elapsed,
			budget,
			dumpMixedSnapshotState(f, java),
		)
	}
	take, resolved := agent.LastSnapshotTake()
	require.True(
		t,
		resolved,
		"snapshot take missing:\n%s",
		dumpMixedSnapshotState(f, java),
	)
	require.NoError(t, take.Err)
	require.NoError(t, take.ForceErr)
	require.Equal(t, actionPosition, take.LogPosition)
	require.Equal(t, term, take.LeadershipTermID)
	require.Equal(t, nowNs, take.Timestamp)
	require.Empty(t, take.ServiceRecordingIDs)
	t.Logf(
		"go leader snapshot elapsed=%s budget=%s logPosition=%d",
		elapsed,
		budget,
		take.LogPosition,
	)
	return take
}

// The go-leader-snapshot overlay restarts Java member 1 only.
func restartJavaMember(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	wantPosition int64,
	budget time.Duration,
) {
	t.Helper()

	const memberID = 1
	probePath := clusterjava.SnapshotProbePath(memberID)
	_, err := os.Stat(probePath)
	require.ErrorIs(t, err, os.ErrNotExist)
	writeMarkerFile(t, clusterjava.MixedRestartPathMember1, "restart\n")

	start := time.Now()
	// The watcher deletes the restart file before relaunch, and the probe
	// starts absent, so a matching probe proves the restart.
	ok := f.tickUntil(budget, func() bool {
		data, readErr := os.ReadFile(probePath)
		if readErr != nil {
			return false
		}
		probe, parseErr := clusterjava.ParseSnapshotProbe(data)
		return parseErr == nil && probe.LogPosition == wantPosition
	})
	elapsed := time.Since(start)
	if !ok {
		restartState := "gone"
		if _, statErr := os.Stat(clusterjava.MixedRestartPathMember1); statErr == nil {
			restartState = "present"
		} else if !os.IsNotExist(statErr) {
			restartState = statErr.Error()
		}
		data, readErr := os.ReadFile(probePath)
		content := string(data)
		if readErr != nil {
			content = readErr.Error()
		}
		t.Fatalf(
			"java member %d did not restart from snapshot after %s (budget %s): "+
				"restartFile=%s probe=%q\n%s",
			memberID,
			elapsed,
			budget,
			restartState,
			content,
			dumpMixedState(f.module, java),
		)
	}
	t.Logf(
		"java restart elapsed=%s budget=%s member=%d logPosition=%d",
		elapsed,
		budget,
		memberID,
		wantPosition,
	)
}

func awaitJavaFollowerRejoin(
	t *testing.T,
	f *mixedMemberFixture,
	java []*harness.DriverCounters,
	memberID int,
	term int64,
	floor int64,
	budget time.Duration,
) {
	t.Helper()

	start := time.Now()
	c := java[memberID]
	ok := f.tickUntil(budget, func() bool {
		role, roleOK := javaMemberRole(c)
		if !roleOK || role != consensus.RoleFollower {
			return false
		}
		gotTerm, termOK := c.Value(consensus.ClusterLeadershipTermIDCounterTypeID)
		if !termOK || gotTerm != term {
			return false
		}
		if !javaElectionClosed(c) {
			return false
		}
		commit, commitOK := c.Value(consensus.ClusterCommitPositionCounterTypeID)
		return commitOK && commit >= floor
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatalf(
			"java member %d did not rejoin after %s (budget %s): term=%d floor=%d\n%s",
			memberID,
			elapsed,
			budget,
			term,
			floor,
			dumpMixedState(f.module, java),
		)
	}
	t.Logf(
		"java follower rejoin elapsed=%s budget=%s member=%d term=%d floor=%d",
		elapsed,
		budget,
		memberID,
		term,
		floor,
	)
}

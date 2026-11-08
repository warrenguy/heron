//go:build integration && archive_live

package cluster_archive_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/test/integration/harness"
)

const processClusterMemberCount = 3

// TestProcessCluster_restartsAStoppedFollower asserts equal applied lists after a SIGTERM restart.
func TestProcessCluster_restartsAStoppedFollower(t *testing.T) {
	t.Parallel()
	harness.RequireArchiveRuntime(t)

	pc := harness.StartProcessCluster(t, harness.ProcessClusterConfig{
		MemberCount:          processClusterMemberCount,
		PortBase:             harness.ProcessClusterSmokePortBase,
		LogMulticastEndpoint: harness.ProcessClusterSmokeLogEndpoint,
	})

	const (
		electBound = 60 * time.Second
		workBound  = 60 * time.Second
		stopBound  = 30 * time.Second
	)
	requireProcessAwait(t, pc, "election", electBound, func() bool {
		return isElectionComplete(pc.Statuses())
	})

	statuses := pc.Statuses()
	require.Len(t, statuses, processClusterMemberCount, "statuses\n%s", pc.State())
	leaders := 0
	follower := -1
	for i, status := range statuses {
		switch status.Role {
		case consensus.RoleLeader:
			leaders++
		case consensus.RoleFollower:
			if follower < 0 {
				follower = i
			}
		}
	}
	require.Equal(t, 1, leaders, "leaders\n%s", pc.State())
	require.GreaterOrEqual(t, follower, 0, "follower\n%s", pc.State())

	session := harness.StartSequenceClient(t, pc)
	awaitAcked := func(label string, target int) {
		t.Helper()
		requireProcessAwait(t, pc, label, workBound, func() bool {
			session.Pump()
			return session.Acked() >= target
		})
	}
	awaitAcked("echoes", 50)

	victim := pc.Member(follower)
	require.NoError(t, victim.SignalStop())
	requireProcessAwait(t, pc, "follower stop", stopBound, func() bool {
		session.Pump()
		return victim.ProcessState() != nil
	})
	require.NoError(t, victim.AwaitStop(stopBound), "follower stop\n%s", pc.State())
	exitRec, _ := victim.ExitRecord()
	require.Empty(t, exitRec.Error)

	restartStart := time.Now()
	require.NoError(t, pc.Restart(follower, session.Pump))
	t.Logf("restart elapsed=%s\n%s", time.Since(restartStart), pc.State())

	awaitAcked("echoes after restart", session.Acked()+50)

	session.SetSending(false)

	// sequenceService does not retry a failed echo. That sequence stays in
	// Pending. Each applied list must hold the Pending sequences too.
	requireProcessAwait(t, pc, "applied lists", workBound, func() bool {
		session.Pump()

		if !isCaughtUp(pc.Statuses()) {
			return false
		}

		lists := appliedLists(pc)
		if !hasEqualLists(lists) {
			return false
		}

		echoed := session.Echoed()
		return len(echoed) > 0 &&
			hasSeqs(echoed, lists[0]) &&
			hasSeqs(session.Pending(), lists[0])
	})

	stopAllStart := time.Now()
	require.NoError(t, pc.StopAll(stopBound))
	t.Logf("stop all elapsed=%s\n%s", time.Since(stopAllStart), pc.State())
	for i := range processClusterMemberCount {
		for n, inc := range pc.Incarnations(i) {
			require.Equal(t, 0, inc.ProcessState().ExitCode(), "slot %d incarnation %d", i, n)
		}
	}
}

func isElectionComplete(statuses []harness.MemberStatus) bool {
	if len(statuses) != processClusterMemberCount {
		return false
	}
	for _, status := range statuses {
		if !status.ElectionComplete {
			return false
		}
	}
	return true
}

func isCaughtUp(statuses []harness.MemberStatus) bool {
	if len(statuses) != processClusterMemberCount {
		return false
	}

	commit := statuses[0].CommitPosition
	if commit <= 0 {
		return false
	}

	for _, status := range statuses {
		if status.CommitPosition != commit || status.ServiceLogPosition != commit {
			return false
		}
	}
	return true
}

func appliedLists(pc *harness.ProcessCluster) [][]int64 {
	lists := make([][]int64, processClusterMemberCount)
	for i := range processClusterMemberCount {
		applied := pc.Member(i).Applied()
		lists[i] = make([]int64, 0, len(applied))
		for _, rec := range applied {
			lists[i] = append(lists[i], rec.Seq)
		}
	}
	return lists
}

func hasEqualLists(lists [][]int64) bool {
	if len(lists[0]) == 0 {
		return false
	}
	for _, list := range lists[1:] {
		if !slices.Equal(lists[0], list) {
			return false
		}
	}
	return true
}

func hasSeqs(seqs, applied []int64) bool {
	for _, seq := range seqs {
		if !slices.Contains(applied, seq) {
			return false
		}
	}
	return true
}

func requireProcessAwait(
	t *testing.T,
	pc *harness.ProcessCluster,
	label string,
	bound time.Duration,
	cond func() bool,
) {
	t.Helper()

	requireAwait(t, pc, label, bound, func(bound time.Duration) bool {
		return harness.PollUntilBool(bound, time.Millisecond, cond)
	})
}

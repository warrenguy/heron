//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
	"github.com/warrenguy/luas/test/integration/harness"
	"github.com/warrenguy/luas/test/integration/internal/soakrun"
)

const (
	sigkillElectBound = 60 * time.Second
	sigkillWorkBound  = 60 * time.Second
	sigkillReapBound  = 30 * time.Second
	sigkillStopBound  = 30 * time.Second

	sigkillWarmEchoes       = 200
	sigkillNewTermEchoes    = 50
	sigkillRejoinEchoes     = 100
	sigkillRecordingAdvance = 96 * 1024
)

type sigkillRestartRow struct {
	name       string
	portBase   int
	endpoint   string
	killLeader bool
}

// TestProcessCluster_sigkillRestart restarts a member after SIGKILL under traffic.
func TestProcessCluster_sigkillRestart(t *testing.T) {
	t.Parallel()

	rows := []sigkillRestartRow{
		{
			name:       "follower",
			portBase:   harness.CrashRestartFollowerPortBase,
			endpoint:   harness.CrashRestartFollowerLogEndpoint,
			killLeader: false,
		},
		{
			name:       "leader",
			portBase:   harness.CrashRestartLeaderPortBase,
			endpoint:   harness.CrashRestartLeaderLogEndpoint,
			killLeader: true,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			runSigkillRestart(t, row)
		})
	}
}

func runSigkillRestart(t testing.TB, row sigkillRestartRow) {
	t.Helper()

	r := startSigkillRun(t, row)
	r.killUnderTraffic()

	if row.killLeader {
		r.awaitNewTerm()
	}

	r.restartVictim()
	r.awaitRejoin()
	commit := r.quiesce()
	finalTerm := r.requireOutcome()
	r.requireLogs()
	r.checkAfterStop(commit, finalTerm)
}

type sigkillRun struct {
	t           testing.TB
	row         sigkillRestartRow
	pc          *harness.ProcessCluster
	session     *harness.SequenceClient
	st          lazyText
	victim      int
	clientID    int64
	recordingID int64
	pre         harness.MemberStatus
	snap        []harness.MemberStatus
	preTerms    []recordinglog.Entry
}

func startSigkillRun(t testing.TB, row sigkillRestartRow) *sigkillRun {
	t.Helper()

	harness.RequireArchiveRuntime(t)

	pc := harness.StartProcessCluster(t, harness.ProcessClusterConfig{
		MemberCount:          processClusterMemberCount,
		PortBase:             row.portBase,
		LogMulticastEndpoint: row.endpoint,
	})

	requireProcessAwait(t, pc, "election", sigkillElectBound, func() bool {
		return isElectionComplete(pc.Statuses())
	})

	victim := sigkillVictim(t, pc, row.killLeader)
	t.Logf("victim member %d", victim)

	session := harness.StartSequenceClient(t, pc)
	requireProcessAwait(t, pc, "warm-up", sigkillWorkBound, func() bool {
		session.Pump()
		return session.Acked() >= sigkillWarmEchoes
	})

	return &sigkillRun{
		t:       t,
		row:     row,
		pc:      pc,
		session: session,
		st:      lazyText(pc.State),
		victim:  victim,
	}
}

func (r *sigkillRun) killUnderTraffic() {
	r.t.Helper()

	killStatus, ok := r.pc.Member(r.victim).Status()
	require.True(r.t, ok, "kill status\n%s", r.st)

	r.recordingID = killStatus.LocalLogRecordingID
	recordingPosition, posOK := r.pc.RecordingPosition(r.victim, r.recordingID)
	require.True(r.t, posOK, "recording position\n%s", r.st)

	ready, ok := r.pc.Member(r.victim).Ready()
	require.True(r.t, ok, "victim ready\n%s", r.st)

	r.clientID = ready.ClientID
	threshold := recordingPosition + sigkillRecordingAdvance

	requireProcessAwait(r.t, r.pc, "kill position", sigkillWorkBound, func() bool {
		r.session.Pump()

		pos, posOK := r.pc.RecordingPosition(r.victim, r.recordingID)
		if !posOK || pos < threshold {
			return false
		}

		if r.session.InFlight() == 0 {
			return false
		}

		r.snap = r.pc.Statuses()
		return len(r.snap) == processClusterMemberCount
	})

	r.pre = r.snap[r.victim]
	require.Equal(r.t, r.recordingID, r.pre.LocalLogRecordingID, "recording id\n%s", r.st)

	require.NoError(r.t, r.pc.Kill(r.victim), "kill\n%s", r.st)
	requireSigkill(r.t, r.pc.Member(r.victim))
	require.True(r.t, r.pc.HasClient(r.victim, r.clientID), "dead client\n%s", r.st)
}

// An echo offered in the old term can arrive after the client reports the new
// term. awaitNewTerm counts only echoes offered after that report.
func (r *sigkillRun) awaitNewTerm() {
	r.t.Helper()

	var newTerm int64
	var newTermOffered int64

	requireProcessAwait(r.t, r.pc, "new term", sigkillElectBound, func() bool {
		r.session.Pump()

		leader, leaderOK := survivorLeader(r.pc, r.victim, r.pre.LeadershipTermID)
		if !leaderOK {
			return false
		}

		if r.session.LeadershipTermID() != leader.LeadershipTermID ||
			r.session.LeaderMemberID() != leader.MemberID {
			return false
		}

		newTerm = leader.LeadershipTermID
		newTermOffered = int64(r.session.Offered())
		return true
	})

	requireProcessAwait(r.t, r.pc, "new term echoes", sigkillWorkBound, func() bool {
		r.session.Pump()
		return r.session.LeadershipTermID() == newTerm &&
			countEchoesAfter(r.session.Echoed(), newTermOffered) >= sigkillNewTermEchoes
	})
}

func (r *sigkillRun) restartVictim() {
	r.t.Helper()

	requireProcessAwait(r.t, r.pc, "dead client", sigkillReapBound, func() bool {
		r.session.Pump()
		return !r.pc.HasClient(r.victim, r.clientID)
	})

	r.preTerms = loadTermRows(r.t, r.pc.ClusterDir(r.victim))
	require.NotEmpty(r.t, r.preTerms, "term rows\n%s", r.st)
	require.Equal(
		r.t,
		r.recordingID,
		r.preTerms[len(r.preTerms)-1].RecordingID,
		"last term recording\n%s",
		r.st,
	)

	require.NoError(r.t, r.pc.Restart(r.victim, r.session.Pump), "restart\n%s", r.st)

	restarted, ok := r.pc.Member(r.victim).Ready()
	require.True(r.t, ok, "restart ready\n%s", r.st)
	require.Equal(r.t, r.recordingID, restarted.TermRecordingID, "term recording\n%s", r.st)
	require.GreaterOrEqual(
		r.t,
		restarted.LogRecordingStopPosition,
		r.pre.ServiceLogPosition,
		"recording stop\n%s",
		r.st,
	)
}

func (r *sigkillRun) awaitRejoin() {
	r.t.Helper()

	var rejoinOffered int64

	requireProcessAwait(r.t, r.pc, "rejoin", sigkillWorkBound, func() bool {
		r.session.Pump()

		if !victimRejoined(r.pc, r.victim, r.pre, r.row.killLeader) {
			return false
		}

		rejoinOffered = int64(r.session.Offered())
		return true
	})

	requireProcessAwait(r.t, r.pc, "echoes after rejoin", sigkillWorkBound, func() bool {
		r.session.Pump()
		return countEchoesAfter(r.session.Echoed(), rejoinOffered) >= sigkillRejoinEchoes
	})
}

func (r *sigkillRun) quiesce() int64 {
	r.t.Helper()

	r.session.SetSending(false)

	var commit int64

	requireProcessAwait(r.t, r.pc, "quiesce", sigkillWorkBound, func() bool {
		r.session.Pump()

		var quiesced bool
		commit, quiesced = clusterQuiesced(r.pc)
		return quiesced
	})

	return commit
}

func (r *sigkillRun) checkAfterStop(commit, finalTerm int64) {
	r.t.Helper()

	require.NoError(r.t, r.pc.StopAll(sigkillStopBound), "stop\n%s", r.st)
	requireSigkillExits(r.t, r.pc, r.victim)

	postRows := make([][]recordinglog.Entry, processClusterMemberCount)

	for i := range processClusterMemberCount {
		postRows[i] = loadTermRows(r.t, r.pc.ClusterDir(i))
	}

	requireSameTermRows(r.t, postRows)
	requireTermRowsKept(r.t, r.preTerms, postRows[r.victim])

	if r.row.killLeader {
		for i, rows := range postRows {
			_, ok := termByID(rows, finalTerm)
			require.True(r.t, ok, "slot %d term %d", i, finalTerm)
		}
	}

	requireStoppedRecording(r.t, r.pc.DriverDir(r.victim), r.recordingID, commit)
}

func sigkillVictim(t testing.TB, pc *harness.ProcessCluster, killLeader bool) int {
	t.Helper()

	st := lazyText(pc.State)

	statuses := pc.Statuses()
	require.Len(t, statuses, processClusterMemberCount, "statuses\n%s", st)

	victim := -1
	leaderCount := 0

	for i, status := range statuses {
		switch status.Role {
		case consensus.RoleLeader:
			leaderCount++

			if killLeader {
				victim = i
			}

		case consensus.RoleFollower:
			if !killLeader && victim < 0 {
				victim = i
			}
		}
	}

	require.Equal(t, 1, leaderCount, "leaders\n%s", st)
	require.GreaterOrEqual(t, victim, 0, "victim\n%s", st)
	return victim
}

func survivorLeader(
	pc *harness.ProcessCluster,
	victim int,
	preTerm int64,
) (harness.MemberStatus, bool) {
	live := make([]harness.MemberStatus, 0, processClusterMemberCount-1)

	for i := range processClusterMemberCount {
		if i == victim {
			continue
		}

		status, ok := pc.Member(i).Status()
		if !ok || !status.Started || !status.ElectionComplete {
			return harness.MemberStatus{}, false
		}

		live = append(live, status)
	}

	term := live[0].LeadershipTermID
	if term <= preTerm {
		return harness.MemberStatus{}, false
	}

	var leader harness.MemberStatus

	leaders := 0

	for _, status := range live {
		if status.LeadershipTermID != term {
			return harness.MemberStatus{}, false
		}

		if status.Role != consensus.RoleLeader {
			continue
		}

		leaders++
		leader = status
	}

	if leaders != 1 {
		return harness.MemberStatus{}, false
	}

	// The leader leaves LeaderMemberID unset.
	if leader.LeaderMemberID != election.NullMemberID {
		return harness.MemberStatus{}, false
	}

	for _, status := range live {
		if status.Role == consensus.RoleLeader {
			continue
		}

		if status.LeaderMemberID != leader.MemberID {
			return harness.MemberStatus{}, false
		}
	}

	return leader, true
}

func victimRejoined(
	pc *harness.ProcessCluster,
	victim int,
	pre harness.MemberStatus,
	killLeader bool,
) bool {
	status, ok := pc.Member(victim).Status()
	if !ok || !status.Started || !status.ElectionComplete {
		return false
	}

	if status.Role != consensus.RoleFollower {
		return false
	}

	if killLeader {
		leader, leaderOK := survivorLeader(pc, victim, pre.LeadershipTermID)
		if !leaderOK {
			return false
		}

		return status.LeadershipTermID == leader.LeadershipTermID &&
			status.LeaderMemberID == leader.MemberID
	}

	return status.LeaderMemberID == pre.LeaderMemberID &&
		status.LeadershipTermID == pre.LeadershipTermID
}

func clusterQuiesced(pc *harness.ProcessCluster) (int64, bool) {
	statuses := pc.Statuses()
	if !isCaughtUp(statuses) {
		return 0, false
	}

	commit := statuses[0].CommitPosition

	for i, status := range statuses {
		pos, posOK := pc.RecordingPosition(i, status.LocalLogRecordingID)
		if !posOK || pos != commit {
			return 0, false
		}
	}

	return commit, true
}

func (r *sigkillRun) requireOutcome() int64 {
	r.t.Helper()

	statuses := r.pc.Statuses()
	require.Len(r.t, statuses, processClusterMemberCount, "statuses\n%s", r.st)

	isLeader := func(status harness.MemberStatus) bool {
		return status.Role == consensus.RoleLeader
	}

	leaderIndex := slices.IndexFunc(statuses, isLeader)
	require.GreaterOrEqual(r.t, leaderIndex, 0, "leader\n%s", r.st)
	require.Equal(
		r.t,
		-1,
		slices.IndexFunc(statuses[leaderIndex+1:], isLeader),
		"leaders\n%s",
		r.st,
	)

	leader := statuses[leaderIndex]

	if r.row.killLeader {
		require.NotEqual(r.t, int32(r.victim), leader.MemberID, "leader id\n%s", r.st)
		require.Greater(r.t, leader.LeadershipTermID, r.pre.LeadershipTermID, "term\n%s", r.st)
	} else {
		require.Equal(r.t, r.pre.LeaderMemberID, leader.MemberID, "leader id\n%s", r.st)
		require.Equal(r.t, r.pre.LeadershipTermID, leader.LeadershipTermID, "term\n%s", r.st)
	}

	for i, status := range statuses {
		require.True(r.t, status.ElectionComplete, "election %d\n%s", i, r.st)
		require.Equal(
			r.t,
			reportedLeaderID(status, leader),
			status.LeaderMemberID,
			"leader %d\n%s",
			i,
			r.st,
		)
		require.Equal(r.t, leader.LeadershipTermID, status.LeadershipTermID, "term %d\n%s", i, r.st)

		if i == r.victim {
			require.Equal(r.t, consensus.RoleFollower, status.Role, "victim role\n%s", r.st)
			require.Equal(
				r.t,
				r.pre.LocalLogRecordingID,
				status.LocalLogRecordingID,
				"recording\n%s",
				r.st,
			)
			require.True(r.t, status.RecordingActive, "recording active\n%s", r.st)
			continue
		}

		if r.row.killLeader {
			require.GreaterOrEqual(
				r.t,
				status.ElectionCount,
				r.snap[i].ElectionCount+1,
				"elections %d\n%s",
				i,
				r.st,
			)
			continue
		}

		require.Equal(
			r.t,
			r.snap[i].ElectionCount,
			status.ElectionCount,
			"elections %d\n%s",
			i,
			r.st,
		)
	}

	require.False(r.t, r.session.IsClosed(), "client closed\n%s", r.st)
	require.Equal(r.t, leader.MemberID, r.session.LeaderMemberID(), "client leader\n%s", r.st)
	require.Equal(r.t, leader.LeadershipTermID, r.session.LeadershipTermID(), "client term\n%s", r.st)
	return leader.LeadershipTermID
}

func (r *sigkillRun) requireLogs() {
	r.t.Helper()

	echoed := r.session.Echoed()
	lists := appliedLists(r.pc)
	require.True(r.t, strictlyIncreasing(lists[0]), "applied\n%s", r.st)

	for _, list := range lists[1:] {
		require.Equal(r.t, lists[0], list, "applied lists\n%s", r.st)
	}

	incs := r.pc.Incarnations(r.victim)
	require.Len(r.t, incs, 2, "victim incarnations\n%s", r.st)

	killedSeqs := appliedSeqs(incs[0])
	require.NotEmpty(r.t, killedSeqs, "killed applied\n%s", r.st)
	require.True(r.t, isPrefix(killedSeqs, lists[0]), "killed prefix\n%s", r.st)
	require.True(r.t, strictlyIncreasing(echoed), "echoes\n%s", r.st)
	require.True(r.t, hasSeqs(echoed, lists[0]), "echoed applied\n%s", r.st)
}

func reportedLeaderID(status, leader harness.MemberStatus) int32 {
	if status.MemberID == leader.MemberID {
		return election.NullMemberID
	}

	return leader.MemberID
}

func requireSigkill(t testing.TB, member *harness.ProcessMember) {
	t.Helper()

	state := member.ProcessState()
	require.NotNil(t, state, "killed process")

	ws, ok := state.Sys().(syscall.WaitStatus)
	require.True(t, ok, "process state %s", state)
	require.True(t, ws.Signaled(), "process state %s", state)
	require.Equal(t, syscall.SIGKILL, ws.Signal(), "process state %s", state)

	_, hasExit := member.ExitRecord()
	require.False(t, hasExit, "killed exit")
}

func requireSigkillExits(t testing.TB, pc *harness.ProcessCluster, victim int) {
	t.Helper()

	for i := range processClusterMemberCount {
		incs := pc.Incarnations(i)
		require.NotEmpty(t, incs, "slot %d", i)

		for n, inc := range incs {
			require.Empty(t, inc.RaceReports(), "slot %d incarnation %d", i, n)

			killed := i == victim && n == 0
			if killed {
				continue
			}

			state := inc.ProcessState()
			require.NotNil(t, state, "slot %d incarnation %d", i, n)
			require.Equal(t, 0, state.ExitCode(), "slot %d incarnation %d", i, n)

			exitRec, hasExit := inc.ExitRecord()
			require.True(t, hasExit, "slot %d incarnation %d exit", i, n)
			require.Equal(t, harness.MemberExit{}, exitRec, "slot %d incarnation %d", i, n)
		}
	}
}

func loadTermRows(t testing.TB, dir string) []recordinglog.Entry {
	t.Helper()

	entries, err := recordinglog.Reload(filepath.Join(dir, "recording.log"))
	require.NoError(t, err, "recording.log %s", dir)

	var terms []recordinglog.Entry

	for _, entry := range entries {
		if entry.Valid && entry.Type == recordinglog.EntryTypeTerm {
			terms = append(terms, entry)
		}
	}

	return terms
}

func requireSameTermRows(t testing.TB, rows [][]recordinglog.Entry) {
	t.Helper()

	files := make([][]soakrun.TermRow, len(rows))

	for i, entries := range rows {
		for _, entry := range entries {
			files[i] = append(files[i], soakrun.TermRow{
				Term:                entry.LeadershipTermID,
				TermBaseLogPosition: entry.TermBaseLogPosition,
				LogPosition:         entry.LogPosition,
			})
		}
	}

	require.NoError(t, soakrun.CheckTermRows(files))

	// CheckTermRows compares only the final base across files. Compare every base.
	for _, other := range files[1:] {
		for i, row := range other {
			require.Equal(t, files[0][i].TermBaseLogPosition, row.TermBaseLogPosition, "term base %d", i)
		}
	}
}

func requireTermRowsKept(t testing.TB, before, after []recordinglog.Entry) {
	t.Helper()

	for _, prior := range before {
		match, ok := termByID(after, prior.LeadershipTermID)
		require.True(t, ok, "term %d", prior.LeadershipTermID)
		require.Equal(
			t,
			prior.RecordingID,
			match.RecordingID,
			"term %d recording",
			prior.LeadershipTermID,
		)
		require.Equal(
			t,
			prior.TermBaseLogPosition,
			match.TermBaseLogPosition,
			"term %d base",
			prior.LeadershipTermID,
		)
	}
}

func termByID(rows []recordinglog.Entry, termID int64) (recordinglog.Entry, bool) {
	for _, row := range rows {
		if row.LeadershipTermID == termID {
			return row, true
		}
	}

	return recordinglog.Entry{}, false
}

func countEchoesAfter(echoed []int64, offered int64) int {
	n := 0

	for _, seq := range echoed {
		if seq > offered {
			n++
		}
	}

	return n
}

func strictlyIncreasing(seqs []int64) bool {
	if len(seqs) == 0 {
		return false
	}

	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			return false
		}
	}

	return true
}

func isPrefix(prefix, full []int64) bool {
	return len(prefix) <= len(full) && slices.Equal(prefix, full[:len(prefix)])
}

func appliedSeqs(member *harness.ProcessMember) []int64 {
	applied := member.Applied()
	seqs := make([]int64, 0, len(applied))

	for _, rec := range applied {
		seqs = append(seqs, rec.Seq)
	}

	return seqs
}

func requireStoppedRecording(
	t testing.TB,
	driverDir string,
	recordingID, commit int64,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), sigkillStopBound)
	defer cancel()

	desc, err := harness.AwaitRecordingStop(ctx, driverDir, recordingID, commit)
	require.NoError(t, err, "recording %d stop", recordingID)
	require.Equal(t, int64(0), desc.StartPosition, "start position")
}

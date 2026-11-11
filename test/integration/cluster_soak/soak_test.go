//go:build integration && cluster_soak

package cluster_soak_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/cluster/election"
	"github.com/warrenguy/luas/cluster/recordinglog"
	"github.com/warrenguy/luas/internal/linearizability"
	"github.com/warrenguy/luas/test/integration/harness"
	"github.com/warrenguy/luas/test/integration/internal/soakrun"
)

const (
	soakMembers       = kvSessions
	noMember          = -1
	soakFirstRoundEnd = 9
	soakSteady        = 120 * time.Second
	soakElection      = 60 * time.Second
	soakExitBound     = 30 * time.Second
	soakReapBound     = 30 * time.Second
	soakRecovery      = 120 * time.Second
	soakClientStop    = 60 * time.Second
	soakStopAll       = 30 * time.Second
	soakListBound     = 30 * time.Second
	soakReserve       = 20 * time.Minute
	soakMinReturned   = 1000
	soakRecoveryOps   = 20
	soakSteadyOps     = 1
	recordingFileName = "recording.log"
)

// TestLiveSoak_faultsAnAllGoCluster runs a faulted three-member key-value cluster.
func TestLiveSoak_faultsAnAllGoCluster(t *testing.T) {
	over, err := soakrun.ParseOverrides(
		os.Getenv("LUAS_SOAK_SEED"),
		os.Getenv("LUAS_SOAK_DURATION"),
	)
	require.NoError(t, err)

	if !over.HasSeed {
		over.Seed = rand.Uint64()
	}

	deadline, ok := t.Deadline()
	if ok && time.Until(deadline) < over.Duration+soakReserve {
		t.Fatalf("soak duration %s plus reserve exceeds the test deadline", over.Duration)
	}

	t.Logf("soak seed %d duration %s", over.Seed, over.Duration)

	netw := harness.StartFaultNetwork(t, soakMembers)
	runner := newSoakRunner(t, over.Seed, over.Duration, netw)
	runner.openTimeline()
	runner.startCluster()
	runner.startClient()
	runner.runFaults()
	runner.checkAfter()
}

type soakRunner struct {
	t        *testing.T
	seed     uint64
	duration time.Duration
	net      *harness.FaultNetwork
	pc       *harness.ProcessCluster
	kv       *keyValueClient
	obs      *soakrun.Observer
	timeline *os.File

	leaderFaults int
	firstTerm    int64
	lastTerm     int64
	stepCounts   map[string]int
	longest      map[string]time.Duration
	snap         *clientSnap
}

type clientSnap struct {
	returned     int
	nonEmptyGets int
	reopens      int
	ops          []porcupine.Operation
}

type timelineRow struct {
	Index      int    `json:"index"`
	Kind       string `json:"kind"`
	MemberID   int    `json:"memberId"`
	Start      string `json:"start"`
	Clear      string `json:"clear"`
	Recovery   string `json:"recovery"`
	TermBefore int64  `json:"termBefore"`
	TermAfter  int64  `json:"termAfter"`
}

func newSoakRunner(
	t *testing.T,
	seed uint64,
	duration time.Duration,
	netw *harness.FaultNetwork,
) *soakRunner {
	t.Helper()

	return &soakRunner{
		t:          t,
		seed:       seed,
		duration:   duration,
		net:        netw,
		obs:        &soakrun.Observer{},
		stepCounts: map[string]int{},
		longest:    map[string]time.Duration{},
	}
}

func (r *soakRunner) openTimeline() {
	r.t.Helper()
	require.NoError(r.t, soakrun.WriteSeedFile(r.t.ArtifactDir(), r.seed))
	path := filepath.Join(r.t.ArtifactDir(), "timeline.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(r.t, err)
	r.timeline = file
	r.t.Cleanup(func() { _ = file.Close() })
	r.t.Cleanup(r.writeResult)
}

func (r *soakRunner) startCluster() {
	r.t.Helper()
	r.pc = harness.StartProcessCluster(r.t, harness.ProcessClusterConfig{
		MemberCount:          soakMembers,
		PortBase:             harness.LiveSoakPortBase,
		LogMulticastEndpoint: harness.LiveSoakLogEndpoint,
		Network:              r.net,
		LogDir:               r.t.ArtifactDir(),
		Workload:             "keyvalue",
		StatusIntervalMs:     50,
		StatusHandler:        r.observe,
		AfterStop:            r.copyFailure,
	})
}

func (r *soakRunner) observe(slot, incarnation int, status harness.MemberStatus) {
	r.obs.Observe(soakrun.Sample{
		Slot:               slot,
		Incarnation:        incarnation,
		ReportedLeaderID:   reportedLeader(status),
		LeadershipTermID:   status.LeadershipTermID,
		ElectionComplete:   status.ElectionComplete,
		CommitPosition:     status.CommitPosition,
		ServiceLogPosition: status.ServiceLogPosition,
	})
}

func (r *soakRunner) startClient() {
	r.t.Helper()
	driver := r.net.StartClientDriver(r.t)
	transport := harness.ConnectClient(r.t, driver, aeron.Config{})
	r.kv = newKeyValueClient(
		transport,
		r.pc.IngressEndpoints(),
		r.net.ClientHost(),
		harness.LiveSoakPortBase,
		r.seed,
	)

	for i := range soakMembers {
		require.NoError(r.t, r.kv.Connect(r.t.Context(), i), "connect session %d\n%s", i, r.pc.State())
	}

	r.kv.Run()
	r.t.Cleanup(func() {
		if r.snap != nil {
			return
		}

		r.kv.Stop()
		select {
		case <-r.kv.Done():
			r.snapshotClient()
		case <-time.After(soakClientStop):
			r.t.Errorf("soak: client did not stop")
		}
	})
}

func (r *soakRunner) runFaults() {
	r.t.Helper()
	sched := soakrun.NewSchedule(r.seed)
	start := time.Now()

	for {
		step := sched.Next()
		r.runStep(step)

		if step.Index >= soakFirstRoundEnd && time.Since(start) >= r.duration {
			return
		}
	}
}

func (r *soakRunner) runStep(step soakrun.Step) {
	r.t.Helper()
	r.kv.AdvanceGeneration()
	require.NoError(r.t, r.awaitProgress(soakSteady, "steady", soakSteadyOps))

	member, err := r.resolve(step)
	require.NoError(r.t, err)
	termBefore := r.currentTerm()

	if step.Index == 0 {
		r.firstTerm = termBefore
	}

	if step.VictimRole == soakrun.Leader && countsLeaderFault(step.Kind) {
		r.leaderFaults++
	}

	r.stepCounts[step.Kind.String()]++
	r.t.Logf(
		"soak step index=%d kind=%s role=%s member=%d",
		step.Index,
		step.Kind,
		step.VictimRole,
		member,
	)
	started := time.Now()
	err = r.inject(step, member, termBefore)
	var clear time.Time
	var recovery time.Duration

	if err == nil {
		clear = time.Now()
		err = r.awaitProgress(soakRecovery, "recovery", soakRecoveryOps)
		recovery = time.Since(clear)
	}

	if err == nil {
		kind := step.Kind.String()
		r.longest[kind] = max(r.longest[kind], recovery)
		err = r.pause(step.Settle)
	}

	r.appendRow(step, member, started, clear, recovery, termBefore, r.currentTerm())
	require.NoError(r.t, err)
}

func (r *soakRunner) inject(step soakrun.Step, member int, termBefore int64) error {
	switch step.Kind {
	case soakrun.MemberKill, soakrun.ArchiveKill, soakrun.DriverKill:
		return r.injectKill(step, member, termBefore)
	case soakrun.PacketLoss, soakrun.Partition:
		return r.injectNet(step, member, termBefore)
	default:
		return fmt.Errorf("soak: unknown kind %s", step.Kind)
	}
}

func (r *soakRunner) injectKill(step soakrun.Step, member int, termBefore int64) error {
	victim := r.pc.Member(member)

	switch step.Kind {
	case soakrun.MemberKill:
		if err := r.pc.Kill(member); err != nil {
			return err
		}

		if err := checkKilled(victim); err != nil {
			return err
		}

	case soakrun.ArchiveKill:
		if err := r.pc.KillArchive(member); err != nil {
			return err
		}

		text := consensus.ErrLocalArchiveDisconnected.Error()

		if err := r.awaitExit(member, victim, 4, text); err != nil {
			return err
		}

	case soakrun.DriverKill:
		if err := r.pc.KillDriver(member); err != nil {
			return err
		}

		if err := r.awaitExit(member, victim, 3, ""); err != nil {
			return err
		}

		if err := r.pc.KillArchive(member); err != nil {
			return err
		}
	}

	if step.VictimRole == soakrun.Leader {
		if err := r.waitElection(member, member, termBefore); err != nil {
			return err
		}
	}

	switch step.Kind {
	case soakrun.MemberKill:
		if err := r.reap(member, victim); err != nil {
			return err
		}

	case soakrun.ArchiveKill:
		if err := r.pc.RestartArchive(member); err != nil {
			return err
		}

		if err := r.reap(member, victim); err != nil {
			return err
		}

	case soakrun.DriverKill:
		if err := r.pc.RestartDriver(member); err != nil {
			return err
		}

		if err := r.pc.RestartArchive(member); err != nil {
			return err
		}
	}

	return r.pc.RestartReturning(member)
}

func (r *soakRunner) injectNet(step soakrun.Step, member int, termBefore int64) error {
	if err := r.net.Apply(soakrun.Rules(step, member)); err != nil {
		return err
	}

	if step.Kind == soakrun.Partition && step.VictimRole == soakrun.Leader {
		if err := r.waitElection(member, noMember, termBefore); err != nil {
			return err
		}
	}

	if err := r.pause(step.Hold); err != nil {
		return err
	}

	return r.net.Heal()
}

func (r *soakRunner) awaitExit(
	member int,
	victim *harness.ProcessMember,
	code int,
	text string,
) error {
	if err := r.wait(soakExitBound, "exit bound", member, victim.HasExited); err != nil {
		return err
	}

	return checkExited(victim, code, text)
}

func (r *soakRunner) waitElection(victim, exempt int, termBefore int64) error {
	return r.wait(soakElection, "election", exempt, func() bool {
		term, _, ok := r.agreedTerm(victim)

		return ok && term > termBefore
	})
}

func (r *soakRunner) reap(member int, victim *harness.ProcessMember) error {
	ready, ok := victim.Ready()

	if !ok {
		return fmt.Errorf("soak: member %d ready record missing", member)
	}

	return r.wait(soakReapBound, "reap bound", member, func() bool {
		return !r.pc.HasClient(member, ready.ClientID)
	})
}

func (r *soakRunner) awaitProgress(bound time.Duration, label string, need int64) error {
	base := r.kv.Returned()

	return r.wait(bound, label, noMember, func() bool {
		if !r.quiescedTerm() {
			return false
		}

		returned := r.kv.Returned()

		for i := range soakMembers {
			if returned[i] < base[i]+need {
				return false
			}
		}

		return true
	})
}

func (r *soakRunner) wait(
	bound time.Duration,
	label string,
	exempt int,
	done func() bool,
) error {
	deadline := time.Now().Add(bound)

	for {
		if err := r.healthy(exempt); err != nil {
			return err
		}

		if done() {
			return nil
		}

		if !time.Now().Before(deadline) {
			return fmt.Errorf("soak: %s elapsed\n%s", label, r.pc.State())
		}

		time.Sleep(time.Millisecond)
	}
}

func (r *soakRunner) pause(d time.Duration) error {
	deadline := time.Now().Add(d)

	for {
		if err := r.healthy(noMember); err != nil {
			return err
		}

		if !time.Now().Before(deadline) {
			return nil
		}

		time.Sleep(time.Millisecond)
	}
}

func (r *soakRunner) healthy(exempt int) error {
	if err := r.obs.Err(); err != nil {
		return err
	}

	return r.alive(exempt)
}

func (r *soakRunner) alive(exempt int) error {
	return soakrun.CheckAlive(r.pc.ChildStates(), exempt)
}

func (r *soakRunner) quiescedTerm() bool {
	_, leaders, ok := r.agreedTerm(noMember)

	return ok && leaders == 1
}

func (r *soakRunner) agreedTerm(skip int) (term int64, leaders int, ok bool) {
	var leader int32
	seen := false

	for i := range soakMembers {
		if i == skip {
			continue
		}

		status, statusOK := r.pc.Member(i).Status()

		if !statusOK || !status.Started || !status.ElectionComplete {
			return 0, 0, false
		}

		if status.Role == consensus.RoleLeader {
			leaders++
		}

		id := reportedLeader(status)

		if id < 0 {
			return 0, 0, false
		}

		if !seen {
			term = status.LeadershipTermID
			leader = id
			seen = true

			continue
		}

		if term != status.LeadershipTermID || leader != id {
			return 0, 0, false
		}
	}

	return term, leaders, true
}

func (r *soakRunner) resolve(step soakrun.Step) (int, error) {
	leader := -1
	var followers []int

	for i := range soakMembers {
		status, ok := r.pc.Member(i).Status()

		if !ok {
			return 0, fmt.Errorf("soak: member %d has no status\n%s", i, r.pc.State())
		}

		if status.Role == consensus.RoleLeader {
			leader = i

			continue
		}

		followers = append(followers, i)
	}

	if leader < 0 {
		return 0, fmt.Errorf("soak: no leader\n%s", r.pc.State())
	}

	if step.VictimRole == soakrun.Leader {
		return leader, nil
	}

	if len(followers) == 0 {
		return 0, fmt.Errorf("soak: no follower\n%s", r.pc.State())
	}

	return followers[step.FollowerPick%len(followers)], nil
}

func (r *soakRunner) currentTerm() int64 {
	statuses := r.pc.Statuses()

	for _, status := range statuses {
		if status.Role == consensus.RoleLeader {
			return status.LeadershipTermID
		}
	}

	if len(statuses) == 0 {
		return 0
	}

	return statuses[0].LeadershipTermID
}

func (r *soakRunner) checkAfter() {
	r.t.Helper()
	require.NoError(r.t, r.finishClient())
	term, commit := r.mustQuiesce()
	r.lastTerm = term
	r.mustDigest()
	r.mustPrefix()
	r.mustStop()
	r.mustTermRows(term)
	r.mustRecordings(commit)
	r.mustHistory()
	r.mustRaces()
	r.mustReplyPath()
}

func (r *soakRunner) finishClient() error {
	r.kv.Stop()

	return r.wait(soakClientStop, "client stop", noMember, func() bool {
		select {
		case <-r.kv.Done():
			r.snapshotClient()

			return true
		default:
			return false
		}
	})
}

func (r *soakRunner) mustQuiesce() (int64, int64) {
	r.t.Helper()
	var term int64
	var commit int64
	err := r.wait(soakRecovery, "quiesce", noMember, func() bool {
		ok := false
		term, commit, ok = r.positions()

		return ok
	})
	require.NoError(r.t, err)

	return term, commit
}

func (r *soakRunner) positions() (int64, int64, bool) {
	term, leaders, ok := r.agreedTerm(noMember)

	if !ok || leaders != 1 {
		return 0, 0, false
	}

	var commit int64

	for i := range soakMembers {
		status, statusOK := r.pc.Member(i).Status()

		if !statusOK || status.LeadershipTermID != term {
			return 0, 0, false
		}

		if status.ServiceLogPosition != status.CommitPosition {
			return 0, 0, false
		}

		if i > 0 && status.CommitPosition != commit {
			return 0, 0, false
		}

		commit = status.CommitPosition
		pos, posOK := r.pc.RecordingPosition(i, status.LocalLogRecordingID)

		if !posOK || pos != commit {
			return 0, 0, false
		}
	}

	return term, commit, true
}

func (r *soakRunner) mustDigest() {
	r.t.Helper()
	var count int64
	var digest uint64

	for i := range soakMembers {
		status, ok := r.pc.Member(i).Status()
		require.True(r.t, ok, "member %d status", i)

		if i == 0 {
			count = status.AppliedCount
			digest = status.ApplyDigest

			continue
		}

		require.Equal(r.t, count, status.AppliedCount, "member %d applied count", i)
		require.Equal(r.t, digest, status.ApplyDigest, "member %d digest", i)
	}
}

func (r *soakRunner) mustPrefix() {
	r.t.Helper()
	lists := make([][]harness.MemberApplied, soakMembers)

	for i := range soakMembers {
		lists[i] = r.pc.Member(i).Applied()
	}

	require.Equal(r.t, lists[0], lists[1])
	require.Equal(r.t, lists[0], lists[2])

	for i := range soakMembers {
		for _, inc := range r.pc.Incarnations(i) {
			require.True(r.t, appliedPrefix(inc.Applied(), lists[0]), "member %d prefix", i)
		}
	}
}

func (r *soakRunner) mustStop() {
	r.t.Helper()
	stopErr := r.pc.StopAll(soakStopAll)
	var reports []string

	for i := range soakMembers {
		reports = append(reports, r.pc.Member(i).RaceReports()...)
	}

	require.NoError(r.t, soakrun.CheckStop(stopErr, reports))

	if stopErr != nil {
		r.t.Logf("stop: %v", stopErr)
	}
}

func (r *soakRunner) mustTermRows(quiesceTerm int64) {
	r.t.Helper()
	files := make([][]soakrun.TermRow, 0, soakMembers)
	finals := make([]int64, 0, soakMembers)

	for i := range soakMembers {
		path := filepath.Join(r.pc.ClusterDir(i), recordingFileName)
		entries, err := recordinglog.Reload(path)
		require.NoError(r.t, err, "recording log %d", i)
		var rows []soakrun.TermRow

		for _, entry := range entries {
			if !entry.Valid || entry.Type != recordinglog.EntryTypeTerm {
				continue
			}

			rows = append(rows, soakrun.TermRow{
				Term:                entry.LeadershipTermID,
				TermBaseLogPosition: entry.TermBaseLogPosition,
				LogPosition:         entry.LogPosition,
			})
		}

		if len(rows) > 0 {
			finals = append(finals, rows[len(rows)-1].Term)
		}

		files = append(files, rows)
	}

	message := fmt.Sprintf("final terms %v beside quiesce term %d", finals, quiesceTerm)
	require.NoError(r.t, soakrun.CheckTermRows(files), message)
	require.Equal(r.t, quiesceTerm, finals[0], message)
}

func (r *soakRunner) mustRecordings(commit int64) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), soakListBound)
	defer cancel()

	for i := range soakMembers {
		status, ok := r.pc.Member(i).Status()
		require.True(r.t, ok, "member %d status", i)
		_, err := harness.AwaitRecordingStop(ctx, r.pc.DriverDir(i), status.LocalLogRecordingID, commit)
		require.NoError(r.t, err, "member %d recording stop", i)
	}
}

func (r *soakRunner) mustHistory() {
	r.t.Helper()
	require.GreaterOrEqual(r.t, r.snap.returned, soakMinReturned)
	require.Positive(r.t, r.snap.nonEmptyGets)
	linearizability.RequireLinearizable(r.t, r.snap.ops, 5*time.Minute)
	mutated := slices.Clone(r.snap.ops)
	require.True(r.t, mutateOne(mutated), "soak: no append or non-empty get to mutate")
	require.Equal(r.t, porcupine.Illegal, linearizability.Check(mutated, 2*time.Minute))
}

func (r *soakRunner) mustRaces() {
	r.t.Helper()

	for i := range soakMembers {
		for _, inc := range r.pc.Incarnations(i) {
			require.Empty(r.t, inc.RaceReports(), "member %d race", i)
		}
	}
}

func (r *soakRunner) mustReplyPath() {
	r.t.Helper()

	for _, event := range r.kv.events {
		switch event.Code {
		case client.EventCodeOK, client.EventCodeRedirect:
			r.t.Logf("cluster event %v %s", event.Code, strconv.QuoteToASCII(event.Detail))

		case client.EventCodeError, client.EventCodeAuthenticationRejected:
			require.Failf(
				r.t,
				"reply path",
				"event %v %s",
				event.Code,
				strconv.QuoteToASCII(event.Detail),
			)
		}
	}

	for _, msg := range r.kv.connectErrors {
		r.t.Logf("connect error %s", strconv.QuoteToASCII(msg))
	}

	require.NoError(r.t, r.kv.replyErr)

	for i := range soakMembers {
		for _, inc := range r.pc.Incarnations(i) {
			rec, ok := inc.ExitRecord()

			if !ok {
				continue
			}

			require.NotContains(r.t, rec.Error, harness.ExitTextDecodeRequest)
		}
	}
}

func (r *soakRunner) snapshotClient() {
	ops := r.kv.history.Operations()
	r.snap = &clientSnap{
		returned:     r.kv.history.Counts().Returned.Total(),
		nonEmptyGets: nonEmptyGets(ops),
		reopens:      r.kv.reopens,
		ops:          ops,
	}
}

func (r *soakRunner) appendRow(
	step soakrun.Step,
	member int,
	start time.Time,
	clear time.Time,
	recovery time.Duration,
	termBefore int64,
	termAfter int64,
) {
	row := timelineRow{
		Index:      step.Index,
		Kind:       step.Kind.String(),
		MemberID:   member,
		Start:      start.Format(time.RFC3339Nano),
		Recovery:   recovery.String(),
		TermBefore: termBefore,
		TermAfter:  termAfter,
	}

	if !clear.IsZero() {
		row.Clear = clear.Format(time.RFC3339Nano)
	}

	raw, err := json.Marshal(row)
	if err != nil {
		r.t.Errorf("timeline: %v", err)

		return
	}

	if err = writeSynced(r.timeline, raw); err != nil {
		r.t.Errorf("timeline: %v", err)

		return
	}

	r.lastTerm = termAfter
}

func (r *soakRunner) writeResult() {
	if r.t.Skipped() {
		return
	}

	res := r.result()
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		r.t.Errorf("soak result: %v", err)

		return
	}

	path := filepath.Join(r.t.ArtifactDir(), "soak-result.json")
	file, err := os.Create(path)
	if err != nil {
		r.t.Errorf("soak result: %v", err)

		return
	}

	defer func() { _ = file.Close() }()

	if err = writeSynced(file, raw); err != nil {
		r.t.Errorf("soak result: %v", err)
	}
}

func (r *soakRunner) result() soakrun.Result {
	res := soakrun.Result{
		Seed:               r.seed,
		Duration:           r.duration.String(),
		Steps:              r.stepCounts,
		UnplannedElections: soakrun.UnplannedElections(r.firstTerm, r.lastTerm, r.leaderFaults),
		LongestRecovery:    map[string]string{},
	}

	for kind, elapsed := range r.longest {
		res.LongestRecovery[kind] = elapsed.String()
	}

	if r.snap == nil {
		return res
	}

	res.Returned = &r.snap.returned
	res.NonEmptyGets = &r.snap.nonEmptyGets
	res.Reopens = &r.snap.reopens

	return res
}

func (r *soakRunner) copyFailure(pc *harness.ProcessCluster) {
	if !r.t.Failed() {
		return
	}

	dir := r.t.ArtifactDir()
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", pc.State())
	rules, err := r.net.Rules()
	fmt.Fprintf(&b, "rules:\n%s\nrules error: %v\n", rules, err)

	if r.snap != nil {
		fmt.Fprintf(&b, "counts: %+v\n", r.kv.history.Counts())
	} else {
		fmt.Fprintf(&b, "client did not stop\n")
	}

	statePath := filepath.Join(dir, "state.txt")
	if writeErr := os.WriteFile(statePath, []byte(b.String()), 0o644); writeErr != nil {
		r.t.Errorf("state.txt: %v", writeErr)
	}

	for i := range soakMembers {
		clusterDir := pc.ClusterDir(i)
		prefix := fmt.Sprintf("member-%d-", i)
		r.copyOne(
			filepath.Join(dir, prefix+recordingFileName),
			filepath.Join(clusterDir, recordingFileName),
		)
		r.copyOne(
			filepath.Join(dir, prefix+election.NodeStateFileName),
			filepath.Join(clusterDir, election.NodeStateFileName),
		)
		r.copyArchive(filepath.Join(dir, fmt.Sprintf("archive-%d", i)), pc.ArchiveDir(i))
	}
}

func (r *soakRunner) copyOne(dst, src string) {
	if err := copyFile(dst, src); err != nil {
		r.t.Errorf("copy %s: %v", src, err)
	}
}

func (r *soakRunner) copyArchive(dstRoot, src string) {
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			return nil
		}

		if strings.HasSuffix(entry.Name(), ".rec") {
			return nil
		}

		rel, relErr := filepath.Rel(src, path)

		if relErr != nil {
			return relErr
		}

		return copyFile(filepath.Join(dstRoot, rel), path)
	})
	if err != nil {
		r.t.Errorf("copy archive %s: %v", src, err)
	}
}

func checkKilled(member *harness.ProcessMember) error {
	state := member.ProcessState()

	if state == nil {
		return errors.New("soak: process state missing")
	}

	ws, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		return fmt.Errorf("soak: process state %s, want wait status", state)
	}

	if ws.Signal() != syscall.SIGKILL {
		return fmt.Errorf("soak: process %s, want %s", state, syscall.SIGKILL)
	}

	rec, hasRecord := member.ExitRecord()

	if hasRecord {
		return fmt.Errorf("soak: unexpected exit record %q", rec.Error)
	}

	return nil
}

func checkExited(member *harness.ProcessMember, code int, text string) error {
	state := member.ProcessState()

	if state == nil {
		return errors.New("soak: process state missing")
	}

	if state.ExitCode() != code {
		return fmt.Errorf("soak: exit code %d, want %d", state.ExitCode(), code)
	}

	if text == "" {
		return nil
	}

	rec, hasRecord := member.ExitRecord()

	if !hasRecord {
		return errors.New("soak: exit record missing")
	}

	if !strings.Contains(rec.Error, text) {
		return fmt.Errorf("soak: exit record %q, want text %q", rec.Error, text)
	}

	if !rec.Terminated {
		return fmt.Errorf("soak: exit record %q, want termination", rec.Error)
	}

	return nil
}

func reportedLeader(status harness.MemberStatus) int32 {
	if status.Role == consensus.RoleLeader {
		return status.MemberID
	}

	return status.LeaderMemberID
}

func countsLeaderFault(kind soakrun.StepKind) bool {
	switch kind {
	case soakrun.MemberKill, soakrun.ArchiveKill, soakrun.DriverKill, soakrun.Partition:
		return true
	default:
		return false
	}
}

func appliedPrefix(prefix, full []harness.MemberApplied) bool {
	return len(prefix) <= len(full) && slices.Equal(prefix, full[:len(prefix)])
}

func nonEmptyGets(ops []porcupine.Operation) int {
	n := 0

	for _, op := range ops {
		in, ok := op.Input.(linearizability.Input)

		if !ok || in.Kind != linearizability.OpGet {
			continue
		}

		out, ok := op.Output.(linearizability.Output)

		if !ok || out.Unknown || out.Value == "" {
			continue
		}

		n++
	}

	return n
}

func mutateOne(ops []porcupine.Operation) bool {
	for i := range ops {
		in, inOK := ops[i].Input.(linearizability.Input)
		out, outOK := ops[i].Output.(linearizability.Output)

		if !inOK || !outOK || out.Unknown {
			continue
		}

		if in.Kind != linearizability.OpAppend && (in.Kind != linearizability.OpGet || out.Value == "") {
			continue
		}

		out.Value = "x"
		ops[i].Output = out

		return true
	}

	return false
}

func writeSynced(file *os.File, raw []byte) error {
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return err
	}

	return file.Sync()
}

func copyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	defer func() { _ = in.Close() }()

	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}

	return out.Close()
}

//go:build integration && cluster_mixed && cluster_java

package cluster_mixed_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/consensus"
	"github.com/warrenguy/luas/test/integration/cluster_java/clusterjava"
	"github.com/warrenguy/luas/test/integration/harness"
)

// unappointedGoSpec holds the id and directories of one Go member.
type unappointedGoSpec struct {
	memberID   int32
	clusterDir string
	aeronDir   string
}

// mixedGoMembers holds the Go members of one unappointed smoke, lowest id first.
type mixedGoMembers []*mixedMemberFixture

// agreedLeader is the leader that a passing poll reports.
type agreedLeader struct {
	memberID int32
	language string
	term     int64
}

// tick runs one Runner.DoWork on each member in slice order.
func (g mixedGoMembers) tick() {
	for _, member := range g {
		member.tick()
	}
}

// tickUntil runs tick until ok reports true or budget elapses. It sleeps 1ms
// between ticks. Returns false on timeout.
func (g mixedGoMembers) tickUntil(budget time.Duration, ok func() bool) bool {
	return harness.PollUntilBool(budget, time.Millisecond, func() bool {
		g.tick()
		return ok()
	})
}

// TestClusterMixedUnappointedJavaMajority elects a leader with a Java majority.
//
// Do not add t.Parallel. This test owns the compose-fixed MixedAeronDir paths,
// MixedClusterDirGo, and the multicast log endpoint.
func TestClusterMixedUnappointedJavaMajority(t *testing.T) {
	runUnappointedSmoke(
		t,
		clusterjava.MixedComposeUnappointedJavaMajorityEnvKey,
		clusterjava.MixedComposeSkipHintUnappointedJavaMajority,
		[]int{0, 1},
		[]unappointedGoSpec{{
			memberID:   clusterjava.MixedGoMemberID,
			clusterDir: clusterjava.MixedClusterDirGo,
			aeronDir:   clusterjava.MixedAeronDirGo,
		}},
	)
}

// TestClusterMixedUnappointedGoMajority elects a leader with a Go majority.
//
// Do not add t.Parallel. This test owns the compose-fixed MixedAeronDir paths,
// MixedClusterDirMember1, MixedClusterDirGo, and the multicast log endpoint.
func TestClusterMixedUnappointedGoMajority(t *testing.T) {
	runUnappointedSmoke(
		t,
		clusterjava.MixedComposeUnappointedGoMajorityEnvKey,
		clusterjava.MixedComposeSkipHintUnappointedGoMajority,
		[]int{0},
		[]unappointedGoSpec{
			{
				memberID:   clusterjava.MixedSecondGoMemberID,
				clusterDir: clusterjava.MixedClusterDirMember1,
				aeronDir:   clusterjava.MixedAeronDirGoMember1,
			},
			{
				memberID:   clusterjava.MixedGoMemberID,
				clusterDir: clusterjava.MixedClusterDirGo,
				aeronDir:   clusterjava.MixedAeronDirGo,
			},
		},
	)
}

// runUnappointedSmoke opens the Go members without an appointed leader.
// javaIDs must start at 0 without a gap, so a slice index equals the
// member id.
//
// The smoke does not open a client session. The term-start entry alone moves
// commit above 0. The smoke deliberately does not call
// awaitJavaServiceSawTermStart.
func runUnappointedSmoke(
	t testing.TB,
	envKey string,
	skipHint string,
	javaIDs []int,
	goSpecs []unappointedGoSpec,
) {
	t.Helper()

	requireMixedCompose(t, envKey, skipHint)

	goDirs := make([]string, len(goSpecs))

	for i, spec := range goSpecs {
		goDirs[i] = spec.aeronDir
	}

	requireMixedAeronDirsPresent(t, javaIDs, goDirs...)

	java := awaitJavaModuleState(t, nil, javaIDs, clusterjava.MixedStartupBudget)

	members := make(mixedGoMembers, 0, len(goSpecs))

	for _, spec := range goSpecs {
		cfg := buildMixedMulticastConfig(t, spec.memberID, spec.clusterDir, spec.aeronDir)
		members = append(members, openMixedGoMember(t, cfg))
	}

	writeReady(t)

	state := mixedState(members, java)

	for _, member := range members {
		topology := member.module.Topology()
		require.NotNil(t, topology, "%s", state)
		require.Contains(
			t,
			topology.LogChannel(),
			clusterjava.MixedLogEndpoint,
			"member %d log channel %q\n%s",
			member.module.ThisMemberID(),
			topology.LogChannel(),
			state,
		)
		require.False(
			t,
			topology.IsLogMDC(),
			"member %d log mode\n%s",
			member.module.ThisMemberID(),
			state,
		)
	}

	leader := awaitAgreedLeader(t, members, java, clusterjava.MixedElectionBudget)
	state = mixedState(members, java)

	for _, member := range members {
		require.True(
			t,
			member.module.IsColdStart(),
			"member %d cold start\n%s",
			member.module.ThisMemberID(),
			state,
		)
	}

	logSession := unappointedLogSession(t, members, java, state)
	recordings := make([]goLogRecording, 0, len(members))

	for _, member := range members {
		recordingID, _ := requireGoLogRecording(t, member, logSession, state)
		recordings = append(recordings, goLogRecording{
			member:      member,
			recordingID: recordingID,
		})
	}

	commit := awaitMixedCommit(
		t,
		members,
		java,
		leader.memberID,
		clusterjava.MixedCommitBudget,
	)
	awaitLogRecordingsReach(
		t,
		members.tick,
		recordings,
		java,
		logSession,
		commit,
		clusterjava.MixedCommitBudget,
	)

	state = mixedState(members, java)

	for _, member := range members {
		require.NoError(
			t,
			member.module.Close(),
			"close member %d\n%s",
			member.module.ThisMemberID(),
			state,
		)
	}

	markMixedSmokeCompleted(t)
}

// awaitAgreedLeader ticks every Go member until scanAgreedLeader reports a
// leader. On timeout it fails the test with the member dump.
func awaitAgreedLeader(
	t testing.TB,
	members mixedGoMembers,
	java []*harness.DriverCounters,
	budget time.Duration,
) agreedLeader {
	t.Helper()

	start := time.Now()
	var found agreedLeader
	ok := members.tickUntil(budget, func() bool {
		leader, agreed := scanAgreedLeader(members, java)
		if !agreed {
			return false
		}

		found = leader
		return true
	})
	elapsed := time.Since(start)

	if !ok {
		fatalMixed(
			t,
			members,
			java,
			"members did not agree on one leader after %s (budget %s)",
			elapsed,
			budget,
		)
	}

	t.Logf(
		"agreed leader id=%d language=%s term=%d elapsed=%s budget=%s",
		found.memberID,
		found.language,
		found.term,
		elapsed,
		budget,
	)
	return found
}

// scanAgreedLeader reports the leader when every member has closed the
// election on one term and exactly one member leads. It does not tick and does
// not fail the test. The Java slice index is the member id.
func scanAgreedLeader(
	members mixedGoMembers,
	java []*harness.DriverCounters,
) (agreedLeader, bool) {
	for _, member := range members {
		if !member.module.ElectionComplete() {
			return agreedLeader{}, false
		}
	}

	for _, counters := range java {
		if !javaElectionClosed(counters) {
			return agreedLeader{}, false
		}
	}

	term := members[0].module.LeadershipTermID()
	if term < 0 {
		return agreedLeader{}, false
	}

	for _, member := range members[1:] {
		if member.module.LeadershipTermID() != term {
			return agreedLeader{}, false
		}
	}

	if !javaMembersAtTerm(java, term) {
		return agreedLeader{}, false
	}

	leaderCount := 0
	var leader agreedLeader

	for _, member := range members {
		if !member.module.IsLeader() {
			continue
		}

		leaderCount++
		leader = agreedLeader{
			memberID: member.module.ThisMemberID(),
			language: "go",
			term:     term,
		}
	}

	for i, counters := range java {
		role, roleOK := javaMemberRole(counters)
		if !roleOK {
			return agreedLeader{}, false
		}

		if role != consensus.RoleLeader {
			continue
		}

		leaderCount++
		leader = agreedLeader{
			memberID: int32(i),
			language: "java",
			term:     term,
		}
	}

	if leaderCount != 1 {
		return agreedLeader{}, false
	}

	for _, member := range members {
		if member.module.IsLeader() {
			if member.module.LeaderMemberID() != -1 {
				return agreedLeader{}, false
			}

			continue
		}

		if member.module.LeaderMemberID() != leader.memberID {
			return agreedLeader{}, false
		}
	}

	return leader, true
}

// awaitMixedCommit waits until every member's commit position is above 0.
// Returns the lowest position from the passing poll.
func awaitMixedCommit(
	t testing.TB,
	members mixedGoMembers,
	java []*harness.DriverCounters,
	leaderID int32,
	budget time.Duration,
) int64 {
	t.Helper()

	start := time.Now()
	var lowest int64
	positions := make([]int64, 0, len(members)+len(java))
	ok := members.tickUntil(budget, func() bool {
		positions = positions[:0]

		for _, member := range members {
			position := member.module.NotifiedCommitPosition()
			if member.module.ThisMemberID() == leaderID {
				position = member.module.CommitPosition()
			}

			if position <= 0 {
				return false
			}

			positions = append(positions, position)
		}

		javaPositions, allocated := javaCommitPositions(java)
		if !allocated {
			return false
		}

		positions = append(positions, javaPositions...)
		lowest = slices.Min(positions)
		return lowest > 0
	})
	elapsed := time.Since(start)

	if !ok {
		fatalMixed(
			t,
			members,
			java,
			"commit did not rise above 0 after %s (budget %s)",
			elapsed,
			budget,
		)
	}

	t.Logf("mixed commit elapsed=%s budget=%s lowest=%d", elapsed, budget, lowest)
	return lowest
}

// unappointedLogSession returns this term's log session and requires every Go
// follower's image to use it. A Go leader must not hold a log image. state is
// the failure dump.
func unappointedLogSession(
	t testing.TB,
	members mixedGoMembers,
	java []*harness.DriverCounters,
	state string,
) int32 {
	t.Helper()

	var session int32
	sessionKnown := false

	for _, member := range members {
		if !member.module.IsLeader() {
			continue
		}

		adapter := member.module.LogAdapter()
		if adapter == nil {
			fatalMixed(t, members, java, "go leader log adapter missing")
		}

		if adapter.Image() != nil {
			fatalMixed(t, members, java, "go leader must not hold a log image")
		}

		streams := member.module.LogStreams()
		if streams == nil {
			fatalMixed(t, members, java, "go leader log streams missing")
		}

		publication := streams.Publication()
		if publication == nil {
			fatalMixed(t, members, java, "go leader log publication missing")
		}

		session = publication.SessionID()
		sessionKnown = true
	}

	for _, member := range members {
		if member.module.IsLeader() {
			continue
		}

		requireLiveLogImage(t, member, state)
		imageSession := member.module.LogAdapter().Image().SessionID()

		if !sessionKnown {
			session = imageSession
			sessionKnown = true
			continue
		}

		require.Equal(t, session, imageSession, "follower image session\n%s", state)
	}

	return session
}

// mixedState returns one dump line per Go module and per Java member.
func mixedState(members mixedGoMembers, java []*harness.DriverCounters) string {
	modules := make([]*cluster.ConsensusModule, len(members))

	for i, member := range members {
		modules[i] = member.module
	}

	return dumpMixedMembers(modules, java)
}

// fatalMixed fails the test and appends the mixedState dump.
func fatalMixed(
	t testing.TB,
	members mixedGoMembers,
	java []*harness.DriverCounters,
	format string,
	args ...any,
) {
	t.Helper()

	all := make([]any, 0, len(args)+1)
	all = append(all, args...)
	all = append(all, mixedState(members, java))
	t.Fatalf(format+"\n%s", all...)
}

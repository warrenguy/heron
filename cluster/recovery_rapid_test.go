//go:build rapid

package cluster_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// logShape names the recording-log fixture shapes the properties draw over.
type logShape int

const (
	shapeEmpty logShape = iota
	shapeTerms
	shapeSnapshot
	shapeTermsAndSnapshot
)

// drawLogShape draws a log shape over all fixture shapes. drawLogShape writes
// the fixture into dir through the exported append API. drawLogShape registers
// any recording ids used with lister. drawLogShape never computes a plan.
// rapid's log_shape draw label names the shape on failure.
//
// Returns the drawn shape so a caller can assert the nil-lister refusal.
func drawLogShape(rt *rapid.T, dir string, lister *fakeLister) logShape {
	rt.Helper()
	return drawLogShapeFrom(rt, dir, lister, []logShape{
		shapeEmpty,
		shapeTerms,
		shapeSnapshot,
		shapeTermsAndSnapshot,
	})
}

// drawLogShapeFrom draws one shape from shapes and writes that fixture.
func drawLogShapeFrom(
	rt *rapid.T,
	dir string,
	lister *fakeLister,
	shapes []logShape,
) logShape {
	rt.Helper()

	shape := rapid.SampledFrom(shapes).Draw(rt, "log_shape")

	if shape == shapeEmpty {
		return shape
	}

	log, err := recordinglog.Open(dir, true)
	require.NoError(rt, err)

	if shape == shapeTerms || shape == shapeTermsAndSnapshot {
		n := rapid.IntRange(1, 4).Draw(rt, "term_count")
		recordingID := int64(42)
		for i := range n {
			base := int64(i) * 512
			require.NoError(rt, log.AppendTerm(recordingID, int64(i), base, int64(1000+i)))
		}
		if lister != nil {
			lister.extents[recordingID] = extent(0, 4096)
		}
	}

	if shape == shapeSnapshot || shape == shapeTermsAndSnapshot {
		require.NoError(rt, log.AppendSnapshot(55, 3, 0, 128, 1000, -1))
	}

	require.NoError(rt, log.Close())
	return shape
}

// requireOpenFailed asserts the failed-open contract: nil value, cluster-scoped
// error, nothing left open under dir.
func requireOpenFailed(
	t testing.TB,
	rt *rapid.T,
	recovery *cluster.Recovery,
	err error,
	dir string,
) {
	rt.Helper()
	require.Nil(rt, recovery)
	require.True(rt, strings.HasPrefix(err.Error(), "cluster: "), "got %v", err)
	require.Empty(rt, fdsUnder(t, dir))
}

func TestOpenRecovery_guardMatrix_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()

		haveLister := rapid.Bool().Draw(rt, "have_lister")
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "service_count")
		memberID := rapid.IntRange(0, 2).Draw(rt, "member_id")

		var lister *fakeLister
		if haveLister {
			lister = &fakeLister{
				extents: make(map[int64]recordinglog.LogRecordingExtents),
			}
		}

		shape := drawLogShape(rt, dir, lister)

		cfg := cluster.Config{
			MemberID:                      int32(memberID),
			ClusterMembers:                goldenClusterMembersSharedLog,
			ClusterDir:                    dir,
			AeronDir:                      "/tmp/aeron",
			ServiceCount:                  serviceCount,
			ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
			ArchiveControlResponseChannel: testArchiveControlResponseChannel,
		}.Normalize()

		// True nil interface: a nil *fakeLister would make ArchiveLister != nil.
		deps := cluster.RecoveryDeps{Config: cfg}
		if lister != nil {
			deps.ArchiveLister = lister
		}

		recovery, err := cluster.OpenRecovery(deps)

		// Branch by expected OpenRecovery outcome.
		// An unexpected open failure on the success path must fail the property.
		if !haveLister && shape != shapeEmpty {
			require.ErrorContains(rt, err, "needs an ArchiveLister")
			requireOpenFailed(t, rt, recovery, err, dir)
			return
		}

		if shape == shapeSnapshot || shape == shapeTermsAndSnapshot {
			// Snapshot fixture with a lister. OpenRecovery must refuse.
			// Nil-lister non-empty already returned above.
			// PlanRecovery may fail when ServiceCount is greater than zero and
			// service snapshot extents are incomplete.
			// A plan that holds snapshots is refused with "snapshot load is not implemented".
			require.Error(rt, err)
			requireOpenFailed(t, rt, recovery, err, dir)
			return
		}

		// Success path: empty log (any lister), or terms with a lister.
		require.NoError(rt, err)
		require.NotNil(rt, recovery)
		require.NotNil(rt, recovery.RecordingLog())
		require.NotNil(rt, recovery.NodeState())
		require.NoError(rt, recovery.Close())
		require.NoError(rt, recovery.Close())
		require.Empty(rt, fdsUnder(t, dir))
	})
}

func drawElectionInput(rt *rapid.T) (
	timeouts election.ElectionConfig,
	appointed *int32,
	graceful *int32,
	memberID int32,
) {
	rt.Helper()

	memberID = int32(rapid.IntRange(0, 2).Draw(rt, "member_id"))
	appointed = drawLeaderPointer(rt, "appointed")
	graceful = drawLeaderPointer(rt, "graceful")

	timeouts = election.ElectionConfig{
		AppVersion:                rapid.Int32Range(0, 100).Draw(rt, "app_version"),
		ElectionTimeoutNs:         rapid.Int64Range(1, 1_000_000_000_000).Draw(rt, "election_timeout"),
		ElectionStatusIntervalNs:  rapid.Int64Range(1, 1_000_000_000_000).Draw(rt, "status_interval"),
		LeaderHeartbeatTimeoutNs:  rapid.Int64Range(1, 1_000_000_000_000).Draw(rt, "hb_timeout"),
		LeaderHeartbeatIntervalNs: rapid.Int64Range(1, 1_000_000_000_000).Draw(rt, "hb_interval"),
		StartupCanvassTimeoutNs:   rapid.Int64Range(1, 1_000_000_000_000).Draw(rt, "canvass_timeout"),
		// Noise that the seed must overwrite.
		LeadershipTermID:    rapid.Int64Range(-10, 100).Draw(rt, "noise_term"),
		TermBaseLogPosition: rapid.Int64Range(0, 10_000).Draw(rt, "noise_term_base"),
		LogPosition:         rapid.Int64Range(0, 10_000).Draw(rt, "noise_log"),
		AppendPosition:      rapid.Int64Range(0, 10_000).Draw(rt, "noise_append"),
		IsNodeStartup:       rapid.Bool().Draw(rt, "noise_startup"),
	}
	return timeouts, appointed, graceful, memberID
}

// drawSeededConfig draws an open-success log shape into dir and a normalized
// config whose election fields carry the noise the recovery seed must overwrite.
//
// Only empty and terms fixtures. Snapshot shapes refuse OpenRecovery; seed and
// reopen properties require a successful open.
func drawSeededConfig(rt *rapid.T, dir string) (cluster.Config, *fakeLister) {
	rt.Helper()

	lister := &fakeLister{
		extents: make(map[int64]recordinglog.LogRecordingExtents),
	}
	drawLogShapeFrom(rt, dir, lister, []logShape{shapeEmpty, shapeTerms})

	electionIn, appointed, graceful, memberID := drawElectionInput(rt)
	cfg := cluster.Config{
		MemberID:                      memberID,
		ClusterMembers:                goldenClusterMembersSharedLog,
		ClusterDir:                    dir,
		AeronDir:                      "/tmp/aeron",
		AppointedLeaderID:             appointed,
		GracefulClosedLeaderID:        graceful,
		Election:                      electionIn,
		ServiceCount:                  rapid.IntRange(0, 3).Draw(rt, "service_count"),
		ArchiveControlRequestChannel:  testArchiveControlRequestChannel,
		ArchiveControlResponseChannel: testArchiveControlResponseChannel,
	}.Normalize()
	return cfg, lister
}

func TestRecoveryElectionConfig_seedOracle_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		cfg, lister := drawSeededConfig(rt, dir)

		recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
			Config:        cfg,
			ArchiveLister: lister,
		})
		require.NoError(rt, err)
		require.NotNil(rt, recovery)

		// election.ApplyRecoveryPlan has its own transform tests; this
		// property checks the OpenRecovery wiring and noise overwrite.
		want := cfg.Election
		want.IsNodeStartup = true
		election.ApplyRecoveryPlan(&want, recovery.Plan())
		require.Equal(rt, want, recovery.ElectionConfig())
		require.Equal(rt, recovery.Plan().IsEmpty(), recovery.IsColdStart())
		require.NoError(rt, recovery.Close())
		require.Empty(rt, fdsUnder(t, dir))
	})
}

// Serial: OpenRecovery holds process FDs and an advisory flock. Parallel
// neighbours can make a failed open look like a property bug (rapid "flaky").
// Each cycle closes and waits for zero FDs under dir before the next open.
func TestOpenRecovery_reopenStable_rapid(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		cfg, lister := drawSeededConfig(rt, dir)

		cycles := rapid.IntRange(2, 4).Draw(rt, "cycles")
		var (
			firstPlan     recordinglog.RecoveryPlan
			firstElection election.ElectionConfig
			firstCold     bool
			firstEntries  int
		)

		for i := range cycles {
			recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
				Config:        cfg,
				ArchiveLister: lister,
			})
			require.NoError(rt, err, "open cycle %d", i)
			require.NotNil(rt, recovery)

			// Close even when a later assert fails so a held flock cannot
			// poison a later open on the same dir within this check.
			func() {
				defer func() {
					require.NoError(rt, recovery.Close(), "close cycle %d", i)
					require.Empty(rt, fdsUnder(t, dir), "fds after close cycle %d", i)
				}()

				if i == 0 {
					firstPlan = recovery.Plan()
					firstElection = recovery.ElectionConfig()
					firstCold = recovery.IsColdStart()
					firstEntries = len(recovery.RecordingLog().Entries())
					return
				}
				require.Equal(rt, firstPlan, recovery.Plan())
				require.Equal(rt, firstElection, recovery.ElectionConfig())
				require.Equal(rt, firstCold, recovery.IsColdStart())
				require.Len(rt, recovery.RecordingLog().Entries(), firstEntries)
			}()
		}
	})
}

//go:build rapid

package cluster_test

import (
	"fmt"
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

const (
	drawnSnapshotLogPosition    int64 = 128
	drawnSnapshotLeadershipTerm int64 = 3
	drawnSnapshotTimestamp      int64 = 1000
)

type drawnSnapshot struct {
	termCount int

	// moduleIndex is the consensus-module row index. Term rows and included
	// service rows precede the module row.
	moduleIndex int

	// lowestMissing is the smallest service id omitted from a snapshot group.
	// The value is -1 when the group includes every id in [0, serviceCount).
	lowestMissing int
}

// drawLogShape writes one fixture shape into dir. A non-nil lister receives
// the term recording when the shape includes terms. This function does not
// call PlanRecovery.
//
// A snapshot shape appends a subset of the service ids, highest id first.
// The module row follows those ids.
func drawLogShape(
	rt *rapid.T,
	dir string,
	lister *fakeLister,
	serviceCount int,
) (logShape, drawnSnapshot) {
	rt.Helper()
	return drawLogShapeFrom(rt, dir, lister, []logShape{
		shapeEmpty,
		shapeTerms,
		shapeSnapshot,
		shapeTermsAndSnapshot,
	}, serviceCount)
}

// drawLogShapeFrom draws one shape from shapes and writes that fixture.
func drawLogShapeFrom(
	rt *rapid.T,
	dir string,
	lister *fakeLister,
	shapes []logShape,
	serviceCount int,
) (logShape, drawnSnapshot) {
	rt.Helper()

	shape := rapid.SampledFrom(shapes).Draw(rt, "log_shape")
	drawn := drawnSnapshot{lowestMissing: -1}

	if shape == shapeEmpty {
		return shape, drawn
	}

	log, err := recordinglog.Open(dir, true)
	require.NoError(rt, err)

	if shape == shapeTerms || shape == shapeTermsAndSnapshot {
		n := rapid.IntRange(1, 4).Draw(rt, "term_count")
		drawn.termCount = n
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
		appendSnap := func(serviceID int32) {
			rt.Helper()
			require.NoError(rt, log.AppendSnapshot(
				55,
				drawnSnapshotLeadershipTerm,
				0,
				drawnSnapshotLogPosition,
				drawnSnapshotTimestamp,
				serviceID,
			))
		}

		included := 0

		// The loop visits high ids first. The last skip is the smallest
		// missing id.
		for id := serviceCount - 1; id >= 0; id-- {
			if !rapid.Bool().Draw(rt, "include_service") {
				drawn.lowestMissing = id
				continue
			}
			appendSnap(int32(id))
			included++
		}
		drawn.moduleIndex = drawn.termCount + included
		appendSnap(recordinglog.ConsensusModuleServiceID)
	}

	require.NoError(rt, log.Close())
	return shape, drawn
}

// restartHistory holds durable state a previous boot left under the cluster directory.
//
// Fields:
//   - terms adopted on that boot
//   - whether the last term was committed
//   - Archive extent for the recording those terms name
type restartHistory struct {
	recordingID   int64
	terms         int
	lastCommitted bool
	stopPosition  int64
}

// drawRestartHistory writes a previous boot's recording.log into dir through
// writeTermHistory. Registers the Archive extent with lister when lister is
// non-nil.
//
// stopPosition draws from a fixed end position and from
// recordinglog.NullPosition. That draw hits the still-active-recording path in
// election.ApplyRecoveryPlan.
func drawRestartHistory(
	t *testing.T,
	rt *rapid.T,
	dir string,
	lister *fakeLister,
) restartHistory {
	rt.Helper()

	recordingID := rapid.Int64Range(1, 1000).Draw(rt, "recording_id")
	terms := rapid.IntRange(1, 5).Draw(rt, "term_count")
	lastCommitted := rapid.Bool().Draw(rt, "last_committed")
	stopPosition := rapid.SampledFrom([]int64{
		termBaseStride * int64(terms),
		recordinglog.NullPosition,
	}).Draw(rt, "stop_position")

	writeTermHistory(t, dir, recordingID, terms, lastCommitted)

	if lister != nil {
		lister.extents[recordingID] = extent(0, stopPosition)
	}

	return restartHistory{
		recordingID:   recordingID,
		terms:         terms,
		lastCommitted: lastCommitted,
		stopPosition:  stopPosition,
	}
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

		shape, drawn := drawLogShape(rt, dir, lister, serviceCount)

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

		snapshotShape := shape == shapeSnapshot || shape == shapeTermsAndSnapshot
		if snapshotShape {
			// selectRecoveryAnchor returns ErrSnapshotMissingForService when the
			// module row index is below ServiceCount.
			if drawn.moduleIndex < serviceCount {
				require.ErrorIs(rt, err, recordinglog.ErrSnapshotMissingForService)
				require.ErrorContains(rt, err, "cluster: plan recovery")
				require.NotContains(rt, err.Error(), "lacks service")
				requireOpenFailed(t, rt, recovery, err, dir)
				return
			}
			if drawn.termCount == 0 {
				require.EqualError(rt, err, fmt.Sprintf(
					"cluster: recovery plan holds a snapshot at log position %d and no log term",
					drawnSnapshotLogPosition,
				))
				requireOpenFailed(t, rt, recovery, err, dir)
				return
			}
			if drawn.lowestMissing >= 0 {
				require.EqualError(rt, err, fmt.Sprintf(
					"cluster: recovery plan snapshot group at log position %d in term %d lacks service %d",
					drawnSnapshotLogPosition,
					drawnSnapshotLeadershipTerm,
					drawn.lowestMissing,
				))
				requireOpenFailed(t, rt, recovery, err, dir)
				return
			}
		}

		require.NoError(rt, err)
		require.NotNil(rt, recovery)
		require.NotNil(rt, recovery.RecordingLog())
		require.NotNil(rt, recovery.NodeState())
		if snapshotShape {
			require.Equal(rt, drawnSnapshotLogPosition, recovery.ElectionConfig().LogPosition)
		}
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

// drawRecoveryConfig draws a normalized config over dir. Election fields carry
// noise the recovery seed must overwrite.
func drawRecoveryConfig(rt *rapid.T, dir string) cluster.Config {
	rt.Helper()

	electionIn, appointed, graceful, memberID := drawElectionInput(rt)
	return cluster.Config{
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
}

// drawSeededConfig draws an empty or terms fixture so OpenRecovery succeeds.
//
// The fixture draw passes serviceCount 0. The config draws its own service
// count.
func drawSeededConfig(rt *rapid.T, dir string) (cluster.Config, *fakeLister, logShape) {
	rt.Helper()

	lister := &fakeLister{
		extents: make(map[int64]recordinglog.LogRecordingExtents),
	}
	shape, _ := drawLogShapeFrom(rt, dir, lister, []logShape{shapeEmpty, shapeTerms}, 0)
	return drawRecoveryConfig(rt, dir), lister, shape
}

func TestRecoveryElectionConfig_seedOracle_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		cfg, lister, _ := drawSeededConfig(rt, dir)

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
		require.NoError(rt, recovery.Close())
		require.Empty(rt, fdsUnder(t, dir))
	})
}

func TestOpenRecovery_reopenStable_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		cfg, lister, shape := drawSeededConfig(rt, dir)
		wroteTerms := shape == shapeTerms

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
					// A drawn shape that wrote terms must not report a cold start.
					if wroteTerms {
						require.False(rt, firstCold)
					}
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

func TestOpenRecovery_restartSeed_rapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		lister := &fakeLister{
			extents: make(map[int64]recordinglog.LogRecordingExtents),
		}
		history := drawRestartHistory(t, rt, dir, lister)
		cfg := drawRecoveryConfig(rt, dir)

		recovery, err := cluster.OpenRecovery(cluster.RecoveryDeps{
			Config:        cfg,
			ArchiveLister: lister,
		})
		require.NoError(rt, err)
		require.NotNil(rt, recovery)
		// Hold the open handle so a failed assert still releases the flock.
		// restartCycle closes the first handle and returns the reopened one.
		toClose := recovery
		defer func() {
			require.NoError(rt, toClose.Close())
			require.Empty(rt, fdsUnder(t, dir))
		}()

		require.False(rt, recovery.IsColdStart())
		require.False(rt, recovery.Plan().IsEmpty())

		// The fixture writer supplies this plan geometry. The election does not
		// supply this plan geometry.
		plan := recovery.Plan()
		require.Equal(rt, int64(history.terms-1), plan.LastLeadershipTermID)
		require.Equal(rt, termBaseStride*int64(history.terms-1), plan.LastTermBaseLogPosition)
		if history.lastCommitted {
			require.Equal(rt, termBaseStride*int64(history.terms), plan.CommittedLogPosition)
		} else {
			require.Equal(rt, int64(0), plan.CommittedLogPosition)
		}
		require.Equal(rt, history.stopPosition, plan.AppendedLogPosition)
		require.Equal(rt, history.recordingID, plan.TermRecordingID())
		require.Empty(rt, plan.Snapshots)

		// Election seed: production transform only (same shape as seedOracle_rapid).
		want := cfg.Election
		want.IsNodeStartup = true
		election.ApplyRecoveryPlan(&want, plan)
		require.Equal(rt, want, recovery.ElectionConfig())

		reopened := restartCycle(rt, recovery, cfg, lister)
		toClose = reopened
		require.Equal(rt, plan, reopened.Plan())
		require.Equal(rt, want, reopened.ElectionConfig())
		require.False(rt, reopened.IsColdStart())
	})
}

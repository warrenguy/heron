//go:build rapid

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

func drawLeaderPointer(t *rapid.T, label string) *int32 {
	t.Helper()
	if rapid.Bool().Draw(t, label+"_nil") {
		return nil
	}

	// goldenClusterMembersSharedLog holds members 0, 1, and 2 only.
	v := rapid.Int32Range(0, 2).Draw(t, label+"_id")
	return new(v)
}

func drawOptionalPositiveInt64(t *rapid.T, label string) int64 {
	t.Helper()
	if rapid.Bool().Draw(t, label+"_zero") {
		return 0
	}
	return rapid.Int64Range(1, 1_000_000_000_000).Draw(t, label)
}

// drawOptionalChannel: empty, package default, or a custom UDP channel.
func drawOptionalChannel(t *rapid.T, label, defaultChannel string) string {
	t.Helper()
	switch rapid.IntRange(0, 2).Draw(t, label) {
	case 1:
		return defaultChannel
	case 2:
		return "aeron:udp?term-length=128k"
	default:
		return ""
	}
}

// drawOptionalFileSyncLevel: zero (Normalize fills), Data, or Full.
func drawOptionalFileSyncLevel(t *rapid.T, label string) recordinglog.FileSyncLevel {
	t.Helper()
	switch rapid.IntRange(0, 2).Draw(t, label) {
	case 1:
		return recordinglog.FileSyncData
	case 2:
		return recordinglog.FileSyncFull
	default:
		return 0
	}
}

func requireMappedLeaderID(t *rapid.T, ptr *int32, got int32) {
	t.Helper()
	if ptr == nil {
		require.Equal(t, election.NullMemberID, got)
		return
	}
	require.Equal(t, *ptr, got)
}

// drawValidPartialConfig draws a Config that Validate accepts after Normalize.
// ClusterMembers stays on goldenClusterMembersSharedLog so this file never needs
// the members-string formatter.
func drawValidPartialConfig(t *rapid.T) cluster.Config {
	t.Helper()

	cfg := cluster.Config{
		MemberID:       rapid.Int32Range(0, 2).Draw(t, "member_id"),
		ClusterMembers: goldenClusterMembersSharedLog,
		ClusterDir:     "/tmp/cluster-" + rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "cluster_dir"),
		AeronDir:       "/tmp/aeron-" + rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "aeron_dir"),
		ServiceCount:   rapid.IntRange(0, 16).Draw(t, "service_count"),
	}

	cfg.ConsensusChannel = drawOptionalChannel(
		t,
		"consensus_channel_mode",
		cluster.DefaultConsensusChannel,
	)
	cfg.LogChannel = drawOptionalChannel(t, "log_channel_mode", cluster.DefaultLogChannel)

	if rapid.Bool().Draw(t, "custom_streams") {
		// Distinct non-zero pair so ConsensusStreamID and LogStreamID still
		// differ after Normalize.
		cfg.LogStreamID = rapid.Int32Range(1, 500).Draw(t, "log_stream_id")
		cfg.ConsensusStreamID = rapid.Int32Range(501, 1000).Draw(t, "consensus_stream_id")
	}

	cfg.RecordingLogFileSyncLevel = drawOptionalFileSyncLevel(t, "recording_sync")
	cfg.NodeStateFileSyncLevel = drawOptionalFileSyncLevel(t, "node_state_sync")

	cfg.AppointedLeaderID = drawLeaderPointer(t, "appointed")
	cfg.GracefulClosedLeaderID = drawLeaderPointer(t, "graceful")

	cfg.Election = election.ElectionConfig{
		ElectionTimeoutNs:         drawOptionalPositiveInt64(t, "election_timeout"),
		ElectionStatusIntervalNs:  drawOptionalPositiveInt64(t, "election_status"),
		LeaderHeartbeatTimeoutNs:  drawOptionalPositiveInt64(t, "leader_hb_timeout"),
		LeaderHeartbeatIntervalNs: drawOptionalPositiveInt64(t, "leader_hb_interval"),
		StartupCanvassTimeoutNs:   drawOptionalPositiveInt64(t, "startup_canvass"),
		// Hand-set sentinels are overwritten by Normalize from the pointer fields.
		AppointedLeaderID:      rapid.Int32Range(-1, 10).Draw(t, "hand_appointed"),
		GracefulClosedLeaderID: rapid.Int32Range(-1, 10).Draw(t, "hand_graceful"),
		MemberID:               rapid.Int32Range(-1, 10).Draw(t, "hand_member_id"),
	}

	if rapid.Bool().Draw(t, "archive_udp_control") {
		cfg.ArchiveControlRequestChannel = archive.DefaultControlRequestChannel
		cfg.ArchiveControlResponseChannel = archive.DefaultControlResponseChannel
	} else {
		cfg.ArchiveControlRequestChannel = archive.DefaultLocalControlChannel
		cfg.ArchiveControlResponseChannel = archive.DefaultLocalControlChannel
	}

	return cfg
}

func TestConfigNormalize_idempotent_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		cfg := drawValidPartialConfig(t)
		once := cfg.Normalize()
		twice := once.Normalize()

		// Idle is an interface; compare identity after Normalize fills it.
		require.NotNil(t, once.Idle)
		require.Same(t, once.Idle, twice.Idle)

		require.NotZero(t, once.ConsensusStreamID)
		require.NotZero(t, once.LogStreamID)
		require.NotEqual(t, once.ConsensusStreamID, once.LogStreamID)

		validSync := []recordinglog.FileSyncLevel{
			recordinglog.FileSyncData,
			recordinglog.FileSyncFull,
		}
		require.Contains(t, validSync, once.RecordingLogFileSyncLevel)
		require.Contains(t, validSync, once.NodeStateFileSyncLevel)

		require.Equal(t, cfg.MemberID, once.Election.MemberID)
		requireMappedLeaderID(t, cfg.AppointedLeaderID, once.Election.AppointedLeaderID)
		requireMappedLeaderID(t, cfg.GracefulClosedLeaderID, once.Election.GracefulClosedLeaderID)

		require.NoError(t, once.Validate())

		once.Idle = nil
		twice.Idle = nil
		require.Equal(t, once, twice)
	})
}

//go:build integration && archive_live

package cluster_archive_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

// amendModuleSnapshotMember sets session auth and a non-zero app version.
// ServiceCount stays 0.
func amendModuleSnapshotMember(cfg cluster.Config) cluster.Config {
	return withSnapshotAppVersion(amendSessionAuth(cfg))
}

// requestModuleSnapshot is requestSnapshotAction plus the module take checks.
// Returns the resolved take.
func requestModuleSnapshot(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	session *client.AeronCluster,
	label string,
) consensus.SnapshotTakeResult {
	t.Helper()

	nowNs, actionPosition := requestSnapshotAction(t, sc, leader, transport, session, label)
	result, ok := leader.Agent().LastSnapshotTake()
	require.True(t, ok, "%s resolved without a result:\n%s", label, sc.dump())
	require.NoError(t, result.Err, "%s:\n%s", label, sc.dump())
	require.Equal(t, actionPosition, result.LogPosition, label)
	require.Equal(t, leader.LeadershipTermID(), result.LeadershipTermID, label)
	require.Equal(t, nowNs, result.Timestamp, label)
	return result
}

// requireModuleSnapshotReplay asserts payloads decode as BEGIN, the
// ConsensusModule row, one ClusterSession row for sessionID, then END. The
// session OpenedLogPosition is at or below openBound.
func requireModuleSnapshotReplay(
	t *testing.T,
	payloads [][]byte,
	actionPosition int64,
	leadershipTermID int64,
	sessionID int64,
	openBound int64,
) {
	t.Helper()

	require.Len(t, payloads, 4, "replayed fragments")
	requireSnapshotMarkers(
		t, payloads, codecs.ConsensusModuleSnapshotTypeID, 3, actionPosition, leadershipTermID,
	)

	// The one connect takes session id 1. ServiceCount 0 zeroes the tracker
	// fields.
	module, ok := codecs.TryDecodeConsensusModuleAt(payloads[1], 0)
	require.True(t, ok, "consensus module decode")
	require.Equal(t, codecs.ConsensusModuleFields{NextSessionID: 2}, module)

	row, ok := codecs.TryDecodeClusterSessionAt(payloads[2], 0)
	require.True(t, ok, "cluster session decode")
	require.Equal(t, sessionID, row.ClusterSessionID)
	require.Positive(t, row.OpenedLogPosition)
	require.LessOrEqual(t, row.OpenedLogPosition, openBound)
	require.Equal(t, clusterwire.CloseReason_NULL_VALUE, row.CloseReason)
	require.Equal(t, int64(-1), row.TimeOfLastActivity)
	require.Equal(t, client.DefaultEgressStreamID, row.ResponseStreamID)
	require.True(t, strings.HasPrefix(string(row.ResponseChannel), "aeron:udp?"),
		"response channel %q", row.ResponseChannel)
}

// TestClusterModule_recordsModuleSnapshot asserts a one-member leader with
// ServiceCount 0 records each module snapshot to a new archive recording.
func TestClusterModule_recordsModuleSnapshot(t *testing.T) {
	t.Parallel()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          1,
		PortBase:             harness.ClusterModuleSnapshotPortBase,
		LogMulticastEndpoint: harness.ClusterModuleSnapshotLogEndpoint,
		AmendMemberConfig:    amendModuleSnapshotMember,
	})
	require.Len(t, set.Modules(), 1)
	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)

	sc := serviceCluster{set: set}
	leader := requireSingleLeader(t, set, "")
	_, resolved := leader.Agent().LastSnapshotTake()
	require.False(t, resolved)

	// Phase: one session, one message.
	transport, session := openSession(t, set, leader)
	wantSession := session.ClusterSessionID()
	require.Equal(t, int64(1), wantSession)

	payload := []byte(snapshotSessionPayload)
	preMessagePosition := leader.LogPublisher().Position()
	offerServiceMessage(t, sc, session, payload)
	requireServiceAwaitTick(t, sc, "session message committed", durableWorkBound, sessionPump(
		transport,
		session,
		func() bool {
			appended := leader.LogPublisher().Position()
			return appended > preMessagePosition && leader.CommitPosition() >= appended
		},
	))

	// Phase: first snapshot.
	first := requestModuleSnapshot(
		t, sc, leader, transport, session, "first snapshot",
	)
	require.Empty(t, first.ServiceRecordingIDs)
	logRecordingID := leader.Agent().LocalLogRecordingID()
	require.GreaterOrEqual(t, logRecordingID, int64(0), "log recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, first.RecordingID, "first recording id:\n%s", sc.dump())

	// Phase: list and replay the first recording.
	desc := requireSnapshotRecordingDescriptor(
		t, sc, leader, first.RecordingID, cluster.DefaultSnapshotStreamID,
	)
	payloads := replayRecordingPayloads(t, sc, leader, desc)
	requireModuleSnapshotReplay(
		t, payloads, first.LogPosition, leader.LeadershipTermID(), wantSession,
		preMessagePosition,
	)

	// Phase: second snapshot records a new recording.
	second := requestModuleSnapshot(
		t, sc, leader, transport, session, "second snapshot",
	)
	require.GreaterOrEqual(t, second.RecordingID, int64(0), "second recording id:\n%s", sc.dump())
	require.NotEqual(t, first.RecordingID, second.RecordingID, "second recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, second.RecordingID, "second recording id:\n%s", sc.dump())

	// Neither take writes a SNAPSHOT entry. The TERM entry check keeps the
	// loop from passing on an empty log.
	recordingLog := leader.RecordingLog()
	require.NotNil(t, recordingLog, "recording log:\n%s", sc.dump())
	require.NotNil(t, recordingLog.FindTermEntry(leader.LeadershipTermID()),
		"term entry:\n%s", sc.dump())
	for _, entry := range recordingLog.Entries() {
		require.NotEqual(t, recordinglog.EntryTypeSnapshot, entry.Type, "entry %+v", entry)
	}
}

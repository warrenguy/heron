//go:build integration && archive_live

package cluster_archive_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	archivecodecs "github.com/warrenguy/heron/archive/codecs"
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

	pump := &transferPump{transport: transport, session: session}
	nowNs, actionPosition := requestSnapshotAction(t, sc, leader, pump, label)
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

// TestClusterModule_recordsModuleSnapshot asserts a ServiceCount 0 leader
// records module snapshots and restarts from the latest one.
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
	requireModuleStateCounterActive(t, leader, "after election")
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
	requireModuleStateCounterActive(t, leader, "after first snapshot")
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
	requireModuleStateCounterActive(t, leader, "after second snapshot")
	require.GreaterOrEqual(t, second.RecordingID, int64(0), "second recording id:\n%s", sc.dump())
	require.NotEqual(t, first.RecordingID, second.RecordingID, "second recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, second.RecordingID, "second recording id:\n%s", sc.dump())

	// Each take writes one group: the module row alone.
	requireSnapshotGroups(t, sc, leader, first, second)

	controlSessions := archiveControlSessions(t, leader)
	require.NoError(t, set.StopMember(0))
	dir := set.ClusterDir(0)
	const missingRecordingID int64 = 8_000_000_001

	withRecordingLog(t, dir, func(log *recordinglog.Log) {
		require.NoError(t, log.AppendSnapshot(
			missingRecordingID,
			second.LeadershipTermID,
			0,
			second.LogPosition+64,
			second.Timestamp+1,
			recordinglog.ConsensusModuleServiceID,
		))
	})
	unknownErr := set.RestartMember(0)
	require.ErrorContains(t, unknownErr, "cluster: load module snapshot recording")
	code, ok := archive.ExceptionCode(unknownErr)
	require.True(t, ok, "%v", unknownErr)
	require.Equal(t, int32(archive.ErrorUnknownRecording), code)

	withRecordingLog(t, dir, func(log *recordinglog.Log) {
		found, err := log.InvalidateLatestSnapshot()
		require.NoError(t, err)
		require.True(t, found)
		require.NoError(t, log.AppendSnapshot(
			logRecordingID,
			second.LeadershipTermID,
			0,
			second.LogPosition+128,
			second.Timestamp+2,
			recordinglog.ConsensusModuleServiceID,
		))
	})
	corruptErr := set.RestartMember(0)
	require.ErrorContains(t, corruptErr, "cluster: load module snapshot")
	require.ErrorContains(t, corruptErr, "consensus: load snapshot")

	withRecordingLog(t, dir, func(log *recordinglog.Log) {
		found, err := log.InvalidateLatestSnapshot()
		require.NoError(t, err)
		require.True(t, found)
	})
	require.NoError(t, set.RestartMember(0))

	// FindCounterOfType returns the first type 200 counter. Stop left one at
	// CLOSED. Each failed Open left one at INIT, because Open stores no state
	// before the snapshot load fails. Those counters hide the new ACTIVE
	// counter until the driver frees them.
	requireServiceAwaitTick(
		t, sc,
		"module state counter active",
		durableWorkBound,
		func() bool { return readModuleStateCounter(set.Module(0)).isActive() },
		func() string { return readModuleStateCounter(set.Module(0)).String() },
	)
	restarted := set.Module(0)
	require.Equal(t, second.LogPosition, restarted.CommitPosition())
	require.Equal(t, 1, restarted.Agent().OpenSessionCount())
	requireServiceAwaitTick(
		t, sc,
		"archive control sessions",
		durableWorkBound,
		func() bool {
			return archiveControlSessions(t, restarted) == controlSessions
		},
	)

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "post-restart election", durableElectBound, set.AwaitElection)
	restarted = requireSingleLeader(t, set, "")
	require.Greater(t, restarted.LeadershipTermID(), second.LeadershipTermID, sc.dump())
	requireServiceAwaitTick(
		t, sc,
		"restored session closed",
		durableWorkBound,
		func() bool { return restoredSessionClosed(set) },
	)

	freshTransport, fresh := openSession(t, set, restarted)
	require.Equal(t, int64(2), fresh.ClusterSessionID(), sc.dump())
	appended := restarted.LogPublisher().Position()
	require.Greater(t, appended, second.LogPosition, sc.dump())
	requireServiceAwaitTick(
		t, sc,
		"fresh append committed",
		durableWorkBound,
		sessionPump(freshTransport, fresh, func() bool {
			return restarted.CommitPosition() >= appended
		}),
	)
	require.Equal(t, logRecordingID, restarted.Agent().LocalLogRecordingID(), sc.dump())

	replayed := replayRecordingPayloads(t, sc, restarted, archivecodecs.RecordingDescriptorFields{
		RecordingID:   logRecordingID,
		StartPosition: second.LogPosition,
		StopPosition:  appended,
	})
	opens, closes := 0, 0
	for _, payload := range replayed {
		if event, ok := codecs.TryDecodeSessionOpenEventAt(payload, 0); ok {
			opens++
			require.Equal(t, int64(2), event.ClusterSessionID)
			continue
		}
		if event, ok := codecs.TryDecodeSessionCloseEventAt(payload, 0); ok {
			closes++
			require.Equal(t, int64(1), event.ClusterSessionID)
			require.Equal(t, clusterwire.CloseReason_TIMEOUT, event.CloseReason)
		}
	}
	require.Equal(t, 1, opens, "session opens in [%d, %d)", second.LogPosition, appended)
	require.Equal(t, 1, closes, "session closes in [%d, %d)", second.LogPosition, appended)
}

const moduleStateCounterLabel = "Consensus Module state - clusterId=0"

type moduleStateCounterView struct {
	found bool
	key   [cnc.CounterMetadataMaxKeyLength]byte
	label string
	value int64
}

func readModuleStateCounter(member *cluster.ConsensusModule) moduleStateCounterView {
	var view moduleStateCounterView
	if member == nil || member.Client() == nil {
		return view
	}
	reader, err := member.Client().CountersReader()
	if err != nil {
		return view
	}
	id, found := reader.FindCounterOfType(consensus.ClusterConsensusModuleStateCounterTypeID)
	if !found {
		return view
	}
	key, keyOK := reader.CounterKey(id)
	label, labelOK := reader.CounterLabel(id)
	value, err := reader.CounterValue(id)
	if !keyOK || !labelOK || err != nil {
		return view
	}
	return moduleStateCounterView{found: true, key: key, label: label, value: value}
}

func (v moduleStateCounterView) isActive() bool {
	var wantKey [cnc.CounterMetadataMaxKeyLength]byte
	return v.found &&
		v.key == wantKey &&
		v.label == moduleStateCounterLabel &&
		v.value == int64(consensus.ModuleStateActive)
}

func (v moduleStateCounterView) String() string {
	return fmt.Sprintf(
		"module state counter found=%v key=%x label=%q value=%d",
		v.found,
		v.key[:4],
		v.label,
		v.value,
	)
}

func requireModuleStateCounterActive(t *testing.T, member *cluster.ConsensusModule, label string) {
	t.Helper()
	view := readModuleStateCounter(member)
	require.True(t, view.isActive(), "%s:\n%s", label, view)
}

func withRecordingLog(t *testing.T, dir string, fn func(*recordinglog.Log)) {
	t.Helper()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, log.Close())
	}()
	fn(log)
}

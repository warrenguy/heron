//go:build integration && archive_live

package cluster_archive_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/warrenguy/heron/archive"
	archivecodecs "github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	retentionFirstPayload  = "retention-first-message"
	retentionSecondPayload = "retention-second-message"
	retentionPostPayload   = "retention-post-message"

	// A recording.log row with no endpoint is 64 bytes. Reload omits a
	// removed slot. A removed slot, or a standby endpoint that does not
	// fit one record, makes the file longer than the entry count times 64.
	recordingLogRowBytes = 64

	// A snapshot group holds one row per service and one module row.
	snapshotGroupRows = multiServiceCount + 1

	// The offline archive client of a member replies on this base plus the
	// member index. The module client replies on the request stream, 10.
	// Service containers reply on 121 plus the service id.
	offlineArchiveResponseStreamBase int32 = 10000
)

// TestClusterMultiService_retainsLatestSnapshotAcrossRestart asserts every
// member restarts from the snapshot group that offline retention keeps.
func TestClusterMultiService_retainsLatestSnapshotAcrossRestart(t *testing.T) {
	t.Parallel()

	recorders, wrap := newSnapshotStateWrap()
	set, seats := startMultiServiceCluster(
		t,
		harness.ClusterMultiServiceSnapshotRetentionPortBase,
		harness.ClusterMultiServiceSnapshotRetentionLogEndpoint,
		wrap,
	)
	dump := func() string { return dumpMultiService(set, seats) }
	leader := requireSingleLeader(t, set, "")

	requireMultiServiceAwaitTick(
		t, set, seats,
		"join settle",
		durableWorkBound,
		func() bool { return multiServiceJoinSettled(set, seats) },
	)

	transport, session := openSession(t, set, leader)
	restoredID := session.ClusterSessionID()
	require.Positive(t, restoredID)
	requireMultiServiceSessionOpen(
		t, set, seats, transport, session, restoredID, "session open committed",
	)

	first := []byte(retentionFirstPayload)
	requireMultiServiceMessageDelivered(
		t, set, seats, transport, session, "first message delivered", first,
	)

	firstTakes := requireMultiServiceSnapshot(
		t, set, seats, leader, transport, session, "first snapshot",
	)
	requireSnapshotStates(t, recorders, dump, [][]byte{first})

	second := []byte(retentionSecondPayload)
	requireMultiServiceMessageDelivered(
		t, set, seats, transport, session, "second message delivered", second, first,
	)

	secondTakes := requireMultiServiceSnapshot(
		t, set, seats, leader, transport, session, "second snapshot",
	)
	requireSnapshotStates(t, recorders, dump, [][]byte{first}, [][]byte{first, second})
	secondPosition := secondTakes[int(leader.ThisMemberID())].LogPosition

	requireMultiServiceAwaitTick(
		t, set, seats,
		"members at second snapshot",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return multiServiceMembersAt(set, seats, secondPosition)
		}),
	)

	termIDs := make([]int64, len(set.Modules()))
	recordingIDs := make([][]int64, len(set.Modules()))
	for i, module := range set.Modules() {
		termID := module.RecordingLog().TermRecordingID()
		require.Equal(
			t,
			termID,
			module.Agent().LocalLogRecordingID(),
			"term recording member %d:\n%s",
			module.ThisMemberID(),
			dump(),
		)
		termIDs[i] = termID
		ids := slices.Concat(
			snapshotRecordingIDs(firstTakes[i]),
			snapshotRecordingIDs(secondTakes[i]),
			[]int64{termID},
		)
		require.Len(t, ids, 2*snapshotGroupRows+1, "recording ids member %d", i)
		requireDistinctRecordingIDs(t, i, ids)
		recordingIDs[i] = ids
	}
	preTerm := leader.LeadershipTermID()

	requireMultiServiceCommitBound(t, set, seats)
	requireNoMultiServiceFailures(t, set, seats, " pre-stop")

	// The test does not tick between these calls. Each call waits out the
	// directory lock.
	for i, module := range set.Modules() {
		requireRetentionRefused(
			t,
			module,
			set.MemberConfig(i),
			recordingIDs[i],
			dump,
		)
	}

	// The test does not tick between the client close and the last stop.
	require.NoError(t, session.Close())
	require.NoError(t, transport.Close())
	for index := range 3 {
		stopMultiServiceMember(t, set, seats, index)
	}

	termStarts := make([]int64, len(set.Modules()))
	for i := range set.Modules() {
		termStarts[i] = requireLatestSnapshotKept(
			t,
			i,
			set.MemberConfig(i),
			firstTakes[i],
			secondTakes[i],
			termIDs[i],
			secondPosition,
			dump,
		)
	}

	// The test does not tick until every container attaches.
	for i := range 3 {
		seats[i] = restartMultiServiceMember(t, set, i, wrap)
		module := set.Module(i)
		require.Equal(t, secondPosition, module.CommitPosition(),
			"commit member %d:\n%s", module.ThisMemberID(), dump())
		require.Equal(t, 1, module.Agent().OpenSessionCount(),
			"open sessions member %d:\n%s", module.ThisMemberID(), dump())
		require.Equal(t, termIDs[i], module.RecordingLog().TermRecordingID(),
			"term recording member %d:\n%s", module.ThisMemberID(), dump())
		for sid, seat := range seats[i] {
			where := lazyText(func() string {
				return fmt.Sprintf(
					"member %d service %d:\n%s",
					module.ThisMemberID(),
					sid,
					dump(),
				)
			})
			require.Equal(
				t,
				[][]byte{snapshotState(i, sid, first, second)},
				recorders[i][sid].loaded,
				"loaded %s",
				where,
			)
			requireRestoredSession(
				t, seat.agent, seat.recorder, secondPosition, restoredID, where,
			)
			require.Empty(t, seat.recorder.opens, "opens %s", where)
			require.Empty(t, seat.recorder.messages, "messages %s", where)
		}
	}

	newLeader := requireMultiServiceRestartSettled(t, set, seats, preTerm, restoredID)
	for i, module := range set.Modules() {
		require.Equal(t, termIDs[i], module.Agent().LocalLogRecordingID(),
			"term recording member %d:\n%s", module.ThisMemberID(), dump())
		require.True(t, module.Agent().IsRecordingActive(),
			"recording active member %d:\n%s", module.ThisMemberID(), dump())
	}

	post := []byte(retentionPostPayload)
	freshTransport, fresh := requireMultiServicePostMessage(
		t, set, seats, newLeader, restoredID, post,
	)

	appended := newLeader.LogPublisher().Position()
	require.Greater(t, appended, secondPosition, "appended:\n%s", dump())
	requireMultiServiceAwaitTick(
		t, set, seats,
		"term recordings extended",
		durableWorkBound,
		sessionPump(freshTransport, fresh, func() bool {
			return membersReachCommit(set, appended) &&
				survivorsRecordingAtLeast(set, nil, termIDs, appended)
		}),
	)
	for i, module := range set.Modules() {
		desc := requireListedRecording(t, module.ArchiveClient(), termIDs[i], i, dump)
		require.Equal(t, termStarts[i], desc.StartPosition,
			"term start member %d:\n%s", module.ThisMemberID(), dump())
		require.Equal(t, archive.NullPosition, desc.StopPosition,
			"term stop member %d:\n%s", module.ThisMemberID(), dump())
	}

	requireMultiServiceCommitBound(t, set, seats)
	requireNoMultiServiceFailures(t, set, seats, " final")
}

func retentionConfig(
	cfg cluster.Config,
	archiveClient cluster.SnapshotRetentionArchive,
) cluster.SnapshotRetentionConfig {
	return cluster.SnapshotRetentionConfig{
		ClusterDir:                cfg.ClusterDir,
		ServiceCount:              cfg.ServiceCount,
		RecordingLogFileSyncLevel: cfg.RecordingLogFileSyncLevel,
		Archive:                   archiveClient,
	}
}

func connectMemberArchive(t *testing.T, index int, cfg cluster.Config) *archive.Client {
	t.Helper()

	archiveCfg := archive.Config{
		ControlRequestChannel:   cfg.ArchiveControlRequestChannel,
		ControlResponseChannel:  cfg.ArchiveControlResponseChannel,
		ControlResponseStreamID: offlineArchiveResponseStreamBase + int32(index),
		AeronDir:                cfg.AeronDir,
		AeronConfig:             cfg.AeronConfig,
	}.WithRemoteControl()
	client, err := archive.Connect(t.Context(), archiveCfg)
	require.NoError(t, err, "archive connect member %d", index)
	harness.CleanupClose(t, fmt.Sprintf("member %d archive client", index), client.Close)
	return client
}

// ListRecording draws a new CorrelationID. ControlSessionID identifies
// the control session, not the recording.
func descriptorWithoutReplyIDs(
	desc archivecodecs.RecordingDescriptorFields,
) archivecodecs.RecordingDescriptorFields {
	desc.ControlSessionID = 0
	desc.CorrelationID = 0
	return desc
}

func requireRetentionRefused(
	t *testing.T,
	module *cluster.ConsensusModule,
	cfg cluster.Config,
	recordingIDs []int64,
	dump func() string,
) {
	t.Helper()

	memberID := int(module.ThisMemberID())
	path := filepath.Join(cfg.ClusterDir, "recording.log")
	beforeBytes, err := os.ReadFile(path)
	require.NoError(t, err, "read recording.log member %d", memberID)

	archiveClient := module.ArchiveClient()
	before := make([]archivecodecs.RecordingDescriptorFields, len(recordingIDs))
	for i, id := range recordingIDs {
		before[i] = requireListedRecording(t, archiveClient, id, memberID, dump)
	}

	result, err := cluster.RetainLatestSnapshot(t.Context(), retentionConfig(cfg, archiveClient))
	require.ErrorIs(t, err, unix.EWOULDBLOCK, "member %d:\n%s", memberID, dump())
	require.True(
		t,
		strings.HasPrefix(err.Error(), "cluster: lock cluster dir"),
		"member %d: %v",
		memberID,
		err,
	)
	require.Equal(t, cluster.SnapshotRetentionResult{}, result,
		"member %d:\n%s", memberID, dump())

	afterBytes, err := os.ReadFile(path)
	require.NoError(t, err, "read recording.log member %d", memberID)
	require.Equal(t, beforeBytes, afterBytes, "recording.log member %d", memberID)

	for i, id := range recordingIDs {
		desc := requireListedRecording(t, archiveClient, id, memberID, dump)
		require.Equal(
			t,
			descriptorWithoutReplyIDs(before[i]),
			descriptorWithoutReplyIDs(desc),
			"recording %d member %d:\n%s",
			id,
			memberID,
			dump(),
		)
	}
}

func requireLatestSnapshotKept(
	t *testing.T,
	index int,
	cfg cluster.Config,
	firstTake consensus.SnapshotTakeResult,
	secondTake consensus.SnapshotTakeResult,
	termID, secondPosition int64,
	dump func() string,
) int64 {
	t.Helper()

	client := connectMemberArchive(t, index, cfg)
	path := filepath.Join(cfg.ClusterDir, "recording.log")
	beforeEntries, err := recordinglog.Reload(path)
	require.NoError(t, err, "reload member %d", index)
	info, err := os.Stat(path)
	require.NoError(t, err, "stat member %d", index)
	require.Equal(
		t,
		int64(len(beforeEntries)*recordingLogRowBytes),
		info.Size(),
		"recording.log size member %d",
		index,
	)
	withRecordingLog(t, cfg.ClusterDir, func(log *recordinglog.Log) {
		requireSnapshotGroupsInLog(t, log, dump, firstTake, secondTake)
	})

	snapshotIDs := slices.Concat(snapshotRecordingIDs(firstTake), snapshotRecordingIDs(secondTake))
	beforeDesc := make(map[int64]archivecodecs.RecordingDescriptorFields, len(snapshotIDs))
	for _, id := range snapshotIDs {
		desc := requireListedRecording(t, client, id, index, dump)
		require.Greater(t, desc.StopPosition, desc.StartPosition,
			"snapshot stop %d member %d", id, index)
		beforeDesc[id] = desc
	}
	termDesc := requireListedRecording(t, client, termID, index, dump)
	require.Equal(t, secondPosition, termDesc.StopPosition, "term stop member %d", index)

	result, err := cluster.RetainLatestSnapshot(t.Context(), retentionConfig(cfg, client))
	require.NoError(t, err, "retain member %d:\n%s", index, dump())
	require.Equal(t, secondTake.LeadershipTermID, result.LeadershipTermID, "member %d", index)
	require.Equal(t, secondTake.LogPosition, result.LogPosition, "member %d", index)
	require.Equal(t, snapshotGroupRows, result.InvalidatedEntries, "member %d", index)
	require.Equal(t, snapshotGroupRows, result.DroppedEntries, "member %d", index)
	truncated := snapshotRecordingIDs(firstTake)
	slices.Sort(truncated)
	require.Equal(t, truncated, result.TruncatedRecordingIDs, "member %d", index)
	require.Empty(t, result.SkippedRecordingIDs, "member %d", index)

	afterEntries, err := recordinglog.Reload(path)
	require.NoError(t, err, "reload after member %d", index)
	afterInfo, err := os.Stat(path)
	require.NoError(t, err, "stat after member %d", index)
	require.Len(t, afterEntries, len(beforeEntries)-snapshotGroupRows, "entry count member %d", index)
	require.Equal(
		t,
		int64(len(afterEntries)*recordingLogRowBytes),
		afterInfo.Size(),
		"recording.log size member %d",
		index,
	)
	for _, entry := range afterEntries {
		require.True(t, entry.Valid, "entry member %d index %d", index, entry.Index)
	}
	require.Equal(t, termRows(beforeEntries), termRows(afterEntries), "term rows member %d", index)
	withRecordingLog(t, cfg.ClusterDir, func(log *recordinglog.Log) {
		requireSnapshotGroupsInLog(t, log, dump, secondTake)
	})

	for _, id := range snapshotRecordingIDs(firstTake) {
		desc := requireListedRecording(t, client, id, index, dump)
		require.Equal(t, beforeDesc[id].StartPosition, desc.StartPosition,
			"start %d member %d", id, index)
		require.Equal(t, desc.StartPosition, desc.StopPosition,
			"truncated %d member %d", id, index)
	}
	for _, id := range snapshotRecordingIDs(secondTake) {
		desc := requireListedRecording(t, client, id, index, dump)
		require.Equal(
			t,
			descriptorWithoutReplyIDs(beforeDesc[id]),
			descriptorWithoutReplyIDs(desc),
			"kept recording %d member %d",
			id,
			index,
		)
	}
	afterTerm := requireListedRecording(t, client, termID, index, dump)
	require.Equal(
		t,
		descriptorWithoutReplyIDs(termDesc),
		descriptorWithoutReplyIDs(afterTerm),
		"term recording member %d",
		index,
	)

	require.NoError(t, client.Close())
	return termDesc.StartPosition
}

func requireListedRecording(
	t *testing.T,
	client *archive.Client,
	recordingID int64,
	index int,
	dump func() string,
) archivecodecs.RecordingDescriptorFields {
	t.Helper()

	desc, err := client.ListRecording(t.Context(), recordingID)
	require.NoError(t, err, "list %d member %d:\n%s", recordingID, index, dump())
	return desc
}

// Compact renumbers Index and Position.
func termRows(entries []recordinglog.Entry) []recordinglog.Entry {
	var rows []recordinglog.Entry
	for _, entry := range entries {
		if entry.Type != recordinglog.EntryTypeTerm {
			continue
		}
		entry.Index = 0
		entry.Position = 0
		rows = append(rows, entry)
	}
	return rows
}

//go:build integration && archive_live

package cluster_archive_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	multiServicePreSnapshotPayload  = "multi-service-pre-snapshot"
	multiServicePostSnapshotPayload = "multi-service-post-snapshot"
)

type snapshotStateRecorder struct {
	*sessionRecorder

	memberIndex int
	serviceID   int
	taken       [][]byte
	loaded      [][]byte
}

func snapshotState(memberIndex, serviceID int, payloads ...[]byte) []byte {
	return fmt.Appendf(
		nil,
		"member=%d service=%d messages=%q",
		memberIndex,
		serviceID,
		payloads,
	)
}

func (s *snapshotStateRecorder) TakeSnapshot(publication *aeron.ExclusivePublication) error {
	payloads := make([][]byte, len(s.messages))
	for i, msg := range s.messages {
		payloads[i] = msg.payload
	}
	state := snapshotState(s.memberIndex, s.serviceID, payloads...)
	if err := offerSnapshotPayload(publication, state); err != nil {
		return err
	}
	s.taken = append(s.taken, state)
	return nil
}

// LoadSnapshot does not refill the message list.
func (s *snapshotStateRecorder) LoadSnapshot(image *aeron.Image) error {
	payload, err := readSnapshotPayload(image)
	if err != nil {
		return err
	}
	s.loaded = append(s.loaded, payload)
	return nil
}

// lazyText formats through its func only when a failure message prints.
type lazyText func() string

func (l lazyText) String() string {
	return l()
}

// The startup leader closes restoredID with TIMEOUT. That close may appear
// at any point, and a complete record holds it once. Pass freshID 0 when a
// fresh session is not open. The pair false, false means the events seen so
// far are a short correct prefix.
func restartedRecorderStatus(
	rec *sessionRecorder,
	restoredID, freshID int64,
	payloads ...[]byte,
) (complete, corrupt bool) {
	timedOut := recordedClose{sessionID: restoredID, reason: clusterwire.CloseReason_TIMEOUT}
	if len(rec.closes) > 1 {
		return false, true
	}
	for _, closed := range rec.closes {
		if closed != timedOut {
			return false, true
		}
	}
	if len(rec.closes) == 0 {
		return false, false
	}
	live := &sessionRecorder{
		opens:    rec.opens,
		messages: rec.messages,
		order: slices.DeleteFunc(slices.Clone(rec.order), func(event string) bool {
			return event == "close"
		}),
	}
	if freshID == 0 {
		return len(live.order) == 0, len(live.order) != 0
	}
	return recorderSequenceStatus(live, freshID, payloads...)
}

func multiServiceJoinSettled(set *harness.ClusterMemberSet, seats [][]*serviceSeat) bool {
	for i, module := range set.Modules() {
		if module == nil {
			return false
		}
		agent := module.Agent()
		if agent.PendingServiceAckWaits() != 0 {
			return false
		}
		expected := agent.ExpectedServiceAckID()
		for sid, seat := range seats[i] {
			if agent.ServiceAckQueueDepth(int32(sid)) != 0 {
				return false
			}
			if !seat.agent.IsLogBound() ||
				seat.agent.HasPendingJoin() ||
				seat.agent.NextAckID() != expected {
				return false
			}
		}
	}
	return true
}

func multiServiceRecoveryCountersFreed(t *testing.T, set *harness.ClusterMemberSet) bool {
	t.Helper()

	for _, module := range set.Modules() {
		if module == nil {
			return false
		}
		reader, err := module.Client().CountersReader()
		require.NoError(t, err)
		if _, found := reader.FindCounterOfType(codecs.RecoveryStateCounterTypeID); found {
			return false
		}
	}
	return true
}

// restoredSessionClosed reports whether every module removed the session
// it restored. A startup leader closes it, and followers apply the close.
func restoredSessionClosed(set *harness.ClusterMemberSet) bool {
	for _, module := range set.Modules() {
		if module == nil {
			return false
		}
		agent := module.Agent()
		if agent.OpenSessionCount() != 0 || agent.ClosingSessionCount() != 0 {
			return false
		}
	}
	return true
}

func multiServiceRecordersRestarted(
	t *testing.T,
	set *harness.ClusterMemberSet,
	seats [][]*serviceSeat,
	restoredID, freshID int64,
	payloads ...[]byte,
) bool {
	t.Helper()

	for i, memberSeats := range seats {
		module := set.Module(i)
		for sid, seat := range memberSeats {
			complete, corrupt := restartedRecorderStatus(
				seat.recorder, restoredID, freshID, payloads...,
			)
			if corrupt {
				require.FailNow(
					t,
					"restarted service sequence corrupted",
					"member %d service %d:\n%s",
					module.ThisMemberID(),
					sid,
					dumpMultiService(set, seats),
				)
			}
			if !complete {
				return false
			}
		}
	}
	return true
}

// TestClusterMultiService_restartsEveryMemberFromSnapshots asserts three
// members with two services each restart from their own snapshots.
func TestClusterMultiService_restartsEveryMemberFromSnapshots(t *testing.T) {
	t.Parallel()

	recorders := make([][]*snapshotStateRecorder, 3)
	for i := range recorders {
		recorders[i] = make([]*snapshotStateRecorder, multiServiceCount)
	}
	wrap := func(memberIndex, serviceID int, rec *sessionRecorder) service.ClusteredService {
		state := &snapshotStateRecorder{
			sessionRecorder: rec,
			memberIndex:     memberIndex,
			serviceID:       serviceID,
		}
		recorders[memberIndex][serviceID] = state
		return state
	}

	set, seats := startMultiServiceCluster(
		t,
		harness.ClusterMultiServiceSnapshotRestartPortBase,
		harness.ClusterMultiServiceSnapshotRestartLogEndpoint,
		wrap,
	)
	sc := serviceCluster{set: set}
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
	requireMultiServiceAwaitTick(
		t, set, seats,
		"session open committed",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return everyMultiServiceRecorder(seats, func(rec *sessionRecorder) bool {
				return rec.holdsOpen(restoredID)
			})
		}),
	)

	pre := []byte(multiServicePreSnapshotPayload)
	offerServiceMessage(t, sc, session, pre)
	requireMultiServiceAwaitTick(
		t, set, seats,
		"pre message delivered",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return everyMultiServiceRecorder(seats, func(rec *sessionRecorder) bool {
				return rec.holdsMessagesAfterOpen(restoredID, pre)
			})
		}),
	)

	leaderTake := requestModuleSnapshot(t, sc, leader, transport, session, "snapshot")
	actionPosition := leaderTake.LogPosition
	requireMultiServiceAwaitTick(
		t, set, seats,
		"snapshot on every member",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return snapshotTakenOnEveryMember(t, set, actionPosition, dump)
		}),
	)
	// The await above resolved every take without error.
	takes := make([]consensus.SnapshotTakeResult, 3)
	for i, module := range set.Modules() {
		result, _ := module.Agent().LastSnapshotTake()
		require.NoError(t, result.ForceErr, "snapshot member %d:\n%s", module.ThisMemberID(), dump())
		require.Len(t, result.ServiceRecordingIDs, multiServiceCount, "member %d", module.ThisMemberID())
		for sid, seat := range seats[i] {
			where := lazyText(func() string {
				return fmt.Sprintf("member %d service %d:\n%s", module.ThisMemberID(), sid, dump())
			})
			require.Equal(t, result.ServiceRecordingIDs[sid], seat.agent.LastSnapshotRecordingID(),
				"snapshot recording %s", where)
			require.Equal(t, [][]byte{snapshotState(i, sid, pre)}, recorders[i][sid].taken,
				"taken %s", where)
		}
		result.ServiceRecordingIDs = slices.Clone(result.ServiceRecordingIDs)
		takes[i] = result
	}

	requireMultiServiceAwaitTick(
		t, set, seats,
		"members at snapshot",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			if !membersReachCommit(set, actionPosition) ||
				!survivorsAppendAtLeast(set, nil, actionPosition) {
				return false
			}
			for _, memberSeats := range seats {
				for _, seat := range memberSeats {
					if seat.agent.LogPosition() != actionPosition {
						return false
					}
				}
			}
			return true
		}),
	)
	type storedEgress struct {
		channel string
		stream  int32
	}
	stored := make([][]storedEgress, 3)
	for i, memberSeats := range seats {
		stored[i] = make([]storedEgress, multiServiceCount)
		for sid, seat := range memberSeats {
			restored, ok := seat.agent.ClientSession(restoredID)
			require.True(t, ok, "session %d member %d service %d:\n%s", restoredID, i, sid, dump())
			stored[i][sid] = storedEgress{
				channel: restored.ResponseChannel(),
				stream:  restored.ResponseStreamID(),
			}
		}
	}
	preTerm := leader.LeadershipTermID()

	requireMultiServiceCommitBound(t, set, seats)
	requireNoMultiServiceFailures(t, set, seats, " pre-stop")

	// The test does not tick between the client close and the last stop.
	require.NoError(t, session.Close())
	require.NoError(t, transport.Close())
	for index := range 3 {
		stopMultiServiceMember(t, set, seats, index)
	}

	for i, take := range takes {
		withRecordingLog(t, set.ClusterDir(i), func(log *recordinglog.Log) {
			requireSnapshotGroupsInLog(t, log, dump, take)
		})
		ids := slices.Concat(take.ServiceRecordingIDs, []int64{take.RecordingID})
		seen := make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			require.GreaterOrEqual(t, id, int64(0), "recording id member %d", i)
			_, dup := seen[id]
			require.False(t, dup, "recording id %d repeats on member %d", id, i)
			seen[id] = struct{}{}
		}
	}

	// The test does not tick until every container attaches.
	for i := range 3 {
		seats[i] = restartMultiServiceMember(t, set, i, wrap)
		module := set.Module(i)
		require.Equal(t, actionPosition, module.CommitPosition(),
			"commit member %d:\n%s", module.ThisMemberID(), dump())
		require.Equal(t, 1, module.Agent().OpenSessionCount(),
			"open sessions member %d:\n%s", module.ThisMemberID(), dump())
		for sid, seat := range seats[i] {
			where := lazyText(func() string {
				return fmt.Sprintf("member %d service %d:\n%s", module.ThisMemberID(), sid, dump())
			})
			require.Equal(t, [][]byte{snapshotState(i, sid, pre)}, recorders[i][sid].loaded,
				"loaded %s", where)
			restored := requireRestoredSession(
				t, seat.agent, seat.recorder, actionPosition, restoredID, where,
			)
			egress := storedEgress{channel: restored.ResponseChannel(), stream: restored.ResponseStreamID()}
			require.Equal(t, stored[i][sid], egress, "response egress %s", where)
			require.Empty(t, seat.recorder.opens, "opens %s", where)
			require.Empty(t, seat.recorder.messages, "messages %s", where)
		}
	}

	requireAwait(t, set, "post-restart log images", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "post-restart election", durableElectBound, set.AwaitElection)
	newLeader := requireSingleLeader(t, set, "")
	term := newLeader.LeadershipTermID()
	leaderID := newLeader.ThisMemberID()
	require.Greater(t, term, preTerm, "term:\n%s", dump())

	// The leader has no append target. LeaderMemberID stays -1.
	require.Equal(t, int32(-1), newLeader.LeaderMemberID(),
		"leader append target:\n%s", dump())
	for _, module := range set.Modules() {
		require.Equal(t, term, module.LeadershipTermID(),
			"term member %d:\n%s", module.ThisMemberID(), dump())
		if module.ThisMemberID() == leaderID {
			continue
		}
		require.Equal(t, leaderID, module.LeaderMemberID(),
			"leader member %d:\n%s", module.ThisMemberID(), dump())
	}

	requireMultiServiceAwaitTick(
		t, set, seats,
		"restart settle",
		durableWorkBound,
		func() bool {
			return multiServiceJoinSettled(set, seats) &&
				multiServiceRecoveryCountersFreed(t, set) &&
				restoredSessionClosed(set) &&
				multiServiceRecordersRestarted(t, set, seats, restoredID, 0)
		},
	)

	freshTransport, fresh := openSession(t, set, newLeader)
	freshID := fresh.ClusterSessionID()
	require.Equal(t, restoredID+1, freshID, dump())
	requireMultiServiceAwaitTick(
		t, set, seats,
		"fresh session open committed",
		durableWorkBound,
		sessionPump(freshTransport, fresh, func() bool {
			return everyMultiServiceRecorder(seats, func(rec *sessionRecorder) bool {
				return rec.holdsOpen(freshID)
			})
		}),
	)

	post := []byte(multiServicePostSnapshotPayload)
	offerServiceMessage(t, sc, fresh, post)
	requireMultiServiceAwaitTick(
		t, set, seats,
		"post message delivered",
		durableWorkBound,
		sessionPump(freshTransport, fresh, func() bool {
			return multiServiceRecordersRestarted(
				t, set, seats, restoredID, freshID, post,
			)
		}),
	)

	requireMultiServiceCommitBound(t, set, seats)
	requireNoMultiServiceFailures(t, set, seats, " final")
}

//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	archivecodecs "github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
	"github.com/warrenguy/heron/cluster/service"
	"github.com/warrenguy/heron/idlestrategy"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	snapshotServicePayload = "service-snapshot-state"
	snapshotSessionPayload = "service-snapshot-message"

	snapshotReplayStreamID int32 = 9106

	snapshotOfferBound = 5 * time.Second

	// snapshotAppVersion is non-zero so a marker with app version 0 fails.
	snapshotAppVersion int32 = 7

	// snapshotLoadServiceStreamID keeps load container control ingress off
	// the module service stream.
	snapshotLoadServiceStreamID int32 = 9104

	// snapshotLoadModuleStreamID keeps load container control egress off
	// the module stream. The module never reads the load container start ack.
	snapshotLoadModuleStreamID int32 = 9105

	snapshotLoadReplayStreamID int32 = 9103

	snapshotLoadResponseStreamID int32 = 9121

	snapshotLoadTimestamp int64 = 987_654_321

	snapshotUnknownRecordingID int64 = 1_000_000
)

func amendSnapshotMember(cfg cluster.Config) cluster.Config {
	return withSnapshotAppVersion(amendServiceMember(cfg))
}

// withSnapshotAppVersion sets the non-zero marker app version on cfg.
func withSnapshotAppVersion(cfg cluster.Config) cluster.Config {
	cfg.Election.AppVersion = snapshotAppVersion
	return cfg
}

// snapshotRecorder adds a counted TakeSnapshot to sessionRecorder.
type snapshotRecorder struct {
	*sessionRecorder
	takes int
}

var _ service.TakeSnapshotHandler = (*snapshotRecorder)(nil)

func (s *snapshotRecorder) TakeSnapshot(publication *aeron.ExclusivePublication) error {
	s.takes++
	payload := []byte(snapshotServicePayload)
	deadline := time.Now().Add(snapshotOfferBound)
	for {
		result := publication.Offer(payload)
		switch {
		case result > 0:
			return nil
		case !aeron.IsIngressRetriable(result):
			return fmt.Errorf("offer snapshot payload: code %d", result)
		case time.Now().After(deadline):
			return fmt.Errorf("offer snapshot payload: timed out, code %d", result)
		}
		time.Sleep(time.Millisecond)
	}
}

// openSnapshotServiceCluster starts one member and one service container.
// The container connects its own archive client. TakeSnapshot records
// through it.
func openSnapshotServiceCluster(t *testing.T) (serviceCluster, *snapshotRecorder) {
	t.Helper()

	set := harness.StartClusterMemberSet(t, harness.ClusterMemberSetConfig{
		MemberCount:          1,
		PortBase:             harness.ClusterServiceSnapshotPortBase,
		LogMulticastEndpoint: harness.ClusterServiceSnapshotLogEndpoint,
		AmendMemberConfig:    amendSnapshotMember,
	})
	require.Len(t, set.Modules(), 1)

	rec := &snapshotRecorder{sessionRecorder: &sessionRecorder{}}
	module := set.Module(0)
	container, err := service.Open(service.Config{
		Aeron:             module.Client(),
		Service:           rec,
		CommittedPosition: module.CommitPosition,
		Archive:           &archive.Config{},
		Recovery:          &service.RecoveryConfig{},
	})
	require.NoError(t, err)
	harness.CleanupClose(t, "service container", container.Close)

	agent := container.Agent()
	checked := &boundCheckedService{agent: agent, module: module}
	set.AttachService(t, 0, checked)

	requireAwait(t, set, "log image fan-in", durableImageBound, set.AwaitLogImages)
	requireAwait(t, set, "election", durableElectBound, set.AwaitElection)

	sc := serviceCluster{
		set: set,
		members: []serviceMember{{
			rec:       rec.sessionRecorder,
			agent:     agent,
			checked:   checked,
			container: container,
		}},
	}
	return sc, rec
}

// requestSnapshotAction requests one snapshot on leader. Ticks until the
// leader's snapshot wait clears and its module snapshot take resolves.
//
// Returns the request time and the action frame end.
func requestSnapshotAction(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	session *client.AeronCluster,
	label string,
) (
	nowNs int64,
	actionPosition int64,
) {
	t.Helper()

	nowNs = leader.NanoClock()()
	if err := leader.RequestSnapshot(nowNs); err != nil {
		require.NoError(t, err, "RequestSnapshot %s:\n%s", label, sc.dump())
	}
	// No tick runs between the append and this read, so Position is the
	// action frame end.
	actionPosition = leader.LogPublisher().Position()
	require.True(t, leader.Agent().HasPendingSnapshotAction(), "%s:\n%s", label, sc.dump())

	requireServiceAwaitTick(
		t, sc,
		label,
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return !leader.Agent().HasPendingSnapshotAction()
		}),
	)
	return nowNs, actionPosition
}

// requestSnapshotRecording is requestModuleSnapshot plus the service ack.
// The take holds the acked recording id as its service 0 row.
//
// Returns the resolved take and the acked recording id.
func requestSnapshotRecording(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	session *client.AeronCluster,
	label string,
) (
	take consensus.SnapshotTakeResult,
	recordingID int64,
) {
	t.Helper()

	agent := sc.members[0].agent
	ackID := agent.NextAckID()
	take = requestModuleSnapshot(t, sc, leader, transport, session, label)
	requireNoServiceFailure(t, sc, 0, " after "+label)
	require.Equal(t, ackID+1, agent.NextAckID(), "%s:\n%s", label, sc.dump())
	recordingID = agent.LastSnapshotRecordingID()
	require.Equal(t, []int64{recordingID}, take.ServiceRecordingIDs, label)
	return take, recordingID
}

// requireSnapshotRecordingDescriptor lists recordingID on the leader archive.
// Asserts the descriptor names a non-empty snapshot recording on streamID.
func requireSnapshotRecordingDescriptor(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	recordingID int64,
	streamID int32,
) archivecodecs.RecordingDescriptorFields {
	t.Helper()

	desc, err := leader.ArchiveClient().ListRecording(t.Context(), recordingID)
	require.NoError(t, err, "ListRecording:\n%s", sc.dump())
	require.Equal(t, recordingID, desc.RecordingID)
	require.Equal(t, streamID, desc.StreamID)
	require.Contains(t, string(desc.StrippedChannel), "session-id=")
	require.Greater(t, desc.StopPosition, desc.StartPosition)
	return desc
}

// requireSnapshotGroups asserts the leader recording log holds one snapshot
// group per take, in take order. A group holds the service rows from the
// highest service id down to 0, then the module row.
func requireSnapshotGroups(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	takes ...consensus.SnapshotTakeResult,
) {
	t.Helper()

	recordingLog := leader.RecordingLog()
	require.NotNil(t, recordingLog, "recording log:\n%s", sc.dump())
	var want []recordinglog.Entry
	for _, take := range takes {
		term := recordingLog.FindTermEntry(take.LeadershipTermID)
		require.NotNil(t, term, "term entry %d:\n%s", take.LeadershipTermID, sc.dump())
		row := recordinglog.Entry{
			LeadershipTermID:    take.LeadershipTermID,
			TermBaseLogPosition: term.TermBaseLogPosition,
			LogPosition:         take.LogPosition,
			Timestamp:           take.Timestamp,
			Type:                recordinglog.EntryTypeSnapshot,
			Valid:               true,
		}
		for serviceID, recordingID := range slices.Backward(take.ServiceRecordingIDs) {
			row.RecordingID = recordingID
			row.ServiceID = int32(serviceID)
			want = append(want, row)
		}
		row.RecordingID = take.RecordingID
		row.ServiceID = recordinglog.ConsensusModuleServiceID
		want = append(want, row)
	}

	var got []recordinglog.Entry
	for _, entry := range recordingLog.Entries() {
		if entry.Type != recordinglog.EntryTypeSnapshot {
			continue
		}
		entry.Position = 0
		entry.Index = 0
		got = append(got, entry)
	}
	require.Equal(t, want, got, "snapshot entries:\n%s", sc.dump())
}

// replayRecordingPayloads replays desc on the member driver. Returns one
// copied payload per fragment.
func replayRecordingPayloads(
	t *testing.T,
	sc serviceCluster,
	module *cluster.ConsensusModule,
	desc archivecodecs.RecordingDescriptorFields,
) [][]byte {
	t.Helper()

	replayChannel := "aeron:ipc"
	replaySub, err := module.Client().AddSubscription(snapshotReplayStreamID, replayChannel)
	require.NoError(t, err)
	harness.CleanupClose(t, "replay subscription", replaySub.Close)

	archiveClient := module.ArchiveClient()
	replaySessionID, err := archiveClient.StartReplay(
		t.Context(),
		desc.RecordingID,
		desc.StartPosition,
		desc.StopPosition-desc.StartPosition,
		snapshotReplayStreamID,
		[]byte(replayChannel),
	)
	require.NoError(t, err, "StartReplay:\n%s", sc.dump())

	var (
		image    *aeron.Image
		payloads [][]byte
	)
	collect := func(buffer atomicbuffer.AtomicBuffer, offset, length int, _ *aeron.Header) {
		payloads = append(payloads, slices.Clone(buffer.Bytes()[offset:offset+length]))
	}
	// want is a cap only. The stop position ends the drain first.
	const want = 64
	requireServiceAwaitTick(
		t, sc,
		"snapshot replay",
		durableWorkBound,
		func() bool {
			if image == nil {
				found, ok, findErr := archive.FindReplayImage(replaySub, replaySessionID)
				require.NoError(t, findErr)
				if !ok {
					return false
				}
				image = found
			}
			_, done, pollErr := archive.PollReplayFragments(
				image,
				want,
				desc.StopPosition,
				len(payloads),
				collect,
			)
			require.NoError(t, pollErr)
			return done
		},
	)
	return payloads
}

// requireSnapshotMarkers asserts payloads[0] decodes as BEGIN and
// payloads[endRow] as END. Both markers carry typeID, actionPosition and
// leadershipTermID.
func requireSnapshotMarkers(
	t *testing.T,
	payloads [][]byte,
	typeID int64,
	endRow int,
	actionPosition int64,
	leadershipTermID int64,
) {
	t.Helper()

	markers := []struct {
		row  int
		mark clusterwire.SnapshotMark
	}{
		{row: 0, mark: clusterwire.SnapshotMark_BEGIN},
		{row: endRow, mark: clusterwire.SnapshotMark_END},
	}
	for _, want := range markers {
		marker, ok := codecs.TryDecodeSnapshotMarkerAt(payloads[want.row], 0)
		require.True(t, ok, "marker decode at row %d", want.row)
		require.Equal(t, codecs.SnapshotMarkerFields{
			TypeID:           typeID,
			LogPosition:      actionPosition,
			LeadershipTermID: leadershipTermID,
			Index:            0,
			Mark:             want.mark,
			TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:       snapshotAppVersion,
		}, marker, "marker at row %d", want.row)
	}
}

// requireSnapshotReplay asserts payloads decode as BEGIN, one ClientSession
// for sessionID, END, then snapshotServicePayload.
func requireSnapshotReplay(
	t *testing.T,
	payloads [][]byte,
	actionPosition int64,
	leadershipTermID int64,
	sessionID int64,
) {
	t.Helper()

	require.Len(t, payloads, 4, "replayed fragments")
	requireSnapshotMarkers(
		t, payloads, codecs.ServiceSnapshotTypeID, 2, actionPosition, leadershipTermID,
	)

	row, ok := codecs.TryDecodeClientSessionAt(payloads[1], 0)
	require.True(t, ok, "client session decode")
	require.Equal(t, sessionID, row.ClusterSessionID)
	require.Equal(t, client.DefaultEgressStreamID, row.ResponseStreamID)
	require.True(t, strings.HasPrefix(string(row.ResponseChannel), "aeron:udp?"),
		"response channel %q", row.ResponseChannel)
	require.Equal(t, sessionSmokePrincipal, string(row.EncodedPrincipal))

	require.Equal(t, snapshotServicePayload, string(payloads[3]))
}

// snapshotLoadRecorder adds LoadSnapshot to sessionRecorder. LoadSnapshot
// reads one user payload.
type snapshotLoadRecorder struct {
	*sessionRecorder
	loaded []string
}

var _ service.LoadSnapshotHandler = (*snapshotLoadRecorder)(nil)

func (s *snapshotLoadRecorder) Start(cluster service.Cluster) error {
	s.order = append(s.order, "start")
	return s.sessionRecorder.Start(cluster)
}

func (s *snapshotLoadRecorder) LoadSnapshot(image *aeron.Image) error {
	s.order = append(s.order, "load")
	var payload []byte
	read := pollUntil(snapshotOfferBound, func() bool {
		image.Poll(func(buffer atomicbuffer.AtomicBuffer, offset, length int, _ *aeron.Header) {
			payload = slices.Clone(buffer.Bytes()[offset : offset+length])
		}, 1)
		return payload != nil
	})
	if !read {
		return errors.New("read snapshot payload: timed out")
	}
	s.loaded = append(s.loaded, string(payload))
	return nil
}

// openLoadContainer opens a second container on module with Recovery set.
func openLoadContainer(
	module *cluster.ConsensusModule,
	svc service.ClusteredService,
) (*service.Container, error) {
	return service.Open(service.Config{
		Aeron:                   module.Client(),
		Service:                 svc,
		CommittedPosition:       module.CommitPosition,
		ServiceStreamID:         snapshotLoadServiceStreamID,
		ConsensusModuleStreamID: snapshotLoadModuleStreamID,
		Archive:                 &archive.Config{ControlResponseStreamID: snapshotLoadResponseStreamID},
		Recovery:                &service.RecoveryConfig{ReplayStreamID: snapshotLoadReplayStreamID},
	})
}

// allocateRecoveryState allocates a recovery state counter on module with
// fields as its key. The returned func closes the counter. The func then
// waits until the driver frees the counter.
func allocateRecoveryState(
	t *testing.T,
	sc serviceCluster,
	module *cluster.ConsensusModule,
	fields codecs.RecoveryStateKeyFields,
) func() {
	t.Helper()

	// The module's recovery state counter stays until the service start wait
	// pops. A service.Open call reads the first counter of that type. Wait
	// until the driver frees the module counter.
	reader, err := module.Client().CountersReader()
	require.NoError(t, err)
	requireServiceAwaitTick(
		t, sc,
		"module recovery state counter freed",
		durableWorkBound,
		func() bool {
			_, found := reader.FindCounterOfType(codecs.RecoveryStateCounterTypeID)
			return !found
		},
	)

	key := make([]byte, cnc.CounterMetadataMaxKeyLength)
	end, ok := codecs.EncodeRecoveryStateKey(key, 0, fields)
	require.True(t, ok)
	counter, err := module.Client().AddCounter(
		codecs.RecoveryStateCounterTypeID, key[:end], "recovery state",
	)
	require.NoError(t, err, "AddCounter:\n%s", sc.dump())
	counterID := counter.CounterID()
	return func() {
		t.Helper()
		require.NoError(t, counter.Close())
		reader, err := module.Client().CountersReader()
		require.NoError(t, err)
		requireServiceAwaitTick(
			t, sc,
			"recovery state counter freed",
			durableWorkBound,
			func() bool {
				return !reader.IsCounterAllocatedOfType(
					counterID, codecs.RecoveryStateCounterTypeID,
				)
			},
		)
	}
}

func archiveControlSessions(t *testing.T, module *cluster.ConsensusModule) int64 {
	t.Helper()

	reader, err := module.Client().CountersReader()
	require.NoError(t, err)
	counterID, ok := reader.FindCounterOfType(cnc.ArchiveControlSessionsCounterTypeID)
	require.True(t, ok, "archive control sessions counter not found")
	sessions, err := reader.CounterValue(counterID)
	require.NoError(t, err)
	return sessions
}

// requireLoadRefused opens a load container against fields. Asserts Open
// returns a nil container and a load error that names wantCause. Asserts
// the archive drops the load container control session.
func requireLoadRefused(
	t *testing.T,
	sc serviceCluster,
	module *cluster.ConsensusModule,
	fields codecs.RecoveryStateKeyFields,
	wantCause, label string,
) {
	t.Helper()

	release := allocateRecoveryState(t, sc, module, fields)
	defer release()
	sessionsBefore := archiveControlSessions(t, module)
	svc := &snapshotLoadRecorder{sessionRecorder: &sessionRecorder{}}

	container, err := openLoadContainer(module, svc)
	require.Nil(t, container, label)
	require.ErrorContains(t, err, "service: load snapshot", label)
	require.ErrorContains(t, err, wantCause, label)
	require.Empty(t, svc.loaded, label)
	requireServiceAwaitTick(
		t, sc,
		label+": archive drops the load control session",
		durableWorkBound,
		func() bool {
			return archiveControlSessions(t, module) == sessionsBefore
		},
	)
}

// TestClusterService_recordsAndLoadsSnapshot asserts live snapshots record
// to the archive and the recording log, and load into a Recovery container.
func TestClusterService_recordsAndLoadsSnapshot(t *testing.T) {
	t.Parallel()

	sc, rec := openSnapshotServiceCluster(t)
	set := sc.set
	leader := requireSingleLeader(t, set, "")
	agent := sc.members[0].agent

	requireServicesJoined(t, sc)
	requireNoServiceFailure(t, sc, 0, "")
	require.Equal(t, int64(-1), agent.LastSnapshotRecordingID())

	// Phase: one session, one message. The registry holds one row.
	transport := startSessionClientTransport(t)
	result, connectErr := startSessionConnect(
		t, set, leader, transport, []byte(sessionSmokeCredentials),
	)
	require.NoError(t, connectErr)
	session, err := client.NewAeronCluster(result)
	require.NoError(t, err)
	harness.CleanupClose(t, "session", session.Close)
	wantSession := session.ClusterSessionID()
	require.GreaterOrEqual(t, wantSession, int64(0))

	payload := []byte(snapshotSessionPayload)
	offerCtx, cancelOffer := context.WithTimeout(t.Context(), durableWorkBound)
	defer cancelOffer()
	lastKeepalive := time.Now()
	pos, offerErr := session.IngressOfferUntilPositive(
		offerCtx,
		client.IngressOfferConfig{
			IdleStrategy: idlestrategy.NewSleeping(aeron.DefaultIdleSleepDuration),
		},
		func() int64 {
			set.Tick()
			sendKeepAliveIfDue(session, &lastKeepalive)
			return session.Offer(payload)
		},
	)
	if offerErr != nil {
		require.FailNow(t, "session offer failed", "err=%v:\n%s", offerErr, sc.dump())
	}
	require.Positive(t, pos)
	requireServiceAwaitTick(
		t, sc,
		"session message delivered",
		durableWorkBound,
		sessionPump(transport, session, func() bool {
			return rec.holdsMessagesAfterOpen(wantSession, payload)
		}),
	)

	// Phase: first snapshot.
	firstTake, firstID := requestSnapshotRecording(
		t, sc, leader, transport, session, "first snapshot",
	)
	firstPosition := firstTake.LogPosition
	require.Equal(t, 1, rec.takes)
	logRecordingID := leader.Agent().LocalLogRecordingID()
	require.GreaterOrEqual(t, logRecordingID, int64(0), "log recording id:\n%s", sc.dump())
	require.GreaterOrEqual(t, firstID, int64(0), "first recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, firstID, "log recording id:\n%s", sc.dump())

	// Phase: list and replay the first recording.
	desc := requireSnapshotRecordingDescriptor(
		t, sc, leader, firstID, service.DefaultSnapshotStreamID,
	)
	payloads := replayRecordingPayloads(t, sc, leader, desc)
	requireSnapshotReplay(t, payloads, firstPosition, leader.LeadershipTermID(), wantSession)
	recordedRow, ok := codecs.TryDecodeClientSessionAt(payloads[1], 0)
	require.True(t, ok, "client session decode")

	// Phase: unknown recording. The replay start fails.
	recoveryState := codecs.RecoveryStateKeyFields{
		LeadershipTermID:     leader.LeadershipTermID(),
		LogPosition:          firstPosition,
		Timestamp:            snapshotLoadTimestamp,
		SnapshotRecordingIDs: []int64{snapshotUnknownRecordingID},
	}
	requireLoadRefused(t, sc, leader, recoveryState, "start replay", "unknown recording")

	// Phase: corrupt recording. The module log recording is still active.
	// The replay follows the recording until the load stops the replay. The
	// loader rejects the first log frame.
	recoveryState.SnapshotRecordingIDs = []int64{logRecordingID}
	requireLoadRefused(
		t, sc, leader, recoveryState, "before the begin marker", "corrupt recording",
	)

	// Phase: load the first recording.
	recoveryState.SnapshotRecordingIDs = []int64{firstID}
	release := allocateRecoveryState(t, sc, leader, recoveryState)
	loadRec := &snapshotLoadRecorder{sessionRecorder: &sessionRecorder{}}
	loadContainer, err := openLoadContainer(leader, loadRec)
	require.NoError(t, err, "load Open:\n%s", sc.dump())
	harness.CleanupClose(t, "load container", loadContainer.Close)
	require.Equal(t, []string{snapshotServicePayload}, loadRec.loaded)
	loadAgent := loadContainer.Agent()
	restored, ok := loadAgent.ClientSession(wantSession)
	require.True(t, ok, "restored session")
	require.Equal(t, client.DefaultEgressStreamID, restored.ResponseStreamID())
	require.Equal(t, string(recordedRow.ResponseChannel), restored.ResponseChannel())
	require.Equal(t, sessionSmokePrincipal, string(restored.EncodedPrincipal()))
	require.Empty(t, loadRec.opens, "SessionOpen calls")
	require.Equal(t, firstPosition, loadAgent.LogPosition())
	require.Equal(t, snapshotLoadTimestamp, loadAgent.ClusterTime())
	loadAgent.DoWork(leader.NanoClock()())
	require.NoError(t, loadAgent.ServiceFailure())
	require.Equal(t, []string{"load", "start"}, loadRec.order)
	require.Equal(t, 1, loadAgent.ReadyCoordinationAcks(), "start entry ready")
	require.NoError(t, loadContainer.Close())
	release()

	// Phase: second snapshot records a new recording.
	secondTake, secondID := requestSnapshotRecording(
		t, sc, leader, transport, session, "second snapshot",
	)
	require.Equal(t, 2, rec.takes)
	require.GreaterOrEqual(t, secondID, int64(0), "second recording id:\n%s", sc.dump())
	require.NotEqual(t, firstID, secondID, "second recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, secondID, "second recording id:\n%s", sc.dump())

	// Phase: the recording log holds one group per take. The recovery plan
	// picks the second group, module row first.
	requireSnapshotGroups(t, sc, leader, firstTake, secondTake)
	plan, err := leader.RecordingLog().PlanRecovery(
		archive.NewLister(t.Context(), leader.ArchiveClient()),
		recordinglog.NewRecoveryPlanConfig(1),
	)
	require.NoError(t, err, "PlanRecovery:\n%s", sc.dump())
	require.Len(t, plan.Snapshots, 2, "plan snapshots:\n%s", sc.dump())
	require.Equal(t, recordinglog.ConsensusModuleServiceID, plan.Snapshots[0].ServiceID)
	require.Equal(t, secondTake.RecordingID, plan.Snapshots[0].RecordingID)
	require.Equal(t, int32(0), plan.Snapshots[1].ServiceID)
	require.Equal(t, secondID, plan.Snapshots[1].RecordingID)
	requireSnapshotRecordingDescriptor(
		t, sc, leader, secondTake.RecordingID, cluster.DefaultSnapshotStreamID,
	)

	require.NoError(t, sc.members[0].checked.violation, "commit bound:\n%s", sc.dump())
	requireNoServiceFailure(t, sc, 0, " final")
}

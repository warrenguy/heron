//go:build integration && archive_live

package cluster_archive_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	archivecodecs "github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
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

// requestSnapshotRecording is requestSnapshotAction plus the service ack.
//
// Returns the action frame end and the acked recording id.
func requestSnapshotRecording(
	t *testing.T,
	sc serviceCluster,
	leader *cluster.ConsensusModule,
	transport *aeron.Client,
	session *client.AeronCluster,
	label string,
) (
	actionPosition int64,
	recordingID int64,
) {
	t.Helper()

	agent := sc.members[0].agent
	ackID := agent.NextAckID()
	_, actionPosition = requestSnapshotAction(t, sc, leader, transport, session, label)
	requireNoServiceFailure(t, sc, 0, " after "+label)
	require.Equal(t, ackID+1, agent.NextAckID(), "%s:\n%s", label, sc.dump())
	return actionPosition, agent.LastSnapshotRecordingID()
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

// TestClusterService_recordsSnapshotAndAcksRecordingID asserts each live
// snapshot records a new archive recording and acks its id.
func TestClusterService_recordsSnapshotAndAcksRecordingID(t *testing.T) {
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
	firstPosition, firstID := requestSnapshotRecording(
		t, sc, leader, transport, session, "first snapshot",
	)
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

	// Phase: second snapshot records a new recording.
	_, secondID := requestSnapshotRecording(
		t, sc, leader, transport, session, "second snapshot",
	)
	require.Equal(t, 2, rec.takes)
	require.GreaterOrEqual(t, secondID, int64(0), "second recording id:\n%s", sc.dump())
	require.NotEqual(t, firstID, secondID, "second recording id:\n%s", sc.dump())
	require.NotEqual(t, logRecordingID, secondID, "second recording id:\n%s", sc.dump())

	require.NoError(t, sc.members[0].checked.violation, "commit bound:\n%s", sc.dump())
	requireNoServiceFailure(t, sc, 0, " final")
}

//go:build testhooks

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/election"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	catchupSnapshotRecordingID int64 = 42
	catchupSnapshotModuleID    int64 = 31
	catchupSnapshotTimeoutNs   int64 = 1_000_000_000
)

func TestFollowerCatchup_snapshotAfterTermEventWritesGroup(t *testing.T) {
	t.Parallel()

	const (
		priorTerm int64 = 4
		termID    int64 = 5
		leaderID  int32 = 1
		memberID  int32 = 0
	)

	log := openTermLog(t, catchupSnapshotRecordingID, priorTerm)

	agent := testReplayAgent(catchupSnapshotRecordingID)
	adapter := agent.LogAdapter()
	image := newBoundReplayImage(t, 0)
	adapter.SetImage(image)

	baseB := writeReplayFrame(t, image, 0, []byte{1})
	eventEnd := writeCatchupTermEvent(t, image, baseB, termID, leaderID)
	actionEnd := writeCatchupSnapshotAction(t, image, eventEnd, termID)
	tail := writeReplayFrame(t, image, actionEnd, []byte{1})

	nodeState, err := election.OpenNodeStateFile(t.TempDir(), true, recordinglog.FileSyncData)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nodeState.Close() })

	thisMember := &election.Member{ID: memberID}
	leader := &election.Member{ID: leaderID}
	members := []*election.Member{thisMember, leader}
	publisher := &election.SbePublisher{}
	clock := func() int64 { return catchupSnapshotTimeoutNs }

	host := election.NewHostAdapter(election.HostAdapterConfig{
		Agent:                    agent,
		Publisher:                publisher,
		Members:                  members,
		ThisMember:               thisMember,
		LeaderHeartbeatTimeoutNs: catchupSnapshotTimeoutNs,
		NanoClock:                clock,
		LeaderMemberID:           election.NullMemberID,
		EpochClock:               clock,
		RecordingLogLookup:       log,
	})

	cfg := election.ElectionTestConfig()
	cfg.LeadershipTermID = priorTerm
	cfg.TermBaseLogPosition = 0
	cfg.LogPosition = 0
	cfg.AppendPosition = 0
	cfg.MemberID = memberID
	el := election.NewElection(cfg, election.ElectionDeps{
		Handler:                   agent,
		Host:                      host,
		Publisher:                 publisher,
		NanoClock:                 clock,
		EpochClock:                clock,
		NodeState:                 nodeState,
		Members:                   members,
		ThisMember:                thisMember,
		RecordingLog:              log,
		RecordingLogLookup:        log,
		RecordingLogFileSyncLevel: recordinglog.FileSyncData,
	})

	election.SetStateForTest(el, election.ElectionStateCanvass)
	el.OnNewLeadershipTerm(codecs.NewLeadershipTermFields{
		LogLeadershipTermID:     priorTerm,
		NextLeadershipTermID:    election.NullValue,
		NextTermBaseLogPosition: election.NullPosition,
		NextLogPosition:         election.NullPosition,
		LeadershipTermID:        termID,
		TermBaseLogPosition:     0,
		LogPosition:             tail,
		LeaderMemberID:          leaderID,
		LogSessionID:            1,
		CommitPosition:          tail,
	})
	election.SetStateForTest(el, election.ElectionStateFollowerCatchup)
	agent.SetElection(el)

	rig := newCatchupSnapshotRig(t)
	agent.SetSnapshotRecordingControl(rig)
	agent.SetSnapshotPublicationAdder(rig)
	agent.SetSnapshotRecordingLog(log, recordinglog.FileSyncData)
	agent.SetSnapshotStream(DefaultSnapshotChannel, DefaultSnapshotStreamID)

	requireTermRows := func() {
		t.Helper()
		term := log.FindTermEntry(termID)
		require.NotNil(t, term)
		require.Equal(t, recordinglog.EntryTypeTerm, term.Type)
		require.Equal(t, baseB, term.TermBaseLogPosition)
		require.Equal(t, recordinglog.NullPosition, term.LogPosition)
		require.Equal(t, catchupSnapshotRecordingID, term.RecordingID)
		prior := log.FindTermEntry(priorTerm)
		require.NotNil(t, prior)
		require.Equal(t, baseB, prior.LogPosition)
	}

	nowNs := catchupSnapshotTimeoutNs
	agent.DoWork(nowNs)
	require.Equal(t, election.ElectionStateFollowerCatchup, el.State())
	require.NoError(t, el.LastError())
	require.True(t, agent.HasPendingSnapshotAction())
	require.Equal(t, actionEnd, adapter.Position())
	requireTermRows()

	var (
		result   consensus.SnapshotTakeResult
		resolved bool
	)
	for tick := 0; tick < 4 && !resolved; tick++ {
		agent.DoWork(nowNs)
		result, resolved = agent.LastSnapshotTake()
	}
	require.True(t, resolved)
	require.NoError(t, result.Err)
	requireTermRows()

	var snapshots []recordinglog.Entry
	for _, entry := range log.Entries() {
		if entry.Valid && entry.Type == recordinglog.EntryTypeSnapshot {
			snapshots = append(snapshots, entry)
		}
	}
	require.Len(t, snapshots, 1)
	snap := snapshots[0]
	require.Equal(t, recordinglog.ConsensusModuleServiceID, snap.ServiceID)
	require.Equal(t, termID, snap.LeadershipTermID)
	require.Equal(t, baseB, snap.TermBaseLogPosition)
	require.Equal(t, actionEnd, snap.LogPosition)
	require.Equal(t, catchupSnapshotModuleID, snap.RecordingID)
	require.Equal(t, actionEnd, result.LogPosition)
	require.Equal(t, termID, result.LeadershipTermID)
}

func writeCatchupTermEvent(
	t *testing.T,
	image *aeron.Image,
	offset int64,
	termID int64,
	leaderID int32,
) int64 {
	t.Helper()
	end := offset + int64(logbuffer.AlignFragmentChunkLength(
		codecs.NewLeadershipTermEventEncodedLen(),
	))
	buf := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	wireEnd, ok := codecs.EncodeNewLeadershipTermEvent(
		buf,
		0,
		clusterwire.NewLeadershipTermEventSbeSchemaVersion,
		codecs.NewLeadershipTermEventFields{
			LeadershipTermID:    termID,
			LogPosition:         end,
			Timestamp:           9,
			TermBaseLogPosition: offset,
			LeaderMemberID:      leaderID,
			LogSessionID:        1,
			TimeUnit:            clusterwire.ClusterTimeUnit_NANOS,
			AppVersion:          1,
		},
	)
	require.True(t, ok)
	require.Equal(t, end, writeReplayFrame(t, image, offset, buf[:wireEnd]))
	return end
}

func writeCatchupSnapshotAction(
	t *testing.T,
	image *aeron.Image,
	offset int64,
	termID int64,
) int64 {
	t.Helper()
	end := offset + int64(logbuffer.AlignFragmentChunkLength(codecs.ClusterActionRequestEncodedLen()))
	buf := make([]byte, codecs.ClusterActionRequestEncodedLen())
	wireEnd, ok := codecs.EncodeClusterActionRequest(
		buf,
		0,
		clusterwire.ClusterActionRequestSbeSchemaVersion,
		codecs.ClusterActionRequestFields{
			LeadershipTermID: termID,
			LogPosition:      end,
			Timestamp:        11,
			Action:           clusterwire.ClusterAction_SNAPSHOT,
		},
	)
	require.True(t, ok)
	require.Equal(t, end, writeReplayFrame(t, image, offset, buf[:wireEnd]))
	return end
}

type catchupSnapshotRig struct {
	fakeSnapshotRecordingControl
	publication *catchupSnapshotPublication
}

func newCatchupSnapshotRig(tb testing.TB) *catchupSnapshotRig {
	tb.Helper()
	return &catchupSnapshotRig{
		publication: &catchupSnapshotPublication{
			inner: aeron.NewTestExclusivePublicationForHooks(tb),
		},
	}
}

func (r *catchupSnapshotRig) NewStartRecording(
	int32,
	string,
) consensus.SnapshotStartRequest {
	return catchupSnapshotStart{}
}

func (r *catchupSnapshotRig) StopPosition() (int64, bool) {
	if !r.publication.closed {
		return 0, false
	}
	return r.publication.stopPosition, true
}

func (r *catchupSnapshotRig) OfferAddExclusivePublication(int32, string) (int64, error) {
	return 1, nil
}

func (r *catchupSnapshotRig) PollAddExclusivePublication(
	int64,
) (consensus.SnapshotPublication, bool, error) {
	return r.publication, true, nil
}

func (r *catchupSnapshotRig) CancelPending(int64) {}

type catchupSnapshotStart struct{}

func (catchupSnapshotStart) TryOffer() (bool, error) { return true, nil }

func (catchupSnapshotStart) PollSignal() (bool, error) { return true, nil }

func (catchupSnapshotStart) RecordingID() int64 { return catchupSnapshotModuleID }

func (catchupSnapshotStart) Close() {}

// A closed ExclusivePublication returns Closed from Position.
type catchupSnapshotPublication struct {
	inner        *aeron.ExclusivePublication
	closed       bool
	stopPosition int64
}

func (p *catchupSnapshotPublication) Offer(buffer []byte) int64 {
	return p.inner.Offer(buffer)
}

func (p *catchupSnapshotPublication) Position() int64 {
	if p.closed {
		return p.stopPosition
	}
	return p.inner.Position()
}

func (p *catchupSnapshotPublication) SessionID() int32 { return p.inner.SessionID() }

func (p *catchupSnapshotPublication) Close() error {
	if !p.closed {
		p.stopPosition = p.inner.Position()
		p.closed = true
	}
	return p.inner.Close()
}

var (
	_ consensus.SnapshotRecordingControl = (*catchupSnapshotRig)(nil)
	_ consensus.SnapshotPublicationAdder = (*catchupSnapshotRig)(nil)
	_ consensus.SnapshotStartRequest     = catchupSnapshotStart{}
	_ consensus.SnapshotPublication      = (*catchupSnapshotPublication)(nil)
)

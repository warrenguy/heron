//go:build testhooks

package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/archive"
	archivecodecs "github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// takeLog writes committed log frames onto a hooks image in order.
type takeLog struct {
	image     *aeron.Image
	committed *int64
	offset    int
}

// write appends payload as the next frame. Moves the committed bound to the
// frame end. Returns the frame end.
func (l *takeLog) write(tb assertTB, payload []byte) int64 {
	tb.Helper()
	l.offset += writeAgentFrame(tb, l.image, l.offset, payload)
	*l.committed = int64(l.offset)
	return int64(l.offset)
}

// termEvent writes a NewLeadershipTermEvent.
func (l *takeLog) termEvent(
	tb testing.TB,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) int64 {
	tb.Helper()
	return l.write(tb, termEventPayload(tb, timeUnit, appVersion))
}

func termEventPayload(
	tb testing.TB,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) []byte {
	tb.Helper()
	return encodeNLT(tb, codecs.NewLeadershipTermEventFields{
		LeadershipTermID: 1,
		TimeUnit:         timeUnit,
		AppVersion:       appVersion,
	})
}

// sessionOpen writes a SessionOpenEvent for sessionID.
func (l *takeLog) sessionOpen(tb testing.TB, sessionID int64, channel, principal string) int64 {
	tb.Helper()
	return l.write(tb, sessionOpenPayload(tb, sessionID, channel, principal))
}

func sessionOpenPayload(tb testing.TB, sessionID int64, channel, principal string) []byte {
	tb.Helper()
	return encodeSessionOpen(tb, codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		ClusterSessionID: sessionID,
		ResponseStreamID: int32(sessionID) + 10,
		ResponseChannel:  []byte(channel),
		EncodedPrincipal: []byte(principal),
	})
}

// snapshot writes a SNAPSHOT action whose LogPosition is its frame end.
func (l *takeLog) snapshot(tb assertTB, timestamp int64) int64 {
	tb.Helper()
	end := int64(l.offset + clusterActionFrameSpan())
	got := l.write(tb, encodeClusterAction(tb, snapshotActionFields(end, timestamp)))
	require.Equal(tb, end, got, "encoded logPosition vs frame end")
	return end
}

func (l *takeLog) timer(tb assertTB, correlationID int64) int64 {
	tb.Helper()
	return l.write(tb, encodeTimerEvent(tb, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    correlationID,
		Timestamp:        correlationID,
	}))
}

// snapshotTermPayloads returns each frame payload in term 0 of publication
// and the end offset of the last frame.
func snapshotTermPayloads(
	tb testing.TB,
	publication *aeron.ExclusivePublication,
) ([][]byte, int64) {
	tb.Helper()
	termBuf := publication.TermBuffer(0)
	var payloads [][]byte
	off := 0
	for off < termBuf.Capacity() {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, off))
		if frameLength <= 0 {
			break
		}
		payload := termBuf.Bytes()[off+logbuffer.OffsetFramePayload : off+frameLength]
		payloads = append(payloads, append([]byte(nil), payload...))
		off += logbuffer.AlignFrameLength(frameLength)
	}
	return payloads, int64(off)
}

// takeHarness is one bound agent with the take fake and a bound proxy.
type takeHarness struct {
	agent *Agent
	fake  *SnapshotTakeFake
	pub   *aeron.Publication
	log   *takeLog
	svc   *snapshotRecordingService
}

func newTakeHarness(tb testing.TB, svc *snapshotRecordingService) *takeHarness {
	tb.Helper()
	proxy, pub := boundModuleProxy(tb)
	agent, image, committed := bindImage(tb, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	return &takeHarness{
		agent: agent,
		fake:  InstallSnapshotTakeFakeForTest(tb, agent),
		pub:   pub,
		log:   &takeLog{image: image, committed: committed},
		svc:   svc,
	}
}

// requireNoPreProofAck asserts that no sent ack carries the pre-proof id.
func (h *takeHarness) requireNoPreProofAck(tb testing.TB) {
	tb.Helper()
	for _, ack := range decodeServiceAcksFromPub(tb, h.pub) {
		require.NotEqual(tb, snapshotTakeFakePreProofID, ack.RelevantID, "pre-proof ack")
	}
}

// requireSnapshotTakeCleanedUp asserts that the take cleanup left nothing open.
func requireSnapshotTakeCleanedUp(
	tb assertTB,
	agent *Agent,
	fake *SnapshotTakeFake,
	wantCancelled []int64,
) {
	tb.Helper()
	require.Nil(tb, agent.take, "take after cleanup")
	require.Equal(tb, wantCancelled, fake.cancelled, "cancelled adds")
	if fake.startBuilds > 0 {
		require.True(tb, fake.startClosed, "start request closed")
	}
	require.Nil(tb, fake.handler, "session handler")
	for i, publication := range fake.publications {
		require.True(tb, publication.IsClosed(), "publication %d closed", i)
	}
}

func TestSnapshotTake_success(t *testing.T) {
	body := []byte("state")
	h := newTakeHarness(t, &snapshotRecordingService{write: body})
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 3)
	const timestamp int64 = 555
	end := h.log.snapshot(t, timestamp)

	// Tick 1: the action stores the take. No hook runs.
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.Empty(t, h.svc.pubs, "hook calls")
	require.Equal(t, 1, h.agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, end, h.agent.adapter.Position(), "poll position")
	require.Nil(t, h.fake.handler, "session handler before START")

	// Tick 2: the add, START, the rows, the hook, and the close. Each
	// completed take state counts one.
	work := h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 4, "tick 2 work")
	require.Equal(t, []int32{DefaultSnapshotStreamID}, h.fake.addStreamIDs)
	require.Equal(t, []string{DefaultSnapshotChannel}, h.fake.addChannels)
	snapPub := h.fake.LastPublication()
	require.NotNil(t, snapPub)
	require.Equal(t, []int32{DefaultSnapshotStreamID}, h.fake.startStreamIDs)
	require.Equal(t, []string{"aeron:ipc?alias=snapshot|session-id=1001"}, h.fake.startChannels)
	require.Equal(t, int32(1001), snapPub.SessionID())
	require.Equal(t, 1, h.fake.startCloses, "start request closes")
	require.IsType(t, &archive.StopSignalTracker{}, h.fake.handler, "session handler after START")
	require.Equal(t, []*aeron.ExclusivePublication{snapPub}, h.svc.pubs, "hook calls")
	require.True(t, snapPub.IsClosed(), "publication closed")
	require.Equal(t, 0, h.agent.ReadyCoordinationAcks(), "ready acks before STOP")
	require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "acks before STOP")

	// Tick 3: STOP at the close position. The ack carries the recording id.
	work = h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.GreaterOrEqual(t, work, 3, "tick 3 work")
	payloads, written := snapshotTermPayloads(t, snapPub)
	require.Len(t, payloads, 3, "BEGIN, END, handler bytes")
	require.Equal(t, []int64{written}, h.fake.closePositions, "close position")
	recordingID := h.fake.RecordingID()
	require.NotEqual(t, snapshotTakeFakePreProofID, recordingID)
	require.Equal(t, []codecs.ServiceAckFields{{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  recordingID,
		ServiceID:   1,
	}}, decodeServiceAcksFromPub(t, h.pub))
	require.Equal(t, recordingID, h.agent.LastSnapshotRecordingID())
	require.Nil(t, h.fake.handler, "session handler after the ack")
	require.Nil(t, h.agent.take, "take after the ack")
	require.Equal(t, 0, h.agent.PendingCoordinationAcks(), "pending acks")
	h.requireNoPreProofAck(t)
}

// decodeSnapshotRows splits payloads into BEGIN, the session rows, END, and
// the payloads after END.
func decodeSnapshotRows(
	tb testing.TB,
	payloads [][]byte,
) (
	codecs.SnapshotMarkerFields,
	[]codecs.ClientSessionFields,
	codecs.SnapshotMarkerFields,
	[][]byte,
) {
	tb.Helper()
	require.GreaterOrEqual(tb, len(payloads), 2, "rows")
	begin, ok := codecs.TryDecodeSnapshotMarkerAt(payloads[0], 0)
	require.True(tb, ok, "BEGIN decode")
	require.Equal(tb, clusterwire.SnapshotMark_BEGIN, begin.Mark)
	var sessions []codecs.ClientSessionFields
	i := 1
	for ; i < len(payloads); i++ {
		session, ok := codecs.TryDecodeClientSessionAt(payloads[i], 0)
		if !ok {
			break
		}
		sessions = append(sessions, session)
	}
	require.Less(tb, i, len(payloads), "END row")
	end, ok := codecs.TryDecodeSnapshotMarkerAt(payloads[i], 0)
	require.True(tb, ok, "END decode")
	require.Equal(tb, clusterwire.SnapshotMark_END, end.Mark)
	return begin, sessions, end, payloads[i+1:]
}

// runTake ticks at nowNs 0 until the take resolves. Returns the tick count.
func (h *takeHarness) runTake(tb testing.TB) int {
	tb.Helper()
	for tick := 1; tick <= 8; tick++ {
		h.agent.DoWork(0)
		require.NoError(tb, h.agent.ServiceFailure(), "tick %d", tick)
		if h.agent.take == nil && h.agent.ReadyCoordinationAcks() == 0 {
			return tick
		}
	}
	tb.Fatalf("take did not resolve")
	return 0
}

func TestSnapshotTake_recordedBytes(t *testing.T) {
	tests := []struct {
		name string
		svc  *snapshotRecordingService
		want []byte
	}{
		{
			name: "handler",
			svc:  &snapshotRecordingService{write: []byte("user-state")},
			want: []byte("user-state"),
		},
		{name: "noHandlerBytes", svc: &snapshotRecordingService{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTakeHarness(t, tt.svc)
			h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 3)
			h.log.sessionOpen(t, 9, "aeron:udp?endpoint=c:9", "p9")
			h.log.sessionOpen(t, 2, "aeron:udp?endpoint=c:2", "")
			h.log.sessionOpen(t, 5, "aeron:ipc", "p5")
			end := h.log.snapshot(t, 70)

			require.Equal(t, 3, h.runTake(t), "ticks")
			payloads, _ := snapshotTermPayloads(t, h.fake.LastPublication())
			begin, sessions, last, rest := decodeSnapshotRows(t, payloads)
			want := codecs.SnapshotMarkerFields{
				TypeID:           codecs.ServiceSnapshotTypeID,
				LogPosition:      end,
				LeadershipTermID: 1,
				Index:            0,
				Mark:             clusterwire.SnapshotMark_BEGIN,
				TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
				AppVersion:       3,
			}
			require.Equal(t, want, begin)
			want.Mark = clusterwire.SnapshotMark_END
			require.Equal(t, want, last)
			require.Equal(t, []codecs.ClientSessionFields{
				{ClusterSessionID: 2, ResponseStreamID: 12, ResponseChannel: []byte("aeron:udp?endpoint=c:2")},
				{
					ClusterSessionID: 5,
					ResponseStreamID: 15,
					ResponseChannel:  []byte("aeron:ipc"),
					EncodedPrincipal: []byte("p5"),
				},
				{
					ClusterSessionID: 9,
					ResponseStreamID: 19,
					ResponseChannel:  []byte("aeron:udp?endpoint=c:9"),
					EncodedPrincipal: []byte("p9"),
				},
			}, sessions)
			if tt.want == nil {
				require.Empty(t, rest, "handler bytes")
			} else {
				require.Equal(t, [][]byte{tt.want}, rest, "handler bytes")
			}
			acks := decodeServiceAcksFromPub(t, h.pub)
			require.Len(t, acks, 1)
			require.Equal(t, h.fake.RecordingID(), acks[0].RelevantID)
			h.requireNoPreProofAck(t)
		})
	}
}

func TestSnapshotTake_noHandler(t *testing.T) {
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     &recordingService{},
		ModuleProxy: proxy,
	}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)
	log := &takeLog{image: image, committed: committed}
	log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	log.sessionOpen(t, 4, "aeron:ipc", "")
	end := log.snapshot(t, 1)

	for range 3 {
		agent.DoWork(0)
	}
	require.NoError(t, agent.ServiceFailure())
	payloads, _ := snapshotTermPayloads(t, fake.LastPublication())
	_, sessions, _, rest := decodeSnapshotRows(t, payloads)
	require.Len(t, sessions, 1)
	require.Empty(t, rest, "bytes after END")
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, end, acks[0].LogPosition)
	require.Equal(t, fake.RecordingID(), acks[0].RelevantID)
	require.NotEqual(t, snapshotTakeFakePreProofID, acks[0].RelevantID, "pre-proof ack")
}

func TestSnapshotTake_okAloneDoesNotComplete(t *testing.T) {
	h := newTakeHarness(t, &snapshotRecordingService{})
	for range 6 {
		h.fake.startSignals = append(h.fake.startSignals, snapshotTakeFakeStep{})
	}
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	h.log.snapshot(t, 1)

	for tick := range 6 {
		h.agent.DoWork(0)
		require.NoError(t, h.agent.ServiceFailure(), "tick %d", tick)
	}
	require.NotNil(t, h.agent.take)
	require.Equal(t, takeAwaitingStart, h.agent.take.state)
	require.Equal(t, 0, h.fake.startCloses, "start request closes")
	require.Nil(t, h.fake.handler, "session handler")
	require.Empty(t, h.svc.pubs, "hook calls")
	require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "acks")
}

func TestSnapshotTake_backPressureClears(t *testing.T) {
	h := newTakeHarness(t, &snapshotRecordingService{write: []byte("b")})
	h.fake.rowOffers = []int64{0, aeron.BackPressured, aeron.AdminAction}
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	h.log.sessionOpen(t, 1, "aeron:ipc", "")
	h.log.snapshot(t, 1)

	// Tick 1 consumes. Tick 2 writes BEGIN, then waits at the session row.
	h.agent.DoWork(0)
	h.agent.DoWork(0)
	require.Equal(t, takeWriting, h.agent.take.state)
	require.Equal(t, 1, h.agent.take.nextRow)

	// Tick 3: AdminAction waits at the same row.
	h.agent.DoWork(0)
	require.Equal(t, 1, h.agent.take.nextRow)
	require.Empty(t, h.svc.pubs, "hook calls")

	// Tick 4 writes the rest. Tick 5 acks.
	h.agent.DoWork(0)
	require.Equal(t, takeAwaitingStop, h.agent.take.state)
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	payloads, _ := snapshotTermPayloads(t, h.fake.LastPublication())
	_, sessions, _, rest := decodeSnapshotRows(t, payloads)
	require.Len(t, sessions, 1)
	require.Equal(t, [][]byte{[]byte("b")}, rest)
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, h.fake.RecordingID(), acks[0].RelevantID)
	h.requireNoPreProofAck(t)
}

func TestSnapshotTake_holdsLogUntilResolved(t *testing.T) {
	h := newTakeHarness(t, &snapshotRecordingService{})
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	end := h.log.snapshot(t, 1)
	h.log.timer(t, 11)
	last := h.log.timer(t, 12)

	for tick := 1; tick <= 2; tick++ {
		h.agent.DoWork(0)
		require.Empty(t, h.svc.timers, "tick %d timers", tick)
		require.Equal(t, end, h.agent.LogPosition(), "tick %d log position", tick)
		require.Equal(t, end, h.agent.adapter.Position(), "tick %d poll position", tick)
	}

	// Tick 3: the take resolves. The log hold lifts.
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, h.fake.RecordingID(), acks[0].RelevantID)
	require.Len(t, h.svc.timers, 2, "timers after the take")
	require.Equal(t, last, h.agent.LogPosition())
	h.requireNoPreProofAck(t)
}

func TestSnapshotTake_repeatTake(t *testing.T) {
	h := newTakeHarness(t, &snapshotRecordingService{})
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	first := h.log.snapshot(t, 1)
	h.runTake(t)
	firstID := h.fake.RecordingID()
	firstPub := h.fake.LastPublication()

	second := h.log.snapshot(t, 2)
	h.runTake(t)
	secondID := h.fake.RecordingID()

	require.Len(t, h.fake.publications, 2)
	require.NotSame(t, firstPub, h.fake.LastPublication())
	require.NotEqual(t, firstID, secondID)
	require.Equal(t, []codecs.ServiceAckFields{
		{LogPosition: first, Timestamp: 1, AckID: 0, RelevantID: firstID, ServiceID: 1},
		{LogPosition: second, Timestamp: 2, AckID: 1, RelevantID: secondID, ServiceID: 1},
	}, decodeServiceAcksFromPub(t, h.pub))
	require.Equal(t, secondID, h.agent.LastSnapshotRecordingID())
	h.requireNoPreProofAck(t)
}

func TestSnapshotTake_foreignStopDoesNotComplete(t *testing.T) {
	h := newTakeHarness(t, &snapshotRecordingService{})
	h.fake.stopNever = true
	h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
	h.log.snapshot(t, 1)
	h.agent.DoWork(0)
	h.agent.DoWork(0)
	require.Equal(t, takeAwaitingStop, h.agent.take.state)

	recordingID := h.fake.RecordingID()
	h.fake.pending = []archivecodecs.RecordingSignalEventFields{
		{
			ControlSessionID: snapshotTakeFakeControlSessionID,
			RecordingID:      recordingID + 1,
			Position:         1 << 20,
			Signal:           archivewire.RecordingSignal_STOP,
		},
		{
			ControlSessionID: snapshotTakeFakeControlSessionID + 1,
			RecordingID:      recordingID,
			Position:         1 << 20,
			Signal:           archivewire.RecordingSignal_STOP,
		},
	}
	h.agent.DoWork(0)
	require.NoError(t, h.agent.ServiceFailure())
	require.NotNil(t, h.agent.take)
	require.Equal(t, takeAwaitingStop, h.agent.take.state)
	require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "acks after foreign STOP")

	h.fake.stopNever = false
	h.agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, h.pub)
	require.Len(t, acks, 1)
	require.Equal(t, recordingID, acks[0].RelevantID)
	h.requireNoPreProofAck(t)
}

func TestSnapshotTake_brokenState(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*SnapshotTakeFake)
		state   snapshotTakeState
		corrupt func(*Agent)
	}{
		{
			name: "unknownState",
			setup: func(f *SnapshotTakeFake) {
				f.startSignals = []snapshotTakeFakeStep{{}}
			},
			state:   takeAwaitingStart,
			corrupt: func(a *Agent) { a.take.state = snapshotTakeState(99) },
		},
		{
			name: "sessionMissing",
			setup: func(f *SnapshotTakeFake) {
				f.rowOffers = []int64{aeron.BackPressured}
			},
			state:   takeWriting,
			corrupt: func(a *Agent) { delete(a.sessions, 7) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTakeHarness(t, &snapshotRecordingService{})
			tt.setup(h.fake)
			h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
			h.log.sessionOpen(t, 7, "aeron:ipc", "p")
			end := h.log.snapshot(t, 1)
			h.agent.DoWork(0)
			h.agent.DoWork(0)
			require.NotNil(t, h.agent.take)
			require.Equal(t, tt.state, h.agent.take.state)
			require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "acks before mark")

			tt.corrupt(h.agent)
			h.agent.DoWork(0)
			require.ErrorIs(t, h.agent.ServiceFailure(), errSnapshotWrite)
			acks := decodeServiceAcksFromPub(t, h.pub)
			require.Len(t, acks, 1)
			require.Equal(t, end, acks[0].LogPosition)
			require.Equal(t, nullValue, acks[0].RelevantID)
			require.Equal(t, nullValue, h.agent.LastSnapshotRecordingID())
			requireSnapshotTakeCleanedUp(t, h.agent, h.fake, nil)
		})
	}
}

func TestSnapshotTake_closeDuringTake(t *testing.T) {
	timeoutErr := &aeron.RegistrationTimeoutError{Kind: "publication", CorrelationID: 1}
	tests := []struct {
		name          string
		setup         func(*SnapshotTakeFake)
		state         snapshotTakeState
		wantCancelled []int64
	}{
		{
			name: "addingPublicationInFlight",
			setup: func(f *SnapshotTakeFake) {
				f.addPolls = []snapshotTakeFakeAddPoll{{}, {}}
			},
			state:         takeAddingPublication,
			wantCancelled: []int64{1},
		},
		{
			// A conductor tick outside the take read the ready reply.
			name: "addingPublicationReady",
			setup: func(f *SnapshotTakeFake) {
				f.addPolls = []snapshotTakeFakeAddPoll{{}, {done: true}}
			},
			state: takeAddingPublication,
		},
		{
			name: "addingPublicationTerminal",
			setup: func(f *SnapshotTakeFake) {
				f.addPolls = []snapshotTakeFakeAddPoll{{}, {done: true, err: timeoutErr}}
			},
			state:         takeAddingPublication,
			wantCancelled: []int64{1},
		},
		{
			name: "addingPublicationVanished",
			setup: func(f *SnapshotTakeFake) {
				f.addPolls = []snapshotTakeFakeAddPoll{{}, {err: aeron.ErrAwaitInterrupted}}
			},
			state:         takeAddingPublication,
			wantCancelled: []int64{1},
		},
		{
			name: "offeringStart",
			setup: func(f *SnapshotTakeFake) {
				f.startOffers = []snapshotTakeFakeStep{{}}
			},
			state: takeOfferingStart,
		},
		{
			name: "awaitingStart",
			setup: func(f *SnapshotTakeFake) {
				f.startSignals = []snapshotTakeFakeStep{{}}
			},
			state: takeAwaitingStart,
		},
		{
			name: "writing",
			setup: func(f *SnapshotTakeFake) {
				f.rowOffers = []int64{aeron.BackPressured}
			},
			state: takeWriting,
		},
		{
			name:  "awaitingStop",
			setup: func(f *SnapshotTakeFake) { f.stopNever = true },
			state: takeAwaitingStop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTakeHarness(t, &snapshotRecordingService{})
			tt.setup(h.fake)
			h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
			h.log.snapshot(t, 1)
			h.agent.DoWork(0)
			h.agent.DoWork(0)
			require.NotNil(t, h.agent.take)
			require.Equal(t, tt.state, h.agent.take.state)

			h.agent.Close()
			requireSnapshotTakeCleanedUp(t, h.agent, h.fake, tt.wantCancelled)
			require.Empty(t, h.fake.addPolls, "unread add polls")
			require.Equal(t, 0, h.agent.DoWork(0), "work after Close")
			require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "acks after Close")
			require.NoError(t, h.agent.ServiceFailure())
		})
	}
}

func TestSnapshotTake_termEventFields(t *testing.T) {
	type termEvent struct {
		timeUnit   clusterwire.ClusterTimeUnit
		appVersion int32
	}
	tests := []struct {
		name        string
		load        *termEvent
		events      []termEvent
		wantUnit    clusterwire.ClusterTimeUnit
		wantVersion int32
	}{
		{
			name:        "nanos",
			events:      []termEvent{{clusterwire.ClusterTimeUnit_NANOS, 42}},
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 42,
		},
		{
			name:        "micros",
			events:      []termEvent{{clusterwire.ClusterTimeUnit_MICROS, 1}},
			wantUnit:    clusterwire.ClusterTimeUnit_MICROS,
			wantVersion: 1,
		},
		{
			name:        "nullStoresMillis",
			events:      []termEvent{{clusterwire.ClusterTimeUnit_NULL_VALUE, 7}},
			wantUnit:    clusterwire.ClusterTimeUnit_MILLIS,
			wantVersion: 7,
		},
		{
			name: "lastEventWins",
			events: []termEvent{
				{clusterwire.ClusterTimeUnit_MICROS, 1},
				{clusterwire.ClusterTimeUnit_NANOS, 9},
			},
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 9,
		},
		{
			name: "lastNullOverwritesKnownUnit",
			events: []termEvent{
				{clusterwire.ClusterTimeUnit_NANOS, 3},
				{clusterwire.ClusterTimeUnit_NULL_VALUE, 4},
			},
			wantUnit:    clusterwire.ClusterTimeUnit_MILLIS,
			wantVersion: 4,
		},
		{
			name:        "loadWithoutTermEvent",
			load:        &termEvent{clusterwire.ClusterTimeUnit_MICROS, 4},
			wantUnit:    clusterwire.ClusterTimeUnit_MICROS,
			wantVersion: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTakeHarness(t, &snapshotRecordingService{})
			if tt.load != nil {
				loadSnapshotMarkers(t, h.agent, tt.load.timeUnit, tt.load.appVersion)
			}
			for _, event := range tt.events {
				h.log.termEvent(t, event.timeUnit, event.appVersion)
			}
			h.log.snapshot(t, 1)
			h.runTake(t)
			require.Equal(t, tt.wantUnit, h.agent.timeUnit)
			payloads, _ := snapshotTermPayloads(t, h.fake.LastPublication())
			begin, _, last, _ := decodeSnapshotRows(t, payloads)
			for _, marker := range []codecs.SnapshotMarkerFields{begin, last} {
				require.Equal(t, tt.wantUnit, marker.TimeUnit)
				require.Equal(t, tt.wantVersion, marker.AppVersion)
			}
			h.requireNoPreProofAck(t)
		})
	}
}

// takeFailureTickNs is the clock step between ticks in TestSnapshotTake_failures.
// The test sets both archive timeouts to one step.
const takeFailureTickNs int64 = 1_000

func TestSnapshotTake_failures(t *testing.T) {
	addErr := errors.New("add boom")
	startErr := errors.New("start boom")
	hookErr := errors.New("hook boom")
	timeoutErr := &aeron.RegistrationTimeoutError{Kind: "publication", CorrelationID: 1}
	tests := []struct {
		name          string
		svc           *snapshotRecordingService
		noTermEvent   bool
		setup         func(testing.TB, *takeHarness)
		cause         error
		wrapped       error
		wantCancelled []int64
	}{
		{
			name:  "noPublicationAdder",
			setup: func(_ testing.TB, h *takeHarness) { h.agent.snapshotPublications = nil },
			cause: errSnapshotNoArchive,
		},
		{
			name:        "noTimeUnit",
			noTermEvent: true,
			cause:       errSnapshotTimeUnitUnknown,
		},
		{
			name:    "addOfferError",
			setup:   func(_ testing.TB, h *takeHarness) { h.fake.addOffers = []error{addErr} },
			cause:   errSnapshotPublicationAdd,
			wrapped: addErr,
		},
		{
			name: "addPollError",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.addPolls = []snapshotTakeFakeAddPoll{{err: addErr}, {err: addErr}}
			},
			cause:         errSnapshotPublicationAdd,
			wrapped:       addErr,
			wantCancelled: []int64{1},
		},
		{
			// The real client repeats a not-done poll error. The ready cleanup
			// poll here checks the close branch after a failure.
			name: "addPollErrorReadyAtCleanup",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.addPolls = []snapshotTakeFakeAddPoll{{err: addErr}, {done: true}}
			},
			cause:   errSnapshotPublicationAdd,
			wrapped: addErr,
		},
		{
			name: "addPollTimeout",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.addPolls = []snapshotTakeFakeAddPoll{{
					done: true,
					err:  timeoutErr,
				}}
			},
			cause:   errSnapshotPublicationAdd,
			wrapped: timeoutErr,
		},
		{
			name: "terminalStartOffer",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.startOffers = []snapshotTakeFakeStep{{err: startErr}}
			},
			cause:   errSnapshotRecordingStart,
			wrapped: startErr,
		},
		{
			name: "startOfferDeadline",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.startOffers = make([]snapshotTakeFakeStep, 8)
			},
			cause: errSnapshotRecordingStart,
		},
		{
			name: "archiveErrorBeforeStart",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.startSignals = []snapshotTakeFakeStep{{done: true, err: startErr}}
			},
			cause:   errSnapshotRecordingStart,
			wrapped: startErr,
		},
		{
			name: "startDeadline",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.startSignals = make([]snapshotTakeFakeStep, 8)
			},
			cause: errSnapshotRecordingStart,
		},
		{
			name: "terminalRowOffer",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.rowOffers = []int64{aeron.MaxPositionExceeded}
			},
			cause: errSnapshotWrite,
		},
		{
			name: "rowDeadlineUnderBackPressure",
			setup: func(tb testing.TB, h *takeHarness) {
				h.agent.snapshotRowOffer = nil
				h.fake.addPolls = []snapshotTakeFakeAddPoll{{
					done:        true,
					publication: aeron.NewTestExclusivePublicationForHooksWithLimit(tb, 0, true),
				}}
			},
			cause: errSnapshotWrite,
		},
		{
			name: "earlyStopWhileWriting",
			setup: func(_ testing.TB, h *takeHarness) {
				h.fake.stopEarly = true
				h.fake.rowOffers = []int64{aeron.BackPressured}
			},
			cause: errSnapshotWrite,
		},
		{
			name:    "handlerError",
			svc:     &snapshotRecordingService{hookErr: hookErr},
			cause:   errTakeSnapshot,
			wrapped: hookErr,
		},
		{
			name:  "handlerClosesPublication",
			svc:   &snapshotRecordingService{closePublication: true},
			cause: errSnapshotWrite,
		},
		{
			name:  "stopBelowClosePosition",
			setup: func(_ testing.TB, h *takeHarness) { h.fake.stopDelta = -1 },
			cause: errSnapshotRecordingStop,
		},
		{
			name:  "stopDeadline",
			setup: func(_ testing.TB, h *takeHarness) { h.fake.stopNever = true },
			cause: errSnapshotRecordingStop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := tt.svc
			if svc == nil {
				svc = &snapshotRecordingService{}
			}
			h := newTakeHarness(t, svc)
			h.fake.offerRetryTimeout = time.Duration(takeFailureTickNs)
			h.fake.responseTimeout = time.Duration(takeFailureTickNs)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			if !tt.noTermEvent {
				h.log.termEvent(t, clusterwire.ClusterTimeUnit_NANOS, 0)
			}
			end := h.log.snapshot(t, 9)

			for tick := range int64(10) {
				h.agent.DoWork(tick * takeFailureTickNs)
				if h.agent.ServiceFailure() != nil {
					break
				}
				require.Empty(t, decodeServiceAcksFromPub(t, h.pub), "tick %d acks before mark", tick)
			}
			failure := h.agent.ServiceFailure()
			require.ErrorIs(t, failure, tt.cause)
			if tt.wrapped != nil {
				require.ErrorIs(t, failure, tt.wrapped)
			}
			require.Equal(t, []codecs.ServiceAckFields{{
				LogPosition: end,
				Timestamp:   9,
				AckID:       0,
				RelevantID:  nullValue,
				ServiceID:   1,
			}}, decodeServiceAcksFromPub(t, h.pub))
			require.Equal(t, nullValue, h.agent.LastSnapshotRecordingID())

			requireSnapshotTakeCleanedUp(t, h.agent, h.fake, tt.wantCancelled)
			require.Empty(t, h.fake.addPolls, "unread add polls")
			if tt.noTermEvent {
				require.Empty(t, h.fake.addStreamIDs, "publication adds")
			}
		})
	}
}

// loadSnapshotMarkers loads a snapshot of one BEGIN and one END marker into
// agent. Both markers carry timeUnit and appVersion.
func loadSnapshotMarkers(
	t *testing.T,
	agent *Agent,
	timeUnit clusterwire.ClusterTimeUnit,
	appVersion int32,
) {
	t.Helper()
	loaded := serviceMarkerFields(0, clusterwire.SnapshotMark_BEGIN)
	loaded.TimeUnit = timeUnit
	loaded.AppVersion = appVersion
	loader := newServiceSnapshotLoader(agent, 0)
	require.Equal(t, aeron.ControlledContinue, loader.dispatch(encodeSnapshotMarkerFrame(t, loaded)))
	loaded.Mark = clusterwire.SnapshotMark_END
	require.Equal(t, aeron.ControlledBreak, loader.dispatch(encodeSnapshotMarkerFrame(t, loaded)))
	require.NoError(t, loader.err)
}

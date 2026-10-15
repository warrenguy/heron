//go:build testhooks

package cluster

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	snapshotWireLeadershipTermID int64 = 3
	snapshotWireSessionID        int64 = 1
	snapshotWireNowNs            int64 = 5000
	snapshotWireNowNsSecond      int64 = 6000
)

// snapshotWireService embeds wireRecordingService and records each
// TakeSnapshot publication.
type snapshotWireService struct {
	wireRecordingService
	pubs []*aeron.ExclusivePublication
}

func (s *snapshotWireService) TakeSnapshot(
	publication *aeron.ExclusivePublication,
) error {
	s.pubs = append(s.pubs, publication)
	return nil
}

// snapshotWireHarness is an in-process consensus-module-plus-service snapshot
// action path. The harness bridges stream 104, stream 105, and the cluster
// log through test publications on one goroutine.
type snapshotWireHarness struct {
	wireServiceSide

	cm      *consensus.Agent
	logPub  *consensus.LogPublisher
	logTerm *aeron.ExclusivePublication
	offLog  int

	pub104 *aeron.Publication
	off104 int

	// dropRequest skips RequestServiceAck frames on the 104 bridge so the
	// join wait pairs with the join ServiceAck only.
	dropRequest bool

	rec *snapshotWireService
}

func newSnapshotWireHarness(t testing.TB) *snapshotWireHarness {
	t.Helper()

	logPub, logTerm := boundTimerLogPublisher(t)
	commit := &atomic.Int64{}
	cm := consensus.NewAgent(consensus.AgentConfig{
		Role:             consensus.RoleLeader,
		LogPublisher:     logPub,
		LeadershipTermID: snapshotWireLeadershipTermID,
		ServiceCount:     1,
		Commit:           commit,
	})

	pub104 := aeron.NewTestPublicationForHooksWithConnected(t, true)
	proxy104 := consensus.NewServiceProxy()
	proxy104.SetPublication(pub104)
	cm.SetServiceProxy(proxy104)

	rec := &snapshotWireService{}
	return &snapshotWireHarness{
		wireServiceSide: newWireServiceSide(t, cm, rec),
		cm:              cm,
		logPub:          logPub,
		logTerm:         logTerm,
		pub104:          pub104,
		dropRequest:     true,
		rec:             rec,
	}
}

// tick runs consensus module DoWork, service DoWork, then the bridges.
// After the bridges, tick asserts LogPosition is at or below CommitPosition.
func (h *snapshotWireHarness) tick(t testing.TB, nowNs int64) {
	t.Helper()
	h.cm.DoWork(nowNs)
	h.svc.DoWork(nowNs)
	h.bridge104(t)
	h.bridge105(t)
	h.bridgeLog(t)
	require.LessOrEqual(t, h.svc.LogPosition(), h.cm.CommitPosition())
}

func (h *snapshotWireHarness) tickN(t testing.TB, baseNs int64, n int) {
	t.Helper()
	for i := range n {
		h.tick(t, baseNs+int64(i))
	}
}

// requireTickUntil runs one duty-cycle pair until done returns true. After
// eight calls without done, requireTickUntil fatals with harness state.
func (h *snapshotWireHarness) requireTickUntil(
	t testing.TB,
	nowNs int64,
	done func() bool,
	msg string,
) {
	t.Helper()
	for range 8 {
		h.tick(t, nowNs)
		if done() {
			return
		}
	}
	t.Fatalf(
		"%s: bound=%v waits=%d expectedAckID=%d hookCalls=%d messages=%d "+
			"ackFailure=%v svcFailure=%v logPos=%d commit=%d 105pos=%d",
		msg,
		h.svc.IsLogBound(),
		h.cm.PendingServiceAckWaits(),
		h.cm.ExpectedServiceAckID(),
		len(h.rec.pubs),
		h.rec.messages,
		h.cm.ServiceAckFailure(),
		h.svc.ServiceFailure(),
		h.svc.LogPosition(),
		h.cm.CommitPosition(),
		h.pub105.Position(),
	)
}

func (h *snapshotWireHarness) bridge104(t testing.TB) {
	t.Helper()
	bridgePub(t, h.pub104, &h.off104, func(msg []byte) {
		if h.dropRequest {
			_, templateID, _, _, ok := codecs.DecodeMessageHeader(msg, 0)
			if ok && templateID == clusterwire.RequestServiceAckSbeTemplateID {
				return
			}
		}
		h.control.Enqueue(msg)
	})
}

// bridgeLog copies new DATA frames from the consensus module log publication
// onto the service image and advances the committed bound to the image end.
func (h *snapshotWireHarness) bridgeLog(t testing.TB) {
	t.Helper()
	bridgeTerm(t, h.logTerm.TermBuffer(0), &h.offLog, func(msg []byte) {
		h.writeFrame(t, msg)
	})
	h.cm.ProposeMaxCommit(int64(h.frameOffset))
}

// bindJoin delivers a crafted JoinLog and pairs one RequestServiceAck wait at
// position 0. The join ServiceAck is consumed before RequestSnapshot.
func (h *snapshotWireHarness) bindJoin(t testing.TB) {
	t.Helper()
	h.dropRequest = true
	h.deliverJoinLog(t, 0, codecs.RoleLeader)
	require.NoError(t, h.cm.RequestServiceAck(0))
	require.Equal(t, 1, h.cm.PendingServiceAckWaits())
	require.Equal(t, int64(0), h.cm.ExpectedAckPosition())

	h.requireTickUntil(
		t,
		0,
		func() bool {
			return h.svc.IsLogBound() &&
				h.cm.ExpectedServiceAckID() == 1 &&
				h.cm.PendingServiceAckWaits() == 0
		},
		"join did not bind and complete",
	)
	h.dropRequest = false
	require.NoError(t, h.svc.ServiceFailure())
	require.NoError(t, h.cm.ServiceAckFailure())
	require.False(t, h.cm.HasPendingSnapshotAction())
	h.requireAckAt(t, h.decodeServiceAcks(t), codecs.ServiceAckFields{
		LogPosition: 0,
		Timestamp:   0,
		AckID:       0,
		RelevantID:  -1,
		ServiceID:   0,
	})
}

func (h *snapshotWireHarness) writeSessionMessage(t testing.TB, payload string) {
	t.Helper()
	pos := h.logPub.AppendMessage(
		snapshotWireLeadershipTermID,
		snapshotWireSessionID,
		1,
		[]byte(payload),
	)
	require.Positive(t, pos)
}

func (h *snapshotWireHarness) decodeServiceAcks(t testing.TB) []codecs.ServiceAckFields {
	t.Helper()
	var acks []codecs.ServiceAckFields
	off := 0
	bridgeTerm(t, h.pub105.TermBuffer(0), &off, func(msg []byte) {
		got, ok := codecs.TryDecodeServiceAckAt(msg, 0)
		require.True(t, ok, "105 DATA must decode as ServiceAck")
		acks = append(acks, got)
	})
	return acks
}

func (h *snapshotWireHarness) requireAckAt(
	t testing.TB,
	acks []codecs.ServiceAckFields,
	want codecs.ServiceAckFields,
) {
	t.Helper()
	for _, ack := range acks {
		if ack.LogPosition == want.LogPosition && ack.AckID == want.AckID {
			require.Equal(t, want, ack)
			return
		}
	}
	t.Fatalf("no ServiceAck matching %+v in %v", want, acks)
}

// runSnapshotRound appends one snapshot action and ticks until the wait
// completes. Join must already be bound. The join ServiceAck must already
// be consumed.
func (h *snapshotWireHarness) runSnapshotRound(
	t testing.TB,
	nowNs int64,
	wantAckID int64,
	wantExpectedAckID int64,
	wantHookCalls int,
) {
	t.Helper()
	require.NoError(t, h.cm.RequestSnapshot(nowNs))
	require.True(t, h.cm.HasPendingSnapshotAction())
	require.Equal(t, 1, h.cm.PendingServiceAckWaits())
	actionPos := h.cm.ExpectedAckPosition()
	require.Positive(t, actionPos)

	h.requireTickUntil(
		t,
		nowNs,
		func() bool {
			return !h.cm.HasPendingSnapshotAction() &&
				h.cm.ExpectedServiceAckID() == wantExpectedAckID &&
				h.cm.PendingServiceAckWaits() == 0
		},
		"snapshot wait did not complete",
	)

	require.Len(t, h.rec.pubs, wantHookCalls)
	for i, pub := range h.rec.pubs {
		require.Nil(t, pub, "hook publication %d", i)
	}
	require.Equal(t, actionPos, h.svc.LogPosition())
	require.Equal(t, wantExpectedAckID, h.svc.NextAckID())
	require.NoError(t, h.cm.ServiceAckFailure())
	require.NoError(t, h.svc.ServiceFailure())

	h.requireAckAt(t, h.decodeServiceAcks(t), codecs.ServiceAckFields{
		LogPosition: actionPos,
		Timestamp:   nowNs,
		AckID:       wantAckID,
		RelevantID:  -1,
		ServiceID:   0,
	})
}

func TestSnapshotActionWire_RoundTrip(t *testing.T) {
	h := newSnapshotWireHarness(t)
	h.bindJoin(t)

	// Join consumed ack 0. Snapshot is ack 1. ExpectedServiceAckID is 2.
	h.runSnapshotRound(t, snapshotWireNowNs, 1, 2, 1)
}

func TestSnapshotActionWire_prepareThenJoin(t *testing.T) {
	h := newSnapshotWireHarness(t)
	require.NoError(t, h.cm.RequestSnapshot(snapshotWireNowNs))
	actionPos := h.cm.ExpectedAckPosition()
	require.Positive(t, actionPos)
	require.NotEqual(t, int64(0), actionPos)

	_, prepared, err := h.cm.PrepareForNewLeadership(0, 1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.False(t, h.cm.HasPendingSnapshotAction())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())

	h.bindJoin(t)
	require.NoError(t, h.cm.ServiceAckFailure())
	require.NoError(t, h.svc.ServiceFailure())

	h.tickN(t, snapshotWireNowNs, 4)
	require.NoError(t, h.cm.ServiceAckFailure())
	require.NoError(t, h.svc.ServiceFailure())
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.GreaterOrEqual(t, len(h.rec.pubs), 1)
	require.Equal(t, h.svc.NextAckID(), h.cm.ExpectedServiceAckID())
}

func TestSnapshotActionWire_NoPerMessageAck(t *testing.T) {
	h := newSnapshotWireHarness(t)
	h.bindJoin(t)
	h.runSnapshotRound(t, snapshotWireNowNs, 1, 2, 1)

	posBefore105 := h.pub105.Position()
	acksBefore := len(h.decodeServiceAcks(t))
	depthBefore := h.cm.ServiceAckQueueDepth(0)

	h.writeSessionMessage(t, "body")
	h.tickN(t, 200, 4)

	require.Equal(t, int64(h.frameOffset), h.svc.LogPosition())
	require.Equal(t, 1, h.rec.messages)
	require.Equal(t, posBefore105, h.pub105.Position())
	require.Len(t, h.decodeServiceAcks(t), acksBefore)
	require.Equal(t, depthBefore, h.cm.ServiceAckQueueDepth(0))
	require.Equal(t, 0, h.cm.PendingServiceAckWaits())
	require.Equal(t, int64(2), h.cm.ExpectedServiceAckID())
	require.Equal(t, int64(2), h.svc.NextAckID())
	require.Len(t, h.rec.pubs, 1)
	require.NoError(t, h.cm.ServiceAckFailure())
	require.NoError(t, h.svc.ServiceFailure())

	h.runSnapshotRound(t, snapshotWireNowNsSecond, 2, 3, 2)
}

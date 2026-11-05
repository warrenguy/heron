//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func writeAlignedSnapshot(
	tb testing.TB,
	image *aeron.Image,
	committed *int64,
	timestamp int64,
) int {
	tb.Helper()
	return writeAlignedAction(tb, image, committed, timestamp, clusterwire.ClusterAction_SNAPSHOT, 0)
}

func TestAgent_ClusterAction_snapshotHook(t *testing.T) {
	svc := &snapshotRecordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 555
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, ackKindSnapshot, agent.coordinationAcks[0].kind)
	require.Equal(t, end, agent.coordinationAcks[0].logPosition)

	// Tick 2: the hook runs on the fake publication. The take waits for STOP.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Len(t, svc.pubs, 1, "hook calls")
	require.Same(t, fake.LastPublication(), svc.pubs[0])
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack before STOP")

	// Tick 3: STOP resolves the take.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  fake.RecordingID(),
		ServiceID:   1,
	}, acks[0])
	require.NotEqual(t, snapshotTakeFakePreProofID, acks[0].RelevantID)
	require.Len(t, svc.pubs, 1, "hook calls after the ack")
}

func TestAgent_ClusterAction_noHandlerAcks(t *testing.T) {
	svc := &recordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 444
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.order, "callbacks")
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")

	// Tick 2: the rows record.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack before STOP")

	// Tick 3: STOP resolves the take.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  fake.RecordingID(),
		ServiceID:   1,
	}, acks[0])
}

func TestAgent_ClusterAction_minimalWrite(t *testing.T) {
	body := []byte("snap-bytes")
	svc := &snapshotRecordingService{write: body}
	agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
	fake := InstallSnapshotTakeFakeForTest(t, agent)

	end := writeTermEventAndSnapshot(t, image, committed, 1)
	agent.DoWork(0)
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Len(t, svc.pubs, 1, "hook calls")

	payloads, _ := snapshotTermPayloads(t, fake.LastPublication())
	begin, sessions, _, rest := decodeSnapshotRows(t, payloads)
	require.Equal(t, end, begin.LogPosition)
	require.Empty(t, sessions)
	require.Equal(t, [][]byte{body}, rest)
}

func TestAgent_ClusterAction_hookError(t *testing.T) {
	hookErr := errors.New("snap boom")
	svc := &snapshotRecordingService{hookErr: hookErr}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)
	InstallSnapshotTakeFakeForTest(t, agent)

	const timestamp int64 = 321
	end := writeTermEventAndSnapshot(t, image, committed, timestamp)

	// Tick 1: the action stores the take.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, end, agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")
	require.Empty(t, decodeServiceAcksFromPub(t, pub), "no ack on consume tick")

	// Tick 2: the hook fails. The -1 ack sends, then the mark follows.
	agent.DoWork(0)
	require.Len(t, svc.pubs, 1, "hook calls")
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: end,
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])
	require.ErrorIs(t, agent.ServiceFailure(), ErrTakeSnapshot)
	require.ErrorIs(t, agent.ServiceFailure(), hookErr)
	require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks after send")

	// Tick 3: the mark freezes the duty cycle.
	require.Equal(t, 0, agent.DoWork(0))
	require.Equal(t, end, agent.adapter.Position(), "poll position later tick")
	require.Len(t, decodeServiceAcksFromPub(t, pub), 1, "later tick acks")
	require.Len(t, svc.pubs, 1, "later tick hook calls")
}

func TestAgent_ClusterAction_hookErrorTerminalAck(t *testing.T) {
	hookErr := errors.New("snap boom")
	svc := &snapshotRecordingService{hookErr: hookErr}
	proxy, pub := boundModuleProxy(t)
	var got []error
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:    1,
		Service:      svc,
		ModuleProxy:  proxy,
		ErrorHandler: collectErrors(&got),
	}, 0)
	InstallSnapshotTakeFakeForTest(t, agent)
	require.NoError(t, pub.Close())

	end := writeTermEventAndSnapshot(t, image, committed, 321)

	// Tick 1: no mark, one unready entry.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Equal(t, 0, agent.ReadyCoordinationAcks(), "ready acks")
	require.Equal(t, end, agent.adapter.Position(), "poll position after break")

	// Tick 2: the hook fails. A terminal offer marks ErrAckPublish with the
	// offer code, then the take cause. The ack does not send.
	agent.DoWork(0)
	require.Len(t, svc.pubs, 1, "hook calls")
	requireFailureReported(t, agent, got, ErrAckPublish, ErrTakeSnapshot, hookErr)
	want := fmt.Sprintf("%v: %d: %v", ErrAckPublish, aeron.Closed, ErrTakeSnapshot)
	require.ErrorContains(t, agent.ServiceFailure(), want)
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, int64(0), agent.NextAckID())

	// Tick 3: the mark freezes the duty cycle.
	require.Equal(t, 0, agent.DoWork(0))
}

type disconnectedTake struct {
	agent     *Agent
	image     *aeron.Image
	committed *int64
	svc       *snapshotRecordingService
	proxy     *ConsensusModuleProxy
}

func bindDisconnectedTake(t *testing.T, hookErr error, handler func(error)) disconnectedTake {
	t.Helper()
	svc := &snapshotRecordingService{hookErr: hookErr}

	// Limit 0 + disconnected: Offer returns NotConnected.
	proxy := boundModuleProxyWithLimit(t, 0, false)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:    1,
		Service:      svc,
		ModuleProxy:  proxy,
		ErrorHandler: handler,
	}, 0)
	InstallSnapshotTakeFakeForTest(t, agent)
	return disconnectedTake{
		agent:     agent,
		image:     image,
		committed: committed,
		svc:       svc,
		proxy:     proxy,
	}
}

func requireRetriableHold(t *testing.T, agent *Agent, got []error) {
	t.Helper()
	for tick := range 4 {
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure(), "tick %d", tick)
		require.Empty(t, got, "tick %d reports", tick)
		require.Equal(t, 1, agent.PendingCoordinationAcks(), "tick %d pending acks", tick)
		require.Equal(t, int64(0), agent.NextAckID(), "tick %d ack id", tick)
	}
}

func TestAgent_ClusterAction_hookErrorRetriableAck(t *testing.T) {
	t.Run("retriableDefersMark", func(t *testing.T) {
		hookErr := errors.New("snap boom")
		var got []error
		take := bindDisconnectedTake(t, hookErr, collectErrors(&got))
		agent := take.agent

		const timestamp int64 = 321
		end := writeTermEventAndSnapshot(t, take.image, take.committed, timestamp)

		// Consume tick, hook tick, then retriable drain ticks: the entry
		// stays, no mark.
		requireRetriableHold(t, agent, got)
		require.Len(t, take.svc.pubs, 1, "hook calls")
		require.Equal(t, end, agent.adapter.Position(), "poll position")

		// Connected publication: the -1 ack sends, then the mark follows.
		connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
		take.proxy.SetPublication(connected)
		require.Equal(t, 1, agent.DoWork(0))
		acks := decodeServiceAcksFromPub(t, connected)
		require.Len(t, acks, 1)
		require.Equal(t, codecs.ServiceAckFields{
			LogPosition: end,
			Timestamp:   timestamp,
			AckID:       0,
			RelevantID:  nullValue,
			ServiceID:   1,
		}, acks[0])
		require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks after send")
		requireFailureReported(t, agent, got, ErrTakeSnapshot, hookErr)
	})

	t.Run("laterDispatchFailureKeepsCause", func(t *testing.T) {
		var got []error
		take := bindDisconnectedTake(t, errors.New("snap boom"), collectErrors(&got))
		agent := take.agent

		end := writeTermEventAndSnapshot(t, take.image, take.committed, 321)
		// Second SNAPSHOT frame: its LogPosition does not match its frame end.
		payload := encodeClusterAction(t, snapshotActionFields(1, 322))
		second := writeAgentFrame(t, take.image, int(end), payload)
		*take.committed = end + int64(second)

		// Tick 1: the take holds the log. The second frame stays unread.
		agent.DoWork(0)
		require.NoError(t, agent.ServiceFailure())
		require.Empty(t, take.svc.pubs, "hook calls")
		require.Equal(t, end, agent.adapter.Position(), "poll position after break")

		// Tick 2: the hook fails. NotConnected holds the entry. The log poll
		// resumes, so the second frame marks the position mismatch.
		agent.DoWork(0)
		require.Len(t, take.svc.pubs, 1, "hook calls")
		requireFailureReported(t, agent, got, ErrActionPositionMismatch)

		// A later bind sends no -1 ack.
		connected := aeron.NewTestPublicationForHooksWithConnected(t, true)
		take.proxy.SetPublication(connected)
		require.Equal(t, 0, agent.DoWork(0))
		require.Empty(t, decodeServiceAcksFromPub(t, connected), "acks after bind")
		require.ErrorIs(t, agent.ServiceFailure(), ErrActionPositionMismatch)
		require.NotErrorIs(t, agent.ServiceFailure(), ErrTakeSnapshot)
		require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
		require.Len(t, take.svc.pubs, 1, "hook calls after bind")

		// Close keeps the position-mismatch failure. Close does not report
		// the waiting take cause.
		agent.Close()
		require.Len(t, got, 1)
		require.Same(t, agent.ServiceFailure(), got[0])
		require.NotErrorIs(t, agent.ServiceFailure(), ErrTakeSnapshot)
	})

	t.Run("closeMarksWaitingTakeFailure", func(t *testing.T) {
		hookErr := errors.New("snap boom")
		var got []error
		take := bindDisconnectedTake(t, hookErr, collectErrors(&got))
		agent := take.agent

		writeTermEventAndSnapshot(t, take.image, take.committed, 321)

		// NotConnected holds the ack. The hook fails. The agent does not report.
		requireRetriableHold(t, agent, got)
		require.Len(t, take.svc.pubs, 1, "hook calls")

		agent.Close()
		requireFailureReported(t, agent, got, ErrTakeSnapshot, hookErr)
		require.Equal(t, int64(0), agent.NextAckID(), "ack id after close")
		require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks after close")
	})
}

func TestAgent_ClusterAction_hookErrorNoModuleProxy(t *testing.T) {
	tests := []struct {
		name  string
		proxy *ConsensusModuleProxy
	}{
		{name: "nilProxy"},
		{name: "unboundProxy", proxy: NewConsensusModuleProxy()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hookErr := errors.New("snap boom")
			svc := &snapshotRecordingService{hookErr: hookErr}
			agent, image, committed := bindImage(t, AgentConfig{
				ServiceID:   1,
				Service:     svc,
				ModuleProxy: tt.proxy,
			}, 0)
			fake := InstallSnapshotTakeFakeForTest(t, agent)

			end := writeTermEventAndSnapshot(t, image, committed, 321)

			// Tick 1: the action stores the take.
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure())
			require.Equal(t, end, agent.adapter.Position(), "poll position after break")

			// Tick 2: the hook fails. No sender, so the take cause marks at
			// once.
			agent.DoWork(0)
			require.ErrorIs(t, agent.ServiceFailure(), ErrTakeSnapshot)
			require.ErrorIs(t, agent.ServiceFailure(), hookErr)
			require.Len(t, svc.pubs, 1, "hook calls")
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
			require.True(t, fake.LastPublication().IsClosed(), "publication closed")
			require.Nil(t, fake.handler, "session handler")

			// Tick 3: the mark freezes the duty cycle.
			require.Equal(t, 0, agent.DoWork(0))

			if tt.proxy == nil {
				return
			}
			// A later bind sends no -1 ack.
			pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
			tt.proxy.SetPublication(pub)
			require.Equal(t, 0, agent.DoWork(0))
			require.Empty(t, decodeServiceAcksFromPub(t, pub), "acks after bind")
			require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks after bind")
		})
	}
}

func TestAgent_ClusterAction_noArchive(t *testing.T) {
	svc := &snapshotRecordingService{}
	proxy, pub := boundModuleProxy(t)
	agent, image, committed := bindImage(t, AgentConfig{
		ServiceID:   1,
		Service:     svc,
		ModuleProxy: proxy,
	}, 0)

	const timestamp int64 = 88
	span := writeAlignedSnapshot(t, image, committed, timestamp)

	// Tick 1: no archive fails the take on the action.
	agent.DoWork(0)
	require.NoError(t, agent.ServiceFailure())
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, int64(span), agent.adapter.Position(), "poll position after break")
	require.Equal(t, 1, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, 1, agent.ReadyCoordinationAcks(), "ready acks")

	// Tick 2: the -1 ack sends, then the mark follows.
	agent.DoWork(0)
	acks := decodeServiceAcksFromPub(t, pub)
	require.Len(t, acks, 1)
	require.Equal(t, codecs.ServiceAckFields{
		LogPosition: int64(span),
		Timestamp:   timestamp,
		AckID:       0,
		RelevantID:  nullValue,
		ServiceID:   1,
	}, acks[0])
	require.ErrorIs(t, agent.ServiceFailure(), ErrSnapshotNoArchive)
	require.Equal(t, nullValue, agent.LastSnapshotRecordingID())
}

func TestAgent_ClusterAction_consumedWithoutEntry(t *testing.T) {
	tests := []struct {
		name   string
		action clusterwire.ClusterAction
		flags  int32
	}{
		{name: "SUSPEND", action: clusterwire.ClusterAction_SUSPEND},
		{name: "RESUME", action: clusterwire.ClusterAction_RESUME},
		{name: "flags1", action: clusterwire.ClusterAction_SNAPSHOT, flags: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &snapshotRecordingService{}
			agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
			const timestamp int64 = 77
			span := writeAlignedAction(t, image, committed, timestamp, tt.action, tt.flags)
			agent.DoWork(0)
			require.NoError(t, agent.ServiceFailure())
			require.Empty(t, svc.pubs, "hook calls")
			require.Equal(t, int64(span), agent.LogPosition())
			require.Equal(t, timestamp, agent.ClusterTime())
			require.Equal(t, int64(span), agent.adapter.Position(), "poll position after continue")
			require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
		})
	}
}

func TestAgent_ClusterAction_unknownAction(t *testing.T) {
	tests := []struct {
		name   string
		action clusterwire.ClusterAction
	}{
		{name: "nullValue", action: clusterwire.ClusterAction_NULL_VALUE},
		{name: "outOfRange", action: clusterwire.ClusterAction(99)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &snapshotRecordingService{}
			agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
			const timestamp int64 = 9
			span := writeAlignedAction(t, image, committed, timestamp, tt.action, 0)
			join := image.Position()
			agent.DoWork(0)
			require.ErrorIs(t, agent.ServiceFailure(), ErrUnknownClusterAction)
			require.Empty(t, svc.pubs, "hook calls")
			require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
			require.Equal(t, int64(span), agent.LogPosition())
			require.Equal(t, timestamp, agent.ClusterTime())
			require.Equal(t, join, agent.adapter.Position(), "poll position after abort")
		})
	}
}

func TestAgent_ClusterAction_positionMismatch(t *testing.T) {
	svc := &snapshotRecordingService{}
	agent, image, committed := bindImage(t, AgentConfig{Service: svc}, 0)
	const timestamp int64 = 12
	span := writeClusterAction(t, image, committed, snapshotActionFields(1, timestamp))
	join := image.Position()

	agent.DoWork(0)
	require.ErrorIs(t, agent.ServiceFailure(), ErrActionPositionMismatch)
	require.Empty(t, svc.pubs, "hook calls")
	require.Equal(t, 0, agent.PendingCoordinationAcks(), "pending acks")
	require.Equal(t, int64(span), agent.LogPosition())
	require.Equal(t, timestamp, agent.ClusterTime())
	require.Equal(t, join, agent.adapter.Position(), "poll position after abort")
}

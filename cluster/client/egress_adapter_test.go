//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func writeHookCommittedFragmentFrames(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	specs []fragmentedFrameSpec,
) {
	t.Helper()

	writeCommittedFragmentFramesForStream(
		t,
		termBuffer,
		startOffset,
		hookFrameSessionID,
		hookFrameStreamID,
		specs,
	)
}

func writeUnfragmentedWireToTerm(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeHookCommittedFragmentFrames(t, termBuffer, 0, []fragmentedFrameSpec{{
		flags:   logbuffer.Unfragmented,
		payload: wire,
	}})
}

func splitWireFragmentSpecs(wire []byte) []fragmentedFrameSpec {
	split := len(wire) / 2
	return []fragmentedFrameSpec{
		{flags: logbuffer.BeginFlag, payload: wire[:split]},
		{flags: logbuffer.EndFlag, payload: wire[split:]},
	}
}

func newTestEgressAdapter(
	rec EgressHandler,
	sub *aeron.Subscription,
) *EgressAdapter {
	return NewEgressAdapter(
		rec,
		egressTestClusterSessionID,
		sub,
		DefaultEgressFragmentLimit,
		false,
	)
}

func setupEgressAdapterPollFixtures(t *testing.T) (*aeron.Subscription, atomic.AtomicBuffer) {
	t.Helper()

	sub := aeron.NewTestSubscriptionForHooks(t)
	image := aeron.NewTestImageForHooks(t, sub, 0)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return sub, aeron.ImageTermBufferForHooks(image)
}

func TestEgressAdapter_Poll_dispatchesSessionEvent(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, int64(77), rec.gotSessionEventCorrelationID)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
	require.Equal(t, []byte("session-event-detail"), rec.gotSessionEventDetail)
}

func TestEgressAdapter_Poll_dispatchesNewLeader(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeNewLeaderWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	require.Equal(t, 1, rec.newLeaderCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotNewLeaderClusterSessionID)
	require.Equal(t, int64(200), rec.gotNewLeaderLeadershipTermID)
	require.Equal(t, int32(1), rec.gotNewLeaderLeaderMemberID)
	require.Equal(t, []byte("0=localhost:20110"), rec.gotNewLeaderIngressEndpoints)
}

func TestEgressAdapter_Poll_dispatchesAdminResponse(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeAdminResponseWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	require.Equal(t, 1, rec.adminResponseCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotAdminResponseClusterSessionID)
	require.Equal(t, int64(303), rec.gotAdminResponseCorrelationID)
	require.Equal(t, AdminRequestTypeSnapshot, rec.gotAdminResponseRequestType)
	require.Equal(t, AdminResponseCodeOK, rec.gotAdminResponseCode)
	require.Equal(t, []byte("admin-ok"), rec.gotAdminResponseMessage)
	require.Equal(t, []byte("admin-payload"), rec.gotAdminResponsePayload)
}

func TestEgressAdapter_Poll_dispatchesSessionMessageHeader(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	const timestamp = int64(1700000000)
	appPayload := []byte("poll-app-payload")
	wire := encodeSessionMessageHeaderWire(t, egressTestClusterSessionID, timestamp, appPayload)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	require.Equal(t, 1, rec.messageCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotMessageClusterSessionID)
	require.Equal(t, timestamp, rec.gotMessageTimestamp)
	// Unfragmented poll delivers wire at term frame payload offset (DataHeaderLength).
	require.Equal(t, logbuffer.DataHeaderLength+SessionHeaderLength(), rec.gotMessageOffset)
	require.Equal(t, len(appPayload), rec.gotMessageLength)
	require.Equal(t, appPayload, rec.gotMessagePayload)
}

func TestEgressAdapter_Poll_fragmentedSessionMessageHeader(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	const timestamp = int64(1700000000)
	appPayload := []byte("fragmented-poll-payload")
	wire := encodeSessionMessageHeaderWire(t, egressTestClusterSessionID, timestamp, appPayload)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 2, adapter.Poll())
	require.Equal(t, 1, rec.messageCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotMessageClusterSessionID)
	require.Equal(t, timestamp, rec.gotMessageTimestamp)
	require.Equal(t, SessionHeaderLength(), rec.gotMessageOffset)
	require.Equal(t, len(appPayload), rec.gotMessageLength)
	require.Equal(t, appPayload, rec.gotMessagePayload)
}

func TestEgressAdapter_Poll_wrongClusterSessionIDIgnored(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestOtherSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func TestEgressAdapter_Poll_fragmentedWrongClusterSessionIDIgnored(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestOtherSessionID)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 2, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func TestEgressAdapter_Poll_fragmentedSessionEvent(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 2, adapter.Poll())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
	require.Equal(t, []byte("session-event-detail"), rec.gotSessionEventDetail)
}

func TestEgressAdapter_Poll_partialFragmentLimitCompletesAcrossPolls(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))

	rec := newRecordingEgressHandlers()
	adapter := NewEgressAdapter(rec, egressTestClusterSessionID, sub, 1, false)

	require.Equal(t, 1, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)

	require.Equal(t, 1, adapter.Poll())
	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
}

func TestNewEgressAdapterFromContext_directAssemblers(t *testing.T) {
	sub := aeron.NewTestSubscriptionForHooks(t)
	rec := newRecordingEgressHandlers()
	ctx := ClusterContext{DirectAssemblers: true}

	adapter := NewEgressAdapterFromContext(ctx, rec, egressTestClusterSessionID, sub)

	require.True(t, adapter.DirectAssemblers())
	require.Equal(t, DefaultEgressFragmentLimit, adapter.FragmentLimit())
}

func TestEgressAdapter_Poll_zeroWhenClosed(t *testing.T) {
	sub, termBuffer := setupEgressAdapterPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)
	require.NoError(t, sub.Close())

	require.Equal(t, 0, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func TestNewEgressAdapter_nonPositiveFragmentLimitUsesDefault(t *testing.T) {
	cases := []struct {
		name  string
		limit int
	}{
		{name: "zero", limit: 0},
		{name: "negative", limit: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := aeron.NewTestSubscriptionForHooks(t)
			rec := newRecordingEgressHandlers()

			adapter := NewEgressAdapter(rec, egressTestClusterSessionID, sub, tc.limit, false)

			require.Equal(t, DefaultEgressFragmentLimit, adapter.FragmentLimit())
		})
	}
}

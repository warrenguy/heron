//go:build testhooks

package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func newTestEgressAdapter(
	rec EgressHandler,
	sub *aeron.Subscription,
	fragmentLimit ...int,
) *EgressAdapter {
	limit := DefaultEgressFragmentLimit
	if len(fragmentLimit) > 0 {
		limit = fragmentLimit[0]
	}
	return NewEgressAdapter(rec, egressTestClusterSessionID, sub, limit, false)
}

func requireDispatchedSessionMessage(
	t *testing.T,
	rec *recordingEgressHandlers,
	appPayload []byte,
	expectedOffset int,
) {
	t.Helper()

	require.Equal(t, 1, rec.messageCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotMessageClusterSessionID)
	require.Equal(t, egressTestMessageTimestamp, rec.gotMessageTimestamp)
	require.Equal(t, expectedOffset, rec.gotMessageOffset)
	require.Equal(t, len(appPayload), rec.gotMessageLength)
	require.Equal(t, appPayload, rec.gotMessagePayload)
}

func requireDispatchedDefaultSessionEvent(
	t *testing.T,
	rec *recordingEgressHandlers,
	correlationID ...int64,
) {
	t.Helper()

	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
	require.Equal(t, []byte("session-event-detail"), rec.gotSessionEventDetail)
	if len(correlationID) > 0 {
		require.Equal(t, correlationID[0], rec.gotSessionEventCorrelationID)
	}
}

func TestEgressAdapter_Poll_dispatchesSessionEvent(t *testing.T) {
	sub, _, termBuffer := setupEgressPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	requireDispatchedDefaultSessionEvent(t, rec, 77)
}

func TestEgressAdapter_Poll_dispatchesNewLeader(t *testing.T) {
	sub, _, termBuffer := setupEgressPollFixtures(t)

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
	sub, _, termBuffer := setupEgressPollFixtures(t)

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
	sub, _, termBuffer := setupEgressPollFixtures(t)

	appPayload := []byte("poll-app-payload")
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		appPayload,
	)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 1, adapter.Poll())
	// Unfragmented poll delivers wire at term frame payload offset (DataHeaderLength).
	requireDispatchedSessionMessage(
		t,
		rec,
		appPayload,
		logbuffer.DataHeaderLength+SessionHeaderLength(),
	)
}

func TestEgressAdapter_Poll_fragmentedSessionMessageHeader(t *testing.T) {
	sub, _, termBuffer := setupEgressPollFixtures(t)

	appPayload := []byte("fragmented-poll-payload")
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		appPayload,
	)
	writeFragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 2, adapter.Poll())
	requireDispatchedSessionMessage(t, rec, appPayload, SessionHeaderLength())
}

func TestEgressAdapter_Poll_wrongClusterSessionIDIgnored(t *testing.T) {
	cases := []struct {
		name      string
		writeWire func(t fragmentTestHelper, termBuffer atomic.AtomicBuffer, wire []byte)
		pollCount int
	}{
		{
			name:      "unfragmented",
			writeWire: writeUnfragmentedWireToTerm,
			pollCount: 1,
		},
		{
			name:      "fragmented",
			writeWire: writeFragmentedWireToTerm,
			pollCount: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub, _, termBuffer := setupEgressPollFixtures(t)

			wire := encodeSessionEventWire(t, egressTestOtherSessionID)
			tc.writeWire(t, termBuffer, wire)

			rec := newRecordingEgressHandlers()
			adapter := newTestEgressAdapter(rec, sub)

			require.Equal(t, tc.pollCount, adapter.Poll())
			requireZeroRecordingEgressCalls(t, rec)
		})
	}
}

func TestEgressAdapter_Poll_fragmentedSessionEvent(t *testing.T) {
	sub, _, termBuffer := setupEgressPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeFragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)

	require.Equal(t, 2, adapter.Poll())
	requireDispatchedDefaultSessionEvent(t, rec)
}

func TestEgressAdapter_Poll_partialFragmentLimitCompletesAcrossPolls(t *testing.T) {
	adapter, rec := newEgressAdapterWithPendingBeginFragment(t)

	require.Equal(t, 1, adapter.Poll())
	requireDispatchedDefaultSessionEvent(t, rec)
}

func TestNewEgressAdapterFromContext_directAssemblers(t *testing.T) {
	sub := aeron.NewTestSubscriptionForHooks(t)
	rec := newRecordingEgressHandlers()
	ctx := ClusterContext{DirectAssemblers: true}

	adapter := NewEgressAdapterFromContext(ctx, rec, egressTestClusterSessionID, sub)

	require.True(t, adapter.DirectAssemblers())
	require.Equal(t, DefaultEgressFragmentLimit, adapter.FragmentLimit())
}

func TestEgressAdapter_Poll_zeroWhenSubscriptionNil(t *testing.T) {
	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, nil)

	require.Equal(t, 0, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func TestEgressAdapter_Poll_zeroWhenClosed(t *testing.T) {
	sub, _, termBuffer := setupEgressPollFixtures(t)

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeUnfragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	adapter := newTestEgressAdapter(rec, sub)
	require.NoError(t, sub.Close())

	require.Equal(t, 0, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func newEgressAdapterWithPendingBeginFragment(
	t *testing.T,
) (*EgressAdapter, *recordingEgressHandlers) {
	t.Helper()

	sub, _, termBuffer := setupEgressPollFixtures(t)
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	writeFragmentedWireToTerm(t, termBuffer, wire)

	rec := newRecordingEgressHandlers()
	// fragmentLimit=1 polls only the BEGIN frame; END stays on the term
	// buffer with in-progress assembler state until teardown or completion.
	adapter := newTestEgressAdapter(rec, sub, 1)

	require.Equal(t, 1, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
	return adapter, rec
}

func requirePollDoesNotCompletePendingFragment(
	t *testing.T,
	adapter *EgressAdapter,
	rec *recordingEgressHandlers,
) {
	t.Helper()

	require.Equal(t, 1, adapter.Poll())
	requireZeroRecordingEgressCalls(t, rec)
}

func TestEgressAdapter_Clear(t *testing.T) {
	adapter, rec := newEgressAdapterWithPendingBeginFragment(t)

	adapter.Clear()

	require.False(t, adapter.FreeSessionBuffer(hookFrameSessionID))
	requirePollDoesNotCompletePendingFragment(t, adapter, rec)
}

func TestEgressAdapter_FreeSessionBuffer(t *testing.T) {
	adapter, rec := newEgressAdapterWithPendingBeginFragment(t)

	require.True(t, adapter.FreeSessionBuffer(hookFrameSessionID))
	require.False(t, adapter.FreeSessionBuffer(hookFrameSessionID))

	requirePollDoesNotCompletePendingFragment(t, adapter, rec)
}

func TestEgressAdapter_FreeSessionBuffer_wrongSessionID(t *testing.T) {
	adapter, rec := newEgressAdapterWithPendingBeginFragment(t)

	require.False(t, adapter.FreeSessionBuffer(hookFrameSessionID+1))
	require.True(t, adapter.FreeSessionBuffer(hookFrameSessionID))

	requirePollDoesNotCompletePendingFragment(t, adapter, rec)
}

func TestEgressAdapter_teardownGuards_nilAdapterOrAssembler(t *testing.T) {
	cases := []struct {
		name    string
		adapter *EgressAdapter
	}{
		{name: "nil_adapter", adapter: nil},
		{name: "nil_assembler", adapter: &EgressAdapter{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, tc.adapter.FreeSessionBuffer(hookFrameSessionID))
			require.NotPanics(t, func() { tc.adapter.Clear() })
		})
	}
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

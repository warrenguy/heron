package client_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/client"
)

var (
	testEgressPayloadData    = []byte("payload")
	testSessionEventDetail   = []byte("evt-detail")
	testIngressEndpoints     = []byte("0=localhost:20110")
	testAdminResponseMessage = []byte("ok")
	testAdminResponsePayload = []byte("snap")
)

const (
	testMessageClusterSessionID int64 = 1
	testMessageTimestamp        int64 = 2
	testMessageOffset                 = 0

	testSessionEventCorrelationID    int64 = 1
	testSessionEventClusterSessionID int64 = 2
	testSessionEventLeadershipTermID int64 = 3
	testSessionEventLeaderMemberID   int32 = 4

	testNewLeaderClusterSessionID int64 = 1
	testNewLeaderLeadershipTermID int64 = 2
	testNewLeaderLeaderMemberID   int32 = 3

	testAdminResponseClusterSessionID int64 = 1
	testAdminResponseCorrelationID    int64 = 2
)

func testEgressPayload() atomic.AtomicBuffer {
	return atomic.Wrap(testEgressPayloadData)
}

func invokeNonMessageEgressHandlerMethods(h client.EgressHandler) {
	h.SessionEvent(
		testSessionEventCorrelationID,
		testSessionEventClusterSessionID,
		testSessionEventLeadershipTermID,
		testSessionEventLeaderMemberID,
		client.EventCodeOK,
		testSessionEventDetail,
	)
	h.NewLeader(
		testNewLeaderClusterSessionID,
		testNewLeaderLeadershipTermID,
		testNewLeaderLeaderMemberID,
		testIngressEndpoints,
	)
	h.AdminResponse(
		testAdminResponseClusterSessionID,
		testAdminResponseCorrelationID,
		client.AdminRequestTypeSnapshot,
		client.AdminResponseCodeOK,
		testAdminResponseMessage,
		testAdminResponsePayload,
	)
}

func invokeAllEgressHandlerMethods(h client.EgressHandler) {
	buf := testEgressPayload()
	h.Message(
		testMessageClusterSessionID,
		testMessageTimestamp,
		buf,
		testMessageOffset,
		len(testEgressPayloadData),
		nil,
	)
	invokeNonMessageEgressHandlerMethods(h)
}

func TestEgressHandlers_dispatchUnsetFuncsNoOp(t *testing.T) {
	h := &client.EgressHandlers{}

	require.NotPanics(t, func() {
		invokeAllEgressHandlerMethods(h)
	})
}

func TestEgressHandlers_dispatchSetFuncs(t *testing.T) {
	var (
		gotMessageClusterSessionID int64
		gotMessageTimestamp        int64
		gotMessageBuffer           atomic.AtomicBuffer
		gotMessageOffset           int
		gotMessageLength           int
		gotMessageHeader           *aeron.Header

		gotSessionEventCorrelationID    int64
		gotSessionEventClusterSessionID int64
		gotSessionEventLeadershipTermID int64
		gotSessionEventLeaderMemberID   int32
		gotSessionEventCode             client.EventCode
		gotSessionEventDetail           []byte

		gotNewLeaderClusterSessionID int64
		gotNewLeaderLeadershipTermID int64
		gotNewLeaderLeaderMemberID   int32
		gotNewLeaderIngressEndpoints []byte

		gotAdminResponseClusterSessionID int64
		gotAdminResponseCorrelationID    int64
		gotAdminResponseRequestType      client.AdminRequestType
		gotAdminResponseCode             client.AdminResponseCode
		gotAdminResponseMessage          []byte
		gotAdminResponsePayload          []byte
	)

	h := &client.EgressHandlers{
		MessageFunc: func(
			clusterSessionID int64,
			timestamp int64,
			buffer atomic.AtomicBuffer,
			offset int,
			length int,
			header *aeron.Header,
		) {
			gotMessageClusterSessionID = clusterSessionID
			gotMessageTimestamp = timestamp
			gotMessageBuffer = buffer
			gotMessageOffset = offset
			gotMessageLength = length
			gotMessageHeader = header
		},
		SessionEventFunc: func(
			correlationID int64,
			clusterSessionID int64,
			leadershipTermID int64,
			leaderMemberID int32,
			code client.EventCode,
			detail []byte,
		) {
			gotSessionEventCorrelationID = correlationID
			gotSessionEventClusterSessionID = clusterSessionID
			gotSessionEventLeadershipTermID = leadershipTermID
			gotSessionEventLeaderMemberID = leaderMemberID
			gotSessionEventCode = code
			gotSessionEventDetail = detail
		},
		NewLeaderFunc: func(
			clusterSessionID int64,
			leadershipTermID int64,
			leaderMemberID int32,
			ingressEndpoints []byte,
		) {
			gotNewLeaderClusterSessionID = clusterSessionID
			gotNewLeaderLeadershipTermID = leadershipTermID
			gotNewLeaderLeaderMemberID = leaderMemberID
			gotNewLeaderIngressEndpoints = ingressEndpoints
		},
		AdminResponseFunc: func(
			clusterSessionID int64,
			correlationID int64,
			requestType client.AdminRequestType,
			responseCode client.AdminResponseCode,
			message []byte,
			payload []byte,
		) {
			gotAdminResponseClusterSessionID = clusterSessionID
			gotAdminResponseCorrelationID = correlationID
			gotAdminResponseRequestType = requestType
			gotAdminResponseCode = responseCode
			gotAdminResponseMessage = message
			gotAdminResponsePayload = payload
		},
	}

	invokeAllEgressHandlerMethods(h)

	require.Equal(t, testMessageClusterSessionID, gotMessageClusterSessionID)
	require.Equal(t, testMessageTimestamp, gotMessageTimestamp)
	require.Equal(t, testEgressPayloadData, gotMessageBuffer.Bytes())
	require.Equal(t, testMessageOffset, gotMessageOffset)
	require.Equal(t, len(testEgressPayloadData), gotMessageLength)
	require.Nil(t, gotMessageHeader)

	require.Equal(t, testSessionEventCorrelationID, gotSessionEventCorrelationID)
	require.Equal(t, testSessionEventClusterSessionID, gotSessionEventClusterSessionID)
	require.Equal(t, testSessionEventLeadershipTermID, gotSessionEventLeadershipTermID)
	require.Equal(t, testSessionEventLeaderMemberID, gotSessionEventLeaderMemberID)
	require.Equal(t, client.EventCodeOK, gotSessionEventCode)
	require.Equal(t, testSessionEventDetail, gotSessionEventDetail)

	require.Equal(t, testNewLeaderClusterSessionID, gotNewLeaderClusterSessionID)
	require.Equal(t, testNewLeaderLeadershipTermID, gotNewLeaderLeadershipTermID)
	require.Equal(t, testNewLeaderLeaderMemberID, gotNewLeaderLeaderMemberID)
	require.Equal(t, testIngressEndpoints, gotNewLeaderIngressEndpoints)

	require.Equal(t, testAdminResponseClusterSessionID, gotAdminResponseClusterSessionID)
	require.Equal(t, testAdminResponseCorrelationID, gotAdminResponseCorrelationID)
	require.Equal(t, client.AdminRequestTypeSnapshot, gotAdminResponseRequestType)
	require.Equal(t, client.AdminResponseCodeOK, gotAdminResponseCode)
	require.Equal(t, testAdminResponseMessage, gotAdminResponseMessage)
	require.Equal(t, testAdminResponsePayload, gotAdminResponsePayload)
}

func TestEgressHandlers_postAssignMutationVisibleViaInterface(t *testing.T) {
	h := &client.EgressHandlers{}
	handler := client.EgressHandler(h)

	var called bool
	h.MessageFunc = func(
		int64,
		int64,
		atomic.AtomicBuffer,
		int,
		int,
		*aeron.Header,
	) {
		called = true
	}

	buf := testEgressPayload()
	handler.Message(
		testMessageClusterSessionID,
		testMessageTimestamp,
		buf,
		testMessageOffset,
		len(testEgressPayloadData),
		nil,
	)
	require.True(t, called)
}

func TestValidateClusterContext_egressHandlersNilMessageFuncPasses(t *testing.T) {
	ctx := validClusterContext().Normalize()

	require.NoError(t, ctx.Validate())
}

func TestThrowingEgressHandler_panicsOnMessage(t *testing.T) {
	customErr := errors.New("custom egress error")
	buf := testEgressPayload()

	tests := []struct {
		name string
		h    client.ThrowingEgressHandler
		want string
	}{
		{
			name: "default error",
			h:    client.ThrowingEgressHandler{},
			want: "cluster client: EgressHandler Message callback not configured",
		},
		{
			name: "custom error",
			h:    client.ThrowingEgressHandler{Err: customErr},
			want: customErr.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.PanicsWithError(t, tt.want, func() {
				tt.h.Message(
					testMessageClusterSessionID,
					testMessageTimestamp,
					buf,
					testMessageOffset,
					len(testEgressPayloadData),
					nil,
				)
			})
		})
	}
}

func TestThrowingEgressHandler_otherMethodsNoOp(t *testing.T) {
	h := client.ThrowingEgressHandler{}

	require.NotPanics(t, func() {
		invokeNonMessageEgressHandlerMethods(h)
	})
}

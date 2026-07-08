package client_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/atomicbuffer"
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

func testEgressPayload() atomicbuffer.AtomicBuffer {
	return atomicbuffer.Wrap(testEgressPayloadData)
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
		gotMessageBuffer           atomicbuffer.AtomicBuffer
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
			buffer atomicbuffer.AtomicBuffer,
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
		atomicbuffer.AtomicBuffer,
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

func TestValidateClusterContext_egressHandlersMessageCallbacksPass(t *testing.T) {
	tests := []struct {
		name    string
		handler *client.EgressHandlers
	}{
		{name: "nil_callbacks", handler: &client.EgressHandlers{}},
		{
			name: "message_func_only",
			handler: &client.EgressHandlers{
				MessageFunc: func(
					int64,
					int64,
					atomicbuffer.AtomicBuffer,
					int,
					int,
					*aeron.Header,
				) {
				},
			},
		},
		{
			name: "message_bytes_func_only",
			handler: &client.EgressHandlers{
				MessageBytesFunc: func(int64, int64, []byte, *aeron.Header) {},
			},
		},
		{
			name: "both_callbacks_set",
			handler: &client.EgressHandlers{
				MessageFunc: func(
					int64,
					int64,
					atomicbuffer.AtomicBuffer,
					int,
					int,
					*aeron.Header,
				) {
				},
				MessageBytesFunc: func(int64, int64, []byte, *aeron.Header) {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := validClusterContext().Normalize()
			ctx.EgressHandler = tt.handler

			requireNoValidationErrors(t, ctx)
		})
	}
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

func TestEgressHandlers_messageDispatchPrecedence(t *testing.T) {
	raw := []byte("prefix-payload-suffix")
	const offset = 7
	const length = 7
	wantPayload := raw[offset : offset+length]
	header := &aeron.Header{}
	buf := atomicbuffer.Wrap(raw)

	t.Run("message_bytes_func_precedence", func(t *testing.T) {
		var bytesCalls int
		var gotClusterSessionID int64
		var gotTimestamp int64
		var gotPayload []byte
		var gotHeader *aeron.Header

		h := &client.EgressHandlers{
			MessageBytesFunc: func(
				csid int64,
				ts int64,
				payload []byte,
				hdr *aeron.Header,
			) {
				bytesCalls++
				gotClusterSessionID = csid
				gotTimestamp = ts
				gotPayload = payload
				gotHeader = hdr
			},
			MessageFunc: func(
				int64,
				int64,
				atomicbuffer.AtomicBuffer,
				int,
				int,
				*aeron.Header,
			) {
				t.Fatal("MessageFunc must not run when MessageBytesFunc is set")
			},
		}

		h.Message(testMessageClusterSessionID, testMessageTimestamp, buf, offset, length, header)

		require.Equal(t, 1, bytesCalls)
		require.Equal(t, testMessageClusterSessionID, gotClusterSessionID)
		require.Equal(t, testMessageTimestamp, gotTimestamp)
		require.Equal(t, wantPayload, gotPayload)
		require.Equal(t, header, gotHeader)
	})

	t.Run("message_func_only", func(t *testing.T) {
		var messageCalls int
		var gotClusterSessionID int64
		var gotTimestamp int64
		var gotBuffer []byte
		var gotOffset int
		var gotLength int
		var gotHeader *aeron.Header

		h := &client.EgressHandlers{
			MessageFunc: func(
				csid int64,
				ts int64,
				buffer atomicbuffer.AtomicBuffer,
				off int,
				ln int,
				hdr *aeron.Header,
			) {
				messageCalls++
				gotClusterSessionID = csid
				gotTimestamp = ts
				gotBuffer = buffer.Bytes()
				gotOffset = off
				gotLength = ln
				gotHeader = hdr
			},
		}

		h.Message(testMessageClusterSessionID, testMessageTimestamp, buf, offset, length, header)

		require.Equal(t, 1, messageCalls)
		require.Equal(t, testMessageClusterSessionID, gotClusterSessionID)
		require.Equal(t, testMessageTimestamp, gotTimestamp)
		require.Equal(t, raw, gotBuffer)
		require.Equal(t, offset, gotOffset)
		require.Equal(t, length, gotLength)
		require.Equal(t, header, gotHeader)
	})
}

func TestEgressHandlers_messageBytesFuncCopySemantics(t *testing.T) {
	raw := []byte("0123456789")
	const offset = 2
	const length = 4
	wantPayload := raw[offset : offset+length]

	var gotPayload []byte
	h := &client.EgressHandlers{
		MessageBytesFunc: func(
			_ int64,
			_ int64,
			payload []byte,
			_ *aeron.Header,
		) {
			gotPayload = payload
		},
	}

	buf := atomicbuffer.Wrap(raw)
	h.Message(0, 0, buf, offset, length, nil)

	require.Equal(t, wantPayload, gotPayload)
	gotPayload[0] = 'X'
	require.Equal(t, raw, buf.Bytes())
}

func TestEgressHandlers_messageBytesFunc_nilPayloadSpans(t *testing.T) {
	tests := []struct {
		name        string
		offset      int
		length      int
		wantPayload []byte
	}{
		{name: "zero length", offset: 0, length: 0},
		{name: "length past buffer end", offset: 0, length: 99},
		{name: "negative offset", offset: -1, length: 4},
		{name: "negative length", offset: 0, length: -1},
		{name: "offset past buffer end", offset: 7, length: 1},
		{name: "partial overrun past buffer end", offset: 5, length: 4},
		{name: "valid span", offset: 0, length: 7, wantPayload: []byte("payload")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := atomicbuffer.Wrap([]byte("payload"))

			var gotPayload []byte
			var called bool
			h := &client.EgressHandlers{
				MessageBytesFunc: func(
					_ int64,
					_ int64,
					payload []byte,
					_ *aeron.Header,
				) {
					called = true
					gotPayload = payload
				},
			}

			h.Message(0, 0, buf, tt.offset, tt.length, nil)

			require.True(t, called)
			require.Equal(t, tt.wantPayload, gotPayload)
		})
	}
}

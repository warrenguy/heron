//go:build testhooks

package client

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

type handleEgressFragmentCase struct {
	name string
	wire []byte
}

func runHandleEgressFragmentNoDispatchCases(t *testing.T, cases []handleEgressFragmentCase) {
	t.Helper()

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rec := newRecordingEgressHandlers()
			requireHandleEgressFragmentNoCalls(t, rec, egressTestClusterSessionID, tt.wire)
		})
	}
}

func TestThrowingEgressHandler_message_panicsWithValue(t *testing.T) {
	customErr := errors.New("custom egress error")
	buf := atomicbuffer.Wrap([]byte("payload"))
	payloadLen := len(buf.Bytes())

	tests := []struct {
		name string
		h    ThrowingEgressHandler
		want error
	}{
		{
			name: "default error",
			h:    ThrowingEgressHandler{},
			want: errEgressHandlerMessageNotConfigured,
		},
		{
			name: "custom error",
			h:    ThrowingEgressHandler{Err: customErr},
			want: customErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.PanicsWithValue(t, tt.want, func() {
				tt.h.Message(0, 0, buf, 0, payloadLen, nil)
			})
		})
	}
}

func TestHandleEgressFragment_sessionMessageHeader_messageBytesFunc(t *testing.T) {
	tests := []struct {
		name       string
		appPayload []byte
		prefixLen  int
	}{
		{name: "zero_payload"},
		{name: "with_payload", appPayload: []byte("app-payload")},
		{
			name:       "non_zero_offset",
			appPayload: []byte("offset-payload"),
			prefixLen:  24,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := encodeSessionMessageHeaderWire(
				t,
				egressTestClusterSessionID,
				egressTestMessageTimestamp,
				tt.appPayload,
			)
			rec := newRecordingEgressHandlers()
			rec.configureMessageBytesRecorder(t)

			if tt.prefixLen > 0 {
				invokeHandleEgressFragmentWithPrefix(
					t,
					rec,
					egressTestClusterSessionID,
					wire,
					tt.prefixLen,
				)
			} else {
				invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)
			}

			requireDispatchedSessionMessageBytes(t, rec, tt.appPayload)
		})
	}
}

func TestHandleEgressFragment_sessionMessageHeader_messageFunc(t *testing.T) {
	tests := []struct {
		name       string
		appPayload []byte
		prefixLen  int
	}{
		{name: "zero_payload"},
		{name: "with_payload", appPayload: []byte("app-payload")},
		{name: "non_zero_offset", appPayload: []byte("offset-payload"), prefixLen: 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := encodeSessionMessageHeaderWire(
				t,
				egressTestClusterSessionID,
				egressTestMessageTimestamp,
				tt.appPayload,
			)
			rec := newRecordingEgressHandlers()

			if tt.prefixLen > 0 {
				invokeHandleEgressFragmentWithPrefix(
					t,
					rec,
					egressTestClusterSessionID,
					wire,
					tt.prefixLen,
				)
			} else {
				invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)
			}

			requireDispatchedSessionMessage(
				t,
				rec,
				tt.appPayload,
				tt.prefixLen+SessionHeaderLength(),
			)
		})
	}
}

func TestHandleEgressFragment_sessionEvent(t *testing.T) {
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()

	invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, int64(77), rec.gotSessionEventCorrelationID)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
	require.Equal(t, int64(88), rec.gotSessionEventLeadershipTermID)
	require.Equal(t, int32(3), rec.gotSessionEventLeaderMemberID)
	require.Equal(t, EventCodeOK, rec.gotSessionEventCode)
	require.Equal(t, []byte("session-event-detail"), rec.gotSessionEventDetail)
}

func TestHandleEgressFragment_newLeader(t *testing.T) {
	wire := encodeNewLeaderWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()

	invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.newLeaderCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotNewLeaderClusterSessionID)
	require.Equal(t, int64(200), rec.gotNewLeaderLeadershipTermID)
	require.Equal(t, int32(1), rec.gotNewLeaderLeaderMemberID)
	require.Equal(t, []byte("0=localhost:20110"), rec.gotNewLeaderIngressEndpoints)
}

func TestHandleEgressFragment_adminResponse(t *testing.T) {
	wire := encodeAdminResponseWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()

	invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)

	require.Equal(t, 1, rec.adminResponseCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotAdminResponseClusterSessionID)
	require.Equal(t, int64(303), rec.gotAdminResponseCorrelationID)
	require.Equal(t, AdminRequestTypeSnapshot, rec.gotAdminResponseRequestType)
	require.Equal(t, AdminResponseCodeOK, rec.gotAdminResponseCode)
	require.Equal(t, []byte("admin-ok"), rec.gotAdminResponseMessage)
	require.Equal(t, []byte("admin-payload"), rec.gotAdminResponsePayload)
}

func TestHandleEgressFragment_wrongSession(t *testing.T) {
	runHandleEgressFragmentNoDispatchCases(t, []handleEgressFragmentCase{
		{
			name: "sessionMessageHeader",
			wire: encodeSessionMessageHeaderWire(t, egressTestOtherSessionID, 1, []byte("payload")),
		},
		{
			name: "sessionEvent",
			wire: encodeSessionEventWire(t, egressTestOtherSessionID),
		},
		{
			name: "newLeader",
			wire: encodeNewLeaderWire(t, egressTestOtherSessionID),
		},
		{
			name: "adminResponse",
			wire: encodeAdminResponseWire(t, egressTestOtherSessionID),
		},
	})
}

func TestHandleEgressFragment_ignored(t *testing.T) {
	runHandleEgressFragmentNoDispatchCases(t, []handleEgressFragmentCase{
		{
			name: "challenge",
			wire: encodeChallengeWire(t, egressTestClusterSessionID),
		},
		{
			name: "unknownTemplate",
			wire: encodeSessionKeepAliveWire(t, egressTestClusterSessionID),
		},
		{
			name: "badSchema",
			wire: encodeWireWithSchemaID(t, 999),
		},
	})
}

func requireSessionEventFragmentAtNoPanicNoCalls(t *testing.T, offset, length int) {
	t.Helper()

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()
	requireHandleEgressFragmentAtNoPanicNoCalls(
		t,
		rec,
		egressTestClusterSessionID,
		wire,
		offset,
		length,
	)
}

func TestHandleEgressFragment_truncatedBuffer(t *testing.T) {
	requireSessionEventFragmentAtNoPanicNoCalls(t, 0, codecs.MessageHeaderEncodedLen()-1)
}

func TestHandleEgressFragment_negativeLength(t *testing.T) {
	requireSessionEventFragmentAtNoPanicNoCalls(t, 0, -1)
}

func TestHandleEgressFragment_overflowBounds(t *testing.T) {
	requireSessionEventFragmentAtNoPanicNoCalls(t, math.MaxInt-7, codecs.MessageHeaderEncodedLen())
}

func TestHandleEgressFragment_sessionEvent_truncatedBody(t *testing.T) {
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()

	truncated := wire[:len(wire)-4]
	requireHandleEgressFragmentAtNoPanicNoCalls(
		t,
		rec,
		egressTestClusterSessionID,
		truncated,
		0,
		len(truncated),
	)
}

func TestHandleEgressFragment_sessionEvent_nonZeroOffset(t *testing.T) {
	const prefixLen = 16
	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	rec := newRecordingEgressHandlers()

	invokeHandleEgressFragmentWithPrefix(t, rec, egressTestClusterSessionID, wire, prefixLen)

	require.Equal(t, 1, rec.sessionEventCalls)
	require.Equal(t, egressTestClusterSessionID, rec.gotSessionEventClusterSessionID)
}

func TestHandleEgressFragment_sessionMessageHeader_truncatedHeader(t *testing.T) {
	wire := encodeSessionMessageHeaderWire(t, egressTestClusterSessionID, 1, []byte("payload"))
	rec := newRecordingEgressHandlers()

	requireHandleEgressFragmentAtNoPanicNoCalls(
		t,
		rec,
		egressTestClusterSessionID,
		wire,
		0,
		SessionHeaderLength()-1,
	)
}

func TestHandleEgressFragment_sessionMessageHeader_headerOnlySpan(t *testing.T) {
	appPayload := []byte("trailing-app-bytes")
	wire := encodeSessionMessageHeaderWire(
		t,
		egressTestClusterSessionID,
		egressTestMessageTimestamp,
		appPayload,
	)
	rec := newRecordingEgressHandlers()

	invokeHandleEgressFragmentAt(t, rec, egressTestClusterSessionID, wire, 0, SessionHeaderLength())

	requireDispatchedSessionMessage(t, rec, nil, SessionHeaderLength())
}

func TestHandleEgressFragment_largeCapturedPayloads(t *testing.T) {
	const payloadLen = 2000

	cases := []struct {
		name   string
		encode func(t *testing.T) (wire, expected []byte)
		assert func(t *testing.T, rec *recordingEgressHandlers, expected []byte)
	}{
		{
			name: "detail",
			encode: func(t *testing.T) ([]byte, []byte) {
				detail := bytes.Repeat([]byte{0x34}, payloadLen)
				return encodeSessionEventWireWithDetail(t, egressTestClusterSessionID, detail), detail
			},
			assert: func(t *testing.T, rec *recordingEgressHandlers, expected []byte) {
				require.Equal(t, 1, rec.sessionEventCalls)
				require.Equal(t, expected, rec.gotSessionEventDetail)
			},
		},
		{
			name: "ingressEndpoints",
			encode: func(t *testing.T) ([]byte, []byte) {
				endpoints := bytes.Repeat([]byte{0x31}, payloadLen)
				return encodeNewLeaderWireWithEndpoints(t, egressTestClusterSessionID, 1, endpoints), endpoints
			},
			assert: func(t *testing.T, rec *recordingEgressHandlers, expected []byte) {
				require.Equal(t, 1, rec.newLeaderCalls)
				require.Equal(t, expected, rec.gotNewLeaderIngressEndpoints)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, expected := tc.encode(t)
			rec := newRecordingEgressHandlers()

			invokeHandleEgressFragment(t, rec, egressTestClusterSessionID, wire)

			tc.assert(t, rec, expected)
		})
	}
}

func TestEgressFragmentSpanValid(t *testing.T) {
	payload := []byte("0123456789")

	tests := []struct {
		name   string
		offset int
		length int
		want   bool
	}{
		{
			name:   "valid full span",
			offset: 0,
			length: len(payload),
			want:   len(payload) >= codecs.MessageHeaderEncodedLen(),
		},
		{
			name:   "interior span below message header",
			offset: 2,
			length: 4,
			want:   4 >= codecs.MessageHeaderEncodedLen(),
		},
		{
			name:   "negative offset",
			offset: -1,
			length: 4,
			want:   false,
		},
		{
			name:   "negative length",
			offset: 0,
			length: -1,
			want:   false,
		},
		{
			name:   "length below message header",
			offset: 0,
			length: codecs.MessageHeaderEncodedLen() - 1,
			want:   false,
		},
		{
			name:   "offset past buffer",
			offset: len(payload),
			length: 1,
			want:   false,
		},
		{
			name:   "span overflows buffer",
			offset: 1,
			length: len(payload),
			want:   false,
		},
		{
			name:   "zero length",
			offset: 0,
			length: 0,
			want:   false,
		},
		{
			name:   "offset near max int",
			offset: math.MaxInt - 7,
			length: codecs.MessageHeaderEncodedLen(),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, egressFragmentSpanValid(payload, tt.offset, tt.length))
		})
	}
}

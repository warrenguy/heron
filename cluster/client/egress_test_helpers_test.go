//go:build testhooks

package client

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/codecs/gensbe"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/cluster/codecs"
)

const (
	egressTestClusterSessionID int64 = 4242
	egressTestOtherSessionID   int64 = 9999
	egressTestMessageTimestamp int64 = 1700000000
)

func atomicBufferBytesAt(buffer atomic.AtomicBuffer, offset, length int) []byte {
	payload := buffer.Bytes()
	if offset < 0 || length < 0 || offset > len(payload)-length {
		return nil
	}
	if length == 0 {
		return nil
	}
	return bytes.Clone(payload[offset : offset+length])
}

type fragmentTestHelper interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

type recordingEgressHandlers struct {
	EgressHandlers

	messageCalls       int
	sessionEventCalls  int
	newLeaderCalls     int
	adminResponseCalls int

	gotMessageClusterSessionID int64
	gotMessageTimestamp        int64
	gotMessageOffset           int
	gotMessageLength           int
	gotMessagePayload          []byte

	gotSessionEventCorrelationID    int64
	gotSessionEventClusterSessionID int64
	gotSessionEventLeadershipTermID int64
	gotSessionEventLeaderMemberID   int32
	gotSessionEventCode             EventCode
	gotSessionEventDetail           []byte

	gotNewLeaderClusterSessionID int64
	gotNewLeaderLeadershipTermID int64
	gotNewLeaderLeaderMemberID   int32
	gotNewLeaderIngressEndpoints []byte

	gotAdminResponseClusterSessionID int64
	gotAdminResponseCorrelationID    int64
	gotAdminResponseRequestType      AdminRequestType
	gotAdminResponseCode             AdminResponseCode
	gotAdminResponseMessage          []byte
	gotAdminResponsePayload          []byte
}

func newRecordingEgressHandlers() *recordingEgressHandlers {
	rec := &recordingEgressHandlers{}
	rec.MessageFunc = func(
		clusterSessionID int64,
		timestamp int64,
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		_ *aeron.Header,
	) {
		rec.messageCalls++
		rec.gotMessageClusterSessionID = clusterSessionID
		rec.gotMessageTimestamp = timestamp
		rec.gotMessageOffset = offset
		rec.gotMessageLength = length
		rec.gotMessagePayload = atomicBufferBytesAt(buffer, offset, length)
	}
	rec.SessionEventFunc = func(
		correlationID int64,
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		code EventCode,
		detail []byte,
	) {
		rec.sessionEventCalls++
		rec.gotSessionEventCorrelationID = correlationID
		rec.gotSessionEventClusterSessionID = clusterSessionID
		rec.gotSessionEventLeadershipTermID = leadershipTermID
		rec.gotSessionEventLeaderMemberID = leaderMemberID
		rec.gotSessionEventCode = code
		rec.gotSessionEventDetail = detail
	}
	rec.NewLeaderFunc = func(
		clusterSessionID int64,
		leadershipTermID int64,
		leaderMemberID int32,
		ingressEndpoints []byte,
	) {
		rec.newLeaderCalls++
		rec.gotNewLeaderClusterSessionID = clusterSessionID
		rec.gotNewLeaderLeadershipTermID = leadershipTermID
		rec.gotNewLeaderLeaderMemberID = leaderMemberID
		rec.gotNewLeaderIngressEndpoints = ingressEndpoints
	}
	rec.AdminResponseFunc = func(
		clusterSessionID int64,
		correlationID int64,
		requestType AdminRequestType,
		responseCode AdminResponseCode,
		message []byte,
		payload []byte,
	) {
		rec.adminResponseCalls++
		rec.gotAdminResponseClusterSessionID = clusterSessionID
		rec.gotAdminResponseCorrelationID = correlationID
		rec.gotAdminResponseRequestType = requestType
		rec.gotAdminResponseCode = responseCode
		rec.gotAdminResponseMessage = message
		rec.gotAdminResponsePayload = payload
	}
	return rec
}

func encodeEgressWire(
	t *testing.T,
	encode func(buf []byte, offset int) (int, bool),
) []byte {
	t.Helper()

	buf := make([]byte, 4096)
	end, ok := encode(buf, 0)
	require.True(t, ok)
	return buf[:end]
}

func encodeSchemaWire(
	t *testing.T,
	actingVersion uint16,
	encode func(buf []byte, offset int, actingVersion uint16) (int, bool),
) []byte {
	t.Helper()

	return encodeEgressWire(t, func(buf []byte, offset int) (int, bool) {
		return encode(buf, offset, actingVersion)
	})
}

func encodeCodecWire[F any](
	t *testing.T,
	schemaVersion uint16,
	fields F,
	encode func(buf []byte, offset int, actingVersion uint16, fields F) (int, bool),
) []byte {
	t.Helper()

	return encodeSchemaWire(
		t,
		schemaVersion,
		func(buf []byte, offset int, actingVersion uint16) (int, bool) {
			return encode(buf, offset, actingVersion, fields)
		},
	)
}

func encodeSessionMessageHeaderWire(
	t *testing.T,
	clusterSessionID int64,
	timestamp int64,
	appPayload []byte,
) []byte {
	t.Helper()

	fields := codecs.SessionMessageHeaderFields{
		LeadershipTermID: 11,
		ClusterSessionID: clusterSessionID,
		Timestamp:        timestamp,
	}

	return encodeSchemaWire(
		t,
		gensbe.SessionMessageHeaderSbeSchemaVersion,
		func(buf []byte, offset int, actingVersion uint16) (int, bool) {
			end, ok := codecs.EncodeSessionMessageHeader(buf, offset, actingVersion, fields)
			if !ok {
				return 0, false
			}
			if len(appPayload) == 0 {
				return end, true
			}
			if end+len(appPayload) > len(buf) {
				return 0, false
			}
			copy(buf[end:], appPayload)
			return end + len(appPayload), true
		},
	)
}

func defaultSessionEventFields(detail []byte) codecs.SessionEventFields {
	return codecs.SessionEventFields{
		CorrelationID:    77,
		LeadershipTermID: 88,
		LeaderMemberID:   3,
		Code:             gensbe.EventCode_OK,
		Detail:           detail,
	}
}

func sessionEventFieldsForCorrelation(
	correlationID int64,
	detail []byte,
) codecs.SessionEventFields {
	fields := defaultSessionEventFields(detail)
	fields.CorrelationID = correlationID
	return fields
}

func encodeSessionEventWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeSessionEventWireWithFields(
		t,
		clusterSessionID,
		defaultSessionEventFields([]byte("session-event-detail")),
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func encodeSessionEventWireWithDetail(t *testing.T, clusterSessionID int64, detail []byte) []byte {
	t.Helper()

	return encodeSessionEventWireWithFields(
		t,
		clusterSessionID,
		defaultSessionEventFields(detail),
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func encodeSessionEventWireWithFields(
	t *testing.T,
	clusterSessionID int64,
	fields codecs.SessionEventFields,
	actingVersion uint16,
) []byte {
	t.Helper()

	fields.ClusterSessionID = clusterSessionID

	return encodeSchemaWire(
		t,
		actingVersion,
		func(buf []byte, offset int, actingVersion uint16) (int, bool) {
			blockLength := codecs.SessionEventBlockLength(actingVersion)
			return codecs.EncodeSessionEvent(buf, offset, actingVersion, blockLength, fields)
		},
	)
}

func encodeSessionEventWireWithLeaderHeartbeat(
	t *testing.T,
	clusterSessionID int64,
	leaderHeartbeatTimeoutNs int64,
) []byte {
	t.Helper()

	fields := defaultSessionEventFields([]byte("session-event-detail"))
	fields.LeaderHeartbeatTimeoutNs = leaderHeartbeatTimeoutNs
	return encodeSessionEventWireWithFields(
		t,
		clusterSessionID,
		fields,
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func writePreV13SessionEventForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventWireWithFields(
			t,
			egressTestClusterSessionID,
			sessionEventFieldsForCorrelation(correlationID, []byte("pre-v13-heartbeat")),
			12,
		),
	)
}

func encodeSessionEventWireOmittedHeartbeat(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	const actingVersion uint16 = 13

	fields := defaultSessionEventFields([]byte("omit-heartbeat"))
	fields.LeaderHeartbeatTimeoutNs = math.MinInt64
	return encodeSessionEventWireWithFields(
		t,
		clusterSessionID,
		fields,
		actingVersion,
	)
}

func encodeSessionEventClosedWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	fields := defaultSessionEventFields([]byte("session-closed"))
	fields.Code = gensbe.EventCode_CLOSED
	return encodeSessionEventWireWithFields(
		t,
		clusterSessionID,
		fields,
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func encodeNewLeaderWireWithEndpoints(
	t *testing.T,
	clusterSessionID int64,
	leaderMemberID int32,
	ingressEndpoints []byte,
) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.NewLeaderEventSbeSchemaVersion, codecs.NewLeaderEventFields{
		LeadershipTermID: 200,
		ClusterSessionID: clusterSessionID,
		LeaderMemberID:   leaderMemberID,
		IngressEndpoints: ingressEndpoints,
	}, codecs.EncodeNewLeaderEvent)
}

func encodeNewLeaderWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeNewLeaderWireWithEndpoints(t, clusterSessionID, 1, []byte("0=localhost:20110"))
}

func encodeAdminResponseWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.AdminResponseSbeSchemaVersion, codecs.AdminResponseFields{
		ClusterSessionID: clusterSessionID,
		CorrelationID:    303,
		RequestType:      gensbe.AdminRequestType_SNAPSHOT,
		ResponseCode:     gensbe.AdminResponseCode_OK,
		Message:          []byte("admin-ok"),
		Payload:          []byte("admin-payload"),
	}, codecs.EncodeAdminResponse)
}

func encodeChallengeWireWithCorrelation(
	t *testing.T,
	clusterSessionID int64,
	correlationID int64,
) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.ChallengeSbeSchemaVersion, codecs.ChallengeFields{
		CorrelationID:    correlationID,
		ClusterSessionID: clusterSessionID,
		EncodedChallenge: []byte("challenge-bytes"),
	}, codecs.EncodeChallenge)
}

func encodeChallengeWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeChallengeWireWithCorrelation(t, clusterSessionID, 404)
}

func encodeChallengeWireEmpty(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.ChallengeSbeSchemaVersion, codecs.ChallengeFields{
		CorrelationID:    505,
		ClusterSessionID: clusterSessionID,
		EncodedChallenge: nil,
	}, codecs.EncodeChallenge)
}

func encodeSessionKeepAliveWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.SessionKeepAliveSbeSchemaVersion, codecs.SessionKeepAliveFields{
		LeadershipTermID: 1,
		ClusterSessionID: clusterSessionID,
	}, codecs.EncodeSessionKeepAlive)
}

func encodeWireWithSchemaID(t *testing.T, schemaID uint16) []byte {
	t.Helper()

	wire := encodeSessionEventWire(t, egressTestClusterSessionID)
	schemaOffset := codecs.MessageHeaderEncodedLen() - 4
	binary.LittleEndian.PutUint16(wire[schemaOffset:], schemaID)
	return wire
}

func newConnectEgressPollerForTest(sub *aeron.Subscription) *ConnectEgressPoller {
	return NewConnectEgressPoller(sub, DefaultConnectEgressFragmentLimit)
}

func invokeHandleConnectEgressFragment(
	t *testing.T,
	poller *ConnectEgressPoller,
	wire []byte,
) {
	t.Helper()
	invokeHandleConnectEgressFragmentAt(t, poller, wire, 0, len(wire))
}

func invokeHandleConnectEgressFragmentAt(
	t *testing.T,
	poller *ConnectEgressPoller,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	handleConnectEgressFragment(poller, atomic.Wrap(buf), offset, length, &aeron.Header{})
}

func wireBufferWithPrefix(wire []byte, prefixLen int) []byte {
	buf := make([]byte, prefixLen+len(wire))
	copy(buf[prefixLen:], wire)
	return buf
}

func invokeHandleConnectEgressFragmentWithPrefix(
	t *testing.T,
	poller *ConnectEgressPoller,
	wire []byte,
	prefixLen int,
) {
	t.Helper()
	require.GreaterOrEqual(t, prefixLen, 0)

	buf := wireBufferWithPrefix(wire, prefixLen)
	invokeHandleConnectEgressFragmentAt(t, poller, buf, prefixLen, len(wire))
}

func requireHandleConnectEgressFragmentAtNoPanicNoCalls(
	t *testing.T,
	poller *ConnectEgressPoller,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	require.NotPanics(t, func() {
		invokeHandleConnectEgressFragmentAt(t, poller, buf, offset, length)
	})
	require.False(t, poller.IsPollComplete())
}

func invokeHandleEgressFragment(
	t *testing.T,
	handler EgressHandler,
	clusterSessionID int64,
	wire []byte,
) {
	t.Helper()
	invokeHandleEgressFragmentAt(t, handler, clusterSessionID, wire, 0, len(wire))
}

func invokeHandleEgressFragmentWithPrefix(
	t *testing.T,
	handler EgressHandler,
	clusterSessionID int64,
	wire []byte,
	prefixLen int,
) {
	t.Helper()
	require.GreaterOrEqual(t, prefixLen, 0)

	buf := wireBufferWithPrefix(wire, prefixLen)
	invokeHandleEgressFragmentAt(t, handler, clusterSessionID, buf, prefixLen, len(wire))
}

func invokeHandleEgressFragmentAt(
	t *testing.T,
	handler EgressHandler,
	clusterSessionID int64,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	handleEgressFragment(handler, clusterSessionID, atomic.Wrap(buf), offset, length, &aeron.Header{})
}

func invokeHandleSessionEgressFragmentAt(
	t *testing.T,
	ac *AeronCluster,
	handler EgressHandler,
	clusterSessionID int64,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	dispatchSessionEgressFragment(
		ac,
		handler,
		clusterSessionID,
		atomic.Wrap(buf),
		offset,
		length,
		&aeron.Header{},
	)
}

func invokeHandleSessionEgressFragment(
	t *testing.T,
	ac *AeronCluster,
	handler EgressHandler,
	clusterSessionID int64,
	wire []byte,
) {
	t.Helper()
	invokeHandleSessionEgressFragmentAt(t, ac, handler, clusterSessionID, wire, 0, len(wire))
}

func requireHandleEgressFragmentNoPanic(
	t *testing.T,
	handler EgressHandler,
	clusterSessionID int64,
	wire []byte,
	offset int,
	length int,
) {
	t.Helper()

	require.NotPanics(t, func() {
		invokeHandleEgressFragmentAt(t, handler, clusterSessionID, wire, offset, length)
	})
}

func requireHandleEgressFragmentNoCalls(
	t *testing.T,
	rec *recordingEgressHandlers,
	clusterSessionID int64,
	wire []byte,
) {
	t.Helper()

	requireHandleEgressFragmentAtNoCalls(t, rec, clusterSessionID, wire, 0, len(wire))
}

func requireHandleEgressFragmentAtNoCalls(
	t *testing.T,
	rec *recordingEgressHandlers,
	clusterSessionID int64,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	invokeHandleEgressFragmentAt(t, rec, clusterSessionID, buf, offset, length)
	requireZeroRecordingEgressCalls(t, rec)
}

func requireHandleEgressFragmentAtNoPanicNoCalls(
	t *testing.T,
	rec *recordingEgressHandlers,
	clusterSessionID int64,
	buf []byte,
	offset int,
	length int,
) {
	t.Helper()

	requireHandleEgressFragmentNoPanic(t, rec, clusterSessionID, buf, offset, length)
	requireZeroRecordingEgressCalls(t, rec)
}

func requireZeroRecordingEgressCalls(t *testing.T, rec *recordingEgressHandlers) {
	t.Helper()

	require.Zero(t, rec.messageCalls)
	require.Zero(t, rec.sessionEventCalls)
	require.Zero(t, rec.newLeaderCalls)
	require.Zero(t, rec.adminResponseCalls)
}

const (
	hookFrameSessionID = int32(1002)
	hookFrameStreamID  = int32(2003)
	hookFrameTermID    = int32(7)
)

func unfragmentedFrameSpecs(wire []byte) []aeron.FragmentedFrameForTest {
	return []aeron.FragmentedFrameForTest{{
		Flags:   logbuffer.Unfragmented,
		Payload: wire,
	}}
}

func writeHookCommittedFragmentFrames(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	specs []aeron.FragmentedFrameForTest,
) {
	t.Helper()

	aeron.WriteCommittedDataFramesForStreamForTest(
		termBuffer,
		startOffset,
		hookFrameSessionID,
		hookFrameStreamID,
		hookFrameTermID,
		specs,
	)
}

func writeUnfragmentedWireToTerm(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeHookCommittedFragmentFrames(t, termBuffer, 0, unfragmentedFrameSpecs(wire))
}

func appendUnfragmentedWireToTerm(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	offset := nextHookTermWriteOffset(termBuffer)
	writeHookCommittedFragmentFrames(t, termBuffer, offset, unfragmentedFrameSpecs(wire))
}

func nextHookTermWriteOffset(termBuffer atomic.AtomicBuffer) int {
	offset := 0
	for {
		frameLength := logbuffer.FrameLengthVolatile(termBuffer, offset)
		if frameLength <= 0 {
			return offset
		}
		next := offset + logbuffer.AlignFrameLength(int(frameLength))
		if next >= termBuffer.Capacity() {
			return offset
		}
		offset = next
	}
}

func splitWireFragmentSpecs(wire []byte) []aeron.FragmentedFrameForTest {
	split := len(wire) / 2
	return []aeron.FragmentedFrameForTest{
		{Flags: logbuffer.BeginFlag, Payload: wire[:split]},
		{Flags: logbuffer.EndFlag, Payload: wire[split:]},
	}
}

func writeFragmentedWireToTerm(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire))
}

func writeFirstSplitWireFragmentToTerm(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	writeHookCommittedFragmentFrames(t, termBuffer, 0, splitWireFragmentSpecs(wire)[:1])
}

func appendRemainingSplitWireFragmentsToTerm(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	wire []byte,
) {
	t.Helper()

	offset := nextHookTermWriteOffset(termBuffer)
	writeHookCommittedFragmentFrames(t, termBuffer, offset, splitWireFragmentSpecs(wire)[1:])
}

func writeSessionEventForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeSessionEventWireToTerm(t, termBuffer, correlationID, writeUnfragmentedWireToTerm)
}

func appendSessionEventForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeSessionEventWireToTerm(t, termBuffer, correlationID, appendUnfragmentedWireToTerm)
}

func encodeSessionEventForConnectWire(t *testing.T, correlationID int64) []byte {
	t.Helper()

	return encodeSessionEventWireWithFields(
		t,
		egressTestClusterSessionID,
		sessionEventFieldsForCorrelation(correlationID, []byte("session-event-detail")),
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func encodeSessionEventRedirectWire(t *testing.T, correlationID int64, endpoints string) []byte {
	t.Helper()

	fields := sessionEventFieldsForCorrelation(correlationID, []byte(endpoints))
	fields.Code = gensbe.EventCode_REDIRECT
	fields.LeaderMemberID = 1
	return encodeSessionEventWireWithFields(
		t,
		egressTestClusterSessionID,
		fields,
		gensbe.SessionEventSbeSchemaVersion,
	)
}

func writeSessionEventWireToTerm(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
	write func(fragmentTestHelper, atomic.AtomicBuffer, []byte),
) {
	t.Helper()

	write(t, termBuffer, encodeSessionEventForConnectWire(t, correlationID))
}

func writeChallengeForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeChallengeWireWithCorrelation(t, egressTestClusterSessionID, correlationID),
	)
}

func writeRedirectNewLeaderForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	leaderMemberID int32,
	endpoints string,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeNewLeaderWireWithEndpoints(
			t,
			egressTestClusterSessionID,
			leaderMemberID,
			[]byte(endpoints),
		),
	)
}

func writeSessionEventRedirectForConnect(
	t *testing.T,
	termBuffer atomic.AtomicBuffer,
	correlationID int64,
	endpoints string,
) {
	t.Helper()

	writeUnfragmentedWireToTerm(
		t,
		termBuffer,
		encodeSessionEventRedirectWire(t, correlationID, endpoints),
	)
}

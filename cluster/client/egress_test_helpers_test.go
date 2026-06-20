package client

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	gensbe "github.com/warrenguy/heron/cluster/codecs/generated/sbe"

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
		rec.gotSessionEventDetail = bytes.Clone(detail)
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
		rec.gotNewLeaderIngressEndpoints = bytes.Clone(ingressEndpoints)
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
		rec.gotAdminResponseMessage = bytes.Clone(message)
		rec.gotAdminResponsePayload = bytes.Clone(payload)
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

func encodeSessionEventWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	fields := codecs.SessionEventFields{
		ClusterSessionID: clusterSessionID,
		CorrelationID:    77,
		LeadershipTermID: 88,
		LeaderMemberID:   3,
		Code:             gensbe.EventCode_OK,
		Detail:           []byte("session-event-detail"),
	}

	return encodeSchemaWire(
		t,
		gensbe.SessionEventSbeSchemaVersion,
		func(buf []byte, offset int, actingVersion uint16) (int, bool) {
			blockLength := codecs.SessionEventBlockLength(actingVersion)
			return codecs.EncodeSessionEvent(buf, offset, actingVersion, blockLength, fields)
		},
	)
}

func encodeNewLeaderWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.NewLeaderEventSbeSchemaVersion, codecs.NewLeaderEventFields{
		LeadershipTermID: 200,
		ClusterSessionID: clusterSessionID,
		LeaderMemberID:   1,
		IngressEndpoints: []byte("0=localhost:20110"),
	}, codecs.EncodeNewLeaderEvent)
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

func encodeChallengeWire(t *testing.T, clusterSessionID int64) []byte {
	t.Helper()

	return encodeCodecWire(t, gensbe.ChallengeSbeSchemaVersion, codecs.ChallengeFields{
		CorrelationID:    404,
		ClusterSessionID: clusterSessionID,
		EncodedChallenge: []byte("challenge-bytes"),
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

	buf := make([]byte, prefixLen+len(wire))
	copy(buf[prefixLen:], wire)
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

type fragmentedFrameSpec struct {
	flags   uint8
	payload []byte
}

func writeCommittedFragmentFrame(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	spec fragmentedFrameSpec,
) int32 {
	t.Helper()

	writeHeader := logbuffer.WriteDataHeaderFragmented
	if spec.flags&logbuffer.Unfragmented == logbuffer.Unfragmented {
		writeHeader = logbuffer.WriteDataHeaderUnfragmented
	}
	err := writeHeader(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		spec.flags,
	)
	if err != nil {
		t.Errorf("writeCommittedFragmentFrame: %v", err)
		t.FailNow()
	}

	frameLength := int32(len(spec.payload) + logbuffer.DataHeaderLength)
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, spec.payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
	return frameLength
}

const (
	fragmentFrameSessionID = int32(1001)
	fragmentFrameStreamID  = int32(2002)
	hookFrameSessionID     = int32(1002)
	hookFrameStreamID      = int32(2003)
	fragmentFrameTermID    = int32(7)
)

func fragmentTermCapacity(chunks [][]byte) int {
	total := 64
	for _, chunk := range chunks {
		total += logbuffer.AlignFrameLength(len(chunk) + logbuffer.DataHeaderLength)
	}
	return total
}

func fragmentedFrameTermCapacity(specs []fragmentedFrameSpec) int {
	payloads := make([][]byte, len(specs))
	for i, spec := range specs {
		payloads[i] = spec.payload
	}
	return fragmentTermCapacity(payloads)
}

func writeCommittedFragmentFrames(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	specs []fragmentedFrameSpec,
) []int {
	t.Helper()
	return writeCommittedFragmentFramesForStream(
		t,
		termBuffer,
		startOffset,
		fragmentFrameSessionID,
		fragmentFrameStreamID,
		specs,
	)
}

func writeCommittedFragmentFramesForStream(
	t fragmentTestHelper,
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	sessionID int32,
	streamID int32,
	specs []fragmentedFrameSpec,
) []int {
	t.Helper()

	offsets := make([]int, len(specs))
	offset := startOffset
	for i, spec := range specs {
		offsets[i] = offset
		frameLength := writeCommittedFragmentFrame(
			t,
			termBuffer,
			offset,
			sessionID,
			streamID,
			fragmentFrameTermID,
			int32(offset),
			spec,
		)
		offset += logbuffer.AlignFrameLength(int(frameLength))
	}
	return offsets
}

func fragmentHeaderAt(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
) *aeron.Header {
	header := &aeron.Header{}
	header.SetBuffer(termBuffer)
	header.SetOffset(frameOffset)
	header.ResetPollFragmentState()
	return header
}

func deliverFragment(
	t fragmentTestHelper,
	assembler *FragmentAssembler,
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
) {
	t.Helper()

	frameLength := logbuffer.FrameLengthVolatile(termBuffer, frameOffset)
	payloadOffset := frameOffset + logbuffer.OffsetFramePayload
	payloadLength := int(frameLength) - logbuffer.DataHeaderLength
	header := fragmentHeaderAt(termBuffer, frameOffset)
	assembler.Handler()(termBuffer, payloadOffset, payloadLength, header)
}

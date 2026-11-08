package archive

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/aeron"
	"luas.fast/archive/codecs"
	"luas.fast/archive/codecs/archivewire"
	"luas.fast/atomicbuffer"
)

const (
	controlResponseTestControlSessionID int64 = 4242
	controlResponseTestCorrelationID    int64 = 77
)

func encodeArchiveControlWire(
	tb testing.TB,
	encode func(buf []byte, offset int) int,
) []byte {
	tb.Helper()

	buf := make([]byte, 4096)
	end := encode(buf, 0)
	require.NotEmpty(tb, buf[:end])
	return buf[:end]
}

func invokeHandleControlResponseFragment(
	t *testing.T,
	poller *ControlResponsePoller,
	wire []byte,
	offset int,
	length int,
) {
	t.Helper()
	if length < 0 {
		length = len(wire) - offset
	}
	handleControlResponseFragment(poller, atomicbuffer.Wrap(wire), offset, length, &aeron.Header{})
}

func encodeControlResponseWire(
	tb testing.TB,
	fields codecs.ControlResponseFields,
) []byte {
	tb.Helper()

	return encodeArchiveControlWire(tb, func(buf []byte, offset int) int {
		var msg archivewire.ControlResponse
		msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
			SetControlSessionId(fields.ControlSessionID).
			SetCorrelationId(fields.CorrelationID).
			SetRelevantId(fields.RelevantID).
			SetCode(fields.Code).
			SetVersion(fields.Version)
		msg.PutErrorMessage(string(fields.ErrorMessage))
		return int(msg.SbePosition())
	})
}

func encodeChallengeWire(
	t *testing.T,
	controlSessionID int64,
	correlationID int64,
	encodedChallenge []byte,
) []byte {
	t.Helper()

	return encodeArchiveControlWire(t, func(buf []byte, offset int) int {
		var msg archivewire.Challenge
		msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
			SetControlSessionId(controlSessionID).
			SetCorrelationId(correlationID)
		msg.PutEncodedChallenge(string(encodedChallenge))
		return int(msg.SbePosition())
	})
}

func encodeRecordingSignalEventWire(
	tb testing.TB,
	fields codecs.RecordingSignalEventFields,
) []byte {
	tb.Helper()

	return encodeArchiveControlWire(tb, func(buf []byte, offset int) int {
		var msg archivewire.RecordingSignalEvent
		msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
			SetControlSessionId(fields.ControlSessionID).
			SetCorrelationId(fields.CorrelationID).
			SetRecordingId(fields.RecordingID).
			SetSubscriptionId(fields.SubscriptionID).
			SetPosition(fields.Position).
			SetSignal(fields.Signal)
		return int(msg.SbePosition())
	})
}

func encodeRecordingDescriptorWire(
	tb testing.TB,
	fields codecs.RecordingDescriptorFields,
) []byte {
	tb.Helper()

	return encodeArchiveControlWire(tb, func(buf []byte, offset int) int {
		var msg archivewire.RecordingDescriptor
		msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
			SetControlSessionId(fields.ControlSessionID).
			SetCorrelationId(fields.CorrelationID).
			SetRecordingId(fields.RecordingID).
			SetStartTimestamp(fields.StartTimestamp).
			SetStopTimestamp(fields.StopTimestamp).
			SetStartPosition(fields.StartPosition).
			SetStopPosition(fields.StopPosition).
			SetInitialTermId(fields.InitialTermID).
			SetSegmentFileLength(fields.SegmentFileLength).
			SetTermBufferLength(fields.TermBufferLength).
			SetMtuLength(fields.MtuLength).
			SetSessionId(fields.SessionID).
			SetStreamId(fields.StreamID)
		msg.PutStrippedChannel(string(fields.StrippedChannel))
		msg.PutOriginalChannel(string(fields.OriginalChannel))
		msg.PutSourceIdentity(string(fields.SourceIdentity))
		return int(msg.SbePosition())
	})
}

func encodeKeepAliveRequestWire(t *testing.T, controlSessionID int64) []byte {
	t.Helper()

	return encodeArchiveControlWire(t, func(buf []byte, offset int) int {
		end, ok := codecs.EncodeKeepAliveRequest(
			buf,
			offset,
			archivewire.KeepAliveRequestSbeSchemaVersion,
			codecs.KeepAliveRequestFields{
				ControlSessionID: controlSessionID,
				CorrelationID:    303,
			},
		)
		require.True(t, ok)
		return end
	})
}

func encodeWireWithSchemaID(t *testing.T, schemaID uint16) []byte {
	t.Helper()

	return overrideWireSchemaID(encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7,
		Code:             archivewire.ControlResponseCode_OK,
		Version:          520,
	}), schemaID)
}

func overrideWireSchemaID(wire []byte, schemaID uint16) []byte {
	schemaOffset := codecs.MessageHeaderEncodedLen() - 4
	binary.LittleEndian.PutUint16(wire[schemaOffset:], schemaID)
	return wire
}

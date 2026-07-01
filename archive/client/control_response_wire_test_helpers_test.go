package client

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const (
	controlResponseTestControlSessionID int64 = 4242
	controlResponseTestCorrelationID    int64 = 77
)

func encodeArchiveControlWire(
	t *testing.T,
	encode func(buf []byte, offset int) int,
) []byte {
	t.Helper()

	buf := make([]byte, 4096)
	end := encode(buf, 0)
	require.NotEmpty(t, buf[:end])
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
	handleControlResponseFragment(poller, atomic.Wrap(wire), offset, length, &aeron.Header{})
}

func encodeControlResponseWire(
	t *testing.T,
	fields codecs.ControlResponseFields,
) []byte {
	t.Helper()

	return encodeArchiveControlWire(t, func(buf []byte, offset int) int {
		var msg gensbe.ControlResponse
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
		var msg gensbe.Challenge
		msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
			SetControlSessionId(controlSessionID).
			SetCorrelationId(correlationID)
		msg.PutEncodedChallenge(string(encodedChallenge))
		return int(msg.SbePosition())
	})
}

func encodeRecordingSignalEventWire(
	t *testing.T,
	fields codecs.RecordingSignalEventFields,
) []byte {
	t.Helper()

	return encodeArchiveControlWire(t, func(buf []byte, offset int) int {
		var msg gensbe.RecordingSignalEvent
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

func encodeWireWithSchemaID(t *testing.T, schemaID uint16) []byte {
	t.Helper()

	wire := encodeControlResponseWire(t, codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    controlResponseTestCorrelationID,
		RelevantID:       7,
		Code:             gensbe.ControlResponseCode_OK,
		Version:          520,
	})
	schemaOffset := codecs.MessageHeaderEncodedLen() - 4
	binary.LittleEndian.PutUint16(wire[schemaOffset:], schemaID)
	return wire
}

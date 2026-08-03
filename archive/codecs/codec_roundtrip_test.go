package codecs_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	codecTestMaxVarLen     = 256
	fuzzArchiveCodecMaxLen = 4096
	codecFuzzMaxOffset     = 8
	codecFuzzOffsetSpan    = codecFuzzMaxOffset + 1
)

var (
	messageHeaderLen     = codecs.MessageHeaderEncodedLen()
	archiveSchemaID      = archivewire.MessageHeaderSbeSchemaID
	archiveSchemaVersion = archivewire.MessageHeaderSbeSchemaVersion

	controlResponseVersionMinVersion = uint16((&archivewire.ControlResponse{}).VersionSinceVersion())
	connectRequestVersionMinVersion  = uint16((&archivewire.ConnectRequest{}).VersionSinceVersion())
	authConnectClientInfoMinVersion  = uint16((&archivewire.AuthConnectRequest{}).
						ClientInfoSinceVersion())
	replayFileIoMaxLengthMinVersion = uint16((&archivewire.ReplayRequest{}).
					FileIoMaxLengthSinceVersion())
	replayTokenMinVersion = uint16((&archivewire.ReplayRequest{}).ReplayTokenSinceVersion())

	connectRequestVersionNull      = (&archivewire.ConnectRequest{}).VersionNullValue()
	replayFileIoMaxLengthNullValue = (&archivewire.ReplayRequest{}).FileIoMaxLengthNullValue()
	replayTokenNullValue           = (&archivewire.ReplayRequest{}).ReplayTokenNullValue()

	archiveAllControlResponseCodes = []archivewire.ControlResponseCode{
		archivewire.ControlResponseCode_OK,
		archivewire.ControlResponseCode_ERROR,
		archivewire.ControlResponseCode_RECORDING_UNKNOWN,
		archivewire.ControlResponseCode_SUBSCRIPTION_UNKNOWN,
	}
	archiveAllSourceLocations = []archivewire.SourceLocation{
		archivewire.SourceLocation_LOCAL,
		archivewire.SourceLocation_REMOTE,
	}
	archiveAllBooleanTypes = []archivewire.BooleanType{
		archivewire.BooleanType_FALSE,
		archivewire.BooleanType_TRUE,
	}
	archiveAllRecordingSignals = []archivewire.RecordingSignal{
		archivewire.RecordingSignal_START,
		archivewire.RecordingSignal_STOP,
		archivewire.RecordingSignal_EXTEND,
		archivewire.RecordingSignal_REPLICATE,
		archivewire.RecordingSignal_MERGE,
		archivewire.RecordingSignal_SYNC,
		archivewire.RecordingSignal_DELETE,
		archivewire.RecordingSignal_REPLICATE_END,
	}

	testControlResponseBoom = codecs.ControlResponseFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RelevantID:       3,
		Code:             archivewire.ControlResponseCode_ERROR,
		Version:          7,
		ErrorMessage:     []byte("boom"),
	}
	testRecordingSignalEventStart = codecs.RecordingSignalEventFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
		SubscriptionID:   4,
		Position:         5,
		Signal:           archivewire.RecordingSignal_START,
	}
	testRecordingDescriptorChannels = codecs.RecordingDescriptorFields{
		ControlSessionID:  1,
		CorrelationID:     2,
		RecordingID:       3,
		StartTimestamp:    4,
		StopTimestamp:     5,
		StartPosition:     6,
		StopPosition:      7,
		InitialTermID:     8,
		SegmentFileLength: 9,
		TermBufferLength:  10,
		MtuLength:         11,
		SessionID:         12,
		StreamID:          13,
		StrippedChannel:   []byte("aeron:ipc"),
		OriginalChannel:   []byte("aeron:udp?endpoint=localhost:40123"),
		SourceIdentity:    []byte("localhost:1234"),
	}
	testChallengeWithBytes = codecs.ChallengeFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		Version:          3,
		EncodedChallenge: []byte("challenge"),
	}
	testStartRecordingRequestIPC = codecs.StartRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		StreamID:         3,
		SourceLocation:   archivewire.SourceLocation_LOCAL,
		Channel:          []byte("aeron:ipc"),
	}
)

// connectRequestOracle drops Version below the version that admits it, matching
// EncodeConnectRequest and the generated null read.
func connectRequestOracle(
	fields codecs.ConnectRequestFields,
	actingVersion uint16,
) codecs.ConnectRequestFields {
	if actingVersion < connectRequestVersionMinVersion {
		fields.Version = connectRequestVersionNull
	}
	return fields
}

// authConnectRequestOracle drops ClientInfo below the version that admits it,
// matching EncodeAuthConnectRequest.
func authConnectRequestOracle(
	fields codecs.AuthConnectRequestFields,
	actingVersion uint16,
) codecs.AuthConnectRequestFields {
	if actingVersion < authConnectClientInfoMinVersion {
		fields.ClientInfo = nil
	}
	return fields
}

// replayRequestOracle applies the FileIoMaxLength and ReplayToken null reads for
// versions below the fields' introduction.
func replayRequestOracle(
	fields codecs.ReplayRequestFields,
	actingVersion uint16,
) codecs.ReplayRequestFields {
	if actingVersion < replayFileIoMaxLengthMinVersion {
		fields.FileIoMaxLength = replayFileIoMaxLengthNullValue
	}
	if actingVersion < replayTokenMinVersion {
		fields.ReplayToken = replayTokenNullValue
	}
	return fields
}

func encodeControlResponseAt(buf []byte, offset int, f codecs.ControlResponseFields) (int, bool) {
	var msg archivewire.ControlResponse
	msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
		SetControlSessionId(f.ControlSessionID).
		SetCorrelationId(f.CorrelationID).
		SetRelevantId(f.RelevantID).
		SetCode(f.Code).
		SetVersion(f.Version)
	msg.PutErrorMessage(string(f.ErrorMessage))
	return int(msg.SbePosition()), true
}

// encodeControlResponseVersionAbsentAt writes a ControlResponse at acting version 0
// with the reduced root block that omits the version field, exercising the
// version-absent decode branch.
func encodeControlResponseVersionAbsentAt(
	buf []byte,
	offset int,
	f codecs.ControlResponseFields,
) (int, bool) {
	const reducedBlockLength = archivewire.ControlResponseSbeBlockLength - 4
	var hdr archivewire.MessageHeader
	hdr.Wrap(buf, uint64(offset), 0, uint64(len(buf)))
	hdr.SetBlockLength(reducedBlockLength).
		SetTemplateId(archivewire.ControlResponseSbeTemplateID).
		SetSchemaId(archiveSchemaID).
		SetVersion(0)

	var msg archivewire.ControlResponse
	msg.Wrap(buf, uint64(offset)+hdr.EncodedLength(), uint64(len(buf)), uint64(reducedBlockLength), 0).
		SetControlSessionId(f.ControlSessionID).
		SetCorrelationId(f.CorrelationID).
		SetRelevantId(f.RelevantID).
		SetCode(f.Code)
	msg.PutErrorMessage(string(f.ErrorMessage))
	return int(msg.SbePosition()), true
}

func encodeChallengeAt(buf []byte, offset int, f codecs.ChallengeFields) (int, bool) {
	var msg archivewire.Challenge
	msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
		SetControlSessionId(f.ControlSessionID).
		SetCorrelationId(f.CorrelationID).
		SetVersion(f.Version)
	msg.PutEncodedChallenge(string(f.EncodedChallenge))
	return int(msg.SbePosition()), true
}

func encodeRecordingSignalEventAt(
	buf []byte,
	offset int,
	f codecs.RecordingSignalEventFields,
) (int, bool) {
	var msg archivewire.RecordingSignalEvent
	msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
		SetControlSessionId(f.ControlSessionID).
		SetCorrelationId(f.CorrelationID).
		SetRecordingId(f.RecordingID).
		SetSubscriptionId(f.SubscriptionID).
		SetPosition(f.Position).
		SetSignal(f.Signal)
	return int(msg.SbePosition()), true
}

func encodeRecordingDescriptorAt(
	buf []byte,
	offset int,
	f codecs.RecordingDescriptorFields,
) (int, bool) {
	var msg archivewire.RecordingDescriptor
	msg.WrapAndApplyHeader(buf, uint64(offset), uint64(len(buf))).
		SetControlSessionId(f.ControlSessionID).
		SetCorrelationId(f.CorrelationID).
		SetRecordingId(f.RecordingID).
		SetStartTimestamp(f.StartTimestamp).
		SetStopTimestamp(f.StopTimestamp).
		SetStartPosition(f.StartPosition).
		SetStopPosition(f.StopPosition).
		SetInitialTermId(f.InitialTermID).
		SetSegmentFileLength(f.SegmentFileLength).
		SetTermBufferLength(f.TermBufferLength).
		SetMtuLength(f.MtuLength).
		SetSessionId(f.SessionID).
		SetStreamId(f.StreamID)
	msg.PutStrippedChannel(string(f.StrippedChannel))
	msg.PutOriginalChannel(string(f.OriginalChannel))
	msg.PutSourceIdentity(string(f.SourceIdentity))
	return int(msg.SbePosition()), true
}

func requireBytesEqual(tb require.TestingT, want, got []byte) {
	require.True(tb, bytes.Equal(want, got))
}

func requireArchiveHeader(
	tb require.TestingT,
	buf []byte,
	offset, wireEnd int,
	templateID, actingVersion uint16,
) {
	_, gotTemplateID, schemaID, version, ok := codecs.DecodeMessageHeader(buf, offset)
	require.True(tb, ok)
	require.Equal(tb, templateID, gotTemplateID)
	require.Equal(tb, archiveSchemaID, schemaID)
	require.Equal(tb, actingVersion, version)

	bodyEnd, ok := codecs.ArchiveMessageWireEnd(buf, offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, wireEnd, bodyEnd)
}

func requireDecodedArchiveFields(tb require.TestingT, want, got any) {
	switch w := want.(type) {
	case codecs.ControlResponseFields:
		g := got.(codecs.ControlResponseFields)
		requireBytesEqual(tb, w.ErrorMessage, g.ErrorMessage)
		w.ErrorMessage, g.ErrorMessage = nil, nil
		require.Equal(tb, w, g)
	case codecs.ConnectRequestFields:
		g := got.(codecs.ConnectRequestFields)
		requireBytesEqual(tb, w.ResponseChannel, g.ResponseChannel)
		w.ResponseChannel, g.ResponseChannel = nil, nil
		require.Equal(tb, w, g)
	case codecs.StartRecordingRequestFields:
		g := got.(codecs.StartRecordingRequestFields)
		requireBytesEqual(tb, w.Channel, g.Channel)
		w.Channel, g.Channel = nil, nil
		require.Equal(tb, w, g)
	case codecs.StartRecordingRequest2Fields:
		g := got.(codecs.StartRecordingRequest2Fields)
		requireBytesEqual(tb, w.Channel, g.Channel)
		w.Channel, g.Channel = nil, nil
		require.Equal(tb, w, g)
	case codecs.ExtendRecordingRequest2Fields:
		g := got.(codecs.ExtendRecordingRequest2Fields)
		requireBytesEqual(tb, w.Channel, g.Channel)
		w.Channel, g.Channel = nil, nil
		require.Equal(tb, w, g)
	case codecs.StopRecordingRequestFields:
		g := got.(codecs.StopRecordingRequestFields)
		requireBytesEqual(tb, w.Channel, g.Channel)
		w.Channel, g.Channel = nil, nil
		require.Equal(tb, w, g)
	case codecs.ReplayRequestFields:
		g := got.(codecs.ReplayRequestFields)
		requireBytesEqual(tb, w.ReplayChannel, g.ReplayChannel)
		w.ReplayChannel, g.ReplayChannel = nil, nil
		require.Equal(tb, w, g)
	case codecs.RecordingDescriptorFields:
		g := got.(codecs.RecordingDescriptorFields)
		requireBytesEqual(tb, w.StrippedChannel, g.StrippedChannel)
		requireBytesEqual(tb, w.OriginalChannel, g.OriginalChannel)
		requireBytesEqual(tb, w.SourceIdentity, g.SourceIdentity)
		w.StrippedChannel, g.StrippedChannel = nil, nil
		w.OriginalChannel, g.OriginalChannel = nil, nil
		w.SourceIdentity, g.SourceIdentity = nil, nil
		require.Equal(tb, w, g)
	case codecs.AuthConnectRequestFields:
		g := got.(codecs.AuthConnectRequestFields)
		requireBytesEqual(tb, w.ResponseChannel, g.ResponseChannel)
		requireBytesEqual(tb, w.EncodedCredentials, g.EncodedCredentials)
		requireBytesEqual(tb, w.ClientInfo, g.ClientInfo)
		w.ResponseChannel, g.ResponseChannel = nil, nil
		w.EncodedCredentials, g.EncodedCredentials = nil, nil
		w.ClientInfo, g.ClientInfo = nil, nil
		require.Equal(tb, w, g)
	case codecs.ChallengeFields:
		g := got.(codecs.ChallengeFields)
		requireBytesEqual(tb, w.EncodedChallenge, g.EncodedChallenge)
		w.EncodedChallenge, g.EncodedChallenge = nil, nil
		require.Equal(tb, w, g)
	case codecs.ChallengeResponseFields:
		g := got.(codecs.ChallengeResponseFields)
		requireBytesEqual(tb, w.EncodedCredentials, g.EncodedCredentials)
		w.EncodedCredentials, g.EncodedCredentials = nil, nil
		require.Equal(tb, w, g)
	case codecs.CloseSessionRequestFields:
		require.Equal(tb, w, got.(codecs.CloseSessionRequestFields))
	case codecs.ListRecordingRequestFields:
		require.Equal(tb, w, got.(codecs.ListRecordingRequestFields))
	case codecs.StopReplayRequestFields:
		require.Equal(tb, w, got.(codecs.StopReplayRequestFields))
	case codecs.TruncateRecordingRequestFields:
		require.Equal(tb, w, got.(codecs.TruncateRecordingRequestFields))
	case codecs.RecordingSignalEventFields:
		require.Equal(tb, w, got.(codecs.RecordingSignalEventFields))
	case codecs.KeepAliveRequestFields:
		require.Equal(tb, w, got.(codecs.KeepAliveRequestFields))
	default:
		require.Fail(tb, "unsupported fields type")
	}
}

func runArchiveCodecRoundtripAtOffset(
	tb require.TestingT,
	templateID, actingVersion uint16,
	offset int,
	encode func(buf []byte, offset int) (wireEnd int, ok bool),
	wantFields any,
) {
	buf := make([]byte, offset+fuzzArchiveCodecMaxLen)
	wireEnd, ok := encode(buf, offset)
	require.True(tb, ok)

	requireArchiveHeader(tb, buf, offset, wireEnd, templateID, actingVersion)

	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd], offset, codecTestMaxVarLen)
	require.True(tb, ok)
	require.Equal(tb, templateID, decoded.TemplateID)
	requireDecodedArchiveFields(tb, wantFields, decoded.Fields)
}

func runArchiveCodecRoundtrip(
	tb require.TestingT,
	templateID, actingVersion uint16,
	encode func(buf []byte, offset int) (wireEnd int, ok bool),
	wantFields any,
) {
	for _, offset := range []int{0, 4} {
		runArchiveCodecRoundtripAtOffset(tb, templateID, actingVersion, offset, encode, wantFields)
	}
}

func encodeAt[T any](
	actingVersion uint16,
	fields T,
	encode func([]byte, int, uint16, T) (int, bool),
) func([]byte, int) (int, bool) {
	return func(buf []byte, offset int) (int, bool) {
		return encode(buf, offset, actingVersion, fields)
	}
}

func encodeArchiveWireAt[T any](
	tb require.TestingT,
	offset int,
	fields T,
	encode func([]byte, int, T) (int, bool),
) []byte {
	buf := make([]byte, offset+fuzzArchiveCodecMaxLen)
	wireEnd, ok := encode(buf, offset, fields)
	require.True(tb, ok)
	return buf[:wireEnd]
}

func encodeArchiveWire[T any](
	tb require.TestingT,
	fields T,
	encode func([]byte, int, T) (int, bool),
) []byte {
	return encodeArchiveWireAt(tb, 0, fields, encode)
}

func cloneAndMutate(src []byte, mutate func([]byte)) []byte {
	wire := bytes.Clone(src)
	mutate(wire)
	return wire
}

func TestMessageHeader_roundtrip(t *testing.T) {
	require.Equal(t, 8, messageHeaderLen)

	cases := []struct {
		name        string
		blockLength uint16
		templateID  uint16
		version     uint16
	}{
		{
			name:        "control_response",
			blockLength: archivewire.ControlResponseSbeBlockLength,
			templateID:  archivewire.ControlResponseSbeTemplateID,
			version:     archiveSchemaVersion,
		},
		{
			name:        "recording_descriptor",
			blockLength: archivewire.RecordingDescriptorSbeBlockLength,
			templateID:  archivewire.RecordingDescriptorSbeTemplateID,
			version:     archiveSchemaVersion,
		},
		{
			name:        "reduced_block_v0",
			blockLength: 28,
			templateID:  archivewire.ControlResponseSbeTemplateID,
			version:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, messageHeaderLen)
			var hdr archivewire.MessageHeader
			hdr.Wrap(buf, 0, uint64(tc.version), uint64(len(buf)))
			hdr.SetBlockLength(tc.blockLength).
				SetTemplateId(tc.templateID).
				SetSchemaId(archiveSchemaID).
				SetVersion(tc.version)

			block, templateID, schemaID, version, ok := codecs.DecodeMessageHeader(buf, 0)
			require.True(t, ok)
			require.Equal(t, tc.blockLength, block)
			require.Equal(t, tc.templateID, templateID)
			require.Equal(t, archiveSchemaID, schemaID)
			require.Equal(t, tc.version, version)
		})
	}
}

func TestControlResponse_roundtrip(t *testing.T) {
	fieldCases := []struct {
		name   string
		fields codecs.ControlResponseFields
	}{
		{
			name: "ok_empty_message",
			fields: codecs.ControlResponseFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				RelevantID:       3,
				Code:             archivewire.ControlResponseCode_OK,
				Version:          4,
			},
		},
		{
			name: "error_with_message",
			fields: codecs.ControlResponseFields{
				ControlSessionID: 10,
				CorrelationID:    11,
				RelevantID:       12,
				Code:             archivewire.ControlResponseCode_ERROR,
				Version:          7,
				ErrorMessage:     []byte("boom"),
			},
		},
		{
			name: "max_practical_message",
			fields: codecs.ControlResponseFields{
				ControlSessionID: 20,
				CorrelationID:    21,
				RelevantID:       22,
				Code:             archivewire.ControlResponseCode_RECORDING_UNKNOWN,
				Version:          9,
				ErrorMessage:     bytes.Repeat([]byte{0x41}, codecTestMaxVarLen),
			},
		},
	}

	for _, tc := range fieldCases {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.fields
			runArchiveCodecRoundtrip(
				t,
				archivewire.ControlResponseSbeTemplateID,
				archiveSchemaVersion,
				func(buf []byte, offset int) (int, bool) {
					return encodeControlResponseAt(buf, offset, fields)
				},
				fields,
			)
		})
	}
}

func TestChallenge_roundtrip(t *testing.T) {
	fieldCases := []struct {
		name   string
		fields codecs.ChallengeFields
	}{
		{
			name: "empty_challenge",
			fields: codecs.ChallengeFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				Version:          3,
			},
		},
		{
			name: "max_practical_challenge",
			fields: codecs.ChallengeFields{
				ControlSessionID: 4,
				CorrelationID:    5,
				Version:          6,
				EncodedChallenge: bytes.Repeat([]byte{0x22}, codecTestMaxVarLen),
			},
		},
	}

	for _, tc := range fieldCases {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.fields
			runArchiveCodecRoundtrip(
				t,
				archivewire.ChallengeSbeTemplateID,
				archiveSchemaVersion,
				func(buf []byte, offset int) (int, bool) {
					return encodeChallengeAt(buf, offset, fields)
				},
				fields,
			)
		})
	}
}

func TestRecordingSignalEvent_roundtrip(t *testing.T) {
	for _, signal := range archiveAllRecordingSignals {
		t.Run("signal_"+signal.String(), func(t *testing.T) {
			fields := codecs.RecordingSignalEventFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				RecordingID:      3,
				SubscriptionID:   4,
				Position:         5,
				Signal:           signal,
			}
			runArchiveCodecRoundtrip(
				t,
				archivewire.RecordingSignalEventSbeTemplateID,
				archiveSchemaVersion,
				func(buf []byte, offset int) (int, bool) {
					return encodeRecordingSignalEventAt(buf, offset, fields)
				},
				fields,
			)
		})
	}
}

func TestRecordingDescriptor_roundtrip(t *testing.T) {
	fieldCases := []struct {
		name   string
		fields codecs.RecordingDescriptorFields
	}{
		{
			name: "empty_channels",
			fields: codecs.RecordingDescriptorFields{
				ControlSessionID:  1,
				CorrelationID:     2,
				RecordingID:       3,
				StartTimestamp:    4,
				StopTimestamp:     5,
				StartPosition:     6,
				StopPosition:      7,
				InitialTermID:     8,
				SegmentFileLength: 9,
				TermBufferLength:  10,
				MtuLength:         11,
				SessionID:         12,
				StreamID:          13,
			},
		},
		{
			name: "populated_channels",
			fields: codecs.RecordingDescriptorFields{
				ControlSessionID:  20,
				CorrelationID:     21,
				RecordingID:       22,
				StartTimestamp:    23,
				StopTimestamp:     24,
				StartPosition:     25,
				StopPosition:      26,
				InitialTermID:     27,
				SegmentFileLength: 28,
				TermBufferLength:  29,
				MtuLength:         30,
				SessionID:         31,
				StreamID:          32,
				StrippedChannel:   []byte("aeron:ipc"),
				OriginalChannel:   []byte("aeron:udp?endpoint=localhost:40123"),
				SourceIdentity:    []byte("localhost:1234"),
			},
		},
	}

	for _, tc := range fieldCases {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.fields
			runArchiveCodecRoundtrip(
				t,
				archivewire.RecordingDescriptorSbeTemplateID,
				archiveSchemaVersion,
				func(buf []byte, offset int) (int, bool) {
					return encodeRecordingDescriptorAt(buf, offset, fields)
				},
				fields,
			)
		})
	}
}

func TestConnectRequest_roundtrip(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		fields    codecs.ConnectRequestFields
	}{
		{
			name:      "current_version",
			actingVer: archiveSchemaVersion,
			fields: codecs.ConnectRequestFields{
				CorrelationID:    1,
				ResponseStreamID: 100,
				Version:          5,
				ResponseChannel:  []byte("aeron:udp?endpoint=localhost:40123"),
			},
		},
		{
			name:      "version_absent_pre_v2",
			actingVer: 1,
			fields: codecs.ConnectRequestFields{
				CorrelationID:    2,
				ResponseStreamID: 101,
				Version:          7,
				ResponseChannel:  []byte("aeron:ipc"),
			},
		},
		{
			name:      "null_version_current",
			actingVer: archiveSchemaVersion,
			fields: codecs.ConnectRequestFields{
				CorrelationID:    3,
				ResponseStreamID: 102,
				Version:          connectRequestVersionNull,
				ResponseChannel:  []byte("aeron:ipc"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runArchiveCodecRoundtrip(
				t,
				archivewire.ConnectRequestSbeTemplateID,
				tc.actingVer,
				encodeAt(tc.actingVer, tc.fields, codecs.EncodeConnectRequest),
				connectRequestOracle(tc.fields, tc.actingVer),
			)
		})
	}
}

func TestEncodeConnectRequest_nullVersion_clearsStaleBlockSlot(t *testing.T) {
	versionOffset := messageHeaderLen + int((&archivewire.ConnectRequest{}).VersionEncodingOffset())

	fields := codecs.ConnectRequestFields{
		CorrelationID:    1,
		ResponseStreamID: 100,
		Version:          connectRequestVersionNull,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	buf := make([]byte, fuzzArchiveCodecMaxLen)
	for i := range buf {
		buf[i] = 0xFF
	}

	wireEnd, ok := codecs.EncodeConnectRequest(buf, 0, archiveSchemaVersion, fields)
	require.True(t, ok)
	require.Equal(t, []byte{0, 0, 0, 0}, buf[versionOffset:versionOffset+4])

	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd], 0, codecTestMaxVarLen)
	require.True(t, ok)
	got := decoded.Fields.(codecs.ConnectRequestFields)
	require.Equal(t, connectRequestVersionNull, got.Version)
}

func TestAuthConnectRequest_roundtrip(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		fields    codecs.AuthConnectRequestFields
	}{
		{
			name:      "client_info_present",
			actingVer: archiveSchemaVersion,
			fields: codecs.AuthConnectRequestFields{
				CorrelationID:      1,
				ResponseStreamID:   100,
				Version:            4,
				ResponseChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
				EncodedCredentials: []byte{9, 8, 7},
				ClientInfo:         []byte("heron-client"),
			},
		},
		{
			name:      "client_info_absent_pre_v12",
			actingVer: 6,
			fields: codecs.AuthConnectRequestFields{
				CorrelationID:      2,
				ResponseStreamID:   101,
				Version:            3,
				ResponseChannel:    []byte("aeron:ipc"),
				EncodedCredentials: []byte{1, 2, 3},
				ClientInfo:         []byte("dropped"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runArchiveCodecRoundtrip(
				t,
				archivewire.AuthConnectRequestSbeTemplateID,
				tc.actingVer,
				encodeAt(tc.actingVer, tc.fields, codecs.EncodeAuthConnectRequest),
				authConnectRequestOracle(tc.fields, tc.actingVer),
			)
		})
	}
}

func TestReplayRequest_roundtrip(t *testing.T) {
	cases := []struct {
		name      string
		actingVer uint16
		fields    codecs.ReplayRequestFields
	}{
		{
			name:      "current_version",
			actingVer: archiveSchemaVersion,
			fields: codecs.ReplayRequestFields{
				ControlSessionID: 1,
				CorrelationID:    2,
				RecordingID:      3,
				Position:         4,
				Length:           5,
				ReplayStreamID:   6,
				FileIoMaxLength:  4096,
				ReplayToken:      77,
				ReplayChannel:    []byte("aeron:udp?endpoint=localhost:40123"),
			},
		},
		{
			name:      "file_io_absent_pre_v7",
			actingVer: 6,
			fields: codecs.ReplayRequestFields{
				ControlSessionID: 10,
				CorrelationID:    11,
				RecordingID:      12,
				Position:         13,
				Length:           14,
				ReplayStreamID:   15,
				FileIoMaxLength:  4096,
				ReplayToken:      88,
				ReplayChannel:    []byte("aeron:ipc"),
			},
		},
		{
			name:      "replay_token_absent_pre_v10",
			actingVer: 8,
			fields: codecs.ReplayRequestFields{
				ControlSessionID: 20,
				CorrelationID:    21,
				RecordingID:      22,
				Position:         23,
				Length:           24,
				ReplayStreamID:   25,
				FileIoMaxLength:  8192,
				ReplayToken:      99,
				ReplayChannel:    nil,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runArchiveCodecRoundtrip(
				t,
				archivewire.ReplayRequestSbeTemplateID,
				tc.actingVer,
				encodeAt(tc.actingVer, tc.fields, codecs.EncodeReplayRequest),
				replayRequestOracle(tc.fields, tc.actingVer),
			)
		})
	}
}

func TestStartRecordingRequest_roundtrip(t *testing.T) {
	fields := codecs.StartRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		StreamID:         3,
		SourceLocation:   archivewire.SourceLocation_LOCAL,
		Channel:          []byte("aeron:udp?endpoint=localhost:40123"),
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.StartRecordingRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeStartRecordingRequest),
		fields,
	)
}

func TestStartRecordingRequest2_roundtrip(t *testing.T) {
	fields := codecs.StartRecordingRequest2Fields{
		ControlSessionID: 1,
		CorrelationID:    2,
		StreamID:         3,
		SourceLocation:   archivewire.SourceLocation_REMOTE,
		AutoStop:         archivewire.BooleanType_TRUE,
		Channel:          []byte("aeron:ipc"),
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.StartRecordingRequest2SbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeStartRecordingRequest2),
		fields,
	)
}

func TestExtendRecordingRequest2_roundtrip(t *testing.T) {
	fields := codecs.ExtendRecordingRequest2Fields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
		StreamID:         4,
		SourceLocation:   archivewire.SourceLocation_REMOTE,
		AutoStop:         archivewire.BooleanType_TRUE,
		Channel:          []byte("aeron:ipc"),
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.ExtendRecordingRequest2SbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeExtendRecordingRequest2),
		fields,
	)
}

func TestStopRecordingRequest_roundtrip(t *testing.T) {
	fields := codecs.StopRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		StreamID:         3,
		Channel:          []byte("aeron:ipc"),
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.StopRecordingRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeStopRecordingRequest),
		fields,
	)
}

func TestListRecordingRequest_roundtrip(t *testing.T) {
	fields := codecs.ListRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.ListRecordingRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeListRecordingRequest),
		fields,
	)
}

func TestStopReplayRequest_roundtrip(t *testing.T) {
	fields := codecs.StopReplayRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		ReplaySessionID:  3,
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.StopReplayRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeStopReplayRequest),
		fields,
	)
}

func TestTruncateRecordingRequest_roundtrip(t *testing.T) {
	fields := codecs.TruncateRecordingRequestFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
		Position:         4,
	}
	runArchiveCodecRoundtrip(
		t,
		archivewire.TruncateRecordingRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeTruncateRecordingRequest),
		fields,
	)
}

func TestCloseSessionRequest_roundtrip(t *testing.T) {
	fields := codecs.CloseSessionRequestFields{ControlSessionID: 42}
	runArchiveCodecRoundtrip(
		t,
		archivewire.CloseSessionRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeCloseSessionRequest),
		fields,
	)
}

func TestKeepAliveRequest_roundtrip(t *testing.T) {
	fields := codecs.KeepAliveRequestFields{ControlSessionID: 1, CorrelationID: 2}
	runArchiveCodecRoundtrip(
		t,
		archivewire.KeepAliveRequestSbeTemplateID,
		archiveSchemaVersion,
		encodeAt(archiveSchemaVersion, fields, codecs.EncodeKeepAliveRequest),
		fields,
	)
}

func TestChallengeResponse_roundtrip(t *testing.T) {
	fieldCases := []struct {
		name   string
		fields codecs.ChallengeResponseFields
	}{
		{
			name: "empty_credentials",
			fields: codecs.ChallengeResponseFields{
				ControlSessionID: 1,
				CorrelationID:    2,
			},
		},
		{
			name: "max_practical_credentials",
			fields: codecs.ChallengeResponseFields{
				ControlSessionID:   3,
				CorrelationID:      4,
				EncodedCredentials: bytes.Repeat([]byte{0x33}, codecTestMaxVarLen),
			},
		},
	}

	for _, tc := range fieldCases {
		t.Run(tc.name, func(t *testing.T) {
			runArchiveCodecRoundtrip(
				t,
				archivewire.ChallengeResponseSbeTemplateID,
				archiveSchemaVersion,
				encodeAt(archiveSchemaVersion, tc.fields, codecs.EncodeChallengeResponse),
				tc.fields,
			)
		})
	}
}

func TestControlResponse_roundtrip_versionAbsentReducedBlock(t *testing.T) {
	fields := codecs.ControlResponseFields{
		ControlSessionID: 5,
		CorrelationID:    6,
		RelevantID:       7,
		Code:             archivewire.ControlResponseCode_RECORDING_UNKNOWN,
		Version:          9,
		ErrorMessage:     []byte("absent"),
	}
	want := fields
	want.Version = 0

	runArchiveCodecRoundtrip(
		t,
		archivewire.ControlResponseSbeTemplateID,
		0,
		func(buf []byte, offset int) (int, bool) {
			return encodeControlResponseVersionAbsentAt(buf, offset, fields)
		},
		want,
	)
}

func TestTryDecodeByTemplate_largeVarField(t *testing.T) {
	large := bytes.Repeat([]byte{0x5A}, 2000)
	fields := codecs.RecordingDescriptorFields{
		ControlSessionID: 1,
		CorrelationID:    2,
		RecordingID:      3,
		StrippedChannel:  []byte("aeron:ipc"),
		OriginalChannel:  []byte("aeron:udp?endpoint=localhost:40123"),
		SourceIdentity:   large,
	}
	wireLen, ok := codecs.ArchiveWireMessageLen(
		archivewire.RecordingDescriptorSbeBlockLength,
		len(fields.StrippedChannel),
		len(fields.OriginalChannel),
		len(fields.SourceIdentity),
	)
	require.True(t, ok)

	buf := make([]byte, wireLen)
	wireEnd, ok := encodeRecordingDescriptorAt(buf, 0, fields)
	require.True(t, ok)
	require.Equal(t, wireLen, wireEnd)

	decoded, ok := codecs.TryDecodeByTemplate(buf[:wireEnd], 0, codecs.ArchiveDecodeMaxVarLen)
	require.True(t, ok)
	require.Equal(t, archivewire.RecordingDescriptorSbeTemplateID, decoded.TemplateID)

	gotFields, ok := decoded.Fields.(codecs.RecordingDescriptorFields)
	require.True(t, ok)
	require.Len(t, gotFields.SourceIdentity, len(large))
	requireBytesEqual(t, large, gotFields.SourceIdentity)
}

func TestArchiveRootBlockLengthBounds(t *testing.T) {
	cases := []struct {
		name       string
		templateID uint16
		version    uint16
		wantMin    uint16
		wantMax    uint16
		wantOK     bool
	}{
		{
			name:       "control_response_version_absent",
			templateID: archivewire.ControlResponseSbeTemplateID,
			version:    controlResponseVersionMinVersion - 1,
			wantMin:    28,
			wantMax:    archivewire.ControlResponseSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "control_response_current",
			templateID: archivewire.ControlResponseSbeTemplateID,
			version:    archiveSchemaVersion,
			wantMin:    archivewire.ControlResponseSbeBlockLength,
			wantMax:    archivewire.ControlResponseSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "connect_request_version_absent",
			templateID: archivewire.ConnectRequestSbeTemplateID,
			version:    connectRequestVersionMinVersion - 1,
			wantMin:    12,
			wantMax:    archivewire.ConnectRequestSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "replay_request_file_io_absent",
			templateID: archivewire.ReplayRequestSbeTemplateID,
			version:    replayFileIoMaxLengthMinVersion - 1,
			wantMin:    44,
			wantMax:    archivewire.ReplayRequestSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "replay_request_token_absent",
			templateID: archivewire.ReplayRequestSbeTemplateID,
			version:    replayTokenMinVersion - 1,
			wantMin:    48,
			wantMax:    archivewire.ReplayRequestSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "replay_request_current",
			templateID: archivewire.ReplayRequestSbeTemplateID,
			version:    archiveSchemaVersion,
			wantMin:    archivewire.ReplayRequestSbeBlockLength,
			wantMax:    archivewire.ReplayRequestSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "start_recording_request_fixed",
			templateID: archivewire.StartRecordingRequestSbeTemplateID,
			version:    archiveSchemaVersion,
			wantMin:    archivewire.StartRecordingRequestSbeBlockLength,
			wantMax:    archivewire.StartRecordingRequestSbeBlockLength,
			wantOK:     true,
		},
		{
			name:       "unknown_template",
			templateID: 0x7FFF,
			version:    archiveSchemaVersion,
			wantOK:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			minBlock, maxBlock, ok := codecs.ArchiveRootBlockLengthBounds(tc.templateID, tc.version)
			require.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			require.Equal(t, tc.wantMin, minBlock)
			require.Equal(t, tc.wantMax, maxBlock)
		})
	}
}

func TestArchiveMinRootBlockLength(t *testing.T) {
	require.Equal(
		t,
		uint16(28),
		codecs.ArchiveMinRootBlockLength(
			archivewire.ControlResponseSbeTemplateID, controlResponseVersionMinVersion-1, 999,
		),
	)
	require.Equal(
		t,
		archivewire.ControlResponseSbeBlockLength,
		codecs.ArchiveMinRootBlockLength(
			archivewire.ControlResponseSbeTemplateID, archiveSchemaVersion, 999,
		),
	)
	require.Equal(
		t,
		uint16(999),
		codecs.ArchiveMinRootBlockLength(0x7FFF, archiveSchemaVersion, 999),
	)
}

func TestArchiveVarFieldCount(t *testing.T) {
	cases := []struct {
		name       string
		templateID uint16
		version    uint16
		want       int
		wantOK     bool
	}{
		{
			name:       "close_session_zero_var",
			templateID: archivewire.CloseSessionRequestSbeTemplateID,
			version:    archiveSchemaVersion,
			want:       0,
			wantOK:     true,
		},
		{
			name:       "control_response_one_var",
			templateID: archivewire.ControlResponseSbeTemplateID,
			version:    archiveSchemaVersion,
			want:       1,
			wantOK:     true,
		},
		{
			name:       "recording_descriptor_three_var",
			templateID: archivewire.RecordingDescriptorSbeTemplateID,
			version:    archiveSchemaVersion,
			want:       3,
			wantOK:     true,
		},
		{
			name:       "auth_connect_two_var_pre_client_info",
			templateID: archivewire.AuthConnectRequestSbeTemplateID,
			version:    authConnectClientInfoMinVersion - 1,
			want:       2,
			wantOK:     true,
		},
		{
			name:       "auth_connect_three_var_with_client_info",
			templateID: archivewire.AuthConnectRequestSbeTemplateID,
			version:    authConnectClientInfoMinVersion,
			want:       3,
			wantOK:     true,
		},
		{
			name:       "unknown_template",
			templateID: 0x7FFF,
			version:    archiveSchemaVersion,
			wantOK:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			count, ok := codecs.ArchiveVarFieldCount(tc.templateID, tc.version)
			require.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			require.Equal(t, tc.want, count)
		})
	}
}

func TestArchiveWireMessageLen(t *testing.T) {
	block := archivewire.RecordingDescriptorSbeBlockLength

	t.Run("header_plus_block_plus_var", func(t *testing.T) {
		got, ok := codecs.ArchiveWireMessageLen(block, 10)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+int(block)+codecs.VarLengthPrefixSize+10, got)
	})

	t.Run("no_var_fields", func(t *testing.T) {
		got, ok := codecs.ArchiveWireMessageLen(archivewire.KeepAliveRequestSbeBlockLength)
		require.True(t, ok)
		require.Equal(t, messageHeaderLen+int(archivewire.KeepAliveRequestSbeBlockLength), got)
	})

	t.Run("negative_var_len_rejected", func(t *testing.T) {
		_, ok := codecs.ArchiveWireMessageLen(block, -1)
		require.False(t, ok)
	})

	t.Run("per_field_over_encode_max_rejected", func(t *testing.T) {
		_, ok := codecs.ArchiveWireMessageLen(block, codecs.ArchiveEncodeMaxVarLen+1)
		require.False(t, ok)
	})

	t.Run("total_over_max_message_length_rejected", func(t *testing.T) {
		_, ok := codecs.ArchiveWireMessageLen(
			block, codecs.ArchiveEncodeMaxVarLen, codecs.ArchiveEncodeMaxVarLen,
		)
		require.False(t, ok)
	})

	t.Run("at_encode_max_var_len_ok", func(t *testing.T) {
		got, ok := codecs.ArchiveWireMessageLen(block, codecs.ArchiveEncodeMaxVarLen)
		require.True(t, ok)
		require.Equal(
			t,
			messageHeaderLen+int(block)+codecs.VarLengthPrefixSize+codecs.ArchiveEncodeMaxVarLen,
			got,
		)
	})
}

func TestConnectRequestWireLen(t *testing.T) {
	t.Run("current_version", func(t *testing.T) {
		got, ok := codecs.ConnectRequestWireLen(archiveSchemaVersion, 9)
		require.True(t, ok)
		want, ok := codecs.ArchiveWireMessageLen(
			codecs.ArchiveMinRootBlockLength(
				archivewire.ConnectRequestSbeTemplateID,
				archiveSchemaVersion,
				archivewire.ConnectRequestSbeBlockLength,
			),
			9,
		)
		require.True(t, ok)
		require.Equal(t, want, got)
	})

	t.Run("version_absent_smaller_block", func(t *testing.T) {
		small, ok := codecs.ConnectRequestWireLen(connectRequestVersionMinVersion-1, 9)
		require.True(t, ok)
		large, ok := codecs.ConnectRequestWireLen(archiveSchemaVersion, 9)
		require.True(t, ok)
		require.Less(t, small, large)
	})

	t.Run("negative_len_rejected", func(t *testing.T) {
		_, ok := codecs.ConnectRequestWireLen(archiveSchemaVersion, -1)
		require.False(t, ok)
	})
}

func TestReplayRequestWireLen(t *testing.T) {
	t.Run("grows_with_version_gated_block", func(t *testing.T) {
		preFileIo, ok := codecs.ReplayRequestWireLen(replayFileIoMaxLengthMinVersion-1, 9)
		require.True(t, ok)
		preToken, ok := codecs.ReplayRequestWireLen(replayTokenMinVersion-1, 9)
		require.True(t, ok)
		current, ok := codecs.ReplayRequestWireLen(archiveSchemaVersion, 9)
		require.True(t, ok)
		require.Less(t, preFileIo, preToken)
		require.Less(t, preToken, current)
	})

	t.Run("negative_len_rejected", func(t *testing.T) {
		_, ok := codecs.ReplayRequestWireLen(archiveSchemaVersion, -1)
		require.False(t, ok)
	})
}

func TestAuthConnectRequestVarLens(t *testing.T) {
	t.Run("two_vars_pre_client_info", func(t *testing.T) {
		lens, ok := codecs.AuthConnectRequestVarLens(authConnectClientInfoMinVersion-1, 9, 3, 6)
		require.True(t, ok)
		require.Equal(t, []int{9, 3}, lens)
	})

	t.Run("three_vars_with_client_info", func(t *testing.T) {
		lens, ok := codecs.AuthConnectRequestVarLens(authConnectClientInfoMinVersion, 9, 3, 6)
		require.True(t, ok)
		require.Equal(t, []int{9, 3, 6}, lens)
	})

	t.Run("negative_len_rejected", func(t *testing.T) {
		_, ok := codecs.AuthConnectRequestVarLens(authConnectClientInfoMinVersion, -1, 3, 6)
		require.False(t, ok)
	})
}

func TestAuthConnectRequestWireLen(t *testing.T) {
	t.Run("client_info_adds_one_var_field", func(t *testing.T) {
		withInfo, ok := codecs.AuthConnectRequestWireLen(authConnectClientInfoMinVersion, 9, 3, 6)
		require.True(t, ok)
		withoutInfo, ok := codecs.AuthConnectRequestWireLen(authConnectClientInfoMinVersion-1, 9, 3, 6)
		require.True(t, ok)
		require.Equal(t, codecs.VarLengthPrefixSize+6, withInfo-withoutInfo)
	})

	t.Run("negative_len_rejected", func(t *testing.T) {
		_, ok := codecs.AuthConnectRequestWireLen(archiveSchemaVersion, 9, -1, 6)
		require.False(t, ok)
	})
}

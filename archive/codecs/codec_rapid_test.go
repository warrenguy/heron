//go:build rapid

package codecs_test

import (
	"flag"
	"math"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const (
	rapidChecksSBEDefault = 500
	rapidChecksVarHeavy   = 200
)

var rapidChecksFlagMu sync.Mutex

func rapidCheck(tb testing.TB, checks int, prop func(*rapid.T)) {
	tb.Helper()

	f := flag.Lookup("rapid.checks")
	if f == nil {
		rapid.Check(tb, prop)
		return
	}

	rapidChecksFlagMu.Lock()
	defer rapidChecksFlagMu.Unlock()

	previous := f.Value.String()
	defer func() { _ = flag.Set("rapid.checks", previous) }()

	if err := flag.Set("rapid.checks", strconv.Itoa(checks)); err != nil {
		tb.Fatalf("rapid.checks: %v", err)
	}

	rapid.Check(tb, prop)
}

func rapidCodecContext(rt *rapid.T) (actingVersion uint16, offset int) {
	rt.Helper()
	return uint16(rapid.IntRange(0, 16).Draw(rt, "actingVersion")), rapidCodecOffset(rt)
}

func rapidCodecOffset(rt *rapid.T) int {
	rt.Helper()
	return rapid.IntRange(0, codecFuzzMaxOffset).Draw(rt, "offset")
}

func rapidWireOffset(rt *rapid.T, bufLen int) int {
	rt.Helper()
	return rapid.OneOf(
		rapid.IntRange(-16, bufLen+16),
		rapid.Just(math.MaxInt),
	).Draw(rt, "offset")
}

func rapidGarbageWire(rt *rapid.T) (buf []byte, offset int) {
	rt.Helper()
	bufLen := rapid.IntRange(0, 256).Draw(rt, "bufLen")
	buf = rapid.SliceOfN(rapid.Byte(), bufLen, bufLen).Draw(rt, "buf")
	return buf, rapidWireOffset(rt, bufLen)
}

func rapidVarBytes(rt *rapid.T, name string) []byte {
	rt.Helper()
	return rapid.SliceOfN(rapid.Byte(), 0, codecTestMaxVarLen).Draw(rt, name)
}

func rapidControlResponseFields(rt *rapid.T) codecs.ControlResponseFields {
	rt.Helper()
	return codecs.ControlResponseFields{
		ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
		CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
		RelevantID:       rapid.Int64().Draw(rt, "relevantID"),
		Code:             rapidControlResponseCode(rt),
		Version:          rapid.Int32().Draw(rt, "version"),
		ErrorMessage:     rapidVarBytes(rt, "errorMessage"),
	}
}

func rapidRecordingSignalEventFields(rt *rapid.T) codecs.RecordingSignalEventFields {
	rt.Helper()
	return codecs.RecordingSignalEventFields{
		ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
		CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
		RecordingID:      rapid.Int64().Draw(rt, "recordingID"),
		SubscriptionID:   rapid.Int64().Draw(rt, "subscriptionID"),
		Position:         rapid.Int64().Draw(rt, "position"),
		Signal:           rapidRecordingSignal(rt),
	}
}

func rapidRecordingDescriptorFields(rt *rapid.T) codecs.RecordingDescriptorFields {
	rt.Helper()
	return codecs.RecordingDescriptorFields{
		ControlSessionID:  rapid.Int64().Draw(rt, "controlSessionID"),
		CorrelationID:     rapid.Int64().Draw(rt, "correlationID"),
		RecordingID:       rapid.Int64().Draw(rt, "recordingID"),
		StartTimestamp:    rapid.Int64().Draw(rt, "startTimestamp"),
		StopTimestamp:     rapid.Int64().Draw(rt, "stopTimestamp"),
		StartPosition:     rapid.Int64().Draw(rt, "startPosition"),
		StopPosition:      rapid.Int64().Draw(rt, "stopPosition"),
		InitialTermID:     rapid.Int32().Draw(rt, "initialTermID"),
		SegmentFileLength: rapid.Int32().Draw(rt, "segmentFileLength"),
		TermBufferLength:  rapid.Int32().Draw(rt, "termBufferLength"),
		MtuLength:         rapid.Int32().Draw(rt, "mtuLength"),
		SessionID:         rapid.Int32().Draw(rt, "sessionID"),
		StreamID:          rapid.Int32().Draw(rt, "streamID"),
		StrippedChannel:   rapidVarBytes(rt, "strippedChannel"),
		OriginalChannel:   rapidVarBytes(rt, "originalChannel"),
		SourceIdentity:    rapidVarBytes(rt, "sourceIdentity"),
	}
}

func rapidChallengeFields(rt *rapid.T) codecs.ChallengeFields {
	rt.Helper()
	return codecs.ChallengeFields{
		ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
		CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
		Version:          rapid.Int32().Draw(rt, "version"),
		EncodedChallenge: rapidVarBytes(rt, "encodedChallenge"),
	}
}

func rapidSourceLocation(rt *rapid.T) gensbe.SourceLocation {
	rt.Helper()
	return rapid.SampledFrom(archiveAllSourceLocations).Draw(rt, "sourceLocation")
}

func rapidBooleanType(rt *rapid.T) gensbe.BooleanType {
	rt.Helper()
	return rapid.SampledFrom(archiveAllBooleanTypes).Draw(rt, "autoStop")
}

func rapidControlResponseCode(rt *rapid.T) gensbe.ControlResponseCode {
	rt.Helper()
	return rapid.SampledFrom(archiveAllControlResponseCodes).Draw(rt, "code")
}

func rapidRecordingSignal(rt *rapid.T) gensbe.RecordingSignal {
	rt.Helper()
	return rapid.SampledFrom(archiveAllRecordingSignals).Draw(rt, "signal")
}

func TestConnectRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ConnectRequestFields{
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			ResponseStreamID: rapid.Int32().Draw(rt, "responseStreamID"),
			Version:          rapid.Int32().Draw(rt, "version"),
			ResponseChannel:  rapidVarBytes(rt, "responseChannel"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ConnectRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeConnectRequest),
			connectRequestOracle(fields, actingVersion),
		)
	})
}

func TestAuthConnectRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksVarHeavy, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.AuthConnectRequestFields{
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			ResponseStreamID:   rapid.Int32().Draw(rt, "responseStreamID"),
			Version:            rapid.Int32().Draw(rt, "version"),
			ResponseChannel:    rapidVarBytes(rt, "responseChannel"),
			EncodedCredentials: rapidVarBytes(rt, "encodedCredentials"),
			ClientInfo:         rapidVarBytes(rt, "clientInfo"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.AuthConnectRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeAuthConnectRequest),
			authConnectRequestOracle(fields, actingVersion),
		)
	})
}

func TestReplayRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksVarHeavy, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ReplayRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			RecordingID:      rapid.Int64().Draw(rt, "recordingID"),
			Position:         rapid.Int64().Draw(rt, "position"),
			Length:           rapid.Int64().Draw(rt, "length"),
			ReplayStreamID:   rapid.Int32().Draw(rt, "replayStreamID"),
			FileIoMaxLength:  rapid.Int32().Draw(rt, "fileIoMaxLength"),
			ReplayToken:      rapid.Int64().Draw(rt, "replayToken"),
			ReplayChannel:    rapidVarBytes(rt, "replayChannel"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ReplayRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeReplayRequest),
			replayRequestOracle(fields, actingVersion),
		)
	})
}

func TestStartRecordingRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.StartRecordingRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			StreamID:         rapid.Int32().Draw(rt, "streamID"),
			SourceLocation:   rapidSourceLocation(rt),
			Channel:          rapidVarBytes(rt, "channel"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.StartRecordingRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeStartRecordingRequest),
			fields,
		)
	})
}

func TestStartRecordingRequest2_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.StartRecordingRequest2Fields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			StreamID:         rapid.Int32().Draw(rt, "streamID"),
			SourceLocation:   rapidSourceLocation(rt),
			AutoStop:         rapidBooleanType(rt),
			Channel:          rapidVarBytes(rt, "channel"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.StartRecordingRequest2SbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeStartRecordingRequest2),
			fields,
		)
	})
}

func TestStopRecordingRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.StopRecordingRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			StreamID:         rapid.Int32().Draw(rt, "streamID"),
			Channel:          rapidVarBytes(rt, "channel"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.StopRecordingRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeStopRecordingRequest),
			fields,
		)
	})
}

func TestListRecordingRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ListRecordingRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			RecordingID:      rapid.Int64().Draw(rt, "recordingID"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ListRecordingRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeListRecordingRequest),
			fields,
		)
	})
}

func TestStopReplayRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.StopReplayRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
			ReplaySessionID:  rapid.Int64().Draw(rt, "replaySessionID"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.StopReplayRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeStopReplayRequest),
			fields,
		)
	})
}

func TestCloseSessionRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.CloseSessionRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.CloseSessionRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeCloseSessionRequest),
			fields,
		)
	})
}

func TestKeepAliveRequest_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.KeepAliveRequestFields{
			ControlSessionID: rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.KeepAliveRequestSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeKeepAliveRequest),
			fields,
		)
	})
}

func TestChallengeResponse_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		actingVersion, offset := rapidCodecContext(rt)
		fields := codecs.ChallengeResponseFields{
			ControlSessionID:   rapid.Int64().Draw(rt, "controlSessionID"),
			CorrelationID:      rapid.Int64().Draw(rt, "correlationID"),
			EncodedCredentials: rapidVarBytes(rt, "encodedCredentials"),
		}

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ChallengeResponseSbeTemplateID,
			actingVersion,
			offset,
			encodeAt(actingVersion, fields, codecs.EncodeChallengeResponse),
			fields,
		)
	})
}

func TestControlResponse_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksVarHeavy, func(rt *rapid.T) {
		offset := rapidCodecOffset(rt)
		fields := rapidControlResponseFields(rt)

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ControlResponseSbeTemplateID,
			archiveSchemaVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeControlResponseAt(buf, offset, fields)
			},
			fields,
		)
	})
}

func TestChallenge_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksVarHeavy, func(rt *rapid.T) {
		offset := rapidCodecOffset(rt)
		fields := rapidChallengeFields(rt)

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.ChallengeSbeTemplateID,
			archiveSchemaVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeChallengeAt(buf, offset, fields)
			},
			fields,
		)
	})
}

func TestRecordingSignalEvent_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		offset := rapidCodecOffset(rt)
		fields := rapidRecordingSignalEventFields(rt)

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.RecordingSignalEventSbeTemplateID,
			archiveSchemaVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeRecordingSignalEventAt(buf, offset, fields)
			},
			fields,
		)
	})
}

func TestRecordingDescriptor_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksVarHeavy, func(rt *rapid.T) {
		offset := rapidCodecOffset(rt)
		fields := rapidRecordingDescriptorFields(rt)

		runArchiveCodecRoundtripAtOffset(
			rt,
			gensbe.RecordingDescriptorSbeTemplateID,
			archiveSchemaVersion,
			offset,
			func(buf []byte, offset int) (int, bool) {
				return encodeRecordingDescriptorAt(buf, offset, fields)
			},
			fields,
		)
	})
}

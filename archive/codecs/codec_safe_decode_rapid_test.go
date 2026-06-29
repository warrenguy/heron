//go:build rapid

package codecs_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

func rapidParityWire[T any](
	rt *rapid.T,
	drawFields func(*rapid.T) T,
	encode func([]byte, int, T) (int, bool),
) (buf []byte, offset, maxVarLen int) {
	rt.Helper()
	maxVarLen = codecTestMaxVarLen
	if rapid.Bool().Draw(rt, "useValidWire") {
		offset = rapidCodecOffset(rt)
		return encodeArchiveWireAt(rt, offset, drawFields(rt), encode), offset, maxVarLen
	}
	buf, offset = rapidGarbageWire(rt)
	return buf, offset, maxVarLen
}

func runFastSlowParityRapid[T any](
	t *testing.T,
	checks int,
	drawFields func(*rapid.T) T,
	encode func([]byte, int, T) (int, bool),
	templateID uint16,
	fast func([]byte, int, int) (T, bool),
) {
	t.Helper()
	t.Parallel()
	rapidCheck(t, checks, func(rt *rapid.T) {
		buf, offset, maxVarLen := rapidParityWire(rt, drawFields, encode)
		requireFastSlowParity(rt, buf, offset, maxVarLen, templateID, fast)
	})
}

func TestControlResponse_fastSlowDecodeParity(t *testing.T) {
	runFastSlowParityRapid(
		t,
		rapidChecksVarHeavy,
		rapidControlResponseFields,
		encodeControlResponseAt,
		gensbe.ControlResponseSbeTemplateID,
		codecs.TryDecodeControlResponseAt,
	)
}

func TestRecordingSignalEvent_fastSlowDecodeParity(t *testing.T) {
	runFastSlowParityRapid(
		t,
		rapidChecksSBEDefault,
		rapidRecordingSignalEventFields,
		encodeRecordingSignalEventAt,
		gensbe.RecordingSignalEventSbeTemplateID,
		codecs.TryDecodeRecordingSignalEventAt,
	)
}

func TestRecordingDescriptor_fastSlowDecodeParity(t *testing.T) {
	runFastSlowParityRapid(
		t,
		rapidChecksVarHeavy,
		rapidRecordingDescriptorFields,
		encodeRecordingDescriptorAt,
		gensbe.RecordingDescriptorSbeTemplateID,
		codecs.TryDecodeRecordingDescriptorAt,
	)
}

func TestChallenge_fastSlowDecodeParity(t *testing.T) {
	runFastSlowParityRapid(
		t,
		rapidChecksVarHeavy,
		rapidChallengeFields,
		encodeChallengeAt,
		gensbe.ChallengeSbeTemplateID,
		codecs.TryDecodeChallengeAt,
	)
}

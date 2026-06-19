package clustersbe

import (
	"slices"

	"github.com/warrenguy/heron/aeron/sbe"
	gensbe "github.com/warrenguy/heron/aeron/sbe/generated/sbe"
)

// Cross-language hex contract: Java encode must match catalog.hex
// (make test-cluster-sbe-contract).
type WireFixture struct {
	Name          string
	TemplateID    uint16
	ActingVersion uint16
	Hex           string
	Fields        any
}

const oracleVarFieldSlot = sbe.ClusterCodecMaxVarLen + 8

// Callers must treat returned fixtures as read-only.
func Fixtures() []WireFixture {
	return slices.Clone(wireFixtures)
}

func ReencodeWire(fixture WireFixture) ([]byte, bool) {
	buf := make([]byte, oracleEncodeBufLen(fixture))
	wireEnd, ok := encodeFixture(buf, 0, fixture)
	if !ok {
		return nil, false
	}
	return buf[:wireEnd], true
}

// SessionEvent uses sbe.SessionEventOracleFields for heartbeat sentinel rules.
func ExpectedDecodeFields(fixture WireFixture) any {
	if fields, ok := fixture.Fields.(sbe.SessionEventFields); ok {
		return sbe.SessionEventOracleFields(fields, fixture.ActingVersion)
	}
	return fixture.Fields
}

func oracleEncodeBufLen(fixture WireFixture) int {
	varCount, _ := sbe.ClusterVarFieldCount(fixture.TemplateID, fixture.ActingVersion)
	rootLen := int(sbe.ClusterMinRootBlockLength(fixture.TemplateID, fixture.ActingVersion, 0))
	return sbe.MessageHeaderEncodedLen() + rootLen + varCount*oracleVarFieldSlot
}

func encodeStandard[T any](
	buf []byte,
	offset int,
	fixture WireFixture,
	encode func([]byte, int, uint16, T) (int, bool),
) (int, bool) {
	fields, ok := fixture.Fields.(T)
	if !ok {
		return 0, false
	}
	return encode(buf, offset, fixture.ActingVersion, fields)
}

func encodeSessionEvent(
	buf []byte,
	offset int,
	actingVersion uint16,
	fields sbe.SessionEventFields,
) (int, bool) {
	return sbe.EncodeSessionEvent(
		buf,
		offset,
		actingVersion,
		sbe.SessionEventBlockLength(actingVersion),
		fields,
	)
}

func encodeFixture(buf []byte, offset int, fixture WireFixture) (wireEnd int, ok bool) {
	switch fixture.TemplateID {
	case gensbe.SessionMessageHeaderSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeSessionMessageHeader)
	case gensbe.SessionEventSbeTemplateID:
		return encodeStandard(buf, offset, fixture, encodeSessionEvent)
	case gensbe.SessionConnectRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeSessionConnectRequest)
	case gensbe.SessionCloseRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeSessionCloseRequest)
	case gensbe.SessionKeepAliveSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeSessionKeepAlive)
	case gensbe.NewLeaderEventSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeNewLeaderEvent)
	case gensbe.ChallengeSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeChallenge)
	case gensbe.ChallengeResponseSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeChallengeResponse)
	case gensbe.AdminRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeAdminRequest)
	case gensbe.AdminResponseSbeTemplateID:
		return encodeStandard(buf, offset, fixture, sbe.EncodeAdminResponse)
	default:
		return 0, false
	}
}

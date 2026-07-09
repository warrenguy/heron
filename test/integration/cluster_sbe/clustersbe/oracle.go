package clustersbe

import (
	"slices"

	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

// Cross-language hex contract: Java encode must match catalog.hex
// (make test-cluster-sbe-contract).
type WireFixture struct {
	Name          string
	TemplateID    uint16
	ActingVersion uint16
	Hex           string
	Fields        any
	// LargeVar; see catalogschema.FixtureEntry.LargeVar.
	LargeVar bool
}

// Per-var TryDecodeByTemplate cap for oracle roundtrip.
func (fixture WireFixture) MaxVarLen() int {
	return catalogschema.MaxVarLenForLargeVar(fixture.LargeVar)
}

const oracleVarFieldSlot = codecs.ClusterCodecMaxVarLen + 8

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

// SessionEvent uses codecs.SessionEventOracleFields for heartbeat sentinel rules.
func ExpectedDecodeFields(fixture WireFixture) any {
	if fields, ok := fixture.Fields.(codecs.SessionEventFields); ok {
		return codecs.SessionEventOracleFields(fields, fixture.ActingVersion)
	}
	return fixture.Fields
}

func oracleEncodeBufLen(fixture WireFixture) int {
	if wireLen, ok := oracleExactWireLen(fixture); ok {
		return wireLen
	}

	varCount, _ := codecs.ClusterVarFieldCount(fixture.TemplateID, fixture.ActingVersion)
	rootLen := int(codecs.ClusterMinRootBlockLength(fixture.TemplateID, fixture.ActingVersion, 0))
	slot := oracleVarFieldSlot
	if fixture.LargeVar {
		slot = fixture.MaxVarLen() + codecs.VarLengthPrefixSize
	}
	return codecs.MessageHeaderEncodedLen() + rootLen + varCount*slot
}

func oracleExactWireLen(fixture WireFixture) (int, bool) {
	varLens, ok := oracleVarFieldLens(fixture)
	if !ok {
		return 0, false
	}
	blockLength := oracleRootBlockLength(fixture)
	return codecs.ClusterWireMessageLen(blockLength, varLens...)
}

func oracleRootBlockLength(fixture WireFixture) uint16 {
	switch fixture.TemplateID {
	case clusterwire.SessionEventSbeTemplateID:
		return codecs.SessionEventBlockLength(fixture.ActingVersion)
	default:
		return codecs.ClusterMinRootBlockLength(fixture.TemplateID, fixture.ActingVersion, 0)
	}
}

func oracleVarFieldLensFrom[T any](fields any, lens func(T) []int) ([]int, bool) {
	typed, ok := fields.(T)
	if !ok {
		return nil, false
	}
	return lens(typed), true
}

func oracleVarFieldLens(fixture WireFixture) ([]int, bool) {
	switch fixture.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID,
		clusterwire.SessionCloseRequestSbeTemplateID,
		clusterwire.SessionKeepAliveSbeTemplateID:
		return []int{}, true
	case clusterwire.SessionEventSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.SessionEventFields) []int {
			return []int{len(fields.Detail)}
		})
	case clusterwire.SessionConnectRequestSbeTemplateID:
		fields, ok := fixture.Fields.(codecs.SessionConnectRequestFields)
		if !ok {
			return nil, false
		}
		return codecs.SessionConnectRequestVarLens(
			fixture.ActingVersion,
			len(fields.ResponseChannel),
			len(fields.EncodedCredentials),
			len(fields.ClientInfo),
		)
	case clusterwire.NewLeaderEventSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.NewLeaderEventFields) []int {
			return []int{len(fields.IngressEndpoints)}
		})
	case clusterwire.ChallengeSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.ChallengeFields) []int {
			return []int{len(fields.EncodedChallenge)}
		})
	case clusterwire.ChallengeResponseSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.ChallengeResponseFields) []int {
			return []int{len(fields.EncodedCredentials)}
		})
	case clusterwire.AdminRequestSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.AdminRequestFields) []int {
			return []int{len(fields.Payload)}
		})
	case clusterwire.AdminResponseSbeTemplateID:
		return oracleVarFieldLensFrom(fixture.Fields, func(fields codecs.AdminResponseFields) []int {
			return []int{len(fields.Message), len(fields.Payload)}
		})
	default:
		return nil, false
	}
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
	fields codecs.SessionEventFields,
) (int, bool) {
	return codecs.EncodeSessionEvent(
		buf,
		offset,
		actingVersion,
		codecs.SessionEventBlockLength(actingVersion),
		fields,
	)
}

func encodeFixture(buf []byte, offset int, fixture WireFixture) (wireEnd int, ok bool) {
	switch fixture.TemplateID {
	case clusterwire.SessionMessageHeaderSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeSessionMessageHeader)
	case clusterwire.SessionEventSbeTemplateID:
		return encodeStandard(buf, offset, fixture, encodeSessionEvent)
	case clusterwire.SessionConnectRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeSessionConnectRequest)
	case clusterwire.SessionCloseRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeSessionCloseRequest)
	case clusterwire.SessionKeepAliveSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeSessionKeepAlive)
	case clusterwire.NewLeaderEventSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeNewLeaderEvent)
	case clusterwire.ChallengeSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeChallenge)
	case clusterwire.ChallengeResponseSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeChallengeResponse)
	case clusterwire.AdminRequestSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeAdminRequest)
	case clusterwire.AdminResponseSbeTemplateID:
		return encodeStandard(buf, offset, fixture, codecs.EncodeAdminResponse)
	default:
		return 0, false
	}
}

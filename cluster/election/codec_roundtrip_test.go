package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/election/nodewire"
)

const (
	nodeStateSchemaID      = nodewire.MessageHeaderSbeSchemaID
	nodeStateSchemaVersion = nodewire.MessageHeaderSbeSchemaVersion

	// exampleEpochMs is a representative node-state time_t fixture (epoch ms per XML).
	exampleEpochMs = int64(1_700_000_000_000)
)

func alignFrameLength(frameLength int) int {
	return atomicbuffer.Align(frameLength, 8)
}

type framedRequireT interface {
	require.TestingT
	Helper()
}

func candidateTermFramedFrameLength() int32 {
	var ct nodewire.CandidateTerm
	return int32(ct.SbeBlockAndHeaderLength())
}

func nodeStateFooterFramedFrameLength() int32 {
	var footer nodewire.NodeStateFooter
	return int32(footer.SbeBlockAndHeaderLength())
}

func requireFramedFrameLength(t framedRequireT, messageHeader nodewire.MessageHeader) {
	t.Helper()
	want := int32(nodewire.MessageHeaderEncodedLength + uint64(messageHeader.BlockLength()))
	require.Equal(t, want, messageHeader.FrameLength())
}

func encodeFramedCandidateTerm(
	buf []byte,
	offset uint64,
	candidateTermID, timestamp, logPosition int64,
) nodewire.MessageHeader {
	bufLen := uint64(len(buf))
	var ct nodewire.CandidateTerm
	var hdr nodewire.MessageHeader

	ct.WrapAndApplyHeader(buf, offset, bufLen).
		SetCandidateTermId(candidateTermID).
		SetTimestamp(timestamp).
		SetLogPosition(logPosition)

	hdr.Wrap(buf, offset, uint64(nodeStateSchemaVersion), bufLen)
	hdr.SetFrameLength(int32(ct.SbeBlockAndHeaderLength()))

	return hdr
}

func decodeFramedCandidateTerm(
	buf []byte,
	offset uint64,
) (nodewire.MessageHeader, nodewire.CandidateTerm) {
	bufLen := uint64(len(buf))
	var hdr nodewire.MessageHeader
	var ct nodewire.CandidateTerm

	hdr.Wrap(buf, offset, uint64(nodeStateSchemaVersion), bufLen)
	ct.WrapForDecode(
		buf,
		offset+hdr.EncodedLength(),
		uint64(hdr.BlockLength()),
		uint64(hdr.Version()),
		bufLen,
	)

	return hdr, ct
}

func encodeFramedNodeStateFooter(buf []byte, offset uint64) nodewire.MessageHeader {
	bufLen := uint64(len(buf))
	var footer nodewire.NodeStateFooter
	var hdr nodewire.MessageHeader

	footer.WrapAndApplyHeader(buf, offset, bufLen)

	hdr.Wrap(buf, offset, uint64(nodeStateSchemaVersion), bufLen)
	hdr.SetFrameLength(int32(footer.SbeBlockAndHeaderLength()))

	return hdr
}

func decodeFramedNodeStateFooter(
	buf []byte,
	offset uint64,
) (nodewire.MessageHeader, nodewire.NodeStateFooter) {
	bufLen := uint64(len(buf))
	var hdr nodewire.MessageHeader
	var footer nodewire.NodeStateFooter

	hdr.Wrap(buf, offset, uint64(nodeStateSchemaVersion), bufLen)
	footer.WrapForDecode(
		buf,
		offset+hdr.EncodedLength(),
		uint64(hdr.BlockLength()),
		uint64(hdr.Version()),
		bufLen,
	)

	return hdr, footer
}

func buildMiniLayout(
	t framedRequireT,
	buf []byte,
	fileVersion int32,
	candidateTermID, timestamp, logPosition int64,
) (candidateOffset, footerOffset uint64) {
	t.Helper()
	bufLen := uint64(len(buf))

	var nsh nodewire.NodeStateHeader
	nsh.WrapForEncode(buf, 0, bufLen).SetVersion(fileVersion).SetPadding(0)

	candidateOffset = nsh.SbePosition()
	candidateHeader := encodeFramedCandidateTerm(
		buf, candidateOffset, candidateTermID, timestamp, logPosition,
	)
	requireFramedFrameLength(t, candidateHeader)

	footerOffset = uint64(alignFrameLength(int(candidateOffset) + int(candidateHeader.FrameLength())))
	footerHeader := encodeFramedNodeStateFooter(buf, footerOffset)
	requireFramedFrameLength(t, footerHeader)

	return candidateOffset, footerOffset
}

func requireMiniLayout(
	t framedRequireT,
	buf []byte,
	startOffset uint64,
	candidateTermID, timestamp, logPosition int64,
) uint64 {
	t.Helper()

	candidateHeader, candidateTerm := decodeFramedCandidateTerm(buf, startOffset)
	requireFramedFrameLength(t, candidateHeader)
	require.Equal(t, nodewire.CandidateTermSbeTemplateID, candidateHeader.TemplateId())
	require.Equal(t, nodeStateSchemaID, candidateHeader.SchemaId())
	require.Equal(t, candidateTermID, candidateTerm.CandidateTermId())
	require.Equal(t, timestamp, candidateTerm.Timestamp())
	require.Equal(t, logPosition, candidateTerm.LogPosition())

	footerOffset := uint64(alignFrameLength(int(startOffset) + int(candidateHeader.FrameLength())))
	footerHeader, nodeStateFooter := decodeFramedNodeStateFooter(buf, footerOffset)
	requireFramedFrameLength(t, footerHeader)
	require.Equal(t, nodewire.NodeStateFooterSbeTemplateID, footerHeader.TemplateId())
	require.Equal(t, nodeStateSchemaID, footerHeader.SchemaId())
	require.Equal(t, uint16(0), nodeStateFooter.SbeBlockLength())

	return uint64(alignFrameLength(int(footerOffset) + int(footerHeader.FrameLength())))
}

func TestMessageHeader_roundtrip(t *testing.T) {
	require.Equal(t, uint64(16), nodewire.MessageHeaderEncodedLength)
	require.Equal(t, uint16(112), nodeStateSchemaID)
	require.Equal(t, uint16(10), nodeStateSchemaVersion)

	buf := make([]byte, nodewire.MessageHeaderEncodedLength)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 0, uint64(nodeStateSchemaVersion), uint64(len(buf)))
	hdr.SetBlockLength(nodewire.CandidateTermSbeBlockLength).
		SetTemplateId(nodewire.CandidateTermSbeTemplateID).
		SetSchemaId(nodeStateSchemaID).
		SetVersion(nodeStateSchemaVersion).
		SetFrameLength(candidateTermFramedFrameLength()).
		SetPadding(7)

	var decoded nodewire.MessageHeader
	decoded.Wrap(buf, 0, uint64(nodeStateSchemaVersion), uint64(len(buf)))

	require.Equal(t, nodewire.CandidateTermSbeBlockLength, decoded.BlockLength())
	require.Equal(t, nodewire.CandidateTermSbeTemplateID, decoded.TemplateId())
	require.Equal(t, nodeStateSchemaID, decoded.SchemaId())
	require.Equal(t, nodeStateSchemaVersion, decoded.Version())
	requireFramedFrameLength(t, decoded)
	require.Equal(t, int32(7), decoded.Padding())
}

func TestNodeStateHeader_roundtrip(t *testing.T) {
	require.Equal(t, uint16(8), nodewire.NodeStateHeaderSbeBlockLength)
	require.Equal(t, uint16(300), nodewire.NodeStateHeaderSbeTemplateID)

	const version = int32(0x05030000)
	buf := make([]byte, 64)
	bufLen := uint64(len(buf))

	var nsh nodewire.NodeStateHeader
	nsh.WrapForEncode(buf, 0, bufLen).SetVersion(version).SetPadding(0)
	require.Equal(t, uint64(8), nsh.SbePosition())

	var prefixOnly nodewire.NodeStateHeader
	prefixOnly.WrapForEncode(make([]byte, 8), 0, 8).SetVersion(version).SetPadding(0)
	require.Equal(t, buf[:8], prefixOnly.Buffer()[:8])

	var probe nodewire.MessageHeader
	probe.Wrap(buf, 0, uint64(nodeStateSchemaVersion), bufLen)
	require.NotEqual(t, nodeStateSchemaID, probe.SchemaId())

	var decoded nodewire.NodeStateHeader
	decoded.WrapForDecode(buf, 0, uint64(nsh.SbeBlockLength()), uint64(nodeStateSchemaVersion), bufLen)
	require.Equal(t, version, decoded.Version())
	require.Equal(t, int32(0), decoded.Padding())
}

func TestCandidateTerm_roundtrip(t *testing.T) {
	const (
		candidateTermID = int64(42)
		logPosition     = int64(8192)
	)
	offset := uint64(8)
	buf := make([]byte, 128)

	candidateHeader := encodeFramedCandidateTerm(
		buf, offset, candidateTermID, exampleEpochMs, logPosition,
	)
	requireFramedFrameLength(t, candidateHeader)

	decodedHeader, candidateTerm := decodeFramedCandidateTerm(buf, offset)
	requireFramedFrameLength(t, decodedHeader)
	require.Equal(t, nodewire.CandidateTermSbeTemplateID, decodedHeader.TemplateId())
	require.Equal(t, candidateTermID, candidateTerm.CandidateTermId())
	require.Equal(t, exampleEpochMs, candidateTerm.Timestamp())
	require.Equal(t, logPosition, candidateTerm.LogPosition())
}

func TestNodeStateFooter_roundtrip(t *testing.T) {
	offset := uint64(0)
	buf := make([]byte, 32)

	footerHeader := encodeFramedNodeStateFooter(buf, offset)
	requireFramedFrameLength(t, footerHeader)

	decodedHeader, nodeStateFooter := decodeFramedNodeStateFooter(buf, offset)
	requireFramedFrameLength(t, decodedHeader)
	require.Equal(t, nodewire.NodeStateFooterSbeTemplateID, decodedHeader.TemplateId())
	require.Equal(t, uint16(0), nodeStateFooter.SbeBlockLength())
}

func TestFramedCandidateTermHelpers_tooShortBufferPanics(t *testing.T) {
	shortBuf := make([]byte, 8)

	require.Panics(t, func() {
		encodeFramedCandidateTerm(shortBuf, 0, 1, exampleEpochMs, 2)
	})
	require.Panics(t, func() {
		decodeFramedCandidateTerm(shortBuf, 0)
	})
}

func TestFramedNodeStateFooterHelpers_tooShortBufferPanics(t *testing.T) {
	shortBuf := make([]byte, 8)

	require.Panics(t, func() {
		encodeFramedNodeStateFooter(shortBuf, 0)
	})
	require.Panics(t, func() {
		decodeFramedNodeStateFooter(shortBuf, 0)
	})
}

func TestNodeStateMiniFileLayout_roundtrip(t *testing.T) {
	const (
		fileVersion     = int32(0x05030000)
		candidateTermID = int64(-1)
		timestamp       = int64(-1)
		logPosition     = int64(-1)
	)

	buf := make([]byte, 64)
	bufLen := uint64(len(buf))

	candidateOffset, footerOffset := buildMiniLayout(
		t, buf, fileVersion, candidateTermID, timestamp, logPosition,
	)
	require.Equal(t, uint64(8), candidateOffset)
	expectedFooterOffset := uint64(alignFrameLength(
		int(candidateOffset) + int(candidateTermFramedFrameLength()),
	))
	require.Equal(t, expectedFooterOffset, footerOffset)

	var decodedNsh nodewire.NodeStateHeader
	decodedNsh.WrapForDecode(
		buf, 0, uint64(nodewire.NodeStateHeaderSbeBlockLength), uint64(nodeStateSchemaVersion), bufLen,
	)
	require.Equal(t, fileVersion, decodedNsh.Version())

	offset := requireMiniLayout(t, buf, candidateOffset, candidateTermID, timestamp, logPosition)
	expectedEndOffset := uint64(alignFrameLength(
		int(footerOffset) + int(nodeStateFooterFramedFrameLength()),
	))
	require.Equal(t, expectedEndOffset, offset)
}

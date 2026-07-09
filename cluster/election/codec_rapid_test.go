//go:build rapid

package election_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election/nodewire"
)

func TestMessageHeader_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		blockLength := uint16(rapid.IntRange(0, 65534).Draw(rt, "blockLength"))
		templateID := uint16(rapid.IntRange(0, 65534).Draw(rt, "templateId"))
		schemaID := uint16(rapid.IntRange(0, 65534).Draw(rt, "schemaId"))
		version := uint16(rapid.IntRange(0, 16).Draw(rt, "version"))
		frameLength := rapid.Int32().Draw(rt, "frameLength")
		padding := rapid.Int32().Draw(rt, "padding")
		offset := uint64(rapid.IntRange(0, 8).Draw(rt, "offset"))

		buf := make([]byte, int(offset)+int(nodewire.MessageHeaderEncodedLength))
		bufLen := uint64(len(buf))

		var hdr nodewire.MessageHeader
		hdr.Wrap(buf, offset, uint64(version), bufLen)
		hdr.SetBlockLength(blockLength).
			SetTemplateId(templateID).
			SetSchemaId(schemaID).
			SetVersion(version).
			SetFrameLength(frameLength).
			SetPadding(padding)

		var decoded nodewire.MessageHeader
		decoded.Wrap(buf, offset, uint64(version), bufLen)
		require.Equal(rt, blockLength, decoded.BlockLength())
		require.Equal(rt, templateID, decoded.TemplateId())
		require.Equal(rt, schemaID, decoded.SchemaId())
		require.Equal(rt, version, decoded.Version())
		require.Equal(rt, frameLength, decoded.FrameLength())
		require.Equal(rt, padding, decoded.Padding())
	})
}

func TestNodeStateHeader_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		version := rapid.Int32().Draw(rt, "version")
		padding := rapid.Int32().Draw(rt, "padding")
		offset := uint64(rapid.IntRange(0, 8).Draw(rt, "offset"))

		buf := make([]byte, int(offset)+int(nodewire.NodeStateHeaderSbeBlockLength))
		bufLen := uint64(len(buf))

		var nsh nodewire.NodeStateHeader
		nsh.WrapForEncode(buf, offset, bufLen).SetVersion(version).SetPadding(padding)

		var decoded nodewire.NodeStateHeader
		decoded.WrapForDecode(
			buf,
			offset,
			uint64(nsh.SbeBlockLength()),
			uint64(nodeStateSchemaVersion),
			bufLen,
		)
		require.Equal(rt, version, decoded.Version())
		require.Equal(rt, padding, decoded.Padding())
	})
}

func TestNodeStateFooter_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		offset := uint64(rapid.IntRange(0, 16).Draw(rt, "offset"))
		buf := make([]byte, int(offset)+32)

		footerHeader := encodeFramedNodeStateFooter(buf, offset)
		requireFramedFrameLength(rt, footerHeader)

		decodedHeader, nodeStateFooter := decodeFramedNodeStateFooter(buf, offset)
		requireFramedFrameLength(rt, decodedHeader)
		require.Equal(rt, nodewire.NodeStateFooterSbeTemplateID, decodedHeader.TemplateId())
		require.Equal(rt, uint16(0), nodeStateFooter.SbeBlockLength())
	})
}

func TestCandidateTerm_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		candidateTermID := rapid.Int64().Draw(rt, "candidateTermId")
		timestamp := rapid.Int64().Draw(rt, "timestamp")
		logPosition := rapid.Int64().Draw(rt, "logPosition")
		offset := uint64(rapid.IntRange(0, 16).Draw(rt, "offset"))

		buf := make([]byte, int(offset)+64)
		candidateHeader := encodeFramedCandidateTerm(buf, offset, candidateTermID, timestamp, logPosition)
		requireFramedFrameLength(rt, candidateHeader)

		decodedHeader, candidateTerm := decodeFramedCandidateTerm(buf, offset)
		requireFramedFrameLength(rt, decodedHeader)
		require.Equal(rt, candidateTermID, candidateTerm.CandidateTermId())
		require.Equal(rt, timestamp, candidateTerm.Timestamp())
		require.Equal(rt, logPosition, candidateTerm.LogPosition())
	})
}

func TestNodeStateMiniLayout_codec_rapid(t *testing.T) {
	t.Parallel()
	rapidCheck(t, rapidChecksSBEDefault, func(rt *rapid.T) {
		fileVersion := rapid.Int32().Draw(rt, "fileVersion")
		candidateTermID := rapid.Int64().Draw(rt, "candidateTermId")
		timestamp := rapid.Int64().Draw(rt, "timestamp")
		logPosition := rapid.Int64().Draw(rt, "logPosition")

		buf := make([]byte, 128)
		bufLen := uint64(len(buf))

		candidateOffset, footerOffset := buildMiniLayout(
			rt, buf, fileVersion, candidateTermID, timestamp, logPosition,
		)

		var decodedNsh nodewire.NodeStateHeader
		decodedNsh.WrapForDecode(
			buf, 0, uint64(nodewire.NodeStateHeaderSbeBlockLength), uint64(nodeStateSchemaVersion), bufLen,
		)
		require.Equal(rt, fileVersion, decodedNsh.Version())

		offset := requireMiniLayout(rt, buf, candidateOffset, candidateTermID, timestamp, logPosition)
		expectedEndOffset := uint64(alignFrameLength(
			int(footerOffset) + int(nodeStateFooterFramedFrameLength()),
		))
		require.Equal(rt, expectedEndOffset, offset)
	})
}

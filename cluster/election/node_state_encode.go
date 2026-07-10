package election

import (
	"fmt"

	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/election/nodewire"
)

func alignRecordOffset(off int) int {
	return atomicbuffer.Align(off, NodeStateRecordAlignment)
}

func candidateTermOffsets(headerPos int) (bodyOff, termOffset int, err error) {
	bodyOff = headerPos + int(nodewire.MessageHeaderEncodedLength)
	var ct nodewire.CandidateTerm
	termOffset = bodyOff + int(ct.CandidateTermIdEncodingOffset())
	if termOffset%NodeStateRecordAlignment != 0 {
		return 0, 0, ErrCorruptNodeState
	}
	return bodyOff, termOffset, nil
}

func encodeFramedCandidateTerm(
	buf []byte,
	offset int,
	id, ts, logPos int64,
) error {
	minLen := int(nodewire.MessageHeaderEncodedLength) + int(nodewire.CandidateTermSbeBlockLength)
	if offset < 0 || offset+minLen > len(buf) {
		return fmt.Errorf("%w: candidate term buffer too short", ErrCorruptNodeState)
	}

	bufLen := uint64(len(buf))
	var ct nodewire.CandidateTerm
	var hdr nodewire.MessageHeader

	ct.WrapAndApplyHeader(buf, uint64(offset), bufLen).
		SetCandidateTermId(id).
		SetTimestamp(ts).
		SetLogPosition(logPos)

	hdr.Wrap(buf, uint64(offset), uint64(nodewire.MessageHeaderSbeSchemaVersion), bufLen)
	hdr.SetFrameLength(int32(ct.SbeBlockAndHeaderLength()))

	return nil
}

func encodeFramedNodeStateFooter(buf []byte, offset int) error {
	minLen := int(nodewire.MessageHeaderEncodedLength) + int(nodewire.NodeStateFooterSbeBlockLength)
	if offset < 0 || offset+minLen > len(buf) {
		return fmt.Errorf("%w: node state footer buffer too short", ErrCorruptNodeState)
	}

	bufLen := uint64(len(buf))
	var footer nodewire.NodeStateFooter
	var hdr nodewire.MessageHeader

	footer.WrapAndApplyHeader(buf, uint64(offset), bufLen)

	hdr.Wrap(buf, uint64(offset), uint64(nodewire.MessageHeaderSbeSchemaVersion), bufLen)
	hdr.SetFrameLength(int32(footer.SbeBlockAndHeaderLength()))

	return nil
}

func initialiseOnCreate(
	buf []byte,
	version int32,
) (candidateTermOffset, candidateTermBodyOff int, err error) {
	minLayoutLen := int(nodewire.NodeStateHeaderSbeBlockLength) +
		int(nodewire.MessageHeaderEncodedLength) +
		int(nodewire.CandidateTermSbeBlockLength) +
		int(nodewire.MessageHeaderEncodedLength)
	if len(buf) < minLayoutLen {
		return 0, 0, fmt.Errorf("%w: buffer too short for node state file", ErrCorruptNodeState)
	}

	bufLen := uint64(len(buf))
	var nsh nodewire.NodeStateHeader
	nsh.WrapForEncode(buf, 0, bufLen).SetVersion(version).SetPadding(0)

	candidateHeaderOffset := int(nsh.SbePosition())
	if err := encodeFramedCandidateTerm(
		buf, candidateHeaderOffset, NullValue, NullValue, NullValue,
	); err != nil {
		return 0, 0, err
	}

	footerOffset := alignRecordOffset(candidateHeaderOffset + int(candidateTermFramedLength()))
	if err := encodeFramedNodeStateFooter(buf, footerOffset); err != nil {
		return 0, 0, err
	}

	candidateTermBodyOff, candidateTermOffset, err = candidateTermOffsets(candidateHeaderOffset)
	if err != nil {
		return 0, 0, err
	}

	// Publish candidateTermId last with release store (matches Java putLongVolatile);
	// term id is the volatile sentinel for reads.
	atomicbuffer.Wrap(buf).PutInt64Release(candidateTermOffset, NullValue)
	return candidateTermOffset, candidateTermBodyOff, nil
}

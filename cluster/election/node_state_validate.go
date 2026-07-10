package election

import (
	"github.com/warrenguy/heron/cluster/election/nodewire"
)

func candidateTermFramedLength() int32 {
	var ct nodewire.CandidateTerm
	return int32(ct.SbeBlockAndHeaderLength())
}

func nodeStateFooterFramedLength() int32 {
	var footer nodewire.NodeStateFooter
	return int32(footer.SbeBlockAndHeaderLength())
}

func expectedFramedLength(templateID uint16) (int32, bool) {
	switch templateID {
	case nodewire.CandidateTermSbeTemplateID:
		return candidateTermFramedLength(), true
	case nodewire.NodeStateFooterSbeTemplateID:
		return nodeStateFooterFramedLength(), true
	default:
		return 0, false
	}
}

func validateMessageHeaderBounds(buf []byte, headerPos int) bool {
	return headerPos >= 0 &&
		headerPos+int(nodewire.MessageHeaderEncodedLength) <= len(buf)
}

func validateFramedRecord(buf []byte, headerPos int, templateID uint16) error {
	if !validateMessageHeaderBounds(buf, headerPos) {
		return ErrCorruptNodeState
	}

	bufLen := uint64(len(buf))
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, uint64(headerPos), uint64(nodewire.MessageHeaderSbeSchemaVersion), bufLen)

	if hdr.TemplateId() != templateID {
		return ErrCorruptNodeState
	}

	frameLength := hdr.FrameLength()
	if frameLength <= 0 {
		return ErrCorruptNodeState
	}

	wantLength, ok := expectedFramedLength(templateID)
	if !ok || frameLength != wantLength {
		return ErrCorruptNodeState
	}

	recordEnd := headerPos + int(frameLength)
	if recordEnd > len(buf) {
		return ErrCorruptNodeState
	}

	return nil
}

func validateLoadedLayout(buf []byte, candidateHeaderPos int) error {
	if err := validateFramedRecord(
		buf,
		candidateHeaderPos,
		nodewire.CandidateTermSbeTemplateID,
	); err != nil {
		return err
	}

	footerScanStart := alignRecordOffset(candidateHeaderPos + int(candidateTermFramedLength()))

	footerPos, ok, err := scanForTemplateOffset(
		buf,
		footerScanStart,
		nodewire.NodeStateFooterSbeTemplateID,
	)
	if err != nil {
		return err
	}
	if !ok {
		return ErrCorruptNodeState
	}
	return validateFramedRecord(buf, footerPos, nodewire.NodeStateFooterSbeTemplateID)
}

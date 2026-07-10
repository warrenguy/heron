package election

import (
	"github.com/warrenguy/heron/cluster/election/nodewire"
)

func scanForTemplateOffset(
	buf []byte,
	startPos int,
	wantTemplateID uint16,
) (headerPos int, ok bool, err error) {
	position := alignRecordOffset(startPos)
	schemaVersion := uint64(nodewire.MessageHeaderSbeSchemaVersion)
	bufLen := uint64(len(buf))

	for position+int(nodewire.MessageHeaderEncodedLength) <= len(buf) {
		var hdr nodewire.MessageHeader
		hdr.Wrap(buf, uint64(position), schemaVersion, bufLen)

		templateID := hdr.TemplateId()
		if templateID == wantTemplateID {
			if err := validateFramedRecord(buf, position, wantTemplateID); err != nil {
				return 0, false, err
			}
			return position, true, nil
		}
		if templateID == nodewire.NodeStateFooterSbeTemplateID {
			return 0, false, nil
		}

		frameLength := hdr.FrameLength()
		if frameLength < 0 {
			return 0, false, ErrCorruptNodeState
		}
		if frameLength == 0 {
			return 0, false, nil
		}

		next := alignRecordOffset(position + int(frameLength))
		if next <= position {
			return 0, false, ErrCorruptNodeState
		}
		position = next
	}

	return 0, false, nil
}

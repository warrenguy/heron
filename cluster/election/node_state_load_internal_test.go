package election

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/cluster/election/nodewire"
)

func TestValidateFramedRecord_rejectsTruncatedCandidateTermBody(t *testing.T) {
	buf := make([]byte, 40)
	var hdr nodewire.MessageHeader
	hdr.Wrap(buf, 8, uint64(nodewire.MessageHeaderSbeSchemaVersion), uint64(len(buf)))
	hdr.SetBlockLength(nodewire.CandidateTermSbeBlockLength).
		SetTemplateId(nodewire.CandidateTermSbeTemplateID).
		SetSchemaId(nodewire.CandidateTermSbeSchemaID).
		SetVersion(nodewire.CandidateTermSbeSchemaVersion).
		SetFrameLength(candidateTermFramedLength())

	err := validateFramedRecord(buf, 8, nodewire.CandidateTermSbeTemplateID)
	require.ErrorIs(t, err, ErrCorruptNodeState)
}

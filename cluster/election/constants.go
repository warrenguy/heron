package election

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	// NullValue is Aeron NULL_VALUE for unset term and recording ids.
	NullValue = cnc.NullValue

	// NullMemberID is NullValue as int32 for member and session ids.
	NullMemberID int32 = int32(NullValue)

	// NullPosition marks an uncommitted log position.
	NullPosition = recordinglog.NullPosition

	NodeStateFileName = "node-state.dat"

	// MinimumNodeStateFileLength matches Java NodeStateFile.MINIMUM_FILE_LENGTH (1 MiB).
	MinimumNodeStateFileLength = 1 << 20

	// NodeStateRecordAlignment matches Agrona ALIGNMENT / atomicbuffer 8-byte alignment.
	NodeStateRecordAlignment = 8
)

// NodeStateSemanticVersion is the node-state file ClusterMarkFile SEMANTIC_VERSION.
var NodeStateSemanticVersion = cnc.ComposeSemanticVersion(0, 3, 0)

// ProtocolSemanticVersion is the consensus protocol PROTOCOL_SEMANTIC_VERSION.
func ProtocolSemanticVersion() int32 {
	return cnc.ComposeSemanticVersion(0, 3, 0)
}

package election

import (
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

const (
	// NullValue is Aeron NULL_VALUE for unset term/session ids.
	NullValue = cnc.NullValue

	// NullPosition marks an uncommitted log position.
	NullPosition = recordinglog.NullPosition

	NodeStateFileName = "node-state.dat"

	// MinimumNodeStateFileLength matches Java NodeStateFile.MINIMUM_FILE_LENGTH (1 MiB).
	MinimumNodeStateFileLength = 1 << 20

	// NodeStateRecordAlignment matches Agrona ALIGNMENT / atomicbuffer 8-byte alignment.
	NodeStateRecordAlignment = 8
)

// NodeStateSemanticVersion is ClusterMarkFile.SEMANTIC_VERSION (0.3.0) for Aeron Cluster 1.52.0.
var NodeStateSemanticVersion = cnc.ComposeSemanticVersion(0, 3, 0)

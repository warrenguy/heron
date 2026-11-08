//go:build testhooks

package codecs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
)

// EncodeCommitPositionForTest returns a CommitPosition wire message at the
// current schema version.
func EncodeCommitPositionForTest(tb testing.TB, fields CommitPositionFields) []byte {
	tb.Helper()
	wireLen, ok := ClusterWireMessageLen(clusterwire.CommitPositionSbeBlockLength)
	require.True(tb, ok)
	buf := make([]byte, wireLen)
	wireEnd, ok := EncodeCommitPosition(
		buf,
		0,
		clusterwire.CommitPositionSbeSchemaVersion,
		fields,
	)
	require.True(tb, ok)
	return buf[:wireEnd]
}

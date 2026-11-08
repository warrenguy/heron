//go:build testhooks

package codecs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"luas.fast/cluster/codecs"
	"luas.fast/cluster/codecs/clusterwire"
)

func TestEncodeCommitPositionForTest_roundtrip(t *testing.T) {
	fields := codecs.CommitPositionFields{LeadershipTermID: 1, LogPosition: 2, LeaderMemberID: 3}
	wire := codecs.EncodeCommitPositionForTest(t, fields)
	requireEncodedLengthMatchesWire(
		t,
		wire,
		0,
		len(wire),
		clusterwire.CommitPositionSbeTemplateID,
		clusterwire.CommitPositionSbeSchemaVersion,
	)

	got, ok := codecs.TryDecodeCommitPositionAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, fields, got)
}

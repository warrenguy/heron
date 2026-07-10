package election_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireNodeStatePrefixHex(t *testing.T, buf []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(buf), n)
	t.Logf("node-state prefix: %x", buf[:n])
}

func requireNodeStatePrefixEqual(t *testing.T, got, want []byte, n int) {
	t.Helper()
	require.GreaterOrEqual(t, len(got), n)
	require.GreaterOrEqual(t, len(want), n)
	want = want[:n]
	requireNodeStatePrefixHex(t, got, n)
	require.Equal(t, want, got[:n])
}

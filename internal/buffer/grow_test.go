package buffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/internal/buffer"
)

func TestGrowScratch_reuseWhenCapSufficient(t *testing.T) {
	t.Parallel()

	base := make([]byte, 0, 64)
	got := buffer.GrowScratch(base, 32)
	require.Len(t, got, 32)
	require.Equal(t, 64, cap(got))
	require.Same(t, &base[:cap(base)][0], &got[0])
}

func TestGrowScratch_doublesWhenTotalWithinTwiceCap(t *testing.T) {
	t.Parallel()

	base := make([]byte, 0, 16)
	got := buffer.GrowScratch(base, 20)
	require.Len(t, got, 20)
	require.Equal(t, 32, cap(got))
}

func TestGrowScratch_jumpsToTotalWhenAboveTwiceCap(t *testing.T) {
	t.Parallel()

	base := make([]byte, 0, 8)
	got := buffer.GrowScratch(base, 100)
	require.Len(t, got, 100)
	require.Equal(t, 100, cap(got))
}

func TestGrowScratch_nilScratch(t *testing.T) {
	t.Parallel()

	got := buffer.GrowScratch(nil, 7)
	require.Len(t, got, 7)
	require.Equal(t, 7, cap(got))
}

func TestGrowScratch_zeroTotal(t *testing.T) {
	t.Parallel()

	base := make([]byte, 0, 8)
	got := buffer.GrowScratch(base, 0)
	require.Empty(t, got)
	require.Equal(t, 8, cap(got))
}

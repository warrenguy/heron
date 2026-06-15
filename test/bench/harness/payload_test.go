//go:build bench

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterPayloadSizes(t *testing.T) {
	allowed := map[int]struct{}{
		PayloadSize32:   {},
		PayloadSize1400: {},
	}
	require.Equal(t, []int{32, 1400}, filterPayloadSizes([]int{32, 64, 1400, 4000}, allowed))
}

func TestIsSupportedBenchPayloadSize(t *testing.T) {
	require.True(t, isSupportedBenchPayloadSize(PayloadSize32))
	require.False(t, isSupportedBenchPayloadSize(17))
}

func TestBenchPayloadSizeFilterFromEnv(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "32, 1400")
	got := benchPayloadSizeFilterFromEnv()
	require.Len(t, got, 2)
	_, ok := got[32]
	require.True(t, ok)
	_, ok = got[1400]
	require.True(t, ok)
}

func TestBenchPayloadSizeFilterFromEnv_unset(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "")
	require.Nil(t, benchPayloadSizeFilterFromEnv())
}

func TestBenchPayloadSizeFilterFromEnv_invalid(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "nope")
	require.Panics(t, func() {
		benchPayloadSizeFilterFromEnv()
	})
}

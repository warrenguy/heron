//go:build bench

package harness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func payloadSizeSet(sizes ...int) map[int]struct{} {
	allowed := make(map[int]struct{}, len(sizes))
	for _, size := range sizes {
		allowed[size] = struct{}{}
	}
	return allowed
}

func TestFilterPayloadSizes(t *testing.T) {
	allowed := payloadSizeSet(PayloadSize32, PayloadSize1400)
	require.Equal(
		t,
		[]int{PayloadSize32, PayloadSize1400},
		filterPayloadSizes(
			[]int{PayloadSize32, PayloadSize64, PayloadSize1400, PayloadSize4000},
			allowed,
		),
	)
}

func TestIsSupportedBenchPayloadSize(t *testing.T) {
	require.True(t, isSupportedBenchPayloadSize(PayloadSize32))
	require.False(t, isSupportedBenchPayloadSize(17))
}

func TestBenchPayloadSizeFilterFromEnv(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "32, 1400")
	require.Equal(t, payloadSizeSet(PayloadSize32, PayloadSize1400), benchPayloadSizeFilterFromEnv())
}

func TestBenchPayloadSizeFilterFromEnv_unset(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "")
	require.Nil(t, benchPayloadSizeFilterFromEnv())
}

func TestExclusiveOfferPayloadSizes_unfiltered(t *testing.T) {
	require.Equal(t, allExclusiveOfferPayloadSizes, ExclusiveOfferPayloadSizes())
}

func TestExclusiveOfferPayloadSizeFilter_ciSubset(t *testing.T) {
	allowed := payloadSizeSet(PayloadSize32, PayloadSize1400)
	require.Equal(t, []int{PayloadSize32}, filterPayloadSizes(ExclusiveOfferPayloadSizes(), allowed))
}

func TestExclusiveOfferPayloadSizesForBenchFrom_fragmentedOnlyReturnsEmpty(t *testing.T) {
	allowed := payloadSizeSet(PayloadSize1400)
	require.Empty(t, exclusiveOfferPayloadSizesForBenchFrom(allowed))
}

func TestBenchPayloadSizeFilterFromEnv_invalid(t *testing.T) {
	t.Setenv("BENCH_PAYLOAD_SIZES", "nope")
	require.Panics(t, func() {
		benchPayloadSizeFilterFromEnv()
	})
}

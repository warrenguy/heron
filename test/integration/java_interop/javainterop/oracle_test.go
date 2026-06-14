//go:build integration && java_interop

package javainterop_test

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/test/integration/java_interop/javainterop"
)

func TestOfferStallTimeout_table(t *testing.T) {
	tests := []struct {
		name        string
		timeout     time.Duration
		wantStallMs int64
	}{
		{name: "tenSecondTimeout", timeout: 10 * time.Second, wantStallMs: 2_000},
		{name: "fiveSecondTimeout", timeout: 5 * time.Second, wantStallMs: 1_000},
		{name: "threeSecondTimeout", timeout: 3 * time.Second, wantStallMs: 600},
		{name: "subFiveSecondNoFloor", timeout: 4_999 * time.Millisecond, wantStallMs: 999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := javainterop.OfferStallTimeout(tt.timeout)
			require.Equal(t, tt.wantStallMs, got.Milliseconds())
		})
	}
}

func javaPayloadGoldenCases() []goldenCase {
	return []goldenCase{
		{
			name:    "index0",
			index:   0,
			wantHex: "6a6176612d300000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index1",
			index:   1,
			wantHex: "6a6176612d310000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index10",
			index:   10,
			wantHex: "6a6176612d313000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index31",
			index:   31,
			wantHex: "6a6176612d333100000000000000000000000000000000000000000000000000",
		},
	}
}

func goPayloadGoldenCases() []goldenCase {
	return []goldenCase{
		{
			name:    "index0",
			index:   0,
			wantHex: "676f2d3000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index1",
			index:   1,
			wantHex: "676f2d3100000000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index10",
			index:   10,
			wantHex: "676f2d3130000000000000000000000000000000000000000000000000000000",
		},
		{
			name:    "index31",
			index:   31,
			wantHex: "676f2d3331000000000000000000000000000000000000000000000000000000",
		},
	}
}

type goldenCase struct {
	name    string
	index   int
	wantHex string
}

func TestStreams_duplex(t *testing.T) {
	require.Equal(t, int32(1100), javainterop.StreamJavaToGo)
	require.Equal(t, int32(1101), javainterop.StreamGoToJava)
}

func TestPayloads_goldenBytes(t *testing.T) {
	for _, leg := range []struct {
		name  string
		build func(count, size int) [][]byte
		cases []goldenCase
	}{
		{
			name:  "java",
			build: javainterop.Payloads,
			cases: javaPayloadGoldenCases(),
		},
		{
			name:  "go",
			build: javainterop.GoPayloads,
			cases: goPayloadGoldenCases(),
		},
	} {
		t.Run(leg.name, func(t *testing.T) {
			payloads := leg.build(javainterop.MessageCount, javainterop.PayloadSize)
			for _, tt := range leg.cases {
				t.Run(tt.name, func(t *testing.T) {
					require.Equal(t, tt.wantHex, hex.EncodeToString(payloads[tt.index]))
				})
			}
		})
	}
}

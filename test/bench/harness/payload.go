//go:build bench

package harness

import (
	"os"
	"strconv"
	"strings"
)

const (
	PayloadSize32   = 32
	PayloadSize64   = 64
	PayloadSize256  = 256
	PayloadSize512  = 512
	PayloadSize1024 = 1024
	PayloadSize1400 = 1400
	PayloadSize4000 = 4000
	PayloadSize8192 = 8192
)

var (
	Payload32   [PayloadSize32]byte
	Payload64   [PayloadSize64]byte
	Payload256  [PayloadSize256]byte
	Payload512  [PayloadSize512]byte
	Payload1024 [PayloadSize1024]byte
	Payload1400 [PayloadSize1400]byte
	Payload4000 [PayloadSize4000]byte
	Payload8192 [PayloadSize8192]byte

	benchPayloads = []struct {
		size       int
		data       []byte
		e2e        bool
		fragmented bool
	}{
		{PayloadSize32, Payload32[:], true, false},
		{PayloadSize64, Payload64[:], false, false},
		{PayloadSize256, Payload256[:], true, false},
		{PayloadSize512, Payload512[:], false, false},
		{PayloadSize1024, Payload1024[:], true, false},
		{PayloadSize1400, Payload1400[:], true, true},
		{PayloadSize4000, Payload4000[:], true, true},
		{PayloadSize8192, Payload8192[:], true, true},
	}
	offerPayloadSizes    []int
	endToEndPayloadSizes []int
)

func init() {
	for _, entry := range benchPayloads {
		offerPayloadSizes = append(offerPayloadSizes, entry.size)
		if entry.e2e {
			endToEndPayloadSizes = append(endToEndPayloadSizes, entry.size)
		}
	}
	if allowed := benchPayloadSizeFilterFromEnv(); allowed != nil {
		offerPayloadSizes = filterPayloadSizes(offerPayloadSizes, allowed)
		endToEndPayloadSizes = filterPayloadSizes(endToEndPayloadSizes, allowed)
		if len(offerPayloadSizes) == 0 || len(endToEndPayloadSizes) == 0 {
			panic("harness: BENCH_PAYLOAD_SIZES excludes all offer or end-to-end payloads")
		}
	}
}

func benchPayloadSizeFilterFromEnv() map[int]struct{} {
	raw := strings.TrimSpace(os.Getenv("BENCH_PAYLOAD_SIZES"))
	if raw == "" {
		return nil
	}
	allowed := make(map[int]struct{})
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		size, err := strconv.Atoi(part)
		if err != nil {
			panic("harness: BENCH_PAYLOAD_SIZES entry " + part + " is not an integer")
		}
		if !isSupportedBenchPayloadSize(size) {
			panic("harness: BENCH_PAYLOAD_SIZES entry " + part + " is unsupported")
		}
		allowed[size] = struct{}{}
	}
	if len(allowed) == 0 {
		panic("harness: BENCH_PAYLOAD_SIZES is empty")
	}
	return allowed
}

func isSupportedBenchPayloadSize(size int) bool {
	for _, entry := range benchPayloads {
		if entry.size == size {
			return true
		}
	}
	return false
}

func filterPayloadSizes(sizes []int, allowed map[int]struct{}) []int {
	filtered := make([]int, 0, len(sizes))
	for _, size := range sizes {
		if _, ok := allowed[size]; ok {
			filtered = append(filtered, size)
		}
	}
	return filtered
}

// PayloadSizes lists offer benchmark payload lengths: 32-1024 unfragmented;
// 1400, 4000, 8192 fragmented. BENCH_PAYLOAD_SIZES env narrows the list.
func PayloadSizes() []int {
	return offerPayloadSizes
}

// EndToEndPayloadSizes lists end-to-end payload lengths: 32-1024 unfragmented;
// 1400, 4000, 8192 fragmented. BENCH_PAYLOAD_SIZES env narrows the list.
func EndToEndPayloadSizes() []int {
	return endToEndPayloadSizes
}

// IsFragmentedPayload reports whether size uses the fragmented Offer path (> MTU).
func IsFragmentedPayload(size int) bool {
	for _, entry := range benchPayloads {
		if entry.size == size {
			return entry.fragmented
		}
	}
	panic("harness: unsupported bench payload size")
}

// PayloadForSize returns a slice view for a benchmark payload length.
func PayloadForSize(size int) []byte {
	for _, entry := range benchPayloads {
		if entry.size == size {
			return entry.data
		}
	}
	panic("harness: unsupported bench payload size")
}

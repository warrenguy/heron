package recordinglog

import "github.com/warrenguy/heron/agrona/atomic"

const (
	recordingIDOffset         = 0
	leadershipTermIDOffset    = 8
	termBaseLogPositionOffset = 16
	logPositionOffset         = 24
	timestampOffset           = 32
	serviceIDOffset           = 40
	entryTypeOffset           = 44
	endpointOffset            = 48

	recordAlignment = atomic.CacheLineLength
	maxEntryLength  = 4096
	maxEndpointLen  = maxEntryLength - endpointOffset - atomic.StringASCIIHeaderLength
)

// Endpoint-carrying entries occupy a recordAlignment-byte-aligned span large
// enough for the fixed header, the 4-byte length prefix, and the endpoint
// bytes; all other
// types (including NULL_VALUE removed slots) occupy exactly recordAlignment
// bytes.
func alignedEntryLength(entryType int32, endpoint string) int {
	if hasEndpoint(entryType) {
		return atomic.Align(
			endpointOffset+atomic.StringASCIIEncodedLength(len(endpoint)),
			recordAlignment,
		)
	}
	return recordAlignment
}

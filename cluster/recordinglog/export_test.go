package recordinglog

import "github.com/warrenguy/heron/agrona/atomic"

// DecodeEntries exposes the internal decodeEntries function for black-box tests.
var DecodeEntries = decodeEntries

// Less exposes the internal less comparator for black-box tests.
var Less = less

// CompareEntries exposes the internal compareEntries ordering for black-box tests.
var CompareEntries = compareEntries

// EncodeEntry encodes e into a zero-padded byte slice using the recording.log
// wire layout. Valid==false OR's entryTypeInvalidFlag into the type word. The
// integer writes mirror the decoder's atomic reads; the endpoint is written
// raw to match the decoder's tolerant manual read.
func EncodeEntry(e Entry) []byte {
	endpoint := e.ArchiveEndpoint
	buf := make([]byte, alignedEntryLength(e.Type, endpoint))
	atomicBuffer := atomic.Wrap(buf)
	atomicBuffer.PutInt64(recordingIDOffset, e.RecordingID)
	atomicBuffer.PutInt64(leadershipTermIDOffset, e.LeadershipTermID)
	atomicBuffer.PutInt64(termBaseLogPositionOffset, e.TermBaseLogPosition)
	atomicBuffer.PutInt64(logPositionOffset, e.LogPosition)
	atomicBuffer.PutInt64(timestampOffset, e.Timestamp)
	atomicBuffer.PutInt32(serviceIDOffset, e.ServiceID)
	typeWord := e.Type
	if !e.Valid {
		typeWord |= entryTypeInvalidFlag
	}
	atomicBuffer.PutInt32(entryTypeOffset, typeWord)
	if hasEndpoint(e.Type) {
		atomicBuffer.PutInt32(endpointOffset, int32(len(endpoint)))
		copy(buf[endpointOffset+atomic.StringASCIIHeaderLength:], endpoint)
	}
	return buf
}

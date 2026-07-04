package client

import "github.com/warrenguy/heron/archive/codecs"

// handleRecordingDescriptorFragment decodes a RecordingDescriptor and captures it on the
// descriptor poller for the awaited correlation id. A nil poller, malformed fragment,
// foreign-schema fragment, or descriptor for another correlation id is ignored.
//
// Decode returns var-ascii views into fragment without allocating. Matching capture
// clones those slices at latch for retention.
func handleRecordingDescriptorFragment(poller *RecordingDescriptorPoller, fragment []byte) {
	if poller == nil || poller.IsPollComplete() {
		return
	}
	fields, ok := codecs.TryDecodeRecordingDescriptorAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok {
		return
	}
	captureRecordingDescriptor(poller, fields)
}

// handleDescriptorControlResponseFragment decodes a ControlResponse and captures it on
// the descriptor poller for the awaited correlation id. A non-OK code latches terminal
// completion. A nil poller, malformed fragment, or foreign-schema fragment is ignored.
func handleDescriptorControlResponseFragment(poller *RecordingDescriptorPoller, fragment []byte) {
	if poller == nil || poller.IsPollComplete() {
		return
	}
	resp, ok := codecs.TryDecodeControlResponseAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok {
		return
	}
	captureDescriptorControlResponse(poller, resp)
}

func captureDescriptorControlResponse(
	poller *RecordingDescriptorPoller,
	resp codecs.ControlResponseFields,
) {
	if resp.CorrelationID != poller.awaitCorrelationID {
		return
	}
	poller.latchResponse(resp)
}

func captureRecordingDescriptor(
	poller *RecordingDescriptorPoller,
	fields codecs.RecordingDescriptorFields,
) {
	if fields.CorrelationID != poller.awaitCorrelationID {
		return
	}
	poller.latchDescriptor(fields)
}

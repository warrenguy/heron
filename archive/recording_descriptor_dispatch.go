package archive

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

// handleDescriptorControlResponseFields captures resp on the descriptor poller
// for the awaited correlation id. A non-OK code latches terminal completion. A
// nil or complete poller ignores resp.
func handleDescriptorControlResponseFields(
	poller *RecordingDescriptorPoller,
	resp codecs.ControlResponseFields,
) {
	if poller == nil || poller.IsPollComplete() {
		return
	}
	if !poller.isAwaiting(resp.CorrelationID) {
		return
	}
	poller.latchResponse(resp)
}

func captureRecordingDescriptor(
	poller *RecordingDescriptorPoller,
	fields codecs.RecordingDescriptorFields,
) {
	if !poller.isAwaiting(fields.CorrelationID) {
		return
	}
	poller.latchDescriptor(fields)
}

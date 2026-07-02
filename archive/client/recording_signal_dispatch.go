package client

import "github.com/warrenguy/heron/archive/codecs"

// handleRecordingSignalFromFragment decodes a RecordingSignalEvent and dispatches it
// to handler. A controlSessionFilter other than 0 drops signals from other control
// sessions. A nil handler, malformed fragment, or foreign-schema fragment is ignored.
//
// The decode returns scalar fields only, so the dispatch does not allocate.
func handleRecordingSignalFromFragment(
	handler RecordingSignalHandler,
	controlSessionFilter int64,
	fragment []byte,
) {
	if handler == nil {
		return
	}
	signal, ok := codecs.TryDecodeRecordingSignalEventAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok {
		return
	}
	if controlSessionFilter != 0 && signal.ControlSessionID != controlSessionFilter {
		return
	}
	handler.RecordingSignal(signal)
}

package archive

import "github.com/warrenguy/heron/archive/codecs"

// RecordingSignalHandler receives RecordingSignalEvent dispatches from the archive
// control demux.
//
// RecordingSignal runs on the polling goroutine inside PollControlEvents.
// Implementations must not block.
type RecordingSignalHandler interface {
	RecordingSignal(event codecs.RecordingSignalEventFields)
}

// RecordingSignalHandlerFunc adapts a function to RecordingSignalHandler. A nil func
// drops dispatches.
type RecordingSignalHandlerFunc func(codecs.RecordingSignalEventFields)

var _ RecordingSignalHandler = RecordingSignalHandlerFunc(nil)

// RecordingSignal forwards to the func when set.
func (f RecordingSignalHandlerFunc) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if f != nil {
		f(event)
	}
}

// RecordingSignalHandlers is an ergonomic RecordingSignalHandler adapter with a
// nil-guarded func field. A nil SignalFunc drops dispatches.
type RecordingSignalHandlers struct {
	SignalFunc func(codecs.RecordingSignalEventFields)
}

var _ RecordingSignalHandler = (*RecordingSignalHandlers)(nil)

// RecordingSignal forwards to SignalFunc when set.
func (h *RecordingSignalHandlers) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if h != nil && h.SignalFunc != nil {
		h.SignalFunc(event)
	}
}

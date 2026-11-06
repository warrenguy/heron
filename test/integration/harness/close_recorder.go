//go:build integration

package harness

import (
	"bytes"
	"slices"

	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/service"
)

// RecordedClose is one SessionClose callback.
type RecordedClose struct {
	SessionID int64
	Reason    clusterwire.CloseReason
}

// CloseRecorder stores the SessionClose callbacks and close results of a service
// recorder.
//
// With ClosePayload set, a message with that payload and a non-nil session is a
// close message. CloseOnPayload calls session.Close and stores the result in
// CloseResults.
//
// Concurrency: the goroutine that ticks the service owns the recorder. Not safe
// from other goroutines.
type CloseRecorder struct {
	Closes       []RecordedClose
	ClosePayload []byte
	CloseResults []bool
}

// RecordClose stores one SessionClose callback.
func (r *CloseRecorder) RecordClose(sessionID int64, reason clusterwire.CloseReason) {
	r.Closes = append(r.Closes, RecordedClose{SessionID: sessionID, Reason: reason})
}

// CloseOnPayload closes session when payload is a close message. It reports
// whether payload is a close message.
func (r *CloseRecorder) CloseOnPayload(session service.ClientSession, payload []byte) bool {
	if r.ClosePayload == nil || session == nil || !bytes.Equal(payload, r.ClosePayload) {
		return false
	}
	r.CloseResults = append(r.CloseResults, session.Close())
	return true
}

// HasClose reports whether r holds a SessionClose for sessionID.
func (r *CloseRecorder) HasClose(sessionID int64) bool {
	return slices.ContainsFunc(r.Closes, func(c RecordedClose) bool {
		return c.SessionID == sessionID
	})
}

// HasCloseWithReason reports whether r holds a SessionClose for sessionID with
// reason.
func (r *CloseRecorder) HasCloseWithReason(sessionID int64, reason clusterwire.CloseReason) bool {
	return slices.Contains(r.Closes, RecordedClose{SessionID: sessionID, Reason: reason})
}

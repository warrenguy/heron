package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

const challengeSbeTemplateID = int32(gensbe.ChallengeSbeTemplateID)

// ControlResponsePoller captures archive control response events for AsyncConnect
// and post-connect session duty cycles. Terminal templates are stored on the
// poller until the next Poll reset.
//
// Not safe for concurrent use.
//
// Captured byte slices remain valid until the next Poll that begins with a prior
// pollComplete reset, or until the poller is discarded. Poll never calls DoWork.
type ControlResponsePoller struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *FragmentAssembler

	pollComplete     bool
	templateID       int32
	controlSessionID int64
	correlationID    int64
	relevantID       int64
	code             gensbe.ControlResponseCode
	version          int32
	errorMessage     []byte
	encodedChallenge []byte

	recordingID     int64
	subscriptionID  int64
	position        int64
	recordingSignal gensbe.RecordingSignal
}

// NewControlResponsePoller constructs a control response poller. fragmentLimit <= 0
// normalizes to DefaultControlResponseFragmentLimit.
func NewControlResponsePoller(
	subscription *aeron.Subscription,
	fragmentLimit int,
) *ControlResponsePoller {
	if fragmentLimit <= 0 {
		fragmentLimit = DefaultControlResponseFragmentLimit
	}
	poller := &ControlResponsePoller{
		subscription:    subscription,
		fragmentLimit:   fragmentLimit,
		correlationID:   conductor.NullCorrelationID,
		code:            gensbe.ControlResponseCode_NULL_VALUE,
		recordingSignal: gensbe.RecordingSignal_NULL_VALUE,
	}
	poller.assembler = NewFragmentAssembler(func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		handleControlResponseFragment(poller, buffer, offset, length, header)
	})
	return poller
}

// FragmentLimit reports the per-poll fragment budget passed to Subscription.Poll.
func (poller *ControlResponsePoller) FragmentLimit() int {
	return poller.fragmentLimit
}

// Poll resets prior capture when pollComplete was set, then polls the subscription.
// Returns 0 when subscription is nil.
func (poller *ControlResponsePoller) Poll() int {
	if poller.pollComplete {
		poller.resetCapture()
	}
	if poller.subscription == nil {
		return 0
	}
	return poller.subscription.Poll(
		poller.assembler.Handler(),
		poller.fragmentLimit,
	)
}

// IsPollComplete reports whether the last poll captured a terminal control template.
func (poller *ControlResponsePoller) IsPollComplete() bool {
	return poller.pollComplete
}

// IsChallenged reports whether the last poll captured Challenge (59). Requires
// pollComplete; returns false when an unknown template was polled without capture.
func (poller *ControlResponsePoller) IsChallenged() bool {
	return poller.pollComplete && poller.templateID == challengeSbeTemplateID
}

// TemplateID returns the captured SBE template id, or 0 when not captured.
func (poller *ControlResponsePoller) TemplateID() int32 {
	return poller.templateID
}

// ControlSessionID returns the captured control session id, or 0 when not captured.
func (poller *ControlResponsePoller) ControlSessionID() int64 {
	return poller.controlSessionID
}

// CorrelationID returns the captured correlation id, or conductor.NullCorrelationID
// (-1) when not captured.
func (poller *ControlResponsePoller) CorrelationID() int64 {
	return poller.correlationID
}

// RelevantID returns the captured ControlResponse relevant id. Zero when not captured;
// conductor.NullCorrelationID (-1) when Challenge was captured.
func (poller *ControlResponsePoller) RelevantID() int64 {
	return poller.relevantID
}

// Code returns the captured ControlResponse code, or NULL_VALUE when not captured or
// when Challenge was captured. ControlResponseCode_OK is also 0; read only after
// IsPollComplete with ControlResponse.
func (poller *ControlResponsePoller) Code() gensbe.ControlResponseCode {
	return poller.code
}

// Version returns the captured ControlResponse version, or 0 when not captured.
func (poller *ControlResponsePoller) Version() int32 {
	return poller.version
}

// ErrorMessage returns captured ControlResponse errorMessage bytes. Nil when not captured
// or wire length is zero. Safe to retain until the next Poll reset.
func (poller *ControlResponsePoller) ErrorMessage() []byte {
	return poller.errorMessage
}

// EncodedChallenge returns the captured Challenge payload. Nil when not captured
// or wire length is zero; AsyncConnect must treat nil like an empty challenge byte slice.
// Safe to retain until the next Poll reset.
func (poller *ControlResponsePoller) EncodedChallenge() []byte {
	return poller.encodedChallenge
}

// RecordingID returns the captured RecordingSignalEvent recording id, or 0 when not captured.
func (poller *ControlResponsePoller) RecordingID() int64 {
	return poller.recordingID
}

// SubscriptionID returns the captured RecordingSignalEvent subscription id, or 0 when not captured.
func (poller *ControlResponsePoller) SubscriptionID() int64 {
	return poller.subscriptionID
}

// Position returns the captured RecordingSignalEvent position, or 0 when not captured.
func (poller *ControlResponsePoller) Position() int64 {
	return poller.position
}

// RecordingSignal returns the captured RecordingSignalEvent signal, or NULL_VALUE when
// not captured.
func (poller *ControlResponsePoller) RecordingSignal() gensbe.RecordingSignal {
	return poller.recordingSignal
}

// CapturedControlResponseFields snapshots the last captured ControlResponse.
func (poller *ControlResponsePoller) CapturedControlResponseFields() codecs.ControlResponseFields {
	return codecs.ControlResponseFields{
		ControlSessionID: poller.ControlSessionID(),
		CorrelationID:    poller.CorrelationID(),
		RelevantID:       poller.RelevantID(),
		Code:             poller.Code(),
		Version:          poller.Version(),
		ErrorMessage:     poller.ErrorMessage(),
	}
}

func (poller *ControlResponsePoller) resetCapture() {
	poller.pollComplete = false
	poller.templateID = 0
	poller.controlSessionID = 0
	poller.correlationID = conductor.NullCorrelationID
	poller.relevantID = 0
	poller.code = gensbe.ControlResponseCode_NULL_VALUE
	poller.version = 0
	poller.errorMessage = nil
	poller.encodedChallenge = nil
	poller.recordingID = 0
	poller.subscriptionID = 0
	poller.position = 0
	poller.recordingSignal = gensbe.RecordingSignal_NULL_VALUE
}

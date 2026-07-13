package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

const challengeSbeTemplateID = int32(archivewire.ChallengeSbeTemplateID)

// ControlResponseCapture snapshots one terminal control-response or challenge capture.
type ControlResponseCapture struct {
	TemplateID       int32
	ControlSessionID int64
	CorrelationID    int64
	RelevantID       int64
	Code             archivewire.ControlResponseCode
	Version          int32
	ErrorMessage     []byte
	EncodedChallenge []byte
}

// Challenged reports whether the capture is a Challenge terminal.
func (capture ControlResponseCapture) Challenged() bool {
	return capture.TemplateID == challengeSbeTemplateID
}

// ControlResponsePoller captures archive control response events for AsyncConnect
// and post-connect session duty cycles. Terminal templates are stored on the
// poller until the next Poll reset.
//
// Not safe for concurrent use.
//
// Standalone Poll clones var-ascii at decode time; latched slices remain valid until
// the next Poll that begins with a prior terminal capture reset, or until the poller is
// discarded. Poll never calls DoWork.
//
// Demux capture via PollControlEvents keeps subscription-buffer views; consume or
// copy ErrorMessage and EncodedChallenge before the next demux poll, or call
// ResetCapture before re-arming an await.
type ControlResponsePoller struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	assembler     *FragmentAssembler

	capture ControlResponseCapture
	ready   bool
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
		subscription:  subscription,
		fragmentLimit: fragmentLimit,
		capture: ControlResponseCapture{
			CorrelationID: conductor.NullCorrelationID,
			Code:          archivewire.ControlResponseCode_NULL_VALUE,
		},
	}
	poller.assembler = NewFragmentAssembler(func(
		buffer atomicbuffer.AtomicBuffer,
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

// Poll resets prior capture when a terminal was latched, then polls the subscription.
// Returns 0 when subscription is nil.
func (poller *ControlResponsePoller) Poll() int {
	if poller.ready {
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
	return poller.ready
}

// IsChallenged reports whether the last poll captured Challenge (59).
func (poller *ControlResponsePoller) IsChallenged() bool {
	return poller.ready && poller.capture.Challenged()
}

// Capture returns the terminal capture when IsPollComplete is true.
func (poller *ControlResponsePoller) Capture() (ControlResponseCapture, bool) {
	if !poller.ready {
		return ControlResponseCapture{}, false
	}
	return poller.capture, true
}

// Response returns the captured ControlResponse fields when the terminal template is
// ControlResponse.
func (poller *ControlResponsePoller) Response() (codecs.ControlResponseFields, bool) {
	capture, ok := poller.Capture()
	if !ok || capture.TemplateID != int32(archivewire.ControlResponseSbeTemplateID) {
		return codecs.ControlResponseFields{}, false
	}
	return codecs.ControlResponseFields{
		ControlSessionID: capture.ControlSessionID,
		CorrelationID:    capture.CorrelationID,
		RelevantID:       capture.RelevantID,
		Code:             capture.Code,
		Version:          capture.Version,
		ErrorMessage:     capture.ErrorMessage,
	}, true
}

// Challenge returns the captured Challenge fields when the terminal template is
// Challenge.
func (poller *ControlResponsePoller) Challenge() (codecs.ChallengeFields, bool) {
	capture, ok := poller.Capture()
	if !ok || !capture.Challenged() {
		return codecs.ChallengeFields{}, false
	}
	return codecs.ChallengeFields{
		ControlSessionID: capture.ControlSessionID,
		CorrelationID:    capture.CorrelationID,
		Version:          capture.Version,
		EncodedChallenge: capture.EncodedChallenge,
	}, true
}

// ResetCapture clears a terminal latch so demux routing can capture the next control
// response. Call before a new blocking request on the demux path, after connect hands
// the poller to Client, and after consuming a capture when polling via
// PollControlEvents. Standalone Poll resets automatically; the demux does not.
func (poller *ControlResponsePoller) ResetCapture() {
	if poller == nil {
		return
	}
	poller.resetCapture()
}

func (poller *ControlResponsePoller) resetCapture() {
	poller.ready = false
	poller.capture = ControlResponseCapture{
		CorrelationID: conductor.NullCorrelationID,
		RelevantID:    0,
		Code:          archivewire.ControlResponseCode_NULL_VALUE,
	}
}

func (poller *ControlResponsePoller) latchControlResponse(resp codecs.ControlResponseFields) {
	poller.capture.ControlSessionID = resp.ControlSessionID
	poller.capture.CorrelationID = resp.CorrelationID
	poller.capture.RelevantID = resp.RelevantID
	poller.capture.Code = resp.Code
	poller.capture.Version = resp.Version
	poller.capture.ErrorMessage = resp.ErrorMessage
	poller.capture.EncodedChallenge = nil
	poller.capture.TemplateID = int32(archivewire.ControlResponseSbeTemplateID)
	poller.ready = true
}

func (poller *ControlResponsePoller) latchChallenge(challenge codecs.ChallengeFields) {
	poller.capture.ControlSessionID = challenge.ControlSessionID
	poller.capture.CorrelationID = challenge.CorrelationID
	poller.capture.RelevantID = conductor.NullCorrelationID
	poller.capture.Code = archivewire.ControlResponseCode_NULL_VALUE
	poller.capture.Version = challenge.Version
	poller.capture.ErrorMessage = nil
	poller.capture.EncodedChallenge = challenge.EncodedChallenge
	poller.capture.TemplateID = challengeSbeTemplateID
	poller.ready = true
}

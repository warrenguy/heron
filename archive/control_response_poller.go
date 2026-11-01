package archive

import (
	"bytes"

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

// ControlResponsePoller latches archive control response terminals for
// AsyncConnect and post-connect session duty cycles. The poller holds one
// terminal at a time.
//
// Behaviour:
//   - While the poller holds a terminal, an accepted ControlResponse or
//     Challenge stays in the image for a later poll
//   - Standalone Poll clears a latch before it polls
//   - PollControlEvents clears a latch before it polls only while the filter
//     is disarmed. With an armed filter the latch stays until ResetCapture or
//     ResetAwait
//   - The demux routes an own-session ERROR the filter does not claim to a
//     non-nil ErrorResponseHandler. An armed filter does not drop the ERROR.
//     A disarmed filter does not latch the ERROR
//
// Lifetime: a capture owns its ErrorMessage and EncodedChallenge bytes. Values
// from Capture, Response and Challenge stay valid after later polls.
//
// ResetAwait clears any latch. ResetAwait then arms the correlation filter for
// a session offer+await. ArmAwait alone sets the filter without clearing. The
// poller drops a non-matching ControlResponse or Challenge without a latch. A
// later match in the same poll batch can still latch. Zero awaits any
// correlation (connect and unarmed demux).
//
// Poll never calls DoWork.
//
// Concurrency: not safe for concurrent use.
type ControlResponsePoller struct {
	subscription  *aeron.Subscription
	fragmentLimit int
	handler       aeron.ControlledFragmentHandler

	capture            ControlResponseCapture
	ready              bool
	awaitCorrelationID int64
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
	assembler := aeron.NewControlledFragmentAssembler(func(
		buffer atomicbuffer.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) aeron.ControlledAction {
		return handleControlResponseFragment(poller, buffer, offset, length, header)
	})
	poller.handler = assembler.Handler()
	return poller
}

// FragmentLimit reports the per-poll fragment budget passed to
// Subscription.ControlledPoll.
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
	return poller.subscription.ControlledPoll(poller.handler, poller.fragmentLimit)
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

// ResetCapture clears a terminal latch so demux routing can latch the next
// control response. Standalone Poll clears a latch at poll start.
// PollControlEvents clears a latch at poll start only while the filter is
// disarmed.
//
// Call ResetCapture:
//   - after connect hands the poller to Client
//   - after consuming a capture polled through PollControlEvents
//   - to clear a matched terminal mid-await, for example a stop OK while
//     waiting for STOP
//
// Does not disarm the correlation filter. Prefer ResetAwait for a new
// outstanding request: the filter then covers offer-retry polls as well as
// the response await.
func (poller *ControlResponsePoller) ResetCapture() {
	if poller == nil {
		return
	}
	poller.resetCapture()
}

// ResetAwait clears a terminal latch and arms the correlation filter for correlationID.
// Zero accepts any correlation. Session offer+await paths call this before offer so
// offer-retry polls use the same filter as the response await; defer ArmAwait(0) after
// the whole operation (including offer failure).
func (poller *ControlResponsePoller) ResetAwait(correlationID int64) {
	if poller == nil {
		return
	}
	poller.resetCapture()
	poller.awaitCorrelationID = correlationID
}

// ArmAwait sets the correlation filter for demux and standalone latch without clearing a
// prior capture. Zero accepts any correlation. Prefer ResetAwait at the start of a new
// outstanding request; use ArmAwait(0) to disarm after offer+await.
func (poller *ControlResponsePoller) ArmAwait(correlationID int64) {
	if poller == nil {
		return
	}
	poller.awaitCorrelationID = correlationID
}

// DisarmAwait clears the correlation filter when it is armed for correlationID.
// A filter armed by a later request stays.
func (poller *ControlResponsePoller) DisarmAwait(correlationID int64) {
	if poller == nil || poller.awaitCorrelationID != correlationID {
		return
	}
	poller.awaitCorrelationID = 0
}

func (poller *ControlResponsePoller) resetCapture() {
	poller.ready = false
	poller.capture = ControlResponseCapture{
		CorrelationID: conductor.NullCorrelationID,
		RelevantID:    0,
		Code:          archivewire.ControlResponseCode_NULL_VALUE,
	}
}

// clearDisarmedCapture clears a latch when the correlation filter is
// disarmed. Nil-safe.
func (poller *ControlResponsePoller) clearDisarmedCapture() {
	if poller == nil {
		return
	}
	if poller.ready && !poller.isArmed() {
		poller.resetCapture()
	}
}

// isArmed reports whether the correlation filter holds a request id.
func (poller *ControlResponsePoller) isArmed() bool {
	return poller.awaitCorrelationID != 0
}

// isAwaiting reports whether the armed filter holds correlationID.
func (poller *ControlResponsePoller) isAwaiting(correlationID int64) bool {
	return poller.isArmed() && poller.awaitCorrelationID == correlationID
}

func (poller *ControlResponsePoller) matchesAwaitCorrelation(correlationID int64) bool {
	return !poller.isArmed() || poller.isAwaiting(correlationID)
}

// latchControlResponse latches resp. The caller runs the correlation filter
// first.
func (poller *ControlResponsePoller) latchControlResponse(resp codecs.ControlResponseFields) {
	poller.capture.ControlSessionID = resp.ControlSessionID
	poller.capture.CorrelationID = resp.CorrelationID
	poller.capture.RelevantID = resp.RelevantID
	poller.capture.Code = resp.Code
	poller.capture.Version = resp.Version
	poller.capture.ErrorMessage = cloneCaptureBytes(resp.ErrorMessage)
	poller.capture.EncodedChallenge = nil
	poller.capture.TemplateID = int32(archivewire.ControlResponseSbeTemplateID)
	poller.ready = true
}

// latchChallenge latches challenge. The caller runs the correlation filter
// first.
func (poller *ControlResponsePoller) latchChallenge(challenge codecs.ChallengeFields) {
	poller.capture.ControlSessionID = challenge.ControlSessionID
	poller.capture.CorrelationID = challenge.CorrelationID
	poller.capture.RelevantID = conductor.NullCorrelationID
	poller.capture.Code = archivewire.ControlResponseCode_NULL_VALUE
	poller.capture.Version = challenge.Version
	poller.capture.ErrorMessage = nil
	poller.capture.EncodedChallenge = cloneCaptureBytes(challenge.EncodedChallenge)
	poller.capture.TemplateID = challengeSbeTemplateID
	poller.ready = true
}

// cloneCaptureBytes copies a var-ascii view out of the fragment. Returns nil
// for an empty field.
func cloneCaptureBytes(field []byte) []byte {
	if len(field) == 0 {
		return nil
	}
	return bytes.Clone(field)
}

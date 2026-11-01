package archive

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
)

// handleControlResponseFragment is the standalone ControlResponsePoller
// delegate. It routes ControlResponse and Challenge to
// handleControlResponseFromFragment. A wrong schema id or another template
// returns ControlledContinue without a latch (cluster connect egress parity).
func handleControlResponseFragment(
	poller *ControlResponsePoller,
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
	_ *aeron.Header,
) aeron.ControlledAction {
	fragment, templateID, ok := controlFragmentTemplate(buffer, offset, length)
	if !ok {
		return aeron.ControlledContinue
	}
	return handleControlResponseFromFragment(poller, fragment, templateID)
}

// controlFragmentTemplate returns the fragment payload and its SBE template
// id. ok is false when the frame is shorter than a message header.
func controlFragmentTemplate(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) ([]byte, uint16, bool) {
	fragment, ok := controlResponseFragmentPayload(buffer, offset, length)
	if !ok {
		return nil, 0, false
	}
	// controlResponseFragmentPayload guarantees len(fragment) >= header size, so
	// the decode cannot fail. The ok check covers future payload changes.
	_, templateID, _, _, ok := codecs.DecodeMessageHeader(fragment, 0)
	if !ok {
		return nil, 0, false
	}
	return fragment, templateID, true
}

// handleControlResponseFromFragment latches a ControlResponse or Challenge on
// poller.
//
// Returns:
//   - ControlledContinue for a nil poller, a malformed fragment, another
//     template, or a correlation the filter rejects. The poller drops the
//     fragment
//   - ControlledAbort for a matching fragment while poller holds a latched
//     terminal. The image keeps the frame for a later poll
//   - ControlledContinue after a latch
func handleControlResponseFromFragment(
	poller *ControlResponsePoller,
	fragment []byte,
	templateID uint16,
) aeron.ControlledAction {
	if poller == nil {
		return aeron.ControlledContinue
	}
	switch templateID {
	case archivewire.ControlResponseSbeTemplateID:
		resp, ok := codecs.TryDecodeControlResponseAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
		if !ok {
			return aeron.ControlledContinue
		}
		return handleControlResponseFields(poller, resp)

	case archivewire.ChallengeSbeTemplateID:
		return handleChallengeFragment(poller, fragment)
	}
	return aeron.ControlledContinue
}

func handleChallengeFragment(
	poller *ControlResponsePoller,
	fragment []byte,
) aeron.ControlledAction {
	if poller == nil {
		return aeron.ControlledContinue
	}
	challenge, ok := codecs.TryDecodeChallengeAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok || !poller.matchesAwaitCorrelation(challenge.CorrelationID) {
		return aeron.ControlledContinue
	}
	if poller.ready {
		return aeron.ControlledAbort
	}
	poller.latchChallenge(challenge)
	return aeron.ControlledContinue
}

// handleControlResponseFields requires a non-nil poller.
func handleControlResponseFields(
	poller *ControlResponsePoller,
	resp codecs.ControlResponseFields,
) aeron.ControlledAction {
	if !poller.matchesAwaitCorrelation(resp.CorrelationID) {
		return aeron.ControlledContinue
	}
	if poller.ready {
		return aeron.ControlledAbort
	}
	poller.latchControlResponse(resp)
	return aeron.ControlledContinue
}

func controlResponseFragmentPayload(
	buffer atomicbuffer.AtomicBuffer,
	offset int,
	length int,
) ([]byte, bool) {
	payload := buffer.Bytes()
	if offset < 0 ||
		length < 0 ||
		length < codecs.MessageHeaderEncodedLen() ||
		offset > len(payload)-length {
		return nil, false
	}
	return payload[offset : offset+length], true
}

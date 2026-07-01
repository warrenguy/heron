package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// handleControlResponseFragment decodes archive control response SBE and captures
// terminal templates on poller. Wrong schema id and unknown templates are ignored
// silently (cluster connect egress parity); production poller does not throw.
func handleControlResponseFragment(
	poller *ControlResponsePoller,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	_ *aeron.Header,
) {
	if poller.pollComplete {
		return
	}

	fragment, ok := controlResponseFragmentPayload(buffer, offset, length)
	if !ok {
		return
	}
	decoded, ok := codecs.TryDecodeByTemplate(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok {
		return
	}

	switch decoded.TemplateID {
	case gensbe.ControlResponseSbeTemplateID:
		resp, ok := decoded.Fields.(codecs.ControlResponseFields)
		if !ok {
			return
		}
		poller.controlSessionID = resp.ControlSessionID
		poller.correlationID = resp.CorrelationID
		poller.relevantID = resp.RelevantID
		poller.code = resp.Code
		poller.version = resp.Version
		poller.errorMessage = resp.ErrorMessage
		captureControlResponseTerminal(poller, decoded.TemplateID)

	case gensbe.ChallengeSbeTemplateID:
		challenge, ok := decoded.Fields.(codecs.ChallengeFields)
		if !ok {
			return
		}
		poller.controlSessionID = challenge.ControlSessionID
		poller.correlationID = challenge.CorrelationID
		poller.relevantID = conductor.NullCorrelationID
		poller.code = gensbe.ControlResponseCode_NULL_VALUE
		poller.version = challenge.Version
		poller.errorMessage = nil
		poller.encodedChallenge = challenge.EncodedChallenge
		captureControlResponseTerminal(poller, decoded.TemplateID)

	case gensbe.RecordingSignalEventSbeTemplateID:
		signal, ok := decoded.Fields.(codecs.RecordingSignalEventFields)
		if !ok {
			return
		}
		poller.controlSessionID = signal.ControlSessionID
		poller.correlationID = signal.CorrelationID
		poller.recordingID = signal.RecordingID
		poller.subscriptionID = signal.SubscriptionID
		poller.position = signal.Position
		poller.recordingSignal = signal.Signal
		captureControlResponseTerminal(poller, decoded.TemplateID)

	default:
		return
	}
}

func captureControlResponseTerminal(poller *ControlResponsePoller, templateID uint16) {
	poller.templateID = int32(templateID)
	poller.pollComplete = true
}

func controlResponseFragmentSpanValid(payload []byte, offset, length int) bool {
	return offset >= 0 &&
		length >= 0 &&
		length >= codecs.MessageHeaderEncodedLen() &&
		offset <= len(payload)-length
}

func controlResponseFragmentPayload(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
) ([]byte, bool) {
	payload := buffer.Bytes()
	if !controlResponseFragmentSpanValid(payload, offset, length) {
		return nil, false
	}
	return payload[offset : offset+length], true
}

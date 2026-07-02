package client

import (
	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/gensbe"
)

// handleControlResponseFragment decodes an archive control response fragment and captures
// terminal templates on poller. Wrong schema id and unknown templates are ignored silently
// (cluster connect egress parity); production poller does not throw.
func handleControlResponseFragment(
	poller *ControlResponsePoller,
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	_ *aeron.Header,
) {
	if poller.ready {
		return
	}

	fragment, ok := controlResponseFragmentPayload(buffer, offset, length)
	if !ok {
		return
	}
	// TryDecodeByTemplate clones and nil-normalizes var-ascii fields so a connect-phase
	// capture survives wire reuse; the demux path reads promptly and keeps buffer views.
	decoded, ok := codecs.TryDecodeByTemplate(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
	if !ok {
		return
	}
	captureDecodedControlResponse(poller, decoded.TemplateID, decoded.Fields)
}

func handleControlResponseFromFragment(
	poller *ControlResponsePoller,
	fragment []byte,
	templateID uint16,
) {
	if poller == nil || poller.ready {
		return
	}
	switch templateID {
	case gensbe.ControlResponseSbeTemplateID:
		resp, ok := codecs.TryDecodeControlResponseAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
		if !ok {
			return
		}
		poller.latchControlResponse(resp)

	case gensbe.ChallengeSbeTemplateID:
		challenge, ok := codecs.TryDecodeChallengeAt(fragment, 0, codecs.ArchiveDecodeMaxVarLen)
		if !ok {
			return
		}
		poller.latchChallenge(challenge)
	}
}

func captureDecodedControlResponse(
	poller *ControlResponsePoller,
	templateID uint16,
	fields any,
) {
	switch templateID {
	case gensbe.ControlResponseSbeTemplateID:
		resp, ok := fields.(codecs.ControlResponseFields)
		if !ok {
			return
		}
		poller.latchControlResponse(resp)

	case gensbe.ChallengeSbeTemplateID:
		challenge, ok := fields.(codecs.ChallengeFields)
		if !ok {
			return
		}
		poller.latchChallenge(challenge)
	}
}

func controlResponseFragmentPayload(
	buffer atomic.AtomicBuffer,
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

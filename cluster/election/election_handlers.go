package election

import (
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func (e *Election) handleInit(int64) int { return 0 }

func (e *Election) handleCanvass(int64) int { return 0 }

func (e *Election) handleNominate(int64) int { return 0 }

func (e *Election) handleCandidateBallot(int64) int { return 0 }

func (e *Election) handleFollowerBallot(int64) int { return 0 }

func (e *Election) handleLeaderLogReplication(int64) int { return 0 }

func (e *Election) handleLeaderReplay(int64) int {
	_ = e.logReplay // wired when leader replay path is implemented
	return 0
}

func (e *Election) handleLeaderInit(int64) int { return 0 }

func (e *Election) handleLeaderReady(int64) int { return 0 }

func (e *Election) handleFollowerLogReplication(int64) int { return 0 }

func (e *Election) handleFollowerReplay(int64) int { return 0 }

func (e *Election) handleFollowerCatchupInit(int64) int { return 0 }

func (e *Election) handleFollowerCatchupAwait(int64) int { return 0 }

func (e *Election) handleFollowerCatchup(int64) int { return 0 }

func (e *Election) handleFollowerLogInit(int64) int { return 0 }

func (e *Election) handleFollowerLogAwait(int64) int { return 0 }

func (e *Election) handleFollowerReady(int64) int { return 0 }

// HandleConsensusMessage decodes consensus wire and dispatches to On* handlers.
// Returns true when decode succeeds; handlers are skipped in ElectionStateInit.
func (e *Election) HandleConsensusMessage(buf []byte, offset int) bool {
	_, templateID, schemaID, _, ok := codecs.DecodeMessageHeader(buf, offset)
	if !ok || schemaID != clusterwire.MessageHeaderSbeSchemaID {
		return false
	}
	accept := e.acceptsConsensusInbound()
	switch templateID {
	case clusterwire.CanvassPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeCanvassPositionAt, e.OnCanvassPosition)
	case clusterwire.RequestVoteSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeRequestVoteAt, e.OnRequestVote)
	case clusterwire.VoteSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeVoteAt, e.OnVote)
	case clusterwire.NewLeadershipTermSbeTemplateID:
		return dispatchWire(
			buf,
			offset,
			accept,
			codecs.TryDecodeNewLeadershipTermAt,
			e.OnNewLeadershipTerm,
		)
	case clusterwire.AppendPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeAppendPositionAt, e.OnAppendPosition)
	case clusterwire.CommitPositionSbeTemplateID:
		return dispatchWire(buf, offset, accept, codecs.TryDecodeCommitPositionAt, e.OnCommitPosition)
	default:
		return false
	}
}

func (e *Election) acceptsConsensusInbound() bool {
	return e.electionState != ElectionStateInit
}

func dispatchWire[F any](
	buf []byte,
	offset int,
	accept bool,
	decode func([]byte, int) (F, bool),
	on func(F),
) bool {
	fields, ok := decode(buf, offset)
	if !ok {
		return false
	}
	if accept {
		on(fields)
	}
	return true
}

func (e *Election) OnCanvassPosition(codecs.CanvassPositionFields) {}

func (e *Election) OnRequestVote(fields codecs.RequestVoteFields) {
	timestampMs := e.deps.EpochClock()
	_, _ = e.deps.NodeState.ProposeMaxCandidateTermID(
		fields.CandidateTermID,
		fields.LogPosition,
		timestampMs,
	)
}

func (e *Election) OnVote(codecs.VoteFields) {}

func (e *Election) OnNewLeadershipTerm(codecs.NewLeadershipTermFields) {}

func (e *Election) OnAppendPosition(codecs.AppendPositionFields) {}

func (e *Election) OnCommitPosition(codecs.CommitPositionFields) {}

// OnRecordingSignal handles archive recording signals during log replication.
// Mirrors Election.onRecordingSignal. Stub until logReplication.OnSignal is wired.
func (e *Election) OnRecordingSignal(
	correlationID, recordingID, position int64,
	signal RecordingSignal,
) {
	if !e.acceptsConsensusInbound() {
		return
	}
	if e.logReplication != nil {
		e.logReplication.OnSignal(correlationID, recordingID, position, signal)
		e.deps.Host.LogRecordingID(e.logReplication.RecordingID())
	}
}

// HandleError resets the election to Init regardless of err. Mirrors
// Election.handleError. Stub until error reporting and commit-position
// sync are wired.
func (e *Election) HandleError(nowNs int64, err error) {
	e.state(ElectionStateInit, nowNs)
}

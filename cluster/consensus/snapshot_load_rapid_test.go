//go:build rapid

package consensus

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const (
	roundTripMutationNone = iota
	roundTripMutationDropBegin
	roundTripMutationDropEnd
	roundTripMutationRepeatRow
	roundTripMutationMovePosition
	roundTripMutationTypeID
	roundTripMutationCloseReason
	roundTripMutationCount
)

// TestSnapshotLoader_takeRoundTrip_rapid encodes a writer agent's sessions,
// timers, and next session id through encodeSnapshotRow. A fresh agent
// restores those rows. Each drawn mutation fails the load or stops before
// END. The loader does not panic.
func TestSnapshotLoader_takeRoundTrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()
		mutation := rapid.IntRange(0, roundTripMutationCount-1).Draw(rt, "mutation")
		minSessions := 0
		if mutation == roundTripMutationCloseReason {
			minSessions = 1
		}
		sessionCount := rapid.IntRange(minSessions, 6).Draw(rt, "sessionCount")
		timerCount := rapid.IntRange(0, 4).Draw(rt, "timerCount")
		serviceCount := rapid.IntRange(0, 3).Draw(rt, "serviceCount")
		chunk := rapid.IntRange(8, 20).Draw(rt, "chunk")
		appVersion := cnc.ComposeSemanticVersion(
			uint8(rapid.IntRange(0, 255).Draw(rt, "major")),
			uint8(rapid.IntRange(0, 255).Draw(rt, "minor")),
			uint8(rapid.IntRange(0, 255).Draw(rt, "patch")),
		)
		logPosition := rapid.Int64().Draw(rt, "logPosition")
		leadershipTermID := rapid.Int64Range(0, 1<<20).Draw(rt, "leadershipTermID")

		writer := NewAgent(AgentConfig{
			AppVersion:   appVersion,
			ServiceCount: serviceCount,
		})
		sessionIDs := rapid.SliceOfNDistinct(
			rapid.Int64Range(1, 10_000),
			sessionCount,
			sessionCount,
			rapid.ID[int64],
		).Draw(rt, "sessionIDs")
		for _, id := range sessionIDs {
			channel := rapid.SliceOfN(
				rapid.ByteRange('a', 'z'),
				1,
				40,
			).Draw(rt, "channel")
			writer.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
				ClusterSessionID: id,
				CorrelationID:    rapid.Int64().Draw(rt, "correlationID"),
				ResponseStreamID: rapid.Int32().Draw(rt, "streamID"),
				ResponseChannel:  channel,
			}, rapid.Int64Range(0, 1<<40).Draw(rt, "openedLogPosition"), 0)
		}
		if sessionCount > 0 {
			closingCount := rapid.IntRange(0, sessionCount).Draw(rt, "closingCount")
			indexes := rapid.SliceOfNDistinct(
				rapid.IntRange(0, sessionCount-1),
				closingCount,
				closingCount,
				rapid.ID[int],
			).Draw(rt, "closingIndexes")
			reason := rapid.SampledFrom([]clusterwire.CloseReason{
				clusterwire.CloseReason_CLIENT_ACTION,
				clusterwire.CloseReason_SERVICE_ACTION,
				clusterwire.CloseReason_TIMEOUT,
			}).Draw(rt, "closeReason")
			for _, index := range indexes {
				require.True(rt, writer.sessions.markClosing(sessionIDs[index], reason))
			}
		}
		timerIDs := rapid.SliceOfNDistinct(
			rapid.Int64Range(1, 10_000),
			timerCount,
			timerCount,
			rapid.ID[int64],
		).Draw(rt, "timerIDs")
		for _, id := range timerIDs {
			deadline := rapid.Int64Range(0, 1<<40).Draw(rt, "deadline")
			require.True(rt, writer.timers.schedule(id, deadline))
		}
		extra := rapid.Int64Range(0, 20).Draw(rt, "extraIDs")
		if extra > 0 {
			writer.sessions.reseedSessionIDFloor(writer.sessions.nextSessionID - 1 + extra)
		}

		modelSessions := writer.sessions.snapshotSessionRows()
		modelTimers := writer.timers.snapshotTimerRows()
		modelNext := writer.sessions.nextSessionID
		take := &snapshotTake{
			logPosition:         logPosition,
			leadershipTermID:    leadershipTermID,
			nextSessionID:       modelNext,
			sessionRows:         modelSessions,
			timerRows:           modelTimers,
			serviceRecordingIDs: make([]int64, serviceCount),
		}
		rows := encodeTakeRows(rt, take, appVersion)

		switch mutation {
		case roundTripMutationDropBegin:
			rows = rows[1:]

		case roundTripMutationDropEnd:
			rows = rows[:len(rows)-1]

		case roundTripMutationRepeatRow:
			index := rapid.IntRange(0, len(rows)-2).Draw(rt, "repeatIndex")
			rows = slices.Insert(rows, index+1, slices.Clone(rows[index]))

		case roundTripMutationMovePosition:
			delta := rapid.Int64Range(1, 1<<20).Draw(rt, "positionDelta")
			editRoundTripMarkers(rt, rows, func(fields *codecs.SnapshotMarkerFields) {
				fields.LogPosition += delta
			})

		case roundTripMutationTypeID:
			editRoundTripMarkers(rt, rows, func(fields *codecs.SnapshotMarkerFields) {
				fields.TypeID = codecs.ServiceSnapshotTypeID
			})

		case roundTripMutationCloseReason:
			patched := false
			for i, row := range rows {
				_, templateID, _, _, ok := codecs.DecodeMessageHeader(row, 0)
				if !ok || templateID != clusterwire.ClusterSessionSbeTemplateID {
					continue
				}
				rows[i] = encodeRoundTripCloseReason(rt, row, clusterwire.CloseReason(9))
				patched = true
				break
			}
			require.True(rt, patched)
		}

		reader := NewAgent(AgentConfig{
			AppVersion:   appVersion,
			ServiceCount: serviceCount,
		})
		loader := reader.NewSnapshotLoader(logPosition)
		require.NotPanics(rt, func() {
			feedSnapshotRows(rt, loader, rows, chunk)
		})

		if err := loader.Err(); err != nil {
			require.ErrorIs(rt, err, errSnapshotLoad)
			require.False(rt, loader.IsDone())
		}
		if mutation != roundTripMutationNone {
			require.True(rt, loader.Err() != nil || !loader.IsDone())
			return
		}
		require.NoError(rt, loader.Err())
		require.True(rt, loader.IsDone())
		require.Equal(rt, modelSessions, reader.sessions.snapshotSessionRows())
		require.Equal(rt, modelTimers, reader.timers.snapshotTimerRows())
		require.Equal(rt, modelNext, reader.sessions.nextSessionID)
	})
}

func editRoundTripMarkers(
	rt *rapid.T,
	rows [][]byte,
	edit func(*codecs.SnapshotMarkerFields),
) {
	rt.Helper()
	for i, row := range rows {
		fields, ok := codecs.TryDecodeSnapshotMarkerAt(row, 0)
		if !ok {
			continue
		}
		edit(&fields)
		rows[i] = encodeModuleMarker(rt, fields)
	}
}

func encodeRoundTripCloseReason(
	rt *rapid.T,
	row []byte,
	reason clusterwire.CloseReason,
) []byte {
	rt.Helper()
	fields, ok := codecs.TryDecodeClusterSessionAt(row, 0)
	require.True(rt, ok)
	fields.CloseReason = reason
	return encodeModuleSession(rt, fields)
}

// chunk is the largest payload piece in one fragment.
func feedSnapshotRows(rt *rapid.T, loader *SnapshotLoader, rows [][]byte, chunk int) {
	rt.Helper()
	handler := loader.Handler()
	var termOffset int32
	const sessionID int32 = 1
	for _, row := range rows {
		if loader.Err() != nil || loader.IsDone() {
			return
		}
		for _, part := range splitSnapshotRow(row, chunk) {
			frameLen := logbuffer.AlignFrameLength(logbuffer.DataHeaderLength + len(part.payload))
			raw := make([]byte, frameLen)
			buffer := atomicbuffer.Wrap(raw)
			var err error
			if part.flags == logbuffer.Unfragmented {
				err = logbuffer.WriteDataHeaderUnfragmented(
					buffer, 0, sessionID, 2, 3, termOffset, 0,
				)
			} else {
				err = logbuffer.WriteDataHeaderFragmented(
					buffer, 0, sessionID, 2, 3, termOffset, part.flags,
				)
			}
			require.NoError(rt, err)
			copy(raw[logbuffer.DataHeaderLength:], part.payload)
			logbuffer.PutFrameLengthOrdered(buffer, 0, int32(frameLen))
			header := &aeron.Header{}
			header.SetBuffer(buffer)
			header.SetOffset(0)
			header.ResetPollFragmentState()
			action := handler(buffer, logbuffer.DataHeaderLength, len(part.payload), header)
			termOffset += int32(frameLen)
			if action != aeron.ControlledContinue {
				return
			}
		}
	}
}

type snapshotRowPart struct {
	flags   uint8
	payload []byte
}

func splitSnapshotRow(payload []byte, chunk int) []snapshotRowPart {
	if chunk < 1 {
		chunk = 1
	}
	parts := make([]snapshotRowPart, 0, (len(payload)+chunk-1)/chunk)
	for start := 0; start < len(payload); start += chunk {
		end := min(start+chunk, len(payload))
		var flags uint8
		if start == 0 {
			flags |= logbuffer.BeginFlag
		}
		if end == len(payload) {
			flags |= logbuffer.EndFlag
		}
		parts = append(parts, snapshotRowPart{
			flags:   flags,
			payload: payload[start:end],
		})
	}
	return parts
}

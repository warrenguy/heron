package consensus

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// snapshotLoadLogPosition is the log position every marker in the dispatch
// table must carry.
const snapshotLoadLogPosition int64 = 512

// snapshotLoadAppVersion is the agent app version the accept rows share.
var snapshotLoadAppVersion = cnc.ComposeSemanticVersion(1, 2, 3)

// snapshotMarkerRow returns the module marker row at logPosition.
func snapshotMarkerRow(
	logPosition, leadershipTermID int64,
	appVersion int32,
	mark clusterwire.SnapshotMark,
) codecs.SnapshotMarkerFields {
	return codecs.SnapshotMarkerFields{
		TypeID:           codecs.ConsensusModuleSnapshotTypeID,
		LogPosition:      logPosition,
		LeadershipTermID: leadershipTermID,
		Index:            0,
		Mark:             mark,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       appVersion,
	}
}

func moduleMarkerFields(mark clusterwire.SnapshotMark) codecs.SnapshotMarkerFields {
	return snapshotMarkerRow(snapshotLoadLogPosition, 5, snapshotLoadAppVersion, mark)
}

func fuzzSnapshotFrames(frames ...[]byte) []byte {
	var out []byte
	for _, frame := range frames {
		//nolint:gosec // G115: seed frames stay far below 64 KiB
		out = binary.LittleEndian.AppendUint16(out, uint16(len(frame)))
		out = append(out, frame...)
	}
	return out
}

func splitFuzzSnapshotFrames(data []byte) [][]byte {
	var frames [][]byte
	for len(data) >= 2 {
		length := int(binary.LittleEndian.Uint16(data))
		data = data[2:]
		length = min(length, len(data))
		frames = append(frames, data[:length])
		data = data[length:]
	}
	return frames
}

func fuzzSnapshotSeedRows(tb testing.TB) [][]byte {
	tb.Helper()
	appVersion := snapshotLoadAppVersion
	writer := NewAgent(AgentConfig{AppVersion: appVersion, ServiceCount: 1})
	writer.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
		ClusterSessionID: 4,
		CorrelationID:    40,
		ResponseStreamID: 14,
		ResponseChannel:  []byte("aeron:ipc"),
	}, 32, 0)
	require.True(tb, writer.sessions.markClosing(4, clusterwire.CloseReason_TIMEOUT))
	writer.sessions.learnSessionOpen(codecs.SessionOpenEventFields{
		ClusterSessionID: 9,
		CorrelationID:    90,
		ResponseStreamID: 19,
		ResponseChannel:  []byte("aeron:udp?endpoint=h:1"),
	}, 72, 0)
	require.True(tb, writer.timers.schedule(7, 6_000_000_000))
	require.True(tb, writer.timers.schedule(3, 5_000_000_000))
	writer.sessions.reseedSessionIDFloor(39)
	take := &snapshotTake{
		logPosition:         snapshotLoadLogPosition,
		leadershipTermID:    5,
		nextSessionID:       writer.sessions.nextSessionID,
		sessionRows:         writer.sessions.snapshotSessionRows(),
		timerRows:           writer.timers.snapshotTimerRows(),
		serviceRecordingIDs: make([]int64, 1),
	}
	return encodeTakeRows(tb, take, appVersion)
}

func fuzzSnapshotMarker(tb testing.TB, mark clusterwire.SnapshotMark) []byte {
	tb.Helper()
	return encodeModuleMarker(tb, moduleMarkerFields(mark))
}

func fuzzSnapshotSeeds(tb testing.TB) [][][]byte {
	tb.Helper()
	accept := fuzzSnapshotSeedRows(tb)
	begin, end := accept[0], accept[len(accept)-1]
	return [][][]byte{
		accept,
		{begin, accept[1], end},
		{
			begin,
			fuzzSnapshotMarker(tb, clusterwire.SnapshotMark_SECTION),
			fuzzSnapshotMarker(tb, clusterwire.SnapshotMark_NULL_VALUE),
			end,
		},
		accept[1:],
		accept[:len(accept)-1],
		{end},
		{begin, begin, end},
		{[]byte{1, 2, 3, 4}},
		{begin[:codecs.MessageHeaderEncodedLen()]},
	}
}

// FuzzSnapshotLoaderDispatch feeds length-prefixed frames to the snapshot
// loader. The loader does not panic. The loader never sets both IsDone
// and Err.
func FuzzSnapshotLoaderDispatch(f *testing.F) {
	for _, seed := range fuzzSnapshotSeeds(f) {
		f.Add(fuzzSnapshotFrames(seed...))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		agent := NewAgent(AgentConfig{
			AppVersion:   snapshotLoadAppVersion,
			ServiceCount: 1,
		})
		loader := agent.NewSnapshotLoader(snapshotLoadLogPosition)
		require.NotPanics(t, func() {
			for _, frame := range splitFuzzSnapshotFrames(data) {
				if loader.dispatch(frame) != aeron.ControlledContinue {
					break
				}
			}
		})
		if loader.Err() != nil {
			require.False(t, loader.IsDone())
		}
		if loader.IsDone() {
			require.NoError(t, loader.Err())
		}
	})
}

// encodeSnapshotRow reuses one scratch buffer. encodeTakeRows clones each
// row.
func encodeTakeRows(tb fragmentRequireT, take *snapshotTake, appVersion int32) [][]byte {
	tb.Helper()
	rows := make([][]byte, take.rowCount())
	for i := range rows {
		row, err := take.encodeSnapshotRow(appVersion, i)
		require.NoError(tb, err)
		rows[i] = slices.Clone(row)
	}
	return rows
}

func encodeModuleMarker(tb fragmentRequireT, fields codecs.SnapshotMarkerFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.SnapshotMarkerEncodedLen())
	end, ok := codecs.EncodeSnapshotMarker(
		buf, 0, clusterwire.SnapshotMarkerSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

func encodeModuleSession(tb fragmentRequireT, fields codecs.ClusterSessionFields) []byte {
	tb.Helper()
	length, ok := codecs.ClusterWireMessageLen(
		clusterwire.ClusterSessionSbeBlockLength,
		len(fields.ResponseChannel),
	)
	require.True(tb, ok)
	buf := make([]byte, length)
	end, ok := codecs.EncodeClusterSession(
		buf, 0, clusterwire.ClusterSessionSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

func encodeModuleTimer(tb fragmentRequireT, fields codecs.TimerFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.TimerEncodedLen())
	end, ok := codecs.EncodeTimer(buf, 0, clusterwire.TimerSbeSchemaVersion, fields)
	require.True(tb, ok)
	return buf[:end]
}

func encodeModuleConsensus(tb fragmentRequireT, fields codecs.ConsensusModuleFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.ConsensusModuleEncodedLen())
	end, ok := codecs.EncodeConsensusModule(
		buf, 0, clusterwire.ConsensusModuleSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

func encodeModuleTracker(
	tb fragmentRequireT,
	fields codecs.PendingMessageTrackerFields,
) []byte {
	tb.Helper()
	buf := make([]byte, codecs.PendingMessageTrackerEncodedLen())
	end, ok := codecs.EncodePendingMessageTracker(
		buf, 0, clusterwire.PendingMessageTrackerSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

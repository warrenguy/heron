package service

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// fuzzLoadLogPosition is the log position the loader expects on each marker.
const fuzzLoadLogPosition int64 = 512

// fuzzLoadFrames prefixes each frame with its 2-byte little-endian length.
func fuzzLoadFrames(frames ...[]byte) []byte {
	var out []byte
	for _, frame := range frames {
		//nolint:gosec // G115: seed frames stay far below 64 KiB
		out = binary.LittleEndian.AppendUint16(out, uint16(len(frame)))
		out = append(out, frame...)
	}
	return out
}

// splitFuzzLoadFrames splits data on its length prefixes. A short tail
// becomes the last frame.
func splitFuzzLoadFrames(data []byte) [][]byte {
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

// fuzzLoadSeedRows encodes BEGIN, one row per session, and END with
// encodeSnapshotRow.
func fuzzLoadSeedRows(tb testing.TB, sessionIDs []int64) [][]byte {
	tb.Helper()
	writer := NewAgent(AgentConfig{})
	for _, id := range sessionIDs {
		require.NoError(tb, writer.restoreClientSession(codecs.ClientSessionFields{
			ClusterSessionID: id,
			ResponseStreamID: 10,
			ResponseChannel:  []byte("aeron:udp?endpoint=c:1"),
			EncodedPrincipal: []byte("p"),
		}))
	}
	take := &snapshotTake{
		logPosition:      fuzzLoadLogPosition,
		leadershipTermID: 5,
		timeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		appVersion:       3,
		sessionIDs:       sessionIDs,
	}
	rows := make([][]byte, len(sessionIDs)+2)
	for i := range rows {
		row, err := writer.encodeSnapshotRow(take, i)
		require.NoError(tb, err)
		rows[i] = slices.Clone(row)
	}
	return rows
}

// fuzzLoadMarker encodes a service snapshot marker with mark. A non-nil edit
// changes the fields first.
func fuzzLoadMarker(
	tb testing.TB,
	mark clusterwire.SnapshotMark,
	edit func(*codecs.SnapshotMarkerFields),
) []byte {
	tb.Helper()
	fields := codecs.SnapshotMarkerFields{
		TypeID:           codecs.ServiceSnapshotTypeID,
		LogPosition:      fuzzLoadLogPosition,
		LeadershipTermID: 5,
		Mark:             mark,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       3,
	}
	if edit != nil {
		edit(&fields)
	}
	buf := make([]byte, codecs.SnapshotMarkerEncodedLen())
	end, ok := codecs.EncodeSnapshotMarker(
		buf, 0, clusterwire.SnapshotMarkerSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

// fuzzLoadSeeds returns one frame set per accept and refuse row of the
// dispatch table test.
func fuzzLoadSeeds(tb testing.TB) [][][]byte {
	tb.Helper()
	accept := fuzzLoadSeedRows(tb, []int64{2, 9})
	begin, end := accept[0], accept[len(accept)-1]
	section := fuzzLoadMarker(tb, clusterwire.SnapshotMark_SECTION, nil)
	nullMark := fuzzLoadMarker(tb, clusterwire.SnapshotMark_NULL_VALUE, nil)

	termEvent := make([]byte, codecs.NewLeadershipTermEventEncodedLen())
	termEnd, _ := codecs.EncodeNewLeadershipTermEvent(
		termEvent,
		0,
		clusterwire.NewLeadershipTermEventSbeSchemaVersion,
		codecs.NewLeadershipTermEventFields{},
	)
	timer := make([]byte, codecs.TimerEncodedLen())
	timerEnd, _ := codecs.EncodeTimer(
		timer, 0, clusterwire.TimerSbeSchemaVersion, codecs.TimerFields{},
	)
	headerOnly := begin[:codecs.MessageHeaderEncodedLen()]
	wrongSchema := slices.Clone(begin)
	var header clusterwire.MessageHeader
	header.Wrap(wrongSchema, 0, 0, uint64(len(wrongSchema)))
	header.SetSchemaId(112)
	overCap := slices.Clone(accept[1])
	varLengthOffset := codecs.MessageHeaderEncodedLen() + int(clusterwire.ClientSessionSbeBlockLength)
	binary.LittleEndian.PutUint32(overCap[varLengthOffset:], 0x7fff_ffff)

	withMarker := func(edit func(*codecs.SnapshotMarkerFields)) [][]byte {
		return [][]byte{fuzzLoadMarker(tb, clusterwire.SnapshotMark_BEGIN, edit), end}
	}
	return [][][]byte{
		accept,
		fuzzLoadSeedRows(tb, []int64{9, 2}),
		{begin, section, nullMark, end},
		withMarker(func(f *codecs.SnapshotMarkerFields) {
			f.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
		}),
		{accept[1], end},
		{end},
		{termEvent[:termEnd], begin},
		{begin, begin, end},
		withMarker(func(f *codecs.SnapshotMarkerFields) {
			f.TypeID = codecs.ConsensusModuleSnapshotTypeID
		}),
		withMarker(func(f *codecs.SnapshotMarkerFields) { f.LogPosition = 640 }),
		{begin, accept[1], accept[1], end},
		{begin, timer[:timerEnd], end},
		{wrongSchema},
		{{1, 2, 3, 4}},
		{headerOnly},
		{begin, overCap, end},
		withMarker(func(f *codecs.SnapshotMarkerFields) { f.Mark = clusterwire.SnapshotMark(7) }),
		withMarker(func(f *codecs.SnapshotMarkerFields) { f.TimeUnit = clusterwire.ClusterTimeUnit(9) }),
	}
}

// FuzzSnapshotLoaderDispatch feeds length-prefixed frames to the snapshot
// loader until dispatch returns Break or Abort.
//
// Invariants:
//   - dispatch does not panic, for any mark and time unit value
//   - done implies a service BEGIN first, only markers and session rows
//     between, and END last
//   - an error implies done is false
//   - after done the agent time unit is MILLIS, MICROS or NANOS
func FuzzSnapshotLoaderDispatch(f *testing.F) {
	for _, seed := range fuzzLoadSeeds(f) {
		f.Add(fuzzLoadFrames(seed...))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		agent := NewAgent(AgentConfig{})
		loader := newServiceSnapshotLoader(agent, fuzzLoadLogPosition)
		dispatched := splitFuzzLoadFrames(data)
		for i, frame := range dispatched {
			if loader.dispatch(frame) != aeron.ControlledContinue {
				dispatched = dispatched[:i+1]
				break
			}
		}

		if loader.err != nil {
			require.False(t, loader.done, "done with an error")
		}
		if !loader.done {
			return
		}
		first, ok := codecs.TryDecodeSnapshotMarkerAt(dispatched[0], 0)
		require.True(t, ok, "first frame decodes as a marker")
		require.Equal(t, codecs.ServiceSnapshotTypeID, first.TypeID)
		require.Equal(t, int32(clusterwire.SnapshotMark_BEGIN), int32(first.Mark))
		for _, frame := range dispatched[1 : len(dispatched)-1] {
			_, templateID, _, _, _ := codecs.DecodeMessageHeader(frame, 0)
			require.Contains(t, []uint16{
				clusterwire.SnapshotMarkerSbeTemplateID,
				clusterwire.ClientSessionSbeTemplateID,
			}, templateID)
		}
		last, ok := codecs.TryDecodeSnapshotMarkerAt(dispatched[len(dispatched)-1], 0)
		require.True(t, ok, "last frame decodes as a marker")
		require.Equal(t, int32(clusterwire.SnapshotMark_END), int32(last.Mark))
		require.Contains(t, []int32{
			int32(clusterwire.ClusterTimeUnit_MILLIS),
			int32(clusterwire.ClusterTimeUnit_MICROS),
			int32(clusterwire.ClusterTimeUnit_NANOS),
		}, int32(agent.timeUnit))
	})
}

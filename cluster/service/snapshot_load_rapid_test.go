//go:build rapid && testhooks

package service

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// Load mutations the round trip draws.
const (
	loadMutationNone = iota
	loadMutationDropBegin
	loadMutationDropEnd
	loadMutationRepeatRow
	loadMutationModuleType
	loadMutationMovePosition
	loadMutationCount
)

// modelReplaySessionID is the replay session id. The image carries its low
// 32 bits.
const modelReplaySessionID int64 = 6<<32 | 61

func drawLoadBytes(rt *rapid.T, maxLen int, label string) []byte {
	rt.Helper()
	return rapid.SliceOfN(rapid.ByteRange('a', 'z'), 0, maxLen).Draw(rt, label)
}

func editBeginMarker(
	rt *rapid.T,
	row []byte,
	edit func(*codecs.SnapshotMarkerFields),
) []byte {
	rt.Helper()
	fields, ok := codecs.TryDecodeSnapshotMarkerAt(row, 0)
	require.True(rt, ok)
	edit(&fields)
	return encodeSnapshotMarkerFrame(rt, fields)
}

// Asserts that a snapshot load restores the rows the take writer encodes.
// The writer agent opens the drawn sessions as a follower and encodes the
// rows. The reader agent loads the rows from a replay image through
// snapshotLoad.run.
//
// Invariants:
//   - the reader registry equals the writer registry by id, stream, channel
//     and principal, in ascending order
//   - the reader takes the drawn time unit, app version and egress term id
//   - LoadSnapshot receives the image at the user byte start
//   - LoadSnapshot reads exactly the user bytes
//   - each mutation fails the load with errSnapshotLoad
//   - a failed load does not reach done
func TestSnapshotLoad_takeRoundTrip_rapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		rt.Helper()
		sessionCount := rapid.IntRange(0, 8).Draw(rt, "sessionCount")
		ids := rapid.SliceOfNDistinct(
			rapid.Int64Range(0, 1_000),
			sessionCount,
			sessionCount,
			rapid.ID[int64],
		).Draw(rt, "ids")
		chunk := rapid.IntRange(32, 256).Draw(rt, "chunk")
		timeUnit := rapid.SampledFrom([]clusterwire.ClusterTimeUnit{
			clusterwire.ClusterTimeUnit_MILLIS,
			clusterwire.ClusterTimeUnit_MICROS,
			clusterwire.ClusterTimeUnit_NANOS,
		}).Draw(rt, "timeUnit")
		appVersion := rapid.Int32().Draw(rt, "appVersion")
		logPosition := rapid.Int64Range(0, 1<<40).Draw(rt, "logPosition")
		termID := rapid.Int64Range(0, 1<<20).Draw(rt, "termID")
		userBytes := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "userBytes")
		mutation := rapid.IntRange(0, loadMutationCount-1).Draw(rt, "mutation")

		writer := NewAgent(AgentConfig{})
		for _, id := range ids {
			action := writer.handleSessionOpen(0, codecs.SessionOpenEventFields{
				ClusterSessionID: id,
				ResponseStreamID: rapid.Int32().Draw(rt, "streamID"),
				ResponseChannel:  drawLoadBytes(rt, 200, "channel"),
				EncodedPrincipal: drawLoadBytes(rt, 64, "principal"),
			})
			require.Equal(rt, aeron.ControlledContinue, action)
		}
		sortedIDs := slices.Sorted(slices.Values(ids))
		take := &snapshotTake{
			logPosition:      logPosition,
			leadershipTermID: termID,
			timeUnit:         timeUnit,
			appVersion:       appVersion,
			sessionIDs:       sortedIDs,
		}
		rows := make([][]byte, len(sortedIDs)+2)
		for i := range rows {
			row, err := writer.encodeSnapshotRow(take, i)
			require.NoError(rt, err)
			rows[i] = slices.Clone(row)
		}

		switch mutation {
		case loadMutationDropBegin:
			rows = rows[1:]
		case loadMutationDropEnd:
			rows = rows[:len(rows)-1]
		case loadMutationRepeatRow:
			index := rapid.IntRange(0, len(rows)-2).Draw(rt, "repeatIndex")
			rows = slices.Insert(rows, index+1, rows[index])
		case loadMutationModuleType:
			rows[0] = editBeginMarker(rt, rows[0], func(f *codecs.SnapshotMarkerFields) {
				f.TypeID = codecs.ConsensusModuleSnapshotTypeID
			})
		case loadMutationMovePosition:
			delta := rapid.Int64Range(1, 1<<20).Draw(rt, "positionDelta")
			rows[0] = editBeginMarker(rt, rows[0], func(f *codecs.SnapshotMarkerFields) {
				f.LogPosition += delta
			})
		}

		var frames []aeron.FragmentedFrameForTest
		for _, row := range rows {
			frames = append(frames, fragmentPayload(row, chunk)...)
		}
		userStart := int64(0)
		for _, frame := range frames {
			userStart += int64(logbuffer.AlignFragmentChunkLength(len(frame.Payload)))
		}
		if len(userBytes) > 0 {
			frames = append(frames, wholeFrames(userBytes)...)
		}

		var image *aeron.Image
		var writtenEnd int64
		adder := &snapshotLoadAdder{onAdd: func(sub *aeron.Subscription) {
			// Hook constructors take the outer *testing.T.
			image = aeron.NewTestImageForSubscriptionHooksWithSessionID(t, sub, 61, 9_601, 0)
			writtenEnd = writeSnapshotFrameTrain(rt, image, frames)
			require.True(rt, aeron.SubscriptionAddImageForHooks(sub, image))
		}}
		svc := &loadRecordingService{}
		load := snapshotLoad{
			control:       &fakeSnapshotReplay{sessionID: modelReplaySessionID},
			subscriptions: adder,
			pump: func() int {
				// A replay without END closes after the loader consumes every
				// frame.
				if mutation == loadMutationDropEnd && image.Position() >= writtenEnd {
					aeron.ImageInternalCloseForHooks(image)
				}
				return 0
			},
			channel:  DefaultReplayChannel,
			streamID: DefaultReplayStreamID,
			timeout:  time.Second,
			handler:  svc,
		}
		reader := NewAgent(AgentConfig{})
		reader.seedRecovery(codecs.RecoveryStateKeyFields{
			LeadershipTermID: termID,
			LogPosition:      logPosition,
		})

		err := load.run(reader, 42, logPosition)

		require.True(rt, adder.subs[0].IsClosed(), "replay subscription closed")
		require.Equal(rt, termID, reader.egressLeadershipTermID)
		if mutation != loadMutationNone {
			require.ErrorIs(rt, err, errSnapshotLoad)
			// Only END writes the time unit.
			require.Equal(rt, clusterwire.ClusterTimeUnit_NULL_VALUE, reader.timeUnit, "done")
			require.Zero(rt, reader.appVersion, "app version")
			require.Empty(rt, svc.loadPositions, "LoadSnapshot calls")
			return
		}
		require.NoError(rt, err)
		require.Equal(rt, restoredSessions(writer), restoredSessions(reader))
		require.Equal(rt, timeUnit, reader.timeUnit)
		require.Equal(rt, appVersion, reader.appVersion)
		require.Equal(rt, []int64{userStart}, svc.loadPositions)
		if len(userBytes) == 0 {
			require.Empty(rt, svc.loadBytes)
			return
		}
		require.Equal(rt, [][]byte{userBytes}, svc.loadBytes)
	})
}

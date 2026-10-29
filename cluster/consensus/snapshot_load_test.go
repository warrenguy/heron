//go:build testhooks

package consensus

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func snapshotLoadErr(detail string) string {
	return "consensus: load snapshot: " + detail
}

func wholeSnapshotFrames(payloads ...[]byte) []aeron.FragmentedFrameForTest {
	frames := make([]aeron.FragmentedFrameForTest, len(payloads))
	for i, payload := range payloads {
		frames[i] = aeron.FragmentedFrameForTest{
			Flags:   logbuffer.Unfragmented,
			Payload: payload,
		}
	}
	return frames
}

func fragmentSnapshotPayload(payload []byte, chunk int) []aeron.FragmentedFrameForTest {
	var frames []aeron.FragmentedFrameForTest
	for start := 0; start < len(payload); start += chunk {
		end := min(start+chunk, len(payload))
		var flags uint8
		if start == 0 {
			flags |= logbuffer.BeginFlag
		}
		if end == len(payload) {
			flags |= logbuffer.EndFlag
		}
		frames = append(frames, aeron.FragmentedFrameForTest{
			Flags:   flags,
			Payload: payload[start:end],
		})
	}
	return frames
}

func writeSnapshotLoaderFrames(
	tb testing.TB,
	image *aeron.Image,
	frames []aeron.FragmentedFrameForTest,
) {
	tb.Helper()
	aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		0,
		image.SessionID(),
		image.StreamID(),
		logAdapterHookTermID,
		frames,
	)
}

func pollSnapshotLoader(image *aeron.Image, loader *SnapshotLoader) {
	handler := loader.Handler()
	for range 64 {
		fragments := image.ControlledPoll(handler, 10)
		if loader.Err() != nil || loader.IsDone() || fragments == 0 {
			return
		}
	}
}

func newSnapshotLoadImage(tb testing.TB) *aeron.Image {
	tb.Helper()
	sub := aeron.NewTestSubscriptionForHooks(tb)
	image := aeron.NewTestImageForHooks(tb, sub, 0)
	require.True(tb, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

type restoredSnapshotSession struct {
	id                int64
	correlationID     int64
	openedLogPosition int64
	closeReason       clusterwire.CloseReason
	streamID          int32
	channel           string
}

func wantRestoredSession(fields codecs.ClusterSessionFields) restoredSnapshotSession {
	return restoredSnapshotSession{
		id:                fields.ClusterSessionID,
		correlationID:     fields.CorrelationID,
		openedLogPosition: fields.OpenedLogPosition,
		closeReason:       fields.CloseReason,
		streamID:          fields.ResponseStreamID,
		channel:           string(fields.ResponseChannel),
	}
}

func requireRestoredSnapshot(
	t *testing.T,
	a *Agent,
	nextSessionID int64,
	sessions []restoredSnapshotSession,
	timers []codecs.TimerFields,
) {
	t.Helper()
	require.Equal(t, nextSessionID, a.sessions.nextSessionID)
	gotTimers := a.timers.snapshotTimerRows()
	if timers == nil {
		require.Empty(t, gotTimers)
	} else {
		require.Equal(t, timers, gotTimers)
	}

	rows := a.sessions.snapshotSessionRows()
	require.Len(t, rows, len(sessions))
	closing := 0
	for i, want := range sessions {
		row := rows[i]
		require.Equal(t, want.id, row.ClusterSessionID)
		require.Equal(t, want.correlationID, row.CorrelationID)
		require.Equal(t, want.openedLogPosition, row.OpenedLogPosition)
		require.Equal(t, want.closeReason, row.CloseReason)
		require.Equal(t, want.streamID, row.ResponseStreamID)
		require.Equal(t, want.channel, string(row.ResponseChannel))
		require.Equal(t, int64(nullValue), row.TimeOfLastActivity)

		session, ok := a.sessions.sessionByID(want.id)
		require.True(t, ok)
		require.Zero(t, session.lastActivityNs)
		require.Nil(t, session.egressPublication)
		require.Equal(t, int64(nullValue), session.egressAddCorrelationID)
		require.False(t, session.newLeaderEventPending)
		require.Equal(t, want.correlationID, session.correlationID)
		require.Equal(t, want.streamID, session.responseStreamID)
		require.Equal(t, want.channel, string(session.responseChannel))
		require.Equal(t, want.openedLogPosition, session.openedLogPosition)
		if want.closeReason == clusterwire.CloseReason_NULL_VALUE {
			require.Equal(t, sessionOpen, session.state)
			continue
		}
		require.Equal(t, sessionClosing, session.state)
		require.Equal(t, want.closeReason, session.closeReason)
		closing++
	}
	require.Equal(t, closing, a.ClosingSessionCount())
	require.Equal(t, len(sessions)-closing, a.OpenSessionCount())
	require.Zero(t, a.PendingSessionCount())
	require.NoError(t, a.TimerFailure())
}

// TestSnapshotLoader_dispatch feeds take-shaped frames to the loader handler.
//
// Accept:
//   - A full take restores sessions, timers, and the next session id.
//   - SECTION and NULL_VALUE marks pass.
//   - A replay that stops before END keeps IsDone false. Err stays nil.
//
// Refuse:
//   - Each load failure names the row. IsDone stays false.
//   - Undefined mark, time unit, and close reason values format as integers.
func TestSnapshotLoader_dispatch(t *testing.T) {
	t.Parallel()

	marker := func(mark clusterwire.SnapshotMark) []byte {
		return encodeModuleMarker(t, moduleMarkerFields(mark))
	}
	withMarker := func(edit func(*codecs.SnapshotMarkerFields)) []byte {
		fields := moduleMarkerFields(clusterwire.SnapshotMark_BEGIN)
		edit(&fields)
		return encodeModuleMarker(t, fields)
	}
	begin := marker(clusterwire.SnapshotMark_BEGIN)
	end := marker(clusterwire.SnapshotMark_END)
	moduleRow := func(next int64) []byte {
		return encodeModuleConsensus(t, codecs.ConsensusModuleFields{
			NextSessionID:          next,
			NextServiceSessionID:   99,
			LogServiceSessionID:    98,
			PendingMessageCapacity: 97,
		})
	}
	session := func(fields codecs.ClusterSessionFields) []byte {
		return encodeModuleSession(t, fields)
	}
	openSession := func(id int64, channel string) codecs.ClusterSessionFields {
		return codecs.ClusterSessionFields{
			ClusterSessionID:   id,
			CorrelationID:      id * 10,
			OpenedLogPosition:  id * 8,
			TimeOfLastActivity: 1234,
			CloseReason:        clusterwire.CloseReason_NULL_VALUE,
			ResponseStreamID:   int32(id) + 10,
			ResponseChannel:    []byte(channel),
		}
	}
	closingSession := func(
		id int64,
		reason clusterwire.CloseReason,
	) codecs.ClusterSessionFields {
		fields := openSession(id, "aeron:udp?endpoint=h:1")
		fields.CloseReason = reason
		return fields
	}
	timerRow := func(id, deadline int64) []byte {
		return encodeModuleTimer(t, codecs.TimerFields{
			CorrelationID: id,
			Deadline:      deadline,
		})
	}
	trackerRow := func(serviceID, capacity int32) []byte {
		return encodeModuleTracker(t, codecs.PendingMessageTrackerFields{
			NextServiceSessionID:   50,
			LogServiceSessionID:    40,
			PendingMessageCapacity: capacity,
			ServiceID:              serviceID,
		})
	}

	const (
		deadline3 int64 = 5_000_000_000
		deadline7 int64 = 6_000_000_000
	)
	session2 := openSession(2, "aeron:ipc")
	session4 := closingSession(4, clusterwire.CloseReason_CLIENT_ACTION)
	session6 := closingSession(6, clusterwire.CloseReason_SERVICE_ACTION)
	session9 := closingSession(9, clusterwire.CloseReason_TIMEOUT)
	longChannel := "aeron:udp?endpoint=" + strings.Repeat("c", 300)
	longSession := openSession(3, longChannel)

	fullSessions := []restoredSnapshotSession{
		wantRestoredSession(session2),
		wantRestoredSession(session4),
		wantRestoredSession(session6),
		wantRestoredSession(session9),
	}
	fullTimers := []codecs.TimerFields{
		{CorrelationID: 3, Deadline: deadline3},
		{CorrelationID: 7, Deadline: deadline7},
	}
	fullFrames := wholeSnapshotFrames(
		begin,
		moduleRow(40),
		marker(clusterwire.SnapshotMark_SECTION),
		session(session9),
		session(session2),
		marker(clusterwire.SnapshotMark_NULL_VALUE),
		session(session6),
		session(session4),
		timerRow(7, deadline7),
		timerRow(3, deadline3),
		trackerRow(0, 99),
		trackerRow(1, 98),
		end,
	)

	overCap := session(openSession(4, "aeron:ipc"))
	varLengthOffset := codecs.MessageHeaderEncodedLen() +
		int(clusterwire.ClusterSessionSbeBlockLength)
	binary.LittleEndian.PutUint32(overCap[varLengthOffset:], 0x7fff_ffff)

	type dispatchCase struct {
		name       string
		frames     []aeron.FragmentedFrameForTest
		serviceCnt int
		prepare    func(t *testing.T, a *Agent)
		wantErr    string
		notDone    bool
		nextID     int64
		sessions   []restoredSnapshotSession
		timers     []codecs.TimerFields
	}
	tests := []dispatchCase{
		{
			name:       "full take restores sessions timers and next session id",
			frames:     fullFrames,
			serviceCnt: 2,
			nextID:     40,
			sessions:   fullSessions,
			timers:     fullTimers,
		},
		{
			name: "section and null marks pass",
			frames: wholeSnapshotFrames(
				begin,
				moduleRow(8),
				marker(clusterwire.SnapshotMark_SECTION),
				marker(clusterwire.SnapshotMark_NULL_VALUE),
				end,
			),
			nextID: 8,
		},
		{
			name: "minor app version still matches",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.AppVersion = cnc.ComposeSemanticVersion(1, 0, 9)
			}), moduleRow(4), end),
			nextID: 4,
		},
		{
			name: "masked app version major matches",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.AppVersion = cnc.ComposeSemanticVersion(1, 9, 9)
			}), moduleRow(4), end),
			prepare: func(t *testing.T, a *Agent) {
				t.Helper()
				a.appVersion = int32(0x101)<<16 | int32(4)<<8 | 5
			},
			nextID: 4,
		},
		{
			name: "fragmented session row assembles",
			frames: append(
				append(
					wholeSnapshotFrames(begin, moduleRow(11)),
					fragmentSnapshotPayload(session(longSession), 64)...,
				),
				wholeSnapshotFrames(end)...,
			),
			nextID: 11,
			sessions: []restoredSnapshotSession{
				wantRestoredSession(longSession),
			},
		},
		{
			name: "current next session id stays when the row is lower",
			frames: wholeSnapshotFrames(
				begin, moduleRow(40), session(session2), end,
			),
			prepare: func(t *testing.T, a *Agent) {
				t.Helper()
				a.sessions.nextSessionID = 80
			},
			nextID: 80,
			sessions: []restoredSnapshotSession{
				wantRestoredSession(session2),
			},
		},
		{
			name: "session id raises the floor above the module row",
			frames: wholeSnapshotFrames(
				begin, moduleRow(5), session(openSession(30, "aeron:ipc")), end,
			),
			nextID: 31,
			sessions: []restoredSnapshotSession{
				wantRestoredSession(openSession(30, "aeron:ipc")),
			},
		},
		{
			name:    "replay stops before end",
			frames:  wholeSnapshotFrames(begin, moduleRow(40), session(session2)),
			notDone: true,
			nextID:  40,
			sessions: []restoredSnapshotSession{
				wantRestoredSession(session2),
			},
		},
		{
			name:   "fragment before begin",
			frames: wholeSnapshotFrames(moduleRow(4), begin, end),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"template %d before the begin marker",
				clusterwire.ConsensusModuleSbeTemplateID,
			)),
		},
		{
			name: "template inside the snapshot",
			frames: wholeSnapshotFrames(
				begin,
				moduleRow(4),
				encodeScheduleTimerWire(t, codecs.ScheduleTimerFields{CorrelationID: 1}),
				end,
			),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"template %d inside the snapshot",
				clusterwire.ScheduleTimerSbeTemplateID,
			)),
		},
		{
			name: "leadership term before begin",
			frames: wholeSnapshotFrames(
				encodeLogAdapterNLTPayload(t, codecs.NewLeadershipTermEventFields{}),
				begin,
			),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"template %d before the begin marker",
				clusterwire.NewLeadershipTermEventSbeTemplateID,
			)),
		},
		{
			name:    "begin repeats",
			frames:  wholeSnapshotFrames(begin, begin, moduleRow(4), end),
			wantErr: snapshotLoadErr("begin marker repeated"),
		},
		{
			name:    "end before begin",
			frames:  wholeSnapshotFrames(end),
			wantErr: snapshotLoadErr("end marker before the begin marker"),
		},
		{
			name:   "section before begin",
			frames: wholeSnapshotFrames(marker(clusterwire.SnapshotMark_SECTION)),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"snapshot mark %d before the begin marker",
				int32(clusterwire.SnapshotMark_SECTION),
			)),
		},
		{
			name:   "null mark before begin",
			frames: wholeSnapshotFrames(marker(clusterwire.SnapshotMark_NULL_VALUE)),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"snapshot mark %d before the begin marker",
				int32(clusterwire.SnapshotMark_NULL_VALUE),
			)),
		},
		{
			name: "foreign snapshot type id",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TypeID = codecs.ServiceSnapshotTypeID
			}), end),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"snapshot type id %d, want %d",
				codecs.ServiceSnapshotTypeID,
				codecs.ConsensusModuleSnapshotTypeID,
			)),
		},
		{
			name: "marker log position off the plan",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.LogPosition = 640
			}), end),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"marker log position %d, want %d", 640, snapshotLoadLogPosition,
			)),
		},
		{
			name:    "consensus module row repeats",
			frames:  wholeSnapshotFrames(begin, moduleRow(40), moduleRow(80), end),
			wantErr: snapshotLoadErr("consensus module row repeated"),
		},
		{
			name:    "end without a consensus module row",
			frames:  wholeSnapshotFrames(begin, session(session4), end),
			wantErr: snapshotLoadErr("end marker without a consensus module row"),
		},
		{
			name: "session id repeats",
			frames: wholeSnapshotFrames(
				begin,
				moduleRow(40),
				session(session4),
				session(openSession(4, "aeron:udp")),
				end,
			),
			wantErr: snapshotLoadErr("cluster session 4 repeated"),
		},
		{
			name: "timer correlation id repeats",
			frames: wholeSnapshotFrames(
				begin, moduleRow(4), timerRow(7, deadline7), timerRow(7, deadline3), end,
			),
			wantErr: snapshotLoadErr("timer 7 repeated"),
		},
		{
			name: "tracker service id repeats",
			frames: wholeSnapshotFrames(
				begin, moduleRow(4), trackerRow(0, 1), trackerRow(0, 2), end,
			),
			serviceCnt: 1,
			wantErr:    snapshotLoadErr("tracker service id 0 repeated"),
		},
		{
			name:       "tracker service id outside the service count",
			frames:     wholeSnapshotFrames(begin, moduleRow(4), trackerRow(1, 1), end),
			serviceCnt: 1,
			wantErr:    snapshotLoadErr("tracker service id 1 outside service count 1"),
		},
		{
			name:       "negative tracker service id",
			frames:     wholeSnapshotFrames(begin, moduleRow(4), trackerRow(-1, 1), end),
			serviceCnt: 1,
			wantErr:    snapshotLoadErr("tracker service id -1 outside service count 1"),
		},
		{
			name:   "timer wheel refuses the correlation id",
			frames: wholeSnapshotFrames(begin, moduleRow(4), timerRow(7, 0), end),
			prepare: func(t *testing.T, a *Agent) {
				t.Helper()
				a.timers = newSmallTimerService(testWheelCapacity)
				require.True(t, a.timers.schedule(1, 0))
				require.True(t, a.timers.schedule(2, 0))
			},
			wantErr: snapshotLoadErr("timer wheel refused correlation id 7"),
		},
		{
			name:    "cluster session row does not decode",
			frames:  wholeSnapshotFrames(begin, moduleRow(4), overCap, end),
			wantErr: snapshotLoadErr("cluster session row does not decode"),
		},
		{
			name: "header-only marker",
			frames: wholeSnapshotFrames(encodeLogAdapterMessageHeader(
				t,
				clusterwire.SnapshotMarkerSbeBlockLength,
				clusterwire.SnapshotMarkerSbeTemplateID,
				clusterwire.MessageHeaderSbeSchemaID,
				clusterwire.SnapshotMarkerSbeSchemaVersion,
			)),
			wantErr: snapshotLoadErr("snapshot marker does not decode"),
		},
		{
			name: "schema id 112",
			frames: wholeSnapshotFrames(encodeLogAdapterMessageHeader(
				t, clusterwire.SnapshotMarkerSbeBlockLength, 100, 112, 17,
			)),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"schema id %d, want %d", 112, clusterwire.MessageHeaderSbeSchemaID,
			)),
		},
		{
			name:    "short header",
			frames:  wholeSnapshotFrames([]byte{1, 2, 3, 4}),
			wantErr: snapshotLoadErr("message header short, length 4"),
		},
		{
			name: "layout does not parse",
			frames: wholeSnapshotFrames(encodeLogAdapterMessageHeader(
				t,
				clusterwire.NewLeadershipTermEventSbeBlockLength,
				clusterwire.NewLeadershipTermEventSbeTemplateID,
				clusterwire.MessageHeaderSbeSchemaID,
				clusterwire.NewLeadershipTermEventSbeSchemaVersion,
			)),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"template %d does not decode",
				clusterwire.NewLeadershipTermEventSbeTemplateID,
			)),
		},
		{
			name: "mark 7",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.Mark = clusterwire.SnapshotMark(7)
			})),
			wantErr: snapshotLoadErr("snapshot mark 7 unknown"),
		},
		{
			name: "time unit 9",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TimeUnit = clusterwire.ClusterTimeUnit(9)
			}), end),
			wantErr: snapshotLoadErr("time unit 9 unknown"),
		},
		{
			name: "time unit millis",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TimeUnit = clusterwire.ClusterTimeUnit_MILLIS
			}), end),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"time unit %d, want %d",
				int32(clusterwire.ClusterTimeUnit_MILLIS),
				int32(clusterwire.ClusterTimeUnit_NANOS),
			)),
		},
		{
			name: "time unit null",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
			}), end),
			wantErr: snapshotLoadErr(fmt.Sprintf(
				"time unit %d, want %d",
				int32(clusterwire.ClusterTimeUnit_NULL_VALUE),
				int32(clusterwire.ClusterTimeUnit_NANOS),
			)),
		},
		{
			name: "app version major differs",
			frames: wholeSnapshotFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.AppVersion = cnc.ComposeSemanticVersion(2, 0, 1)
			}), moduleRow(4), end),
			wantErr: snapshotLoadErr("app version major 2, want 1"),
		},
		{
			name: "close reason 9",
			frames: wholeSnapshotFrames(
				begin,
				moduleRow(4),
				session(closingSession(4, clusterwire.CloseReason(9))),
				end,
			),
			wantErr: snapshotLoadErr("close reason 9 unknown"),
		},
	}

	t.Run("fragment outside the buffer", func(t *testing.T) {
		t.Parallel()
		agent := NewAgent(AgentConfig{AppVersion: snapshotLoadAppVersion})
		loader := agent.NewSnapshotLoader(snapshotLoadLogPosition)
		raw := make([]byte, logbuffer.DataHeaderLength)
		buffer := atomicbuffer.Wrap(raw)
		require.NoError(t, logbuffer.WriteDataHeaderUnfragmented(buffer, 0, 1, 2, 3, 0, 0))
		header := &aeron.Header{}
		header.SetBuffer(buffer)
		header.SetOffset(0)
		header.ResetPollFragmentState()

		var action aeron.ControlledAction
		require.NotPanics(t, func() {
			action = loader.Handler()(buffer, 0, len(raw)+8, header)
		})
		require.Equal(t, aeron.ControlledAbort, action)
		require.EqualError(t, loader.Err(), snapshotLoadErr(fmt.Sprintf(
			"fragment offset %d length %d outside the buffer", 0, len(raw)+8,
		)))
		require.False(t, loader.IsDone())
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agent := NewAgent(AgentConfig{
				AppVersion:   snapshotLoadAppVersion,
				ServiceCount: tt.serviceCnt,
			})
			if tt.prepare != nil {
				tt.prepare(t, agent)
			}
			image := newSnapshotLoadImage(t)
			writeSnapshotLoaderFrames(t, image, tt.frames)
			loader := agent.NewSnapshotLoader(snapshotLoadLogPosition)

			require.NotPanics(t, func() { pollSnapshotLoader(image, loader) })

			if tt.wantErr != "" {
				require.ErrorIs(t, loader.Err(), errSnapshotLoad)
				require.EqualError(t, loader.Err(), tt.wantErr)
				require.False(t, loader.IsDone())
				return
			}
			require.NoError(t, loader.Err())
			require.Equal(t, !tt.notDone, loader.IsDone())
			requireRestoredSnapshot(t, agent, tt.nextID, tt.sessions, tt.timers)
		})
	}
}

// TestSnapshotLoader_restoredTimerTailTimerEvent loads a snapshot at P that
// schedules timer 7. A TimerEvent for timer 7 above P cancels that timer
// during replay. After the member leads, timer work does not append a
// TimerEvent. Without the replayed TimerEvent, timer work appends
// one TimerEvent.
func TestSnapshotLoader_restoredTimerTailTimerEvent(t *testing.T) {
	t.Parallel()

	t.Run("tail timer event", func(t *testing.T) {
		t.Parallel()
		assertRestoredTimerTail(t, true)
	})
	t.Run("no tail timer event", func(t *testing.T) {
		t.Parallel()
		assertRestoredTimerTail(t, false)
	})
}

func assertRestoredTimerTail(t *testing.T, replayTail bool) {
	t.Helper()

	const (
		correlationID    int64 = 7
		leadershipTermID int64 = 3
		logPosition      int64 = 128
	)
	deadline := 2 * timerWheelTickResolutionNs
	commit := new(atomic.Int64)
	commit.Store(logPosition)
	publisher, pub := boundLogPublisher(t)
	agent := NewAgent(AgentConfig{
		AppVersion:   snapshotLoadAppVersion,
		Role:         RoleFollower,
		Commit:       commit,
		LogAdapter:   NewLogAdapter(0),
		LogPublisher: publisher,
	})

	marker := func(mark clusterwire.SnapshotMark) []byte {
		fields := moduleMarkerFields(mark)
		fields.LogPosition = logPosition
		return encodeModuleMarker(t, fields)
	}
	image := newSnapshotLoadImage(t)
	writeSnapshotLoaderFrames(t, image, wholeSnapshotFrames(
		marker(clusterwire.SnapshotMark_BEGIN),
		encodeModuleConsensus(t, codecs.ConsensusModuleFields{NextSessionID: 1}),
		encodeModuleTimer(t, codecs.TimerFields{
			CorrelationID: correlationID,
			Deadline:      deadline,
		}),
		marker(clusterwire.SnapshotMark_END),
	))
	loader := agent.NewSnapshotLoader(logPosition)
	require.NotPanics(t, func() { pollSnapshotLoader(image, loader) })
	require.NoError(t, loader.Err())
	require.True(t, loader.IsDone())
	require.Equal(t, int64(1), agent.ScheduledTimerCount())

	if replayTail {
		replay := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), logPosition)
		end := writeTimerEventFrameInTerm(
			t,
			replay,
			int(logPosition),
			correlationID,
			leadershipTermID,
		)
		agent.BindReplayLog(replay)
		require.Equal(t, 1, agent.ReplayLogPoll(end, 0))
		require.Greater(t, agent.LogAdapter().Position(), logPosition)
		require.Equal(t, agent.LogAdapter().Position(), agent.CommitPosition())
		require.Zero(t, agent.ScheduledTimerCount())
		require.Zero(t, agent.ExpiredTimerCount())
	}

	require.NoError(t, agent.JoinLogAsLeader(leadershipTermID, agent.CommitPosition(), 0, false))
	agent.ElectionComplete(0)

	expired := 0
	firedAt := int64(0)
	for tick := int64(1); tick <= 8; tick++ {
		nowNs := tick * timerWheelTickResolutionNs
		if n := agent.timerWork(nowNs); n != 0 {
			expired += n
			firedAt = nowNs
		}
	}
	require.Zero(t, agent.ScheduledTimerCount())
	require.Zero(t, agent.ExpiredTimerCount())
	if replayTail {
		require.Zero(t, expired)
		require.Zero(t, pub.Position())
		return
	}
	require.Equal(t, 1, expired)
	requireTimerEventOnLog(t, pub, leadershipTermID, correlationID, firedAt)
}

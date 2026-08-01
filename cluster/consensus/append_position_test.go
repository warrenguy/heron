//go:build testhooks

package consensus

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
)

// append_position_test uses testhooks fixtures (boundLogPublisher,
// NewImageWithCommittedDataFrameForHooks), same as log_publisher_test and
// log_adapter_test.

func advancedLogPublisher(t *testing.T) (*LogPublisher, int64) {
	t.Helper()
	pub, _ := boundLogPublisher(t)
	pos := pub.AppendMessage(1, 2, 3, []byte("hi"))
	require.Positive(t, pos)
	require.Equal(t, pos, pub.Position())
	return pub, pos
}

func adapterWithReceivedPosition(t *testing.T) (*LogAdapter, int64) {
	t.Helper()
	image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
	want := image.ReceivedPosition()
	require.Positive(t, want)
	adapter := NewLogAdapter(0)
	adapter.SetImage(image)
	return adapter, want
}

// appendAgent wires the log publisher and adapter a LogAppendPosition reads.
func appendAgent(role Role, pub *LogPublisher, adapter *LogAdapter) *Agent {
	agent := NewAgent(AgentConfig{Role: role})
	agent.SetLogPublisher(pub)
	agent.SetLogAdapter(adapter)
	return agent
}

func TestLogAppendPosition_AppendPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  func(t *testing.T) (*LogAppendPosition, int64)
	}{
		{
			name: "nil receiver",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				return nil, 0
			},
		},
		{
			name: "nil agent",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				return NewLogAppendPosition(nil), 0
			},
		},
		{
			name: "leader with bound publication",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pos := advancedLogPublisher(t)
				agent := appendAgent(RoleLeader, pub, NewLogAdapter(0))
				return NewLogAppendPosition(agent), pos
			},
		},
		{
			name: "leader unbound publication",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleLeader, NewLogPublisher(), NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "leader nil publisher",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleLeader, nil, NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower nil adapter",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleFollower, NewLogPublisher(), nil)
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower adapter no image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				agent := appendAgent(RoleFollower, NewLogPublisher(), NewLogAdapter(0))
				return NewLogAppendPosition(agent), 0
			},
		},
		{
			name: "follower bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				adapter, want := adapterWithReceivedPosition(t)
				agent := appendAgent(RoleFollower, NewLogPublisher(), adapter)
				return NewLogAppendPosition(agent), want
			},
		},
		{
			name: "candidate with bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				adapter, want := adapterWithReceivedPosition(t)
				agent := appendAgent(RoleCandidate, NewLogPublisher(), adapter)
				return NewLogAppendPosition(agent), want
			},
		},
		{
			// Dual-bound: leader must ignore image received high-water.
			name: "leader ignores bound image",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pubPos := advancedLogPublisher(t)
				adapter, imagePos := adapterWithReceivedPosition(t)
				require.NotEqual(t, pubPos, imagePos)
				agent := appendAgent(RoleLeader, pub, adapter)
				return NewLogAppendPosition(agent), pubPos
			},
		},
		{
			// Dual-bound: follower must ignore publisher position.
			name: "follower ignores publisher position",
			src: func(t *testing.T) (*LogAppendPosition, int64) {
				pub, pubPos := advancedLogPublisher(t)
				adapter, imagePos := adapterWithReceivedPosition(t)
				require.NotEqual(t, pubPos, imagePos)
				agent := appendAgent(RoleFollower, pub, adapter)
				return NewLogAppendPosition(agent), imagePos
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src, want := tt.src(t)
			requireAppendPosition(t, want, src)
		})
	}
}

func TestLogAppendPosition_roleSwitch(t *testing.T) {
	t.Parallel()

	pub, pubPos := advancedLogPublisher(t)
	adapter, imagePos := adapterWithReceivedPosition(t)
	require.NotEqual(t, pubPos, imagePos)

	agent := appendAgent(RoleFollower, pub, adapter)
	src := NewLogAppendPosition(agent)

	requireAppendPosition(t, imagePos, src)

	agent.RoleSet(RoleLeader)
	requireAppendPosition(t, pubPos, src)

	agent.RoleSet(RoleCandidate)
	requireAppendPosition(t, imagePos, src)
}

// Follower open wires the adapter before any image; join binds the image later.
func TestLogAppendPosition_followerSeesImageBind(t *testing.T) {
	t.Parallel()

	adapter := NewLogAdapter(0)
	agent := appendAgent(RoleFollower, NewLogPublisher(), adapter)
	src := NewLogAppendPosition(agent)
	requireAppendPosition(t, 0, src)

	image, _ := aeron.NewImageWithCommittedDataFrameForHooks(t, 0, 0)
	adapter.SetImage(image)
	requireAppendPosition(t, image.ReceivedPosition(), src)
}

// recordingAgent builds an agent with a recording id, a recording-active state,
// and a recording-derived append source over counters.
//
// Recording-active comes from fakeLogRecordingControl. Agent derives
// IsRecordingActive from the wired control and keeps no flag of its own. The
// recording id is set before the control goes active: SetLogRecordingID refuses
// an overwrite while active.
func recordingAgent(
	t *testing.T,
	role Role,
	recordingID int64,
	active bool,
	counters *cnc.CountersReader,
) *Agent {
	t.Helper()
	a := NewAgent(AgentConfig{
		Role:                      role,
		Commit:                    new(atomic.Int64),
		ThisMemberID:              1,
		LeadershipTermID:          1,
		LeaderHeartbeatIntervalNs: 100,
	})
	// Id before active: SetLogRecordingID refuses overwrite while active.
	a.SetLogRecordingID(recordingID)
	control := &fakeLogRecordingControl{active: active}
	a.SetLogRecordingControl(control)
	a.SetAppendSource(NewRecordingPosAppendPosition(a, counters))
	return a
}

func TestRecordingPosAppendPosition_missingCounterMatrix(t *testing.T) {
	t.Parallel()

	const (
		seedRecordingID int64 = 77
		counterValue    int64 = 9600
	)

	notReadyNoWork := func(
		t *testing.T,
		role Role,
		a *Agent,
		pt *spyPositionTable,
		eg *recordingEgress,
		src *RecordingPosAppendPosition,
		wantLocalID int64,
		ticks []int64,
		wantFailure bool,
	) {
		t.Helper()
		requireAppendNotReady(t, src)
		require.Equal(t, wantLocalID, a.LocalLogRecordingID())

		for _, now := range ticks {
			require.Equal(t, 0, a.consensusWork(now))
		}
		if role == RoleLeader {
			require.Empty(t, pt.updates)
			require.Empty(t, pt.callOrder)
			require.Equal(t, int64(0), a.CommitPosition())
		} else {
			require.Empty(t, eg.appends)
		}
		if wantFailure {
			err := a.AppendRankFailure()
			require.Error(t, err)
			require.ErrorIs(t, err, ErrAppendPositionUnavailable)
			require.Contains(t, err.Error(), fmt.Sprintf("recording_id=%d", wantLocalID))
		} else {
			require.NoError(t, a.AppendRankFailure())
		}
	}

	wireSpies := func(a *Agent) (*spyPositionTable, *recordingEgress) {
		pt := &spyPositionTable{}
		eg := &recordingEgress{}
		a.SetPositions(pt)
		a.SetEgress(eg)
		return pt, eg
	}

	rows := []struct {
		name string
		// counterRecordingID keys the seeded counter; counterPresent omits it.
		counterRecordingID int64
		counterPresent     bool
		// agentRecordingID and active set the agent's recording state.
		agentRecordingID int64
		active           bool
		ticks            []int64
		wantReady        bool
		wantFailure      bool
	}{
		{
			name:               "pre_recording_cold_start",
			counterRecordingID: seedRecordingID,
			agentRecordingID:   nullValue,
			ticks:              []int64{0},
		},
		{
			// Id seeded, control inactive, counter gone (pre-start or after stop).
			name:               "pre_recording_id_seeded",
			counterRecordingID: seedRecordingID,
			agentRecordingID:   seedRecordingID,
			ticks:              []int64{0},
		},
		{
			// Stale counter must not rank or publish.
			name:               "stopped_id_retained_counter_present",
			counterRecordingID: seedRecordingID,
			counterPresent:     true,
			agentRecordingID:   seedRecordingID,
			ticks:              []int64{0},
		},
		{
			// Two ticks 1ms apart: under the 5s default bound.
			name:               "active_counter_absent_within_bound",
			counterRecordingID: seedRecordingID,
			agentRecordingID:   seedRecordingID,
			active:             true,
			ticks:              []int64{0, 1_000_000},
		},
		{
			// First tick opens the run. Second at 6s past bound marks failure.
			name:               "active_counter_absent_past_bound",
			counterRecordingID: seedRecordingID,
			agentRecordingID:   seedRecordingID,
			active:             true,
			ticks:              []int64{0, 6_000_000_000},
			wantFailure:        true,
		},
		{
			name:               "active_counter_found",
			counterRecordingID: seedRecordingID,
			counterPresent:     true,
			agentRecordingID:   seedRecordingID,
			active:             true,
			wantReady:          true,
		},
		{
			// Counter keys 78. Agent holds 77.
			name:               "active_counter_different_recording_id",
			counterRecordingID: seedRecordingID + 1,
			counterPresent:     true,
			agentRecordingID:   seedRecordingID,
			active:             true,
			ticks:              []int64{0},
		},
		{
			// Null id with active: early check; no scan match.
			name:               "active_null_recording_id",
			counterRecordingID: seedRecordingID,
			counterPresent:     true,
			agentRecordingID:   nullValue,
			active:             true,
			ticks:              []int64{0},
		},
	}

	roles := []Role{RoleLeader, RoleFollower}
	for _, tt := range rows {
		for _, role := range roles {
			t.Run(fmt.Sprintf("%s/%s", tt.name, role), func(t *testing.T) {
				t.Parallel()
				counters := countersWithRecordingPosition(
					t, tt.counterRecordingID, counterValue, tt.counterPresent,
				)
				a := recordingAgent(t, role, tt.agentRecordingID, tt.active, counters)
				pt, eg := wireSpies(a)
				src := a.AppendSource().(*RecordingPosAppendPosition)

				if !tt.wantReady {
					notReadyNoWork(
						t, role, a, pt, eg, src,
						tt.agentRecordingID, tt.ticks, tt.wantFailure,
					)
					return
				}

				requireAppendPosition(t, counterValue, src)
				require.Equal(t, 1, a.consensusWork(0))
				require.NoError(t, a.AppendRankFailure())
				if role == RoleLeader {
					require.Equal(t, []memberPositionUpdate{{
						memberID:         1,
						leadershipTermID: 1,
						logPosition:      counterValue,
					}}, pt.updates)
					require.Equal(t, counterValue, a.CommitPosition())
				} else {
					require.Equal(t, []appendPublishCall{{
						term: 1, pos: counterValue, flags: 0,
					}}, eg.appends)
				}
			})
		}
	}

	t.Run("nil_receiver", func(t *testing.T) {
		t.Parallel()
		requireAppendNotReady(t, (*RecordingPosAppendPosition)(nil))
	})
	t.Run("nil_agent_nil_counters", func(t *testing.T) {
		t.Parallel()
		requireAppendNotReady(t, NewRecordingPosAppendPosition(nil, nil))
	})
}

// TestRecordingPosAppendPosition_cachedSlotRevalidates asserts the cached
// counter slot revalidates on every read. The slot follows in-place value
// advances and never reports a slot reused by another recording.
func TestRecordingPosAppendPosition_cachedSlotRevalidates(t *testing.T) {
	t.Parallel()

	const recordingID int64 = 77

	// Three slots: two writable, one unused scan stop.
	meta := make([]byte, 3*cnc.CounterMetadataLength)
	values := make([]byte, 3*cnc.CounterValueLength)
	writeCounter := func(counterID int32, id, value int64) {
		require.NoError(t, cnc.WriteRecordingPositionCounterRecord(
			meta,
			values,
			counterID,
			cnc.CounterRecordAllocated,
			cnc.ArchiveRecordingPositionCounterTypeID,
			id,
			1, // session
			"aeron:ipc",
			cnc.NullValue,
			value,
		))
	}
	writeCounter(0, recordingID, 100)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)

	a := recordingAgent(t, RoleFollower, recordingID, true, reader)
	src := a.AppendSource().(*RecordingPosAppendPosition)

	// First read scans and caches slot 0.
	requireAppendPosition(t, 100, src)
	require.Equal(t, int32(0), src.cachedCounterID)

	// In-place advance: cached slot reads the new value.
	writeCounter(0, recordingID, 150)
	requireAppendPosition(t, 150, src)
	require.Equal(t, int32(0), src.cachedCounterID)

	// Slot 0 reused by another recording: revalidation fails; rescan finds slot 1.
	// Stale slot value 500 is never reported.
	writeCounter(0, recordingID+1, 500)
	writeCounter(1, recordingID, 200)
	requireAppendPosition(t, 200, src)
	require.Equal(t, int32(1), src.cachedCounterID)

	// No slot for the recording: not-ready; cache drops.
	writeCounter(1, recordingID+1, 999)
	requireAppendNotReady(t, src)
	require.Equal(t, cnc.NullCounterID, src.cachedCounterID)

	// Counter returns: fresh scan recaches.
	writeCounter(1, recordingID, 300)
	requireAppendPosition(t, 300, src)
	require.Equal(t, int32(1), src.cachedCounterID)
}

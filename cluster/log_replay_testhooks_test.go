//go:build testhooks

package cluster

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
	"github.com/warrenguy/heron/cluster/consensus"
)

// fakeReplayControl is a test double for replayControl. The fake records every
// call and returns the configured session id or error.
type fakeReplayControl struct {
	startCalls []fakeStartReplayCall
	stopCalls  []int64
	sessionID  int64
	startErr   error
	stopErr    error
}

type fakeStartReplayCall struct {
	recordingID    int64
	position       int64
	length         int64
	replayStreamID int32
	replayChannel  string
}

func (f *fakeReplayControl) StartReplay(
	_ context.Context,
	recordingID int64,
	position int64,
	length int64,
	replayStreamID int32,
	replayChannel []byte,
) (int64, error) {
	f.startCalls = append(f.startCalls, fakeStartReplayCall{
		recordingID:    recordingID,
		position:       position,
		length:         length,
		replayStreamID: replayStreamID,
		replayChannel:  string(replayChannel),
	})
	if f.startErr != nil {
		return 0, f.startErr
	}
	return f.sessionID, nil
}

func (f *fakeReplayControl) StopReplay(_ context.Context, replaySessionID int64) error {
	f.stopCalls = append(f.stopCalls, replaySessionID)
	return f.stopErr
}

var _ replayControl = (*fakeReplayControl)(nil)

// fakeSubscriptionAdder is a scripted SubscriptionAdder. The fake records the
// stream id and channel of each call, and returns either a hook-built
// subscription or a scripted error.
type fakeSubscriptionAdder struct {
	calls  []fakeAddSubscriptionCall
	result *aeron.Subscription
	err    error
}

type fakeAddSubscriptionCall struct {
	streamID int32
	channel  string
}

func (f *fakeSubscriptionAdder) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	f.calls = append(f.calls, fakeAddSubscriptionCall{
		streamID: streamID,
		channel:  channel,
	})
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

var _ SubscriptionAdder = (*fakeSubscriptionAdder)(nil)

const (
	testReplayRecordingID int64 = 77
	testReplayStart       int64 = 1000
	testReplayStop        int64 = 2000
	testReplaySessionID   int64 = 4242
	testReplayStreamID    int32 = 103
	testReplayChannel           = "aeron:ipc"
	testImageAwaitNs      int64 = 1_000_000_000
)

// testReplayAgent returns a follower with a module log adapter, no image and
// no services.
func testReplayAgent(recordingID int64) *consensus.Agent {
	a := consensus.NewAgent(consensus.AgentConfig{
		Commit:     new(atomic.Int64),
		LogAdapter: consensus.NewLogAdapter(0),
	})
	if recordingID >= 0 {
		a.SetLogRecordingID(recordingID)
	}
	return a
}

// writeReplayFrame commits one unfragmented cluster log frame at position
// offset of image. Returns where the frame ends.
func writeReplayFrame(t *testing.T, image *aeron.Image, offset int64, payload []byte) int64 {
	t.Helper()
	aeron.WriteCommittedDataFrameForHooks(
		aeron.ImageTermBufferForHooks(image),
		int(offset),
		image.SessionID(),
		image.StreamID(),
		wireHookTermID,
		int32(offset),
		logbuffer.Unfragmented,
		payload,
	)
	return offset + int64(logbuffer.AlignFragmentChunkLength(len(payload)))
}

// deliverServiceControl hands frame to agent on its service control ingress,
// then runs one agent duty cycle. The cycle polls the frame, then pops a
// satisfied wait.
func deliverServiceControl(t *testing.T, agent *consensus.Agent, frame []byte) {
	t.Helper()
	ingress := &consensus.QueueConsensusIngress{}
	ingress.Enqueue(frame)
	agent.SetServiceIngress(ingress)
	agent.DoWork(0)
	agent.SetServiceIngress(nil)
}

// scheduleReplayTimer schedules one timer on agent through its service
// control ingress.
func scheduleReplayTimer(t *testing.T, agent *consensus.Agent, correlationID, deadline int64) {
	t.Helper()
	buf := make([]byte, codecs.ScheduleTimerEncodedLen())
	wireEnd, ok := codecs.EncodeScheduleTimer(
		buf,
		0,
		clusterwire.ScheduleTimerSbeSchemaVersion,
		codecs.ScheduleTimerFields{CorrelationID: correlationID, Deadline: deadline},
	)
	require.True(t, ok)
	deliverServiceControl(t, agent, buf[:wireEnd])
}

// ackReplayService delivers one ServiceAck from service 0 at logPosition with
// RelevantID -1.
func ackReplayService(t *testing.T, agent *consensus.Agent, logPosition, ackID int64) {
	t.Helper()
	ackReplayServiceRecording(t, agent, logPosition, ackID, -1)
}

// ackReplayServiceRecording delivers one ServiceAck from service 0 at
// logPosition with recordingID as RelevantID.
func ackReplayServiceRecording(
	t *testing.T,
	agent *consensus.Agent,
	logPosition int64,
	ackID int64,
	recordingID int64,
) {
	t.Helper()
	buf := make([]byte, codecs.ServiceAckEncodedLen())
	wireEnd, ok := codecs.EncodeServiceAck(
		buf,
		0,
		clusterwire.ServiceAckSbeSchemaVersion,
		codecs.ServiceAckFields{LogPosition: logPosition, AckID: ackID, RelevantID: recordingID},
	)
	require.True(t, ok)
	deliverServiceControl(t, agent, buf[:wireEnd])
}

// encodeReplaySessionOpen returns a SessionOpenEvent payload for sessionID.
func encodeReplaySessionOpen(t *testing.T, sessionID int64) []byte {
	t.Helper()
	fields := codecs.SessionOpenEventFields{
		LeadershipTermID: 1,
		CorrelationID:    sessionID + 100,
		ClusterSessionID: sessionID,
		Timestamp:        5,
		ResponseStreamID: 102,
		ResponseChannel:  []byte("aeron:udp?endpoint=localhost:20121"),
	}
	length, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength,
		len(fields.ResponseChannel),
		len(fields.EncodedPrincipal),
	)
	require.True(t, ok)
	buf := make([]byte, length)
	wireEnd, ok := codecs.EncodeSessionOpenEvent(
		buf,
		0,
		clusterwire.SessionOpenEventSbeSchemaVersion,
		fields,
	)
	require.True(t, ok)
	return buf[:wireEnd]
}

// encodeReplaySessionClose returns a SessionCloseEvent payload for sessionID.
func encodeReplaySessionClose(t *testing.T, sessionID int64) []byte {
	t.Helper()
	buf := make([]byte, codecs.SessionCloseEventEncodedLen())
	wireEnd, ok := codecs.EncodeSessionCloseEvent(
		buf,
		0,
		clusterwire.SessionCloseEventSbeSchemaVersion,
		codecs.SessionCloseEventFields{
			LeadershipTermID: 1,
			ClusterSessionID: sessionID,
			Timestamp:        6,
			CloseReason:      clusterwire.CloseReason_CLIENT_ACTION,
		},
	)
	require.True(t, ok)
	return buf[:wireEnd]
}

// encodeReplaySnapshotAction returns a SNAPSHOT ClusterActionRequest payload
// with flags 0 whose frame ends at logPosition.
func encodeReplaySnapshotAction(t *testing.T, logPosition int64) []byte {
	t.Helper()
	buf := make([]byte, codecs.ClusterActionRequestEncodedLen())
	wireEnd, ok := codecs.EncodeClusterActionRequest(
		buf,
		0,
		clusterwire.ClusterActionRequestSbeSchemaVersion,
		codecs.ClusterActionRequestFields{
			LeadershipTermID: 1,
			LogPosition:      logPosition,
			Timestamp:        7,
			Action:           clusterwire.ClusterAction_SNAPSHOT,
		},
	)
	require.True(t, ok)
	return buf[:wireEnd]
}

// writeReplaySnapshotAction writes a SNAPSHOT action at offset. Returns where
// the frame ends.
func writeReplaySnapshotAction(t *testing.T, image *aeron.Image, offset int64) int64 {
	t.Helper()
	end := offset + int64(logbuffer.AlignFragmentChunkLength(codecs.ClusterActionRequestEncodedLen()))
	require.Equal(t, end, writeReplayFrame(t, image, offset, encodeReplaySnapshotAction(t, end)))
	return end
}

// writeReplayTimerEvent writes a TimerEvent at offset. Returns where the frame
// ends.
func writeReplayTimerEvent(t *testing.T, image *aeron.Image, offset, correlationID int64) int64 {
	t.Helper()
	return writeReplayFrame(t, image, offset, encodeWireTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    correlationID,
		Timestamp:        5,
	}))
}

func testReplayDeps(
	control *fakeReplayControl,
	adder *fakeSubscriptionAdder,
	agent *consensus.Agent,
) logReplayDeps {
	return logReplayDeps{
		Control:        control,
		Adder:          adder,
		Agent:          agent,
		ReplayChannel:  testReplayChannel,
		ReplayStreamID: testReplayStreamID,
		ImageAwaitNs:   testImageAwaitNs,
	}
}

func testReplaySubscription(t *testing.T) *aeron.Subscription {
	t.Helper()
	return aeron.NewSubscriptionForHooks(1, testReplayStreamID, testReplayChannel)
}

func attachReplayImage(
	t *testing.T,
	sub *aeron.Subscription,
	sessionID int64,
	joinPosition int64,
) *aeron.Image {
	t.Helper()
	image := aeron.NewTestImageForSubscriptionHooksWithSessionID(
		t,
		sub,
		int32(uint32(sessionID)),
		100,
		joinPosition,
	)
	require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
	return image
}

func TestLogReplay_terminalClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T) *logReplay
		wantPos    int64
		wantReason string // substring; empty means require.Empty
	}{
		{
			name: "reachedBound",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				aeron.ImageSetSubscriberPositionForHooks(image, testReplayStop)
				_ = r.DoWork(1)
				return r
			},
			wantPos:    testReplayStop,
			wantReason: "",
		},
		{
			name: "closedShortOfBound",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				mid := testReplayStart + (testReplayStop-testReplayStart)/2
				aeron.ImageSetSubscriberPositionForHooks(image, mid)
				aeron.ImageInternalCloseForHooks(image)
				_ = r.DoWork(1)
				return r
			},
			wantPos:    testReplayStart + (testReplayStop-testReplayStart)/2,
			wantReason: "before the bound",
		},
		{
			name: "closedAtBound",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				aeron.ImageSetSubscriberPositionForHooks(image, testReplayStop)
				aeron.ImageInternalCloseForHooks(image)
				_ = r.DoWork(1)
				return r
			},
			wantPos:    testReplayStop,
			wantReason: "",
		},
		{
			name: "reboundPastBound",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				agent := testReplayAgent(testReplayRecordingID)
				r := newLogReplay(
					testReplayDeps(control, adder, agent),
					testReplayStart,
					testReplayStop,
				)
				_ = attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				other := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), testReplayStop)
				agent.BindReplayLog(other)
				require.Equal(t, 0, r.DoWork(1))

				// The other image feeds nothing and raises no commit.
				require.Equal(t, int64(0), agent.CommitPosition())
				require.Equal(t, testReplayStop, other.Position())
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "before the bound",
		},
		{
			name: "imageNeverAppears",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)

				// First DoWork call records the deadline. Second call exceeds it.
				require.Equal(t, 0, r.DoWork(0))
				require.False(t, r.IsDone())
				_ = r.DoWork(testImageAwaitNs)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "did not appear",
		},
		{
			name: "joinPositionMismatch",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				_ = attachReplayImage(t, sub, testReplaySessionID, testReplayStart+1)
				_ = r.DoWork(0)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "join position mismatch",
		},
		{
			name: "nullRecordingID",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				adder := &fakeSubscriptionAdder{}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(-1)),
					testReplayStart,
					testReplayStop,
				)
				require.Empty(t, control.startCalls)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "no local log recording",
		},
		{
			name: "nonPositiveSpan",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				adder := &fakeSubscriptionAdder{}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStart,
				)
				require.Empty(t, control.startCalls)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "replay bound not above start position",
		},
		{
			name: "startReplayRefused",
			setup: func(t *testing.T) *logReplay {
				control := &fakeReplayControl{
					sessionID: testReplaySessionID,
					startErr:  errors.New("catalog missing"),
				}
				adder := &fakeSubscriptionAdder{}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				require.Empty(t, adder.calls)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "catalog missing",
		},
		{
			name: "subscriptionAddRefused",
			setup: func(t *testing.T) *logReplay {
				stopErr := errors.New("stop after add refuse")
				control := &fakeReplayControl{
					sessionID: testReplaySessionID,
					stopErr:   stopErr,
				}
				adder := &fakeSubscriptionAdder{err: errors.New("driver refused")}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
				require.ErrorIs(t, r.closeError(), stopErr)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "driver refused",
		},
		{
			name: "adderReturnedNilSubscription",
			setup: func(t *testing.T) *logReplay {
				// Nil subscription with no add error is construction-bug shaped.
				// addSubscriptionChecked normalizes the nil result into an error.
				// Construction stops the started session.
				stopErr := errors.New("stop after nil sub")
				control := &fakeReplayControl{
					sessionID: testReplaySessionID,
					stopErr:   stopErr,
				}
				adder := &fakeSubscriptionAdder{result: nil}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
				require.ErrorIs(t, r.closeError(), stopErr)
				return r
			},
			wantPos:    testReplayStart,
			wantReason: "no subscription",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.setup(t)
			require.True(t, r.IsDone())
			require.Equal(t, tt.wantPos, r.Position())

			// Drain paths advance agent commit from observed progress.
			if tt.wantPos != testReplayStart {
				require.Equal(t, tt.wantPos, r.deps.Agent.CommitPosition())
			}
			if tt.wantReason == "" {
				require.Empty(t, r.reason())
				return
			}
			require.Contains(t, r.reason(), tt.wantReason)
		})
	}
}

func TestLogReplay_cancelsTimerInReplayRange(t *testing.T) {
	t.Parallel()

	const correlationID int64 = 31
	agent := testReplayAgent(testReplayRecordingID)
	scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
	require.Equal(t, int64(1), agent.ScheduledTimerCount())

	var stop int64
	r, _, _ := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
		stop = writeReplayTimerEvent(t, image, 0, correlationID)
		return stop
	})
	require.Equal(t, 1, r.DoWork(0))
	for range 4 {
		r.DoWork(1)
	}

	require.True(t, r.IsDone())
	require.Empty(t, r.reason())
	require.Equal(t, int64(0), agent.ScheduledTimerCount())
	require.Equal(t, stop, agent.CommitPosition())
}

func TestLogReplay_replayChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sessionID      int64
		replayChannel  string
		wantSessionFmt string
	}{
		{
			name:           "positiveSessionID",
			sessionID:      42,
			replayChannel:  testReplayChannel,
			wantSessionFmt: "42",
		},
		{
			name:           "negativeSessionID",
			sessionID:      int64(uint32(0xFFFFFFFF)),
			replayChannel:  testReplayChannel,
			wantSessionFmt: "-1",
		},
		{
			name:           "compositeSessionID",
			sessionID:      (int64(7) << 32) | 4242,
			replayChannel:  testReplayChannel,
			wantSessionFmt: "4242",
		},
		{
			name:           "channelAlreadyCarriesParams",
			sessionID:      99,
			replayChannel:  "aeron:ipc?alias=cluster-replay",
			wantSessionFmt: "99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			control := &fakeReplayControl{sessionID: tt.sessionID}
			adder := &fakeSubscriptionAdder{
				result: aeron.NewSubscriptionForHooks(1, testReplayStreamID, tt.replayChannel),
			}
			deps := testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID))
			deps.ReplayChannel = tt.replayChannel
			r := newLogReplay(deps, testReplayStart, testReplayStop)
			require.False(t, r.IsDone())

			require.Len(t, control.startCalls, 1)
			call := control.startCalls[0]
			require.Equal(t, testReplayRecordingID, call.recordingID)
			require.Equal(t, testReplayStart, call.position)
			require.Equal(t, testReplayStop-testReplayStart, call.length)
			require.Equal(t, testReplayStreamID, call.replayStreamID)
			require.Equal(t, tt.replayChannel, call.replayChannel)

			require.Len(t, adder.calls, 1)
			wantChannel := aeron.AppendChannelParam(
				tt.replayChannel,
				aeron.ChannelSessionIDParam,
				strconv.FormatInt(int64(int32(uint32(tt.sessionID))), 10),
			)
			require.Equal(t, wantChannel, adder.calls[0].channel)
			require.Equal(t, testReplayStreamID, adder.calls[0].streamID)
			require.Contains(t, adder.calls[0].channel, "session-id="+tt.wantSessionFmt)
		})
	}
}

func TestLogReplay_close(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "closeCancelsSnapshotTakeBeforeStop",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 1)
				snapshotAdder := &fakeSnapshotPublicationAdder{}
				agent.SetSnapshotRecordingControl(fakeSnapshotRecordingControl{})
				agent.SetSnapshotPublicationAdder(snapshotAdder)
				var actionEnd int64
				r, control, _ := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
					actionEnd = writeReplaySnapshotAction(t, image, 0)
					return writeReplayFrame(t, image, actionEnd, encodeReplaySessionOpen(t, 3))
				})
				snapshotAdder.control = control
				require.Equal(t, 1, r.DoWork(0))
				r.DoWork(1)
				require.True(t, agent.HasPendingSnapshotAction())

				// Phase 1: the ack pops the wait into a take held at its add.
				ackReplayServiceRecording(t, agent, actionEnd, 0, 7)
				agent.DoWork(2)
				require.True(t, agent.HasPendingSnapshotAction())
				require.Equal(t, 1, snapshotAdder.offers)
				require.False(t, r.IsDone())

				// Phase 2: close cancels the take, then stops the replay.
				r.Close()
				require.Equal(t, []int64{1}, snapshotAdder.cancelled)
				require.Equal(t, []int{0}, snapshotAdder.stopsAtCancel)
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
				require.False(t, agent.HasPendingSnapshotAction())
				_, resolved := agent.LastSnapshotTake()
				require.False(t, resolved)
				agent.DoWork(3)
				require.Equal(t, 1, snapshotAdder.offers)
			},
		},
		{
			name: "closeStopsAndCloses",
			run: func(t *testing.T) {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				_ = attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				r.Close()
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
				require.True(t, sub.IsClosed())
			},
		},
		{
			name: "closeIsIdempotent",
			run: func(t *testing.T) {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				r.Close()
				r.Close()
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
			},
		},
		{
			name: "doWorkAfterClose",
			run: func(t *testing.T) {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				require.False(t, r.IsDone())
				posBefore := r.Position()
				reasonBefore := r.reason()
				doneBefore := r.IsDone()

				r.Close()

				// Own teardown must not classify as an external short close.
				require.Equal(t, 0, r.DoWork(1))
				require.Equal(t, doneBefore, r.IsDone())
				require.Equal(t, posBefore, r.Position())
				require.Equal(t, reasonBefore, r.reason())

				// Closing the image after Close must not change classification.
				aeron.ImageInternalCloseForHooks(image)
				require.Equal(t, 0, r.DoWork(2))
				require.Equal(t, doneBefore, r.IsDone())
				require.Equal(t, posBefore, r.Position())
				require.Equal(t, reasonBefore, r.reason())
			},
		},
		{
			name: "closeAtBoundSkipsStop",
			run: func(t *testing.T) {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				aeron.ImageSetSubscriberPositionForHooks(image, testReplayStop)
				_ = r.DoWork(1)
				require.True(t, r.IsDone())
				require.Equal(t, testReplayStop, r.deps.Agent.CommitPosition())

				// Bounded session delivered its full range and has ended. Close skips the
				// stop await and still closes the subscription.
				r.Close()
				require.Empty(t, control.stopCalls)
				require.True(t, sub.IsClosed())
			},
		},
		{
			name: "closeShortOfBoundStops",
			run: func(t *testing.T) {
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				sub := testReplaySubscription(t)
				adder := &fakeSubscriptionAdder{result: sub}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				image := attachReplayImage(t, sub, testReplaySessionID, testReplayStart)
				require.Equal(t, 1, r.DoWork(0))
				mid := testReplayStart + (testReplayStop-testReplayStart)/2
				aeron.ImageSetSubscriberPositionForHooks(image, mid)
				aeron.ImageInternalCloseForHooks(image)
				_ = r.DoWork(1)
				require.True(t, r.IsDone())
				require.Equal(t, mid, r.Position())
				require.NotEmpty(t, r.reason())

				// Done short of the bound still stops the archive session.
				r.Close()
				require.Equal(t, []int64{testReplaySessionID}, control.stopCalls)
				require.True(t, sub.IsClosed())
			},
		},
		{
			name: "closeAfterConstructionFailure",
			run: func(t *testing.T) {
				control := &fakeReplayControl{
					sessionID: testReplaySessionID,
					startErr:  errors.New("refused"),
				}
				adder := &fakeSubscriptionAdder{}
				r := newLogReplay(
					testReplayDeps(control, adder, testReplayAgent(testReplayRecordingID)),
					testReplayStart,
					testReplayStop,
				)
				require.True(t, r.IsDone())
				r.Close()
				require.Empty(t, control.stopCalls)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// testServiceReplayAgent returns a follower with a module log adapter of
// fragmentLimit, serviceCount services, no service proxy and the test
// recording id.
func testServiceReplayAgent(fragmentLimit, serviceCount int) *consensus.Agent {
	a := consensus.NewAgent(consensus.AgentConfig{
		Commit:       new(atomic.Int64),
		LogAdapter:   consensus.NewLogAdapter(fragmentLimit),
		ServiceCount: serviceCount,
	})
	a.SetLogRecordingID(testReplayRecordingID)
	return a
}

// attachServiceProxy wires a service proxy over pub into agent.
func attachServiceProxy(agent *consensus.Agent, pub *aeron.Publication) {
	proxy := consensus.NewServiceProxy()
	proxy.SetPublication(pub)
	agent.SetServiceProxy(proxy)
}

// startArchiveReplay builds an archive mode replay of [0, stop] for agent. It
// returns the replay, the control and the replay image, still unbound.
func startArchiveReplay(
	t *testing.T,
	agent *consensus.Agent,
	writeFrames func(image *aeron.Image) int64,
) (*logReplay, *fakeReplayControl, *aeron.Image) {
	t.Helper()
	control := &fakeReplayControl{sessionID: testReplaySessionID}
	sub := testReplaySubscription(t)
	adder := &fakeSubscriptionAdder{result: sub}
	image := attachReplayImage(t, sub, testReplaySessionID, 0)
	stop := writeFrames(image)
	r := newLogReplay(testReplayDeps(control, adder, agent), 0, stop)
	require.False(t, r.IsDone())
	require.Len(t, control.startCalls, 1)
	return r, control, image
}

func TestLogReplay_dispatchesThroughModuleAdapter(t *testing.T) {
	t.Parallel()

	agent := testServiceReplayAgent(1, 0)
	var openEnd, closeEnd, reopenEnd int64
	r, _, _ := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
		openEnd = writeReplayFrame(t, image, 0, encodeReplaySessionOpen(t, 11))
		closeEnd = writeReplayFrame(t, image, openEnd, encodeReplaySessionClose(t, 11))
		reopenEnd = writeReplayFrame(t, image, closeEnd, encodeReplaySessionOpen(t, 12))
		return reopenEnd
	})
	require.Equal(t, 1, r.DoWork(0))

	// Phase 1: the first batch opens session 11.
	require.Equal(t, 1, r.DoWork(1))
	require.Equal(t, openEnd, agent.CommitPosition())
	require.Equal(t, 1, agent.OpenSessionCount())
	opened, ok := agent.OpenSessionLogPositionForTest(11)
	require.True(t, ok)
	require.Equal(t, openEnd, opened)
	require.Equal(t, int64(12), agent.NextSessionIDForTest())

	// Phase 2: the second batch closes it.
	require.Equal(t, 1, r.DoWork(2))
	require.Equal(t, closeEnd, agent.CommitPosition())
	require.Equal(t, 0, agent.OpenSessionCount())

	// Phase 3: the third batch opens session 12 and reaches the stop.
	require.Equal(t, 1, r.DoWork(3))
	require.Equal(t, reopenEnd, agent.CommitPosition())
	require.Equal(t, 1, agent.OpenSessionCount())
	opened, ok = agent.OpenSessionLogPositionForTest(12)
	require.True(t, ok)
	require.Equal(t, reopenEnd, opened)
	require.Equal(t, int64(13), agent.NextSessionIDForTest())
	require.True(t, r.IsDone())
	require.Empty(t, r.reason())

	// Phase 4: Close unbinds only its own image. A rebound image survives.
	other := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), reopenEnd)
	agent.BindReplayLog(other)
	r.Close()
	require.Same(t, other, agent.LogAdapter().Image())
}

func TestLogReplay_modes(t *testing.T) {
	t.Parallel()

	const correlationID int64 = 41
	// liveImage returns an image that holds one TimerEvent from position 0.
	// The image sits at position.
	liveImage := func(t *testing.T, position int64) (*aeron.Image, int64) {
		t.Helper()
		image := aeron.NewTestImageForHooks(t, aeron.NewTestSubscriptionForHooks(t), 0)
		end := writeReplayTimerEvent(t, image, 0, correlationID)
		aeron.ImageSetSubscriberPositionForHooks(image, position)
		return image, end
	}
	// requireArchiveReplay runs an archive mode replay of one TimerEvent to
	// its stop. Close then unbinds the replay image.
	requireArchiveReplay := func(t *testing.T, agent *consensus.Agent) {
		t.Helper()
		r, control, image := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
			return writeReplayTimerEvent(t, image, 0, correlationID)
		})
		require.Equal(t, int64(0), control.startCalls[0].position)
		require.Equal(t, 1, r.DoWork(0))
		require.Same(t, image, agent.LogAdapter().Image())
		require.Equal(t, 1, r.DoWork(1))
		require.True(t, r.IsDone())
		require.Equal(t, int64(0), agent.ScheduledTimerCount())

		r.Close()
		require.Nil(t, agent.LogAdapter().Image())
		require.Empty(t, control.stopCalls)
	}

	// Archive-mode rows set prepare and leave run nil. The loop builds a
	// service replay agent with one scheduled timer, runs prepare, then
	// requireArchiveReplay.
	tests := []struct {
		name    string
		run     func(t *testing.T)
		prepare func(t *testing.T, agent *consensus.Agent)
	}{
		{
			name: "open image at the start reads that image",
			run: func(t *testing.T) {
				// No local recording: bound-image mode does not need one.
				agent := testReplayAgent(-1)
				scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
				image, stop := liveImage(t, 0)
				agent.LogAdapter().SetImage(image)
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				adder := &fakeSubscriptionAdder{}

				r := newLogReplay(testReplayDeps(control, adder, agent), 0, stop)
				require.Empty(t, control.startCalls)
				require.Empty(t, adder.calls)
				require.Equal(t, 1, r.DoWork(0))
				require.True(t, r.IsDone())
				require.Empty(t, r.reason())
				require.Equal(t, stop, agent.CommitPosition())
				require.Equal(t, int64(0), agent.ScheduledTimerCount())

				r.Close()
				require.Same(t, image, agent.LogAdapter().Image())
				require.Empty(t, control.stopCalls)
			},
		},
		{
			name: "open image at the start sends no replay JoinLog",
			run: func(t *testing.T) {
				agent := consensus.NewAgent(consensus.AgentConfig{
					Commit:       new(atomic.Int64),
					LogAdapter:   consensus.NewLogAdapter(0),
					ServiceCount: 1,
				})
				scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
				pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
				attachServiceProxy(agent, pub)
				image, stop := liveImage(t, 0)
				agent.LogAdapter().SetImage(image)
				control := &fakeReplayControl{sessionID: testReplaySessionID}
				adder := &fakeSubscriptionAdder{}

				r := newLogReplay(testReplayDeps(control, adder, agent), 0, stop)
				require.Equal(t, 1, r.DoWork(0))
				require.True(t, r.IsDone())
				require.Empty(t, r.reason())
				require.Equal(t, stop, agent.CommitPosition())

				var frames [][]byte
				off := 0
				bridgePub(t, pub, &off, func(msg []byte) { frames = append(frames, msg) })
				require.Empty(t, frames)
				require.False(t, agent.HasPendingJoinLog())
				require.Equal(t, 0, agent.PendingServiceAckWaits())
				require.True(t, agent.IsReplayLogJoined())
				r.Close()
			},
		},
		{
			name: "open image at another position replays the archive",
			prepare: func(t *testing.T, agent *consensus.Agent) {
				image, _ := liveImage(t, 64)
				agent.LogAdapter().SetImage(image)
			},
		},
		{
			name: "closed image replays the archive",
			prepare: func(t *testing.T, agent *consensus.Agent) {
				image, _ := liveImage(t, 0)
				aeron.ImageInternalCloseForHooks(image)
				agent.LogAdapter().SetImage(image)
			},
		},
		{
			name:    "no image replays the archive",
			prepare: func(t *testing.T, agent *consensus.Agent) {},
		},
		{
			name: "leader then replay reads the archive",
			prepare: func(t *testing.T, agent *consensus.Agent) {
				image, _ := liveImage(t, 0)
				agent.LogAdapter().SetImage(image)
				require.NoError(t, agent.JoinLogAsLeader(1, 0, 0, false))
				require.Nil(t, agent.LogAdapter().Image())
				agent.RoleSet(consensus.RoleFollower)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.run != nil {
				tt.run(t)
				return
			}
			agent := testServiceReplayAgent(0, 0)
			scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
			tt.prepare(t, agent)
			requireArchiveReplay(t, agent)
		})
	}
}

func TestLogReplay_holdsAtSnapshotWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "action mid-range",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 1)
				var actionEnd, stop int64
				r, _, _ := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
					actionEnd = writeReplaySnapshotAction(t, image, 0)
					stop = writeReplayFrame(t, image, actionEnd, encodeReplaySessionOpen(t, 3))
					return stop
				})
				require.Equal(t, 1, r.DoWork(0))

				// Phase 1: the drain stops at the action end.
				for nowNs := range int64(3) {
					r.DoWork(nowNs + 1)
					require.Equal(t, actionEnd, r.Position())
					require.Equal(t, actionEnd, agent.CommitPosition())
					require.True(t, agent.HasPendingSnapshotAction())
					require.False(t, r.IsDone())
				}

				// Phase 2: the ack pops the wait. The drain reaches the stop.
				ackReplayService(t, agent, actionEnd, 0)
				require.False(t, agent.HasPendingSnapshotAction())
				require.Equal(t, 1, r.DoWork(4))
				require.True(t, r.IsDone())
				require.Empty(t, r.reason())
				require.Equal(t, stop, agent.CommitPosition())
				require.Equal(t, 1, agent.OpenSessionCount())
			},
		},
		{
			name: "action at the stop",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 1)
				var stop int64
				r, _, _ := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
					openEnd := writeReplayFrame(t, image, 0, encodeReplaySessionOpen(t, 3))
					stop = writeReplaySnapshotAction(t, image, openEnd)
					return stop
				})
				require.Equal(t, 1, r.DoWork(0))

				// Phase 1: the drain reaches the stop, held by the wait.
				for nowNs := range int64(3) {
					r.DoWork(nowNs + 1)
					require.Equal(t, stop, r.Position())
					require.True(t, agent.HasPendingSnapshotAction())
					require.False(t, r.IsDone())
				}

				// Phase 2: the pop completes the replay.
				ackReplayService(t, agent, stop, 0)
				r.DoWork(4)
				require.True(t, r.IsDone())
				require.Empty(t, r.reason())
				require.Equal(t, stop, agent.CommitPosition())
			},
		},
		{
			name: "image closed while held below the stop",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 1)
				var actionEnd int64
				r, _, image := startArchiveReplay(t, agent, func(image *aeron.Image) int64 {
					actionEnd = writeReplaySnapshotAction(t, image, 0)
					return writeReplayFrame(t, image, actionEnd, encodeReplaySessionOpen(t, 3))
				})
				require.Equal(t, 1, r.DoWork(0))
				r.DoWork(1)
				require.True(t, agent.HasPendingSnapshotAction())
				require.False(t, r.IsDone())

				aeron.ImageInternalCloseForHooks(image)
				r.DoWork(2)
				require.True(t, r.IsDone())
				require.Equal(t, replayEndedShortReason, r.reason())
				require.Equal(t, actionEnd, r.Position())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestLogReplay_replayJoin(t *testing.T) {
	t.Parallel()

	const correlationID int64 = 51
	// joinedAgent returns a one-service follower with a connected service
	// proxy and one scheduled timer. It returns the proxy publication too.
	joinedAgent := func(t *testing.T) (*consensus.Agent, *aeron.Publication) {
		t.Helper()
		agent := testServiceReplayAgent(0, 1)
		pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
		attachServiceProxy(agent, pub)
		scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
		return agent, pub
	}
	writeTimer := func(image *aeron.Image) int64 {
		return writeReplayTimerEvent(t, image, 0, correlationID)
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "one service joins before the drain",
			run: func(t *testing.T) {
				agent, pub := joinedAgent(t)
				r, _, image := startArchiveReplay(t, agent, writeTimer)
				stop := r.progress.stopPosition
				require.Equal(t, 1, r.DoWork(0))

				var frames [][]byte
				off := 0
				bridgePub(t, pub, &off, func(msg []byte) { frames = append(frames, msg) })
				require.Len(t, frames, 1)
				join, ok := codecs.TryDecodeJoinLogAt(frames[0], 0)
				require.True(t, ok)
				require.Equal(t, int64(0), join.LogPosition)
				require.Equal(t, stop, join.MaxLogPosition)
				require.Equal(t, image.SessionID(), join.LogSessionID)
				require.Equal(t, testReplayStreamID, join.LogStreamID)
				require.Equal(
					t,
					sessionScopedLogChannel(testReplayChannel, image.SessionID()),
					string(join.LogChannel),
				)
				require.Equal(t, clusterwire.BooleanType_TRUE, join.IsStartup)
				require.Equal(t, consensus.RoleFollower.Code(), join.Role)
				require.Equal(t, 1, agent.PendingServiceAckWaits())

				// Phase 1: no drain until the replay JoinLog wait pops.
				for nowNs := range int64(3) {
					require.Equal(t, 0, r.DoWork(nowNs+1))
					require.Equal(t, int64(0), agent.LogAdapter().Position())
					require.Equal(t, int64(0), agent.CommitPosition())
					require.Equal(t, int64(1), agent.ScheduledTimerCount())
					require.False(t, r.IsDone())
				}

				// Phase 2: the join ack pops the wait. The drain runs.
				ackReplayService(t, agent, 0, 0)
				require.True(t, agent.IsReplayLogJoined())
				require.Equal(t, 1, r.DoWork(4))
				require.True(t, r.IsDone())
				require.Equal(t, stop, agent.CommitPosition())
				require.Equal(t, int64(0), agent.ScheduledTimerCount())
			},
		},
		{
			name: "no ack by the deadline",
			run: func(t *testing.T) {
				agent, _ := joinedAgent(t)
				r, _, _ := startArchiveReplay(t, agent, writeTimer)
				require.Equal(t, 1, r.DoWork(0))
				require.Equal(t, 0, r.DoWork(testImageAwaitNs-1))
				require.False(t, r.IsDone())

				require.Equal(t, 0, r.DoWork(testImageAwaitNs))
				require.True(t, r.IsDone())
				require.Equal(t, servicesNotJoinedReason, r.reason())
				require.Equal(t, int64(0), r.Position())
				require.Equal(t, int64(0), agent.CommitPosition())
				require.Equal(t, int64(1), agent.ScheduledTimerCount())

				// Close drops the join tracking. The published wait stays.
				r.Close()
				require.True(t, agent.IsReplayLogJoined())
				require.Equal(t, 1, agent.PendingServiceAckWaits())
			},
		},
		{
			name: "unpublished JoinLog after an ack failure holds to the deadline",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 1)
				scheduleReplayTimer(t, agent, correlationID, 1_000_000_000)
				ackReplayService(t, agent, 0, 5)
				require.ErrorIs(t, agent.ServiceAckFailure(), consensus.ErrServiceAckOutOfSequence)
				attachServiceProxy(agent, aeron.NewTestPublicationForHooksWithLimit(t, 0, false))

				r, _, _ := startArchiveReplay(t, agent, writeTimer)
				require.Equal(t, 1, r.DoWork(0))
				require.True(t, agent.HasPendingJoinLog())
				require.True(t, agent.IsReplayLogJoined())

				// Phase 1: no drain while the replay JoinLog is unpublished.
				require.Equal(t, 0, r.DoWork(testImageAwaitNs-1))
				require.False(t, r.IsDone())
				require.Equal(t, int64(0), agent.CommitPosition())

				// Phase 2: the deadline ends the replay at its start.
				require.Equal(t, 0, r.DoWork(testImageAwaitNs))
				require.True(t, r.IsDone())
				require.Equal(t, servicesNotJoinedReason, r.reason())
				require.Equal(t, int64(0), r.Position())
				require.Equal(t, int64(1), agent.ScheduledTimerCount())

				// Phase 3: Close drops the unpublished replay JoinLog.
				r.Close()
				require.False(t, agent.HasPendingJoinLog())
			},
		},
		{
			name: "zero services drain on the tick after the bind",
			run: func(t *testing.T) {
				agent := testServiceReplayAgent(0, 0)
				r, _, _ := startArchiveReplay(t, agent, writeTimer)
				require.Equal(t, 1, r.DoWork(0))
				require.False(t, agent.HasPendingJoinLog())
				require.Equal(t, 0, agent.PendingServiceAckWaits())

				require.Equal(t, 1, r.DoWork(1))
				require.True(t, r.IsDone())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// fakeSnapshotRecordingControl is a SnapshotRecordingControl that never
// starts a recording. A take wired to it stays at its publication add.
type fakeSnapshotRecordingControl struct{}

func (fakeSnapshotRecordingControl) NewStartRecording(
	int32,
	string,
) consensus.SnapshotStartRequest {
	return nil
}

func (fakeSnapshotRecordingControl) ControlTimeoutsNs() (
	offerRetryTimeoutNs int64,
	responseTimeoutNs int64,
) {
	return testImageAwaitNs, testImageAwaitNs
}

func (fakeSnapshotRecordingControl) WatchStop(int64) {}

func (fakeSnapshotRecordingControl) StopPosition() (stopPosition int64, stopped bool) {
	return 0, false
}

func (fakeSnapshotRecordingControl) ClearStop() {}

// fakeSnapshotPublicationAdder is a SnapshotPublicationAdder whose add never
// completes. Each cancel records how many replay stops control had sent.
type fakeSnapshotPublicationAdder struct {
	control       *fakeReplayControl
	offers        int
	cancelled     []int64
	stopsAtCancel []int
}

func (f *fakeSnapshotPublicationAdder) OfferAddExclusivePublication(int32, string) (int64, error) {
	f.offers++
	return int64(f.offers), nil
}

func (f *fakeSnapshotPublicationAdder) PollAddExclusivePublication(
	int64,
) (consensus.SnapshotPublication, bool, error) {
	return nil, false, nil
}

func (f *fakeSnapshotPublicationAdder) CancelPending(correlationID int64) {
	f.cancelled = append(f.cancelled, correlationID)
	f.stopsAtCancel = append(f.stopsAtCancel, len(f.control.stopCalls))
}

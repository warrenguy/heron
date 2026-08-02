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

func testReplayAgent(recordingID int64) *consensus.Agent {
	a := consensus.NewAgent(consensus.AgentConfig{
		Commit: new(atomic.Int64),
	})
	if recordingID >= 0 {
		a.SetLogRecordingID(recordingID)
	}
	return a
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
		FragmentLimit:  consensus.DefaultLogFragmentLimit,
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

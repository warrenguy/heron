//go:build testhooks

package service

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

const failedTakeTimestamp int64 = 321

// joinAck is the ack the harness join sends first.
var joinAck = serviceAckFields(0, 0, nullValue)

type terminationRun struct {
	*controlAgentHarness

	terminate *terminateRecorder
	reports   []error
}

func newTerminationRun(t *testing.T) *terminationRun {
	t.Helper()
	run := &terminationRun{
		controlAgentHarness: newControlAgentHarness(t, 0),
		terminate:           &terminateRecorder{},
	}

	run.agent.terminateHandler = run.terminate
	run.agent.errorHandler = collectErrors(&run.reports)
	return run
}

// commitTimer writes one timer frame and commits it.
//
// Returns the frame span.
func (r *terminationRun) commitTimer(t *testing.T) int64 {
	t.Helper()
	span := int64(r.writeTimer(t, 0, 1, 0))
	*r.committed = span
	return span
}

// newBoundTerminationRun binds the log at 0 and sends the join ack.
func newBoundTerminationRun(t *testing.T) *terminationRun {
	t.Helper()
	run := newTerminationRun(t)
	run.bindJoin(t, 0)
	run.drainJoin(t)
	return run
}

// holdPendingJoin enqueues a JoinLog at position. The subscription has no image,
// so the join stays unbound.
func (r *terminationRun) holdPendingJoin(t *testing.T, position int64) {
	t.Helper()
	r.agent.subscriptions = &stubAdder{
		sub: aeron.NewSubscriptionForHooks(9003, testLogStreamID, testLogChannel),
	}

	r.control.Enqueue(encodeJoinLogControl(t, defaultJoinLogFields(position)))
}

// disconnectSender binds a publication whose offers return aeron.NotConnected.
func (r *terminationRun) disconnectSender(t *testing.T) {
	t.Helper()
	r.agent.moduleProxy.SetPublication(aeron.NewTestPublicationForHooksWithLimit(t, 0, false))
}

// requireCleanStop asserts one Terminate call, a stop without a failure or a
// report, and the acks on the publication.
func (r *terminationRun) requireCleanStop(t *testing.T, wantAcks ...codecs.ServiceAckFields) {
	t.Helper()
	require.True(t, r.agent.IsTerminated())
	require.NoError(t, r.agent.ServiceFailure())
	require.Equal(t, 1, r.terminate.calls)
	require.Empty(t, r.reports)
	r.requireAcks(t, wantAcks...)
}

func (r *terminationRun) requireAcks(t *testing.T, wantAcks ...codecs.ServiceAckFields) {
	t.Helper()
	require.Equal(t, wantAcks, decodeServiceAcksFromPub(t, r.pub))
}

// requireFailedStop asserts one Terminate call, a stop, and one failure report
// that carries causes.
func (r *terminationRun) requireFailedStop(t *testing.T, causes ...error) {
	t.Helper()
	require.True(t, r.agent.IsTerminated())
	require.Equal(t, 1, r.terminate.calls)
	requireFailureReported(t, r.agent, r.reports, causes...)
}

// requireLostAckStop asserts one Terminate call, a stop without a failure, and
// one ErrTerminationAckPublish report that carries code.
func (r *terminationRun) requireLostAckStop(t *testing.T, code int64) {
	t.Helper()
	require.True(t, r.agent.IsTerminated())
	require.NoError(t, r.agent.ServiceFailure())
	require.Equal(t, 1, r.terminate.calls)
	require.Len(t, r.reports, 1)
	require.ErrorIs(t, r.reports[0], ErrTerminationAckPublish)
	require.NotErrorIs(t, r.reports[0], ErrFailed)
	require.ErrorContains(t, r.reports[0], strconv.FormatInt(code, 10))
}

// stopOnFailedTake runs failTakeAtTermination with ackRequired TRUE. The failed
// entry stays queued ahead of the termination entry. The drain stays open.
//
// Returns the take position.
func (r *terminationRun) stopOnFailedTake(t *testing.T, hookErr error) int64 {
	t.Helper()
	end := r.failTakeAtTermination(t, hookErr, clusterwire.BooleanType_TRUE)
	require.False(t, r.agent.IsTerminated())
	require.NoError(t, r.agent.ServiceFailure())
	require.Equal(t, 2, r.agent.PendingCoordinationAcks())
	return end
}

// failTakeAtTermination fails a take with hookErr at a termination position,
// behind a NotConnected sender. The take uses failedTakeTimestamp. The agent
// calls Terminate.
//
// Returns the take position.
func (r *terminationRun) failTakeAtTermination(
	t *testing.T,
	hookErr error,
	ackRequired clusterwire.BooleanType,
) int64 {
	t.Helper()
	r.agent.snapshotHandler = &snapshotRecordingService{hookErr: hookErr}
	InstallSnapshotTakeFakeForTest(t, r.agent)
	r.disconnectSender(t)
	end := writeTermEventAndSnapshot(t, r.image, r.committed, failedTakeTimestamp)
	enqueueTerminationPosition(t, r.control, end, ackRequired)

	r.agent.DoWork(0)
	require.NotNil(t, r.agent.take)
	require.Equal(t, 0, r.terminate.calls)

	r.agent.DoWork(0)
	require.Nil(t, r.agent.take)
	require.Equal(t, 1, r.terminate.calls)
	return end
}

func timedServiceAck(logPosition, ackID, timestamp int64) codecs.ServiceAckFields {
	ack := serviceAckFields(logPosition, ackID, nullValue)
	ack.Timestamp = timestamp
	return ack
}

func TestAgent_Termination_logBelowPosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	span1 := run.writeTimer(t, 0, 1, 77)
	span2 := run.writeTimer(t, span1, 2, 88)
	position := int64(span1)
	enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_TRUE)

	// Phase "wait": committed sits below the position.
	run.agent.DoWork(0)
	require.False(t, run.agent.IsTerminated())
	require.Equal(t, 0, run.terminate.calls)

	// Phase "stop": committed sits past the position. The poll stops there.
	*run.committed = int64(span1 + span2)
	run.agent.DoWork(0)
	require.Equal(t, position, run.agent.LogPosition())
	require.Equal(t, position, run.image.Position())
	require.Len(t, run.svc.timers, 1)
	run.requireCleanStop(t, joinAck, timedServiceAck(position, 1, 77))

	// Phase "after": the later frame stays undelivered.
	require.Equal(t, 0, run.agent.DoWork(0))
	require.Len(t, run.svc.timers, 1)
}

func TestAgent_Termination_frameAfterLogPassedPosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	span := run.commitTimer(t)
	run.agent.DoWork(0)
	require.Equal(t, span, run.agent.LogPosition())

	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	run.requireFailedStop(t, ErrTerminationPositionPassed)
	require.ErrorContains(
		t,
		run.agent.ServiceFailure(),
		fmt.Sprintf("log position %d, termination position 0", span),
	)
	run.requireAcks(t, joinAck, serviceAckFields(span, 1, nullValue))
}

func TestAgent_Termination_positionInsideFrame(t *testing.T) {
	run := newBoundTerminationRun(t)
	span := run.commitTimer(t)
	enqueueTerminationPosition(t, run.control, span/2, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, span, run.agent.LogPosition())
	require.Len(t, run.svc.timers, 1)
	run.requireFailedStop(t, ErrTerminationPositionPassed)
	run.requireAcks(t, joinAck, serviceAckFields(span, 1, nullValue))
}

func TestAgent_Termination_secondFrameHigherPosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	span1 := run.writeTimer(t, 0, 1, 0)
	span2 := run.writeTimer(t, span1, 2, 0)
	end := int64(span1 + span2)
	*run.committed = end
	enqueueTerminationPosition(t, run.control, int64(span1), clusterwire.BooleanType_TRUE)
	enqueueTerminationPosition(t, run.control, end, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, end, run.agent.LogPosition())
	require.Len(t, run.svc.timers, 2)
	run.requireCleanStop(t, joinAck, serviceAckFields(end, 1, nullValue))
}

func TestAgent_Termination_secondFrameLowerPositionAboveLog(t *testing.T) {
	run := newBoundTerminationRun(t)
	span1 := run.writeTimer(t, 0, 1, 0)
	span2 := run.writeTimer(t, span1, 2, 0)
	position := int64(span1)
	end := int64(span1 + span2)
	*run.committed = end
	enqueueTerminationPosition(t, run.control, end, clusterwire.BooleanType_TRUE)
	enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, position, run.agent.LogPosition())
	require.Len(t, run.svc.timers, 1)
	run.requireCleanStop(t, joinAck, serviceAckFields(position, 1, nullValue))
}

func TestAgent_Termination_secondFrameLowerPositionBelowLog(t *testing.T) {
	run := newBoundTerminationRun(t)
	span := run.commitTimer(t)
	enqueueTerminationPosition(t, run.control, 4096, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	require.Equal(t, span, run.agent.LogPosition())
	require.False(t, run.agent.IsTerminated())

	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	run.requireFailedStop(t, ErrTerminationPositionPassed)
	run.requireAcks(t, joinAck, serviceAckFields(span, 1, nullValue))
}

func TestAgent_Termination_nullPositionClearsStoredPosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	span := run.commitTimer(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	enqueueTerminationPosition(t, run.control, nullValue, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, nullValue, run.agent.terminationPosition)
	require.Equal(t, span, run.agent.LogPosition())
	require.False(t, run.agent.IsTerminated())
	require.Equal(t, 0, run.terminate.calls)
}

func TestAgent_Termination_lastFlagWins(t *testing.T) {
	run := newBoundTerminationRun(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_FALSE)

	run.agent.DoWork(0)
	run.requireCleanStop(t, joinAck)
}

func TestAgent_Termination_takeInFlightAtPosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	fake := InstallSnapshotTakeFakeForTest(t, run.agent)
	const timestamp int64 = 321
	end := writeTermEventAndSnapshot(t, run.image, run.committed, timestamp)
	enqueueTerminationPosition(t, run.control, end, clusterwire.BooleanType_TRUE)

	// Phase "wait": the log sits at the position while the take runs.
	run.agent.DoWork(0)
	require.Equal(t, end, run.agent.LogPosition())
	require.NotNil(t, run.agent.take)
	require.False(t, run.agent.IsTerminated())
	require.Equal(t, 0, run.terminate.calls)

	// Phase "stop": the take resolves. The snapshot ack sends first.
	for range 3 {
		if run.agent.IsTerminated() {
			break
		}

		run.agent.DoWork(0)
	}

	snapshotAck := timedServiceAck(end, 1, timestamp)
	snapshotAck.RelevantID = fake.RecordingID()
	run.requireCleanStop(t, joinAck, snapshotAck, timedServiceAck(end, 2, timestamp))
}

func TestAgent_Termination_failedTakeEntryAhead(t *testing.T) {
	hookErr := errors.New("snap boom")
	run := newBoundTerminationRun(t)
	end := run.stopOnFailedTake(t, hookErr)

	// The sender connects. The snapshot ack sends. The agent then marks its
	// cause.
	run.agent.moduleProxy.SetPublication(run.pub)
	require.Equal(t, 1, run.agent.DoWork(0))
	run.requireFailedStop(t, ErrTakeSnapshot, hookErr)
	run.requireAcks(t, joinAck, timedServiceAck(end, 1, failedTakeTimestamp))
	require.Equal(t, 1, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_unreadyRequestAbovePosition(t *testing.T) {
	run := newBoundTerminationRun(t)
	span1 := run.writeTimer(t, 0, 1, 0)
	span2 := run.writeTimer(t, span1, 2, 0)
	position := int64(span1)
	request := int64(span1 + span2)
	*run.committed = request
	run.control.Enqueue(encodeRequestServiceAckControl(t, request))
	enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, position, run.agent.LogPosition())
	run.requireCleanStop(
		t,
		joinAck,
		serviceAckFields(request, 1, nullValue),
		serviceAckFields(position, 2, nullValue),
	)
}

func TestAgent_Termination_unboundPendingJoin(t *testing.T) {
	const joinPosition int64 = 64
	run := newTerminationRun(t)
	run.holdPendingJoin(t, joinPosition)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.True(t, run.agent.HasPendingJoin())
	run.requireCleanStop(
		t,
		serviceAckFields(joinPosition, 0, nullValue),
		serviceAckFields(0, 1, nullValue),
	)
}

func TestAgent_Termination_requestLogPassedInStopTick(t *testing.T) {
	run := newBoundTerminationRun(t)
	background := &lifecycleRecordingService{}
	run.agent.backgroundWorkHandler = background
	span := run.commitTimer(t)
	run.control.Enqueue(encodeRequestServiceAckControl(t, span/2))
	enqueueTerminationPosition(t, run.control, span, clusterwire.BooleanType_TRUE)

	// Phase "mark": the stop sweep marks ErrAckRequestPassed.
	run.agent.DoWork(0)
	require.Equal(t, span, run.agent.LogPosition())
	requireFailureReported(t, run.agent, run.reports, ErrAckRequestPassed)
	require.False(t, run.agent.IsTerminated())
	require.Equal(t, 0, run.terminate.calls)
	require.Empty(t, background.backgroundNows)
	run.requireAcks(t, joinAck)

	// Phase "close": Close calls Terminate.
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)
	require.False(t, run.agent.IsTerminated())
}

func TestAgent_Termination_notConnectedTwiceThenConnected(t *testing.T) {
	run := newBoundTerminationRun(t)
	background := &lifecycleRecordingService{}
	run.agent.backgroundWorkHandler = background
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	for range 2 {
		run.agent.DoWork(0)
		require.False(t, run.agent.IsTerminated())
	}

	run.agent.moduleProxy.SetPublication(run.pub)
	require.Equal(t, 1, run.agent.DoWork(0))
	run.requireCleanStop(t, joinAck, serviceAckFields(0, 1, nullValue))
	require.Equal(t, 0, run.agent.PendingCoordinationAcks())
	require.Empty(t, background.backgroundNows)
}

func TestAgent_Termination_notConnectedFiveTimes(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	for range 4 {
		run.agent.DoWork(0)
		require.False(t, run.agent.IsTerminated())
		require.Empty(t, run.reports)
	}

	run.agent.DoWork(0)
	run.requireLostAckStop(t, aeron.NotConnected)
	require.Equal(t, 1, run.agent.PendingCoordinationAcks())
	require.Equal(t, 0, run.agent.DoWork(0))
	require.Len(t, run.reports, 1)

	// Close does not report the lost ack again.
	run.agent.Close()
	require.Len(t, run.reports, 1)
	require.NoError(t, run.agent.ServiceFailure())
}

func TestAgent_Termination_notConnectedEntryAheadCountsFailedSends(t *testing.T) {
	run := newTerminationRun(t)
	run.holdPendingJoin(t, 0)
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	// Phase "fail": each failed send holds the join entry.
	for range 4 {
		run.agent.DoWork(0)
		require.False(t, run.agent.IsTerminated())
	}

	require.Equal(t, 2, run.agent.PendingCoordinationAcks())
	require.Equal(t, 2, run.agent.ReadyCoordinationAcks())

	// Phase "end": the fifth failed send ends the drain.
	run.agent.DoWork(0)
	run.requireLostAckStop(t, aeron.NotConnected)
	require.Equal(t, 2, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_notConnectedEntryAheadThenConnected(t *testing.T) {
	run := newTerminationRun(t)
	run.holdPendingJoin(t, 0)
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	require.False(t, run.agent.IsTerminated())

	run.agent.moduleProxy.SetPublication(run.pub)
	require.Equal(t, 2, run.agent.DoWork(0))
	run.requireCleanStop(t, joinAck, serviceAckFields(0, 1, nullValue))
}

func TestAgent_Termination_closedPublicationOnTerminationEntry(t *testing.T) {
	run := newBoundTerminationRun(t)
	require.NoError(t, run.pub.Close())
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	run.requireLostAckStop(t, aeron.Closed)
	require.Equal(t, int64(1), run.agent.NextAckID())
	require.Equal(t, 1, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_closedPublicationOnJoinEntryAhead(t *testing.T) {
	run := newTerminationRun(t)
	run.holdPendingJoin(t, 0)
	require.NoError(t, run.pub.Close())
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	run.requireFailedStop(t, ErrAckPublish)
	require.Equal(t, int64(0), run.agent.NextAckID())
	require.Equal(t, 2, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_nilPublicationWithJoinEntryAhead(t *testing.T) {
	run := newTerminationRun(t)
	run.holdPendingJoin(t, 0)
	run.agent.moduleProxy.SetPublication(nil)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	run.requireLostAckStop(t, aeron.Closed)
	require.Equal(t, int64(0), run.agent.NextAckID())
	require.Equal(t, 2, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_nilModuleProxy(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.agent.moduleProxy = nil
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	run.requireLostAckStop(t, aeron.Closed)
	require.Equal(t, int64(1), run.agent.NextAckID())
	require.Equal(t, 1, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_terminateError(t *testing.T) {
	terminateErr := errors.New("terminate boom")

	for _, tc := range []struct {
		name  string
		err   error
		cause error
	}{
		{name: "terminate error", err: terminateErr, cause: terminateErr},
		{
			name:  "terminate error wraps a client close",
			err:   fmt.Errorf("cleanup: %w", aeron.ErrClientClosed),
			cause: aeron.ErrClientClosed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := newBoundTerminationRun(t)
			run.terminate.err = tc.err
			enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

			run.agent.DoWork(0)
			require.True(t, run.agent.IsTerminated())
			require.NoError(t, run.agent.ServiceFailure())
			require.Len(t, run.reports, 1)
			require.ErrorIs(t, run.reports[0], ErrTerminate)
			require.ErrorIs(t, run.reports[0], tc.err)
			require.ErrorIs(t, run.reports[0], tc.cause)
			require.NotErrorIs(t, run.reports[0], ErrFailed)
			run.requireAcks(t, joinAck, serviceAckFields(0, 1, nullValue))
		})
	}
}

func TestAgent_Termination_markInsideTerminateEndsDrain(t *testing.T) {
	markErr := errors.New("mark boom")
	run := newBoundTerminationRun(t)
	run.terminate.probe = func() { run.agent.markFailure(markErr) }
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	run.requireFailedStop(t, markErr)
	run.requireAcks(t, joinAck)
	require.Equal(t, 1, run.agent.PendingCoordinationAcks())
}

func TestAgent_Termination_endOfStreamAtPositionWithPaddingBelow(t *testing.T) {
	const paddingLength = 64
	run := newBoundTerminationRun(t)
	span := run.writeTimer(t, 0, 1, 0)
	streamEnd := int64(span) + paddingLength
	writeAgentPadding(t, run.image, span, paddingLength)

	// Phase "deliver": the log stops at the message end before the padding.
	*run.committed = int64(span)
	run.agent.DoWork(0)
	require.Equal(t, int64(span), run.agent.LogPosition())

	// Phase "stop": the poll consumes the padding. The unbind adopts the
	// position. The stop runs in the same call.
	enqueueTerminationPosition(t, run.control, streamEnd, clusterwire.BooleanType_TRUE)
	*run.committed = streamEnd
	aeron.ImageSetEndOfStreamPositionForHooks(run.image, streamEnd)
	run.agent.DoWork(0)
	require.False(t, run.agent.IsLogBound())
	require.Equal(t, streamEnd, run.agent.LogPosition())
	run.requireCleanStop(t, joinAck, serviceAckFields(streamEnd, 1, nullValue))
}

func TestAgent_Termination_paddingEndAtPositionOnOpenImage(t *testing.T) {
	cases := []struct {
		name       string
		paddingEnd int
	}{
		{name: "midTerm", paddingEnd: 4096},
		{name: "termEnd", paddingEnd: logbuffer.TermMinLength},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := newBoundTerminationRun(t)
			span := run.writeTimer(t, 0, 1, 0)
			writeAgentPadding(t, run.image, span, int32(tc.paddingEnd-span))
			position := int64(tc.paddingEnd)
			*run.committed = position
			enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_TRUE)

			run.agent.DoWork(0)
			require.True(t, run.agent.IsLogBound())
			require.Equal(t, position, run.agent.LogPosition())
			run.requireCleanStop(t, joinAck, serviceAckFields(position, 1, nullValue))
		})
	}
}

func TestAgent_Termination_paddingConsumedBeforeFrame(t *testing.T) {
	const paddingLength = 64
	run := newBoundTerminationRun(t)
	span := run.writeTimer(t, 0, 1, 0)
	writeAgentPadding(t, run.image, span, paddingLength)
	position := int64(span) + paddingLength
	*run.committed = position

	// Phase "consume": the image passes the padding. The log position stays at
	// the message end.
	run.agent.DoWork(0)
	require.Equal(t, position, run.image.Position())
	require.Equal(t, int64(span), run.agent.LogPosition())
	require.False(t, run.agent.IsTerminated())

	// Phase "stop": the frame arrives with the image already at the position.
	enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_FALSE)
	run.agent.DoWork(0)
	require.Equal(t, position, run.agent.LogPosition())
	run.requireCleanStop(t, joinAck)
}

func TestAgent_Termination_paddingEndAtPositionHoldsLaterFrame(t *testing.T) {
	const paddingLength = 64
	run := newBoundTerminationRun(t)
	span := run.writeTimer(t, 0, 1, 0)
	writeAgentPadding(t, run.image, span, paddingLength)
	position := span + paddingLength
	laterSpan := run.writeTimer(t, position, 2, 0)
	*run.committed = int64(position + laterSpan)
	enqueueTerminationPosition(t, run.control, int64(position), clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.Equal(t, int64(position), run.image.Position())
	require.Len(t, run.svc.timers, 1)
	run.requireCleanStop(t, joinAck, serviceAckFields(int64(position), 1, nullValue))
}

func TestAgent_Termination_positionBetweenFragments(t *testing.T) {
	run := newBoundTerminationRun(t)
	whole := encodeTimerEvent(t, codecs.TimerEventFields{
		LeadershipTermID: 1,
		CorrelationID:    1,
		Timestamp:        1,
	})
	beginSpan, totalSpan := writeBeginEndTrain(t, run.image, whole)
	position := int64(beginSpan)
	*run.committed = totalSpan
	enqueueTerminationPosition(t, run.control, position, clusterwire.BooleanType_FALSE)

	// Phase "hold": the assembler holds the BEGIN frame. The log position
	// stays below the position.
	run.agent.DoWork(0)
	require.Equal(t, position, run.image.Position())
	require.Equal(t, int64(0), run.agent.LogPosition())
	require.False(t, run.agent.IsTerminated())

	// Phase "stop": end of stream at the position unbinds and stops.
	aeron.ImageSetEndOfStreamPositionForHooks(run.image, position)
	run.agent.DoWork(0)
	require.False(t, run.agent.IsLogBound())
	require.Equal(t, position, run.agent.LogPosition())
	require.Empty(t, run.svc.timers)
	run.requireCleanStop(t, joinAck)
}

func TestAgent_Termination_afterStop(t *testing.T) {
	run := newBoundTerminationRun(t)
	background := &lifecycleRecordingService{}
	run.agent.backgroundWorkHandler = background
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	run.agent.DoWork(0)
	require.True(t, run.agent.IsTerminated())
	require.Empty(t, background.backgroundNows)

	run.control.Enqueue(encodeRequestServiceAckControl(t, 0))
	enqueueTerminationPosition(t, run.control, 4096, clusterwire.BooleanType_FALSE)
	require.Equal(t, 0, run.agent.DoWork(0))
	require.Equal(t, 0, run.agent.PendingCoordinationAcks())
	require.Equal(t, int64(0), run.agent.terminationPosition)
	require.True(t, run.agent.terminationAckRequired)
	require.Empty(t, background.backgroundNows)
	run.requireCleanStop(t, joinAck, serviceAckFields(0, 1, nullValue))
}

func TestAgent_Termination_terminatePanicsAckRequired(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.terminate.panicValue = "terminate boom"
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)

	require.PanicsWithValue(t, "terminate boom", func() { run.agent.DoWork(0) })
	require.False(t, run.agent.IsTerminated())
	require.Equal(t, lifecycleCallNone, run.agent.activeLifecycleCall)
	run.requireAcks(t, joinAck)

	require.Equal(t, 1, run.agent.DoWork(0))
	run.requireCleanStop(t, joinAck, serviceAckFields(0, 1, nullValue))
}

func TestAgent_Termination_terminatePanicsNoAck(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.terminate.panicValue = "terminate boom"
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_FALSE)

	require.PanicsWithValue(t, "terminate boom", func() { run.agent.DoWork(0) })
	require.False(t, run.agent.IsTerminated())

	require.Equal(t, 0, run.agent.DoWork(0))
	run.requireCleanStop(t, joinAck)
}

func TestAgent_CloseTerminate_afterSuccessfulStart(t *testing.T) {
	run := newTerminationRun(t)
	run.agent.DoWork(0)
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)
	require.False(t, run.agent.IsTerminated())
	require.NoError(t, run.agent.ServiceFailure())
}

func TestAgent_CloseTerminate_beforeStart(t *testing.T) {
	run := newTerminationRun(t)
	run.agent.Close()
	require.Equal(t, 0, run.terminate.calls)
}

func TestAgent_CloseTerminate_afterStop(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ackRequired clusterwire.BooleanType
		wantAcks    []codecs.ServiceAckFields
	}{
		{
			name:        "after the stop, no ack",
			ackRequired: clusterwire.BooleanType_FALSE,
			wantAcks:    []codecs.ServiceAckFields{joinAck},
		},
		{
			name:        "after the stop, ack sent",
			ackRequired: clusterwire.BooleanType_TRUE,
			wantAcks:    []codecs.ServiceAckFields{joinAck, serviceAckFields(0, 1, nullValue)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := newBoundTerminationRun(t)
			enqueueTerminationPosition(t, run.control, 0, tc.ackRequired)
			run.agent.DoWork(0)
			require.True(t, run.agent.IsTerminated())
			run.agent.Close()
			run.requireCleanStop(t, tc.wantAcks...)
		})
	}
}

func TestAgent_CloseTerminate_afterStartError(t *testing.T) {
	run := newTerminationRun(t)
	run.svc.startErr = errors.New("start boom")
	run.agent.DoWork(0)
	run.agent.Close()
	require.Equal(t, 0, run.terminate.calls)
}

func TestAgent_CloseTerminate_afterFatalMark(t *testing.T) {
	markErr := errors.New("mark boom")
	run := newTerminationRun(t)
	run.agent.DoWork(0)
	run.agent.markFailure(markErr)
	run.terminate.err = errors.New("terminate boom")
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)
	requireFailureReported(t, run.agent, run.reports, markErr)
}

func TestAgent_CloseTerminate_reportsTerminateError(t *testing.T) {
	terminateErr := errors.New("terminate boom")
	run := newTerminationRun(t)
	run.terminate.err = terminateErr
	run.agent.DoWork(0)
	run.agent.Close()
	require.Len(t, run.reports, 1)
	require.ErrorIs(t, run.reports[0], ErrTerminate)
	require.ErrorIs(t, run.reports[0], terminateErr)
	require.NoError(t, run.agent.ServiceFailure())
}

func TestAgent_CloseTerminate_secondClose(t *testing.T) {
	run := newTerminationRun(t)
	run.agent.DoWork(0)
	run.agent.Close()
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)

	// The closed agent does not store a position.
	run.agent.handleServiceTerminationPosition(0, true)
	require.Equal(t, nullValue, run.agent.terminationPosition)
	require.False(t, run.agent.terminationAckRequired)
}

func TestAgent_CloseTerminate_secondCloseAfterPanic(t *testing.T) {
	run := newTerminationRun(t)
	run.terminate.panicValue = "terminate boom"
	run.agent.DoWork(0)
	require.PanicsWithValue(t, "terminate boom", run.agent.Close)
	require.Equal(t, lifecycleCallNone, run.agent.activeLifecycleCall)
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)
	require.Equal(t, 0, run.agent.DoWork(0))
}

func TestAgent_CloseTerminate_stopAfterRecoveredPanic(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.terminate.panicValue = "terminate boom"
	require.PanicsWithValue(t, "terminate boom", run.agent.Close)

	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	run.requireCleanStop(t, joinAck, serviceAckFields(0, 1, nullValue))
}

func TestAgent_CloseTerminate_serviceWithoutHandler(t *testing.T) {
	h := newControlAgentHarness(t, 0)
	h.agent.DoWork(0)
	h.agent.Close()
	require.False(t, h.agent.IsTerminated())
	require.NoError(t, h.agent.ServiceFailure())
}

func TestAgent_CloseTerminate_duringDrain(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	require.False(t, run.agent.IsTerminated())

	run.agent.moduleProxy.SetPublication(run.pub)
	run.agent.Close()
	run.requireLostAckStop(t, aeron.Closed)
	run.requireAcks(t, joinAck)
}

func TestAgent_CloseTerminate_duringDrainWithQueuedCause(t *testing.T) {
	hookErr := errors.New("snap boom")
	run := newBoundTerminationRun(t)
	run.stopOnFailedTake(t, hookErr)

	run.agent.Close()
	run.requireFailedStop(t, ErrTakeSnapshot, hookErr)
	run.requireAcks(t, joinAck)
}

func TestAgent_CloseTerminate_afterStopWithQueuedCause(t *testing.T) {
	hookErr := errors.New("snap boom")
	run := newBoundTerminationRun(t)
	run.failTakeAtTermination(t, hookErr, clusterwire.BooleanType_FALSE)
	require.True(t, run.agent.IsTerminated())
	require.NoError(t, run.agent.ServiceFailure())
	require.Empty(t, run.reports)

	// The sender connects. Close marks the cause. The entry does not send.
	run.agent.moduleProxy.SetPublication(run.pub)
	run.agent.Close()
	run.requireFailedStop(t, ErrTakeSnapshot, hookErr)
	run.requireAcks(t, joinAck)
}

func TestAgent_CloseTerminate_duringDrainAfterOvershoot(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.commitTimer(t)
	run.agent.DoWork(0)
	run.disconnectSender(t)
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_TRUE)
	run.agent.DoWork(0)
	require.False(t, run.agent.IsTerminated())
	require.NoError(t, run.agent.ServiceFailure())

	run.agent.moduleProxy.SetPublication(run.pub)
	run.agent.Close()
	require.Equal(t, 1, run.terminate.calls)
	require.True(t, run.agent.IsTerminated())
	require.ErrorIs(t, run.agent.ServiceFailure(), ErrFailed)
	require.ErrorIs(t, run.agent.ServiceFailure(), ErrTerminationPositionPassed)
	require.Len(t, run.reports, 2)
	require.ErrorIs(t, run.reports[0], ErrTerminationAckPublish)
	require.NotErrorIs(t, run.reports[0], ErrFailed)
	require.ErrorContains(t, run.reports[0], strconv.FormatInt(aeron.Closed, 10))
	require.Same(t, run.agent.ServiceFailure(), run.reports[1])
	run.requireAcks(t, joinAck)
}

func TestAgent_CloseTerminate_duringDrainAfterPanicNoAck(t *testing.T) {
	run := newBoundTerminationRun(t)
	run.terminate.panicValue = "terminate boom"
	enqueueTerminationPosition(t, run.control, 0, clusterwire.BooleanType_FALSE)
	require.PanicsWithValue(t, "terminate boom", func() { run.agent.DoWork(0) })

	run.agent.Close()
	require.True(t, run.agent.IsTerminated())
	require.Equal(t, 1, run.terminate.calls)
	require.Empty(t, run.reports)
}

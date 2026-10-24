//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

const (
	asyncReplicateTestSourceRecordingID     int64 = 31
	asyncReplicateTestSourceControlStreamID int32 = 10
	asyncReplicateTestSourceControlChannel        = "aeron:udp?endpoint=127.0.0.1:28111"
	asyncReplicateTestReplicationID         int64 = 1 << 41
	asyncReplicateTestRecordingID           int64 = 77
	asyncReplicateTestPosition              int64 = 4096
)

// asyncReplicateFixture runs AsyncReplicate over a real control subscription
// so Client.Poll feeds the handle.
type asyncReplicateFixture struct {
	h      *lifecycleHarness
	client *Client
	handle *AsyncReplicate
}

func newAsyncReplicateFixture(t *testing.T) *asyncReplicateFixture {
	t.Helper()

	h := newLifecycleHarness(t)
	f := &asyncReplicateFixture{h: h, client: h.newClient()}
	f.handle = f.newHandle(t, NewReplicationParams())
	return f
}

func (f *asyncReplicateFixture) newHandle(
	t *testing.T,
	params ReplicationParams,
) *AsyncReplicate {
	t.Helper()

	prior := f.client.aeronClient.NextCorrelationID()
	handle := f.client.NewAsyncReplicate(
		asyncReplicateTestSourceRecordingID,
		asyncReplicateTestSourceControlStreamID,
		[]byte(asyncReplicateTestSourceControlChannel),
		params,
	)
	require.Equal(t, prior+1, handle.correlationID)
	require.Equal(t, handle.correlationID, f.h.poller.awaitCorrelationID)
	return handle
}

func (f *asyncReplicateFixture) responseFor(
	handle *AsyncReplicate,
	code archivewire.ControlResponseCode,
	relevantID int64,
) codecs.ControlResponseFields {
	fields := codecs.ControlResponseFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    handle.correlationID,
		RelevantID:       relevantID,
		Code:             code,
		Version:          520,
	}
	if code == archivewire.ControlResponseCode_ERROR {
		fields.ErrorMessage = []byte("refused")
	}
	return fields
}

func (f *asyncReplicateFixture) okFor(handle *AsyncReplicate) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_OK, asyncReplicateTestReplicationID)
}

func (f *asyncReplicateFixture) errorFor(handle *AsyncReplicate) codecs.ControlResponseFields {
	return f.responseFor(handle, archivewire.ControlResponseCode_ERROR, ErrorUnknownRecording)
}

// latch latches fields on the session poller through its fragment handler.
func (f *asyncReplicateFixture) latch(t *testing.T, fields codecs.ControlResponseFields) {
	t.Helper()

	invokeHandleControlResponseFragment(t, f.h.poller, encodeControlResponseWire(t, fields), 0, -1)
}

// deliverSignals appends events to the term and runs one Client.Poll.
func (f *asyncReplicateFixture) deliverSignals(
	t *testing.T,
	events ...codecs.RecordingSignalEventFields,
) {
	t.Helper()

	for _, event := range events {
		appendUnfragmentedWireToTerm(t, f.h.termBuffer, encodeRecordingSignalEventWire(t, event))
	}
	require.Equal(t, len(events), f.client.Poll())
}

func (f *asyncReplicateFixture) requireOfferedReplicate(t *testing.T, params ReplicationParams) {
	t.Helper()

	requireOfferedReplicateWire(t, f.h.pub.lastOffer(), f.handle.correlationID, params)
}

// requireOfferedReplicateWire asserts every field of an offered replicate
// request against the fixture source and params.
func requireOfferedReplicateWire(
	t *testing.T,
	wire []byte,
	correlationID int64,
	params ReplicationParams,
) {
	t.Helper()

	fields := requireProxyWireFields[codecs.ReplicateRequest2Fields](
		t,
		wire,
		archivewire.ReplicateRequest2SbeTemplateID,
	)
	require.Equal(t, controlResponseTestControlSessionID, fields.ControlSessionID)
	require.Equal(t, correlationID, fields.CorrelationID)
	require.Equal(t, asyncReplicateTestSourceRecordingID, fields.SrcRecordingID)
	require.Equal(t, asyncReplicateTestSourceControlStreamID, fields.SrcControlStreamID)
	require.Equal(t, []byte(asyncReplicateTestSourceControlChannel), fields.SrcControlChannel)
	require.Equal(t, params.DestinationRecordingID, fields.DstRecordingID)
	require.Equal(t, params.StopPosition, fields.StopPosition)
	require.Equal(t, params.ChannelTagID, fields.ChannelTagID)
	require.Equal(t, params.SubscriptionTagID, fields.SubscriptionTagID)
	require.Equal(t, params.FileIOMaxLength, fields.FileIoMaxLength)
	require.Equal(t, params.ReplicationSessionID, fields.ReplicationSessionID)
	require.Equal(t, params.LiveDestination, fields.LiveDestination)
	require.Equal(t, params.ReplicationChannel, fields.ReplicationChannel)
	require.Equal(t, params.EncodedCredentials, fields.EncodedCredentials)
	require.Equal(t, params.SourceResponseChannel, fields.SrcResponseChannel)
}

// requireReplicateFlags asserts every fold flag against want.
func requireReplicateFlags(t *testing.T, handle *AsyncReplicate, want replicateFlags) {
	t.Helper()

	require.Equal(t, want, replicateFlagsOf(handle))
}

// replicateFlagsOf reads every fold flag of handle.
func replicateFlagsOf(handle *AsyncReplicate) replicateFlags {
	return replicateFlags{
		extended:         handle.HasExtended(),
		synced:           handle.HasSynced(),
		merged:           handle.HasMerged(),
		stopped:          handle.HasStopped(),
		replicationEnded: handle.HasReplicationEnded(),
		recordingDeleted: handle.IsRecordingDeleted(),
	}
}

type replicateFlags struct {
	extended         bool
	synced           bool
	merged           bool
	stopped          bool
	replicationEnded bool
	recordingDeleted bool
}

func replicationSignal(signal archivewire.RecordingSignal) codecs.RecordingSignalEventFields {
	return codecs.RecordingSignalEventFields{
		ControlSessionID: controlResponseTestControlSessionID,
		CorrelationID:    asyncReplicateTestReplicationID,
		RecordingID:      asyncReplicateTestRecordingID,
		SubscriptionID:   2002,
		Position:         asyncReplicateTestPosition,
		Signal:           signal,
	}
}

// TestAsyncReplicate_RecordingSignal_sameBatchAsOK asserts a REPLICATE signal
// behind the OK in one Client.Poll folds into the handle before PollResponse.
func TestAsyncReplicate_RecordingSignal_sameBatchAsOK(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	f.client.SetRecordingSignalHandler(f.handle)
	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)

	writeUnfragmentedWireToTerm(
		t,
		f.h.termBuffer,
		encodeControlResponseWire(t, f.okFor(f.handle)),
	)
	appendUnfragmentedWireToTerm(
		t,
		f.h.termBuffer,
		encodeRecordingSignalEventWire(t, replicationSignal(archivewire.RecordingSignal_REPLICATE)),
	)
	require.Equal(t, 2, f.client.Poll())

	// Phase: the signal folds before PollResponse consumes the OK.
	require.True(t, f.h.poller.IsPollComplete(), "OK stays latched until PollResponse")
	require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
	require.Equal(t, asyncReplicateTestRecordingID, f.handle.RecordingID())
	require.Equal(t, asyncReplicateTestPosition, f.handle.Position())

	// Phase: PollResponse consumes the OK.
	done, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
	require.False(t, f.h.poller.IsPollComplete(), "matching OK must be consumed")
	require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
}

// resolveOK latches the handle's OK and consumes it through PollResponse.
func (f *asyncReplicateFixture) resolveOK(t *testing.T) {
	t.Helper()

	f.latch(t, f.okFor(f.handle))
	done, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
}

func TestAsyncReplicate_TryOffer_retriableThenAccepted(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "back_pressured", status: aeron.BackPressured},
		{name: "admin_action", status: aeron.AdminAction},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			f.h.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.NoError(t, err, "retriable offer must not be terminal")
			require.False(t, offered, "retriable offer must not report accepted")
			require.Len(t, f.h.pub.offered, 1)
			f.requireOfferedReplicate(t, NewReplicationParams())

			f.h.pub.offerResult = 0
			offered, err = f.handle.TryOffer()
			require.NoError(t, err)
			require.True(t, offered, "accepted offer must report true")
			require.Len(t, f.h.pub.offered, 2)
			f.requireOfferedReplicate(t, NewReplicationParams())
		})
	}
}

func TestAsyncReplicate_TryOffer_terminal(t *testing.T) {
	cases := []struct {
		name   string
		status int64
	}{
		{name: "closed", status: aeron.Closed},
		{name: "not_connected", status: aeron.NotConnected},
		{name: "max_position_exceeded", status: aeron.MaxPositionExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			f.h.pub.offerResult = tc.status

			offered, err := f.handle.TryOffer()
			require.False(t, offered, "terminal offer must not report accepted")
			requireControlTerminal(t, err, tc.status)
			require.Len(t, f.h.pub.offered, 1)
		})
	}
}

func TestAsyncReplicate_TryOffer_params(t *testing.T) {
	params := NewReplicationParams()
	params.DestinationRecordingID = 5
	params.StopPosition = 8192
	params.ReplicationSessionID = 9
	params.FileIOMaxLength = 4096

	f := newAsyncReplicateFixture(t)
	f.handle = f.newHandle(t, params)
	offered, err := f.handle.TryOffer()
	require.NoError(t, err)
	require.True(t, offered)
	f.requireOfferedReplicate(t, params)
}

func TestAsyncReplicate_TryOffer_invalidParams(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	f.handle = f.newHandle(t, ReplicationParams{})

	offered, err := f.handle.TryOffer()
	require.False(t, offered)
	require.ErrorIs(t, err, ErrInvalidReplicationParams)
	require.Empty(t, f.h.pub.offered, "invalid params must not reach the publication")
}

func TestAsyncReplicate_PollResponse_matchingOK(t *testing.T) {
	f := newAsyncReplicateFixture(t)

	done, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, done, "empty capture is not a response")

	// Phase: the OK completes the request and disarms the filter.
	f.latch(t, f.okFor(f.handle))
	done, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, done, "matching OK must complete")
	require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
	require.False(t, f.h.poller.IsPollComplete(), "matching OK must be consumed")
	require.Equal(t, int64(0), f.h.poller.awaitCorrelationID, "OK must disarm the filter")

	// Phase: every later call completes and does not read the capture.
	other := f.okFor(f.handle)
	other.CorrelationID++
	other.RelevantID = 1
	f.latch(t, other)
	require.True(t, f.h.poller.IsPollComplete(), "disarmed poller accepts another correlation")
	done, err = f.handle.PollResponse()
	require.NoError(t, err)
	require.True(t, done, "later call must report done")
	require.True(t, f.h.poller.IsPollComplete(), "later call must not touch the capture")
	require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
}

func TestAsyncReplicate_PollResponse_successorFilterKeptAfterOK(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	first := f.handle
	f.resolveOK(t)
	second := f.newHandle(t, NewReplicationParams())

	// Phase: a completed handle does not read the capture and keeps the
	// successor filter.
	done, err := first.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, second.correlationID, f.h.poller.awaitCorrelationID)

	f.latch(t, f.okFor(second))
	done, err = first.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
	require.True(t, f.h.poller.IsPollComplete(), "completed handle must not consume the successor OK")

	// Phase: Close of the completed handle leaves the successor latch and filter.
	first.Close()
	require.True(t, f.h.poller.IsPollComplete())
	require.Equal(t, second.correlationID, f.h.poller.awaitCorrelationID)
	require.Equal(t, asyncReplicateTestReplicationID, first.ReplicationID())
}

func TestAsyncReplicate_PollResponse_consumeKeepsSuccessorFilter(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	first := f.handle
	second := f.newHandle(t, NewReplicationParams())

	// Phase: latch the first OK, then arm the successor filter over the capture.
	f.h.poller.ArmAwait(first.correlationID)
	f.latch(t, f.okFor(first))
	f.h.poller.ArmAwait(second.correlationID)
	require.True(t, f.h.poller.IsPollComplete())

	// Phase: consuming the first OK leaves the successor filter armed.
	done, err := first.PollResponse()
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, asyncReplicateTestReplicationID, first.ReplicationID())
	require.False(t, f.h.poller.IsPollComplete(), "consumed OK must clear the capture")
	require.Equal(t, second.correlationID, f.h.poller.awaitCorrelationID)
}

func TestAsyncReplicate_PollResponse_errorResponse(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	fields := f.errorFor(f.handle)
	f.latch(t, fields)

	want := MapControlResponse(fields)
	done, err := f.handle.PollResponse()
	require.True(t, done, "matching error must complete")
	require.Equal(t, want, err)
	code, ok := ExceptionCode(err)
	require.True(t, ok)
	require.Equal(t, int32(ErrorUnknownRecording), code)
	require.True(t, f.h.poller.IsPollComplete(), "error capture stays latched")
	require.Equal(t, f.handle.correlationID, f.h.poller.awaitCorrelationID)

	done, err = f.handle.PollResponse()
	require.True(t, done, "latched error remains visible")
	require.Equal(t, want, err)
	require.Equal(t, cnc.NullValue, f.handle.ReplicationID())
}

func TestAsyncReplicate_PollResponse_discardsStaleTerminal(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	stale := f.okFor(f.handle)
	stale.ControlSessionID++
	f.latch(t, stale)
	require.Equal(t, cnc.NullValue, f.handle.ReplicationID(), "foreign session OK must not resolve")

	done, err := f.handle.PollResponse()
	require.NoError(t, err)
	require.False(t, done, "stale terminal is not a match")
	require.False(t, f.h.poller.IsPollComplete(), "stale terminal must be discarded")

	f.resolveOK(t)
	require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
}

func TestAsyncReplicate_ReplicationID(t *testing.T) {
	cases := []struct {
		name     string
		response func(f *asyncReplicateFixture, handle *AsyncReplicate) codecs.ControlResponseFields
		want     int64
	}{
		{name: "before_ok", want: cnc.NullValue},
		{
			name:     "latched_ok_before_poll_response",
			response: (*asyncReplicateFixture).okFor,
			want:     asyncReplicateTestReplicationID,
		},
		{
			name:     "latched_error",
			response: (*asyncReplicateFixture).errorFor,
			want:     cnc.NullValue,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			if tc.response != nil {
				f.latch(t, tc.response(f, f.handle))
			}

			require.Equal(t, tc.want, f.handle.ReplicationID())
		})
	}

	var nilHandle *AsyncReplicate
	require.Equal(t, cnc.NullValue, nilHandle.ReplicationID())
	require.Equal(t, cnc.NullValue, nilHandle.RecordingID())
	require.Equal(t, NullPosition, nilHandle.Position())
}

func TestAsyncReplicate_Close(t *testing.T) {
	t.Run("disarms_filter", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		other := f.okFor(f.handle)
		other.CorrelationID++
		f.latch(t, other)
		require.False(t, f.h.poller.IsPollComplete(), "armed filter must drop other correlation")

		f.handle.Close()
		require.Equal(t, int64(0), f.h.poller.awaitCorrelationID)
		f.latch(t, other)
		require.True(t, f.h.poller.IsPollComplete(), "disarmed poller accepts any correlation")
	})

	t.Run("clears_latched_error", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		f.latch(t, f.errorFor(f.handle))

		f.handle.Close()
		require.False(t, f.h.poller.IsPollComplete(), "Close must clear matching terminal")
		require.Equal(t, cnc.NullValue, f.handle.ReplicationID())
	})

	t.Run("keeps_successor_filter", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		first := f.handle
		second := f.newHandle(t, NewReplicationParams())

		first.Close()
		require.Equal(t, second.correlationID, f.h.poller.awaitCorrelationID)
	})

	t.Run("latched_ok_keeps_replication_id", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		f.latch(t, f.okFor(f.handle))

		f.handle.Close()
		require.False(t, f.h.poller.IsPollComplete(), "Close must clear matching OK")
		require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
	})

	t.Run("before_ok_keeps_null_replication_id", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		f.handle.Close()

		f.latch(t, f.okFor(f.handle))
		require.True(t, f.h.poller.IsPollComplete(), "late OK latches on disarmed poller")
		require.Equal(t, cnc.NullValue, f.handle.ReplicationID(), "closed handle must not resolve")
		done, err := f.handle.PollResponse()
		require.NoError(t, err)
		require.False(t, done, "closed handle must not match")
	})

	t.Run("keeps_accessors", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		f.resolveOK(t)
		f.handle.RecordingSignal(replicationSignal(archivewire.RecordingSignal_STOP))

		f.handle.Close()
		f.handle.Close()
		require.Equal(t, asyncReplicateTestReplicationID, f.handle.ReplicationID())
		require.Equal(t, asyncReplicateTestRecordingID, f.handle.RecordingID())
		require.Equal(t, asyncReplicateTestPosition, f.handle.Position())
		requireReplicateFlags(t, f.handle, replicateFlags{stopped: true})
	})

	t.Run("nil_handle", func(t *testing.T) {
		var nilHandle *AsyncReplicate
		nilHandle.Close()
		offered, err := nilHandle.TryOffer()
		require.NoError(t, err)
		require.False(t, offered)
		done, err := nilHandle.PollResponse()
		require.NoError(t, err)
		require.False(t, done)
		nilHandle.RecordingSignal(replicationSignal(archivewire.RecordingSignal_STOP))
		requireReplicateFlags(t, nilHandle, replicateFlags{})
	})

	t.Run("closed_does_not_offer", func(t *testing.T) {
		f := newAsyncReplicateFixture(t)
		f.handle.Close()

		offered, err := f.handle.TryOffer()
		require.NoError(t, err)
		require.False(t, offered)
		require.Empty(t, f.h.pub.offered)
	})
}

func TestAsyncReplicate_RecordingSignal_signalKinds(t *testing.T) {
	cases := []struct {
		signal archivewire.RecordingSignal
		want   replicateFlags
	}{
		{signal: archivewire.RecordingSignal_REPLICATE},
		{signal: archivewire.RecordingSignal_START},
		{signal: archivewire.RecordingSignal_EXTEND, want: replicateFlags{extended: true}},
		{signal: archivewire.RecordingSignal_SYNC, want: replicateFlags{synced: true}},
		{signal: archivewire.RecordingSignal_MERGE, want: replicateFlags{merged: true}},
		{signal: archivewire.RecordingSignal_STOP, want: replicateFlags{stopped: true}},
		{
			signal: archivewire.RecordingSignal_REPLICATE_END,
			want:   replicateFlags{replicationEnded: true},
		},
		{signal: archivewire.RecordingSignal_DELETE, want: replicateFlags{recordingDeleted: true}},
	}
	for _, tc := range cases {
		t.Run(tc.signal.String(), func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			f.resolveOK(t)

			f.handle.RecordingSignal(replicationSignal(tc.signal))
			requireReplicateFlags(t, f.handle, tc.want)
			require.Equal(t, asyncReplicateTestRecordingID, f.handle.RecordingID())
			require.Equal(t, asyncReplicateTestPosition, f.handle.Position())

			// A repeat keeps every flag.
			f.handle.RecordingSignal(replicationSignal(archivewire.RecordingSignal_REPLICATE))
			requireReplicateFlags(t, f.handle, tc.want)
		})
	}
}

func TestAsyncReplicate_RecordingSignal_filters(t *testing.T) {
	foreign := replicationSignal(archivewire.RecordingSignal_STOP)
	foreign.CorrelationID++
	rebuilt := replicationSignal(archivewire.RecordingSignal_STOP)
	rebuilt.ControlSessionID = 0
	// The signal carries the ERROR's RelevantID, so an ERROR read as the
	// replication id would fold it.
	errorCode := replicationSignal(archivewire.RecordingSignal_STOP)
	errorCode.CorrelationID = ErrorUnknownRecording
	cases := []struct {
		name      string
		setup     func(t *testing.T, f *asyncReplicateFixture)
		event     codecs.RecordingSignalEventFields
		wantFolds bool
	}{
		{
			name:  "foreign_correlation_id",
			setup: func(t *testing.T, f *asyncReplicateFixture) { f.resolveOK(t) },
			event: foreign,
		},
		{
			name:      "rebuilt_event_without_session",
			setup:     func(t *testing.T, f *asyncReplicateFixture) { f.resolveOK(t) },
			event:     rebuilt,
			wantFolds: true,
		},
		{
			name:  "closed_handle",
			setup: func(t *testing.T, f *asyncReplicateFixture) { f.resolveOK(t); f.handle.Close() },
			event: replicationSignal(archivewire.RecordingSignal_STOP),
		},
		{
			name: "error_latched_instead_of_ok",
			setup: func(t *testing.T, f *asyncReplicateFixture) {
				f.latch(t, f.errorFor(f.handle))
			},
			event: errorCode,
		},
		{
			name:  "no_response",
			setup: func(*testing.T, *asyncReplicateFixture) {},
			event: replicationSignal(archivewire.RecordingSignal_STOP),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReplicateFixture(t)
			tc.setup(t, f)

			f.handle.RecordingSignal(tc.event)
			if !tc.wantFolds {
				requireReplicateFlags(t, f.handle, replicateFlags{})
				require.Equal(t, cnc.NullValue, f.handle.RecordingID())
				require.Equal(t, NullPosition, f.handle.Position())
				return
			}
			requireReplicateFlags(t, f.handle, replicateFlags{stopped: true})
			require.Equal(t, asyncReplicateTestRecordingID, f.handle.RecordingID())
			require.Equal(t, asyncReplicateTestPosition, f.handle.Position())
		})
	}
}

func TestAsyncReplicate_RecordingSignal_nullValuesKept(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	f.resolveOK(t)
	f.handle.RecordingSignal(replicationSignal(archivewire.RecordingSignal_EXTEND))

	end := replicationSignal(archivewire.RecordingSignal_REPLICATE_END)
	end.RecordingID = cnc.NullValue
	end.Position = NullPosition
	f.handle.RecordingSignal(end)

	requireReplicateFlags(t, f.handle, replicateFlags{extended: true, replicationEnded: true})
	require.Equal(t, asyncReplicateTestRecordingID, f.handle.RecordingID())
	require.Equal(t, asyncReplicateTestPosition, f.handle.Position())

	stop := replicationSignal(archivewire.RecordingSignal_STOP)
	stop.Position = asyncReplicateTestPosition * 2
	f.handle.RecordingSignal(stop)
	require.Equal(t, asyncReplicateTestPosition*2, f.handle.Position())
}

// TestAsyncReplicate_clientPoll_foreignSession asserts a signal of another
// control session never reaches the handle through Client.Poll.
func TestAsyncReplicate_clientPoll_foreignSession(t *testing.T) {
	f := newAsyncReplicateFixture(t)
	f.client.SetRecordingSignalHandler(f.handle)
	f.resolveOK(t)

	foreign := replicationSignal(archivewire.RecordingSignal_STOP)
	foreign.ControlSessionID++
	f.deliverSignals(t, foreign)
	requireReplicateFlags(t, f.handle, replicateFlags{})

	f.deliverSignals(t, replicationSignal(archivewire.RecordingSignal_STOP))
	requireReplicateFlags(t, f.handle, replicateFlags{stopped: true})
}

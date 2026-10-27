package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/consensus"
)

const (
	testReplicationBuildNs        int64 = 1_000
	testReplicationOfferNs        int64 = 100
	testReplicationResponseNs     int64 = 200
	testReplicationCheckNs        int64 = 10
	testReplicationProgressNs     int64 = 50
	testReplicationID             int64 = 77
	testReplicationDestinationID  int64 = 5
	testReplicationLeaderID       int64 = 3
	testReplicationStopPosition   int64 = 4096
	testReplicationStartPosition  int64 = 1024
	testReplicationCorrelationID  int64 = 900
	testReplicationControlStream  int32 = 10
	testReplicationLeaderEndpoint       = "leader:8010"
	testReplicationChannel              = "aeron:udp?endpoint=127.0.0.1:0"
)

// fakeReplicateRequest mirrors the AsyncReplicate fold. PollResponse latches the OK.
// Signals then fold only when they carry the replication id.
type fakeReplicateRequest struct {
	log *[]string

	offerAccepted bool
	offerErr      error
	responded     bool
	responseErr   error
	latched       bool
	replicationID int64

	recordingID int64
	position    int64
	extended    bool
	stopped     bool
	ended       bool
	deleted     bool

	tryOfferCalls int
	signals       int
	closed        bool
}

func (f *fakeReplicateRequest) TryOffer() (bool, error) {
	f.tryOfferCalls++
	return f.offerAccepted, f.offerErr
}

func (f *fakeReplicateRequest) PollResponse() (bool, error) {
	if f.responseErr != nil {
		return true, f.responseErr
	}
	if f.responded {
		f.latched = true
	}
	return f.responded, nil
}

func (f *fakeReplicateRequest) RecordingSignal(event codecs.RecordingSignalEventFields) {
	if f.closed || !f.latched || event.CorrelationID != f.replicationID {
		return
	}
	f.signals++
	switch event.Signal {
	case archivewire.RecordingSignal_EXTEND:
		f.extended = true
	case archivewire.RecordingSignal_STOP:
		f.stopped = true
	case archivewire.RecordingSignal_REPLICATE_END:
		f.ended = true
	case archivewire.RecordingSignal_DELETE:
		f.deleted = true
	default:
	}
	if event.RecordingID != cnc.NullValue {
		f.recordingID = event.RecordingID
	}
	if event.Position != archive.NullPosition {
		f.position = event.Position
	}
}

func (f *fakeReplicateRequest) ReplicationID() int64 {
	if !f.latched {
		return cnc.NullValue
	}
	return f.replicationID
}

func (f *fakeReplicateRequest) RecordingID() int64        { return f.recordingID }
func (f *fakeReplicateRequest) Position() int64           { return f.position }
func (f *fakeReplicateRequest) HasExtended() bool         { return f.extended }
func (f *fakeReplicateRequest) HasStopped() bool          { return f.stopped }
func (f *fakeReplicateRequest) HasReplicationEnded() bool { return f.ended }
func (f *fakeReplicateRequest) IsRecordingDeleted() bool  { return f.deleted }

func (f *fakeReplicateRequest) Close() {
	if f.closed {
		return
	}
	f.closed = true
	*f.log = append(*f.log, "replicate close")
}

type fakeStopRequest struct {
	log           *[]string
	offerAccepted bool
	offerErr      error
	responded     bool
	responseErr   error
	closed        bool
}

func (f *fakeStopRequest) TryOffer() (bool, error) {
	return f.offerAccepted, f.offerErr
}

func (f *fakeStopRequest) PollResponse() (bool, error) {
	if f.responseErr != nil {
		return true, f.responseErr
	}
	return f.responded, nil
}

func (f *fakeStopRequest) Close() {
	if f.closed {
		return
	}
	f.closed = true
	*f.log = append(*f.log, "stop close")
}

// replicateCall records one NewAsyncReplicate argument set.
type replicateCall struct {
	sourceRecordingID     int64
	sourceControlStreamID int32
	sourceControlChannel  string
	params                archive.ReplicationParams
}

// fakeReplicationControl returns the same replicate and stop request on every build.
// log records the close and stop order.
type fakeReplicationControl struct {
	log            []string
	request        *fakeReplicateRequest
	stop           *fakeStopRequest
	replicateCalls []replicateCall
	stopBuilds     []int64
	blockingStops  []int64
	blockingErr    error
}

func newFakeReplicationControl() *fakeReplicationControl {
	c := &fakeReplicationControl{}
	c.request = &fakeReplicateRequest{
		log:           &c.log,
		replicationID: testReplicationID,
		recordingID:   cnc.NullValue,
		position:      archive.NullPosition,
	}
	c.stop = &fakeStopRequest{log: &c.log}
	return c
}

func (c *fakeReplicationControl) NewAsyncReplicate(
	sourceRecordingID int64,
	sourceControlStreamID int32,
	sourceControlChannel []byte,
	params archive.ReplicationParams,
) replicateRequest {
	c.replicateCalls = append(c.replicateCalls, replicateCall{
		sourceRecordingID:     sourceRecordingID,
		sourceControlStreamID: sourceControlStreamID,
		sourceControlChannel:  string(sourceControlChannel),
		params:                params,
	})
	return c.request
}

func (c *fakeReplicationControl) NewAsyncStopReplication(
	replicationID int64,
) logStopRequest {
	c.stopBuilds = append(c.stopBuilds, replicationID)
	return c.stop
}

func (c *fakeReplicationControl) StopReplication(
	_ context.Context,
	replicationID int64,
) (bool, error) {
	c.blockingStops = append(c.blockingStops, replicationID)
	c.log = append(c.log, "blocking stop")
	if c.blockingErr != nil {
		return false, c.blockingErr
	}
	return true, nil
}

// fakeRecordingPositions serves one counter value per recording id.
type fakeRecordingPositions struct {
	values map[int64]int64
	reads  []int64
}

func (f *fakeRecordingPositions) RecordingPosition(recordingID int64) (int64, bool) {
	f.reads = append(f.reads, recordingID)
	value, ok := f.values[recordingID]
	return value, ok
}

// replicationFixture wires one logReplication helper to the fakes.
type replicationFixture struct {
	control   *fakeReplicationControl
	positions *fakeRecordingPositions
	recording *logRecording
	agent     *consensus.Agent
	r         *logReplication
}

func newReplicationFixture(t *testing.T) *replicationFixture {
	t.Helper()
	return newReplicationFixtureWith(t, testReplicationLeaderEndpoint, "", nil)
}

// newReplicationFixtureWith builds a helper at testReplicationBuildNs. The
// agent holds testReplicationDestinationID as its log recording id. A non-nil
// configure edits the deps before the build.
func newReplicationFixtureWith(
	t *testing.T,
	leaderEndpoint string,
	responseEndpoint string,
	configure func(*logReplicationDeps),
) *replicationFixture {
	t.Helper()

	control := newFakeReplicationControl()
	positions := &fakeRecordingPositions{values: map[int64]int64{}}
	recording := &logRecording{
		recordingID:        recordingIDUnknown,
		latchedRecordingID: recordingIDUnknown,
	}
	agent := consensus.NewAgent(consensus.AgentConfig{})
	agent.SetLogRecordingID(testReplicationDestinationID)
	deps := logReplicationDeps{
		Control:                     control,
		Positions:                   positions,
		NextCorrelationID:           func() int64 { return testReplicationCorrelationID },
		Agent:                       agent,
		Recording:                   recording,
		ReplicationChannel:          testReplicationChannel,
		LeaderArchiveControlChannel: DefaultLeaderArchiveControlChannel,
		ControlRequestStreamID:      testReplicationControlStream,
		ProgressCheckIntervalNs:     testReplicationCheckNs,
		ProgressTimeoutNs:           testReplicationProgressNs,
		OfferTimeoutNs:              testReplicationOfferNs,
		ResponseTimeoutNs:           testReplicationResponseNs,
	}
	if configure != nil {
		configure(&deps)
	}
	r := newLogReplication(
		deps,
		leaderEndpoint,
		responseEndpoint,
		testReplicationLeaderID,
		testReplicationStopPosition,
		testReplicationBuildNs,
	)
	require.NotNil(t, r)
	return &replicationFixture{
		control:   control,
		positions: positions,
		recording: recording,
		agent:     agent,
		r:         r,
	}
}

func (fx *replicationFixture) doWork(t *testing.T, nowNs int64) int {
	t.Helper()
	work, err := fx.r.DoWork(nowNs)
	require.NoError(t, err)
	return work
}

func (fx *replicationFixture) signal(
	signal archivewire.RecordingSignal,
	recordingID int64,
	position int64,
) {
	fx.r.OnSignal(testReplicationID, recordingID, position, signal)
}

func (fx *replicationFixture) signalAt(signal archivewire.RecordingSignal, position int64) {
	fx.signal(signal, testReplicationDestinationID, position)
}

// toAwaitingResponse runs the offer at offerNs.
func (fx *replicationFixture) toAwaitingResponse(t *testing.T, offerNs int64) {
	t.Helper()
	fx.control.request.offerAccepted = true
	require.Equal(t, 1, fx.doWork(t, offerNs))
	require.Equal(t, logReplicationAwaitingResponse, fx.r.phase)
}

// toRunning runs the offer and the OK at okNs.
func (fx *replicationFixture) toRunning(t *testing.T, okNs int64) {
	t.Helper()
	fx.toAwaitingResponse(t, okNs)
	fx.control.request.responded = true
	require.Equal(t, 1, fx.doWork(t, okNs))
	require.Equal(t, logReplicationRunning, fx.r.phase)
}

// toExtended folds EXTEND at testReplicationStartPosition after toRunning.
func (fx *replicationFixture) toExtended(t *testing.T, okNs int64) {
	t.Helper()
	fx.toRunning(t, okNs)
	fx.signalAt(archivewire.RecordingSignal_EXTEND, testReplicationStartPosition)
	require.Equal(t, testReplicationStartPosition, fx.r.Position())
}

func TestArchiveDestinationChannel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		template string
		endpoint string
		want     string
		wantErr  string
	}{
		{
			name:     "default template keeps the media only",
			template: DefaultLeaderArchiveControlChannel,
			endpoint: "leader:8010",
			want:     "aeron:udp?endpoint=leader:8010",
		},
		{
			name:     "interface kept, other parameters dropped",
			template: "aeron:udp?term-length=64k|interface=10.0.0.1|endpoint=old:1|ttl=0",
			endpoint: "leader:8010",
			want:     "aeron:udp?endpoint=leader:8010|interface=10.0.0.1",
		},
		{
			name:     "bare template",
			template: "aeron:udp",
			endpoint: "leader:8010",
			want:     "aeron:udp?endpoint=leader:8010",
		},
		{
			name:     "non-udp template refused",
			template: "aeron:ipc",
			endpoint: "leader:8010",
			wantErr:  "invalid channel: aeron:ipc",
		},
		{
			name:     "empty endpoint refused",
			template: DefaultLeaderArchiveControlChannel,
			wantErr:  "endpoint is required",
		},
		{
			name:     "endpoint with a delimiter refused",
			template: DefaultLeaderArchiveControlChannel,
			endpoint: "leader?8010",
			wantErr:  "invalid endpoint: leader?8010",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := archiveDestinationChannel(tc.template, tc.endpoint)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestResponseReplicationChannel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		channel  string
		endpoint string
		want     string
		wantErr  string
	}{
		{
			name:     "endpoint removed, control added",
			channel:  "aeron:udp?endpoint=127.0.0.1:0",
			endpoint: "leader:8020",
			want:     "aeron:udp?control=leader:8020|control-mode=response",
		},
		{
			name:     "endpoint in the middle removed",
			channel:  "aeron:udp?term-length=64k|endpoint=127.0.0.1:0|ttl=0",
			endpoint: "leader:8020",
			want:     "aeron:udp?term-length=64k|ttl=0|control=leader:8020|control-mode=response",
		},
		{
			name:     "existing control values replaced",
			channel:  "aeron:udp?control=old:1|control-mode=dynamic|term-length=64k",
			endpoint: "leader:8020",
			want:     "aeron:udp?control=leader:8020|control-mode=response|term-length=64k",
		},
		{
			name:     "non-udp channel refused",
			channel:  "aeron:ipc",
			endpoint: "leader:8020",
			wantErr:  "invalid channel: aeron:ipc",
		},
		{
			name:    "empty endpoint refused",
			channel: "aeron:udp?endpoint=127.0.0.1:0",
			wantErr: "endpoint is required",
		},
		{
			name:     "endpoint with a delimiter refused",
			channel:  "aeron:udp?endpoint=127.0.0.1:0",
			endpoint: "leader|8020",
			wantErr:  "invalid endpoint: leader|8020",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := responseReplicationChannel(tc.channel, tc.endpoint)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNewLogReplication_requestFields(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                  string
		replicationChannel    string
		responseEndpoint      string
		wantReplication       string
		wantSourceResponse    string
		leaderControlTemplate string
		wantSourceControl     string
	}{
		{
			name:                  "member replication channel",
			replicationChannel:    testReplicationChannel,
			wantReplication:       testReplicationChannel,
			leaderControlTemplate: DefaultLeaderArchiveControlChannel,
			wantSourceControl:     "aeron:udp?endpoint=" + testReplicationLeaderEndpoint,
		},
		{
			name:                  "empty replication channel sends an empty field",
			leaderControlTemplate: "aeron:udp?term-length=64k|interface=127.0.0.1",
			wantSourceControl: "aeron:udp?endpoint=" + testReplicationLeaderEndpoint +
				"|interface=127.0.0.1",
		},
		{
			name:                  "response endpoint rewrites both channels",
			replicationChannel:    "aeron:udp?endpoint=127.0.0.1:0|term-length=64k",
			responseEndpoint:      "leader:8020",
			wantReplication:       "aeron:udp?term-length=64k|control=leader:8020|control-mode=response",
			wantSourceResponse:    "aeron:udp?term-length=64k|control=leader:8020|control-mode=response",
			leaderControlTemplate: DefaultLeaderArchiveControlChannel,
			wantSourceControl:     "aeron:udp?endpoint=" + testReplicationLeaderEndpoint,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newReplicationFixtureWith(
				t,
				testReplicationLeaderEndpoint,
				tc.responseEndpoint,
				func(deps *logReplicationDeps) {
					deps.NextCorrelationID = func() int64 { return 1<<32 + 9 }
					deps.ReplicationChannel = tc.replicationChannel
					deps.LeaderArchiveControlChannel = tc.leaderControlTemplate
				},
			)

			require.Len(t, fx.control.replicateCalls, 1)
			call := fx.control.replicateCalls[0]
			require.Equal(t, testReplicationLeaderID, call.sourceRecordingID)
			require.Equal(t, testReplicationControlStream, call.sourceControlStreamID)
			require.Equal(t, tc.wantSourceControl, call.sourceControlChannel)

			defaults := archive.NewReplicationParams()
			require.Equal(t, testReplicationDestinationID, call.params.DestinationRecordingID)
			require.Equal(t, testReplicationStopPosition, call.params.StopPosition)
			require.Equal(t, int32(9), call.params.ReplicationSessionID)
			require.Equal(t, defaults.FileIOMaxLength, call.params.FileIOMaxLength)
			require.Equal(t, defaults.ChannelTagID, call.params.ChannelTagID)
			require.Equal(t, defaults.SubscriptionTagID, call.params.SubscriptionTagID)
			require.Empty(t, call.params.LiveDestination)
			require.Equal(t, tc.wantReplication, string(call.params.ReplicationChannel))
			require.Equal(t, tc.wantSourceResponse, string(call.params.SourceResponseChannel))
		})
	}
}

func TestNewLogReplication_buildFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		leaderEndpoint   string
		responseEndpoint string
		wantErr          string
	}{
		{
			name:    "empty leader endpoint",
			wantErr: "cluster: log replication request: endpoint is required",
		},
		{
			name:           "leader endpoint with a delimiter",
			leaderEndpoint: "leader|8010",
			wantErr:        "cluster: log replication request: invalid endpoint: leader|8010",
		},
		{
			name:             "response endpoint with a delimiter",
			leaderEndpoint:   testReplicationLeaderEndpoint,
			responseEndpoint: "leader=8020",
			wantErr:          "cluster: log replication request: invalid endpoint: leader=8020",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newReplicationFixtureWith(t, tc.leaderEndpoint, tc.responseEndpoint, nil)
			require.Empty(t, fx.control.replicateCalls)
			require.False(t, fx.r.IsClosed())

			_, err := fx.r.DoWork(testReplicationBuildNs)
			require.EqualError(t, err, tc.wantErr)
			require.True(t, fx.r.IsClosed())
			require.Zero(t, fx.doWork(t, testReplicationBuildNs+1))
			require.Equal(t, cnc.NullValue, fx.r.RecordingID())
			require.Equal(t, archive.NullPosition, fx.r.Position())
			require.False(t, fx.r.IsDone())
			fx.r.OnSignal(testReplicationID, 1, 2, archivewire.RecordingSignal_EXTEND)
			require.NoError(t, fx.r.stopReplication())
		})
	}
}

func TestLogReplication_offer(t *testing.T) {
	t.Parallel()

	t.Run("retriable offer retries, then a true offer forgets the stored stop", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.recording.storeStopPosition(testReplicationDestinationID, testReplicationStartPosition)

		require.Zero(t, fx.doWork(t, testReplicationBuildNs))
		require.Zero(t, fx.doWork(t, testReplicationBuildNs+testReplicationOfferNs-1))
		require.Equal(t, logReplicationOffering, fx.r.phase)
		require.Equal(t, testReplicationDestinationID, fx.recording.latchedRecordingID)

		fx.control.request.offerAccepted = true
		require.Equal(t, 1, fx.doWork(t, testReplicationBuildNs+testReplicationOfferNs-1))
		require.Equal(t, logReplicationAwaitingResponse, fx.r.phase)
		require.Equal(t, 3, fx.control.request.tryOfferCalls)
		require.Equal(t, recordingIDUnknown, fx.recording.latchedRecordingID)
	})

	t.Run("true offer keeps a stop stored for another recording", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.recording.storeStopPosition(testReplicationDestinationID+1, testReplicationStartPosition)
		fx.control.request.offerAccepted = true

		require.Equal(t, 1, fx.doWork(t, testReplicationBuildNs))
		require.Equal(t, testReplicationDestinationID+1, fx.recording.latchedRecordingID)
	})

	t.Run("offer error closes", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		offerErr := errors.New("not connected")
		fx.control.request.offerErr = offerErr

		_, err := fx.r.DoWork(testReplicationBuildNs)
		require.ErrorIs(t, err, offerErr)
		require.ErrorContains(t, err, "cluster: log replication request: ")
		require.True(t, fx.r.IsClosed())
		require.True(t, fx.control.request.closed)
		require.Empty(t, fx.control.stopBuilds)
	})

	t.Run("offer deadline runs from the build", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		_, err := fx.r.DoWork(testReplicationBuildNs + testReplicationOfferNs)
		require.EqualError(t, err, "cluster: log replication request: no offer within 100ns")
		require.True(t, fx.r.IsClosed())
		// The deadline tick still offers once before it fails.
		require.Equal(t, 1, fx.control.request.tryOfferCalls)

		// DoWork returns the failure once.
		require.Zero(t, fx.doWork(t, testReplicationBuildNs+testReplicationOfferNs+1))
	})
}

func TestLogReplication_response(t *testing.T) {
	t.Parallel()

	const offerNs = testReplicationBuildNs + 30

	t.Run("OK starts both progress clocks", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		require.Zero(t, fx.doWork(t, offerNs+1))

		okNs := offerNs + 5
		fx.control.request.responded = true
		require.Equal(t, 1, fx.doWork(t, okNs))
		require.Equal(t, logReplicationRunning, fx.r.phase)
		require.Equal(t, okNs+testReplicationCheckNs, fx.r.nextProgressCheckNs)
		require.Equal(t, okNs+testReplicationProgressNs, fx.r.progressDeadlineNs)
	})

	t.Run("error closes without a stop", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		responseErr := errors.New("active recording")
		fx.control.request.responseErr = responseErr

		_, err := fx.r.DoWork(offerNs + 1)
		require.ErrorIs(t, err, responseErr)
		require.True(t, fx.r.IsClosed())
		require.Equal(t, []string{"replicate close"}, fx.control.log)
		require.Empty(t, fx.control.stopBuilds)
		require.NoError(t, fx.r.stopReplication())
		require.Empty(t, fx.control.blockingStops)
	})

	t.Run("response deadline runs from the true offer", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		require.Zero(t, fx.doWork(t, offerNs+testReplicationResponseNs-1))

		_, err := fx.r.DoWork(offerNs + testReplicationResponseNs)
		require.EqualError(t, err, "cluster: log replication request: no response within 200ns")
		require.True(t, fx.r.IsClosed())
		require.Empty(t, fx.control.stopBuilds)
	})

	t.Run("OK on the deadline tick starts running", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		fx.control.request.responded = true
		require.Equal(t, 1, fx.doWork(t, offerNs+testReplicationResponseNs))
		require.Equal(t, logReplicationRunning, fx.r.phase)
	})

	t.Run("an end folded before the OK fails on the OK tick", func(t *testing.T) {
		t.Parallel()

		// The archive poll latches the OK and folds the signals before the
		// helper polls the response.
		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		fx.control.request.latched = true
		fx.signalAt(archivewire.RecordingSignal_EXTEND, testReplicationStartPosition)
		fx.signalAt(archivewire.RecordingSignal_STOP, testReplicationStopPosition-1)
		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)
		require.True(t, fx.r.IsDone())

		fx.control.request.responded = true
		_, err := fx.r.DoWork(offerNs + 1)
		require.ErrorIs(t, err, errLogReplicationStoppedShort)
		require.True(t, fx.r.IsClosed())
		require.Empty(t, fx.control.stopBuilds)
	})

	t.Run("a full copy folded before the OK is done on the OK tick", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, offerNs)
		fx.control.request.latched = true
		fx.signalAt(archivewire.RecordingSignal_EXTEND, testReplicationStartPosition)
		fx.signalAt(archivewire.RecordingSignal_STOP, testReplicationStopPosition)
		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)

		fx.control.request.responded = true
		require.Equal(t, 1, fx.doWork(t, offerNs+1))
		require.Equal(t, logReplicationRunning, fx.r.phase)
		require.True(t, fx.r.IsDone())
		require.Equal(t, testReplicationStopPosition, fx.r.Position())
	})
}

func TestLogReplication_progress(t *testing.T) {
	t.Parallel()

	const okNs = testReplicationBuildNs + 40

	t.Run("counter read only after EXTEND", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toRunning(t, okNs)
		fx.positions.values[testReplicationDestinationID] = testReplicationStartPosition + 10

		require.Zero(t, fx.doWork(t, okNs+testReplicationCheckNs))
		require.Empty(t, fx.positions.reads)

		fx.signalAt(archivewire.RecordingSignal_EXTEND, testReplicationStartPosition)
		require.Equal(t, 1, fx.doWork(t, okNs+2*testReplicationCheckNs))
		require.Equal(t, []int64{testReplicationDestinationID}, fx.positions.reads)
		require.Equal(t, testReplicationStartPosition+10, fx.r.Position())
	})

	t.Run("a raise moves the deadline and a flat check leaves it", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)

		// Phase: a check before the interval does not read the counter.
		require.Zero(t, fx.doWork(t, okNs+testReplicationCheckNs-1))
		require.Empty(t, fx.positions.reads)

		// Phase: a raise resets the deadline from its tick.
		raiseNs := okNs + testReplicationCheckNs
		fx.positions.values[testReplicationDestinationID] = testReplicationStartPosition + 64
		require.Equal(t, 1, fx.doWork(t, raiseNs))
		require.Equal(t, raiseNs+testReplicationProgressNs, fx.r.progressDeadlineNs)
		require.Equal(t, raiseNs+testReplicationCheckNs, fx.r.nextProgressCheckNs)

		// Phase: a flat check and a signal position do not move the deadline.
		require.Zero(t, fx.doWork(t, raiseNs+testReplicationCheckNs))
		fx.signalAt(archivewire.RecordingSignal_SYNC, testReplicationStartPosition+128)
		require.Equal(t, testReplicationStartPosition+128, fx.r.Position())
		require.Equal(t, raiseNs+testReplicationProgressNs, fx.r.progressDeadlineNs)

		// Phase: a counter value at or below Position does not raise Position.
		fx.positions.values[testReplicationDestinationID] = testReplicationStartPosition + 100
		require.Zero(t, fx.doWork(t, raiseNs+2*testReplicationCheckNs))
		require.Equal(t, testReplicationStartPosition+128, fx.r.Position())
		require.Equal(t, raiseNs+testReplicationProgressNs, fx.r.progressDeadlineNs)
	})

	t.Run("no progress below the stop at the deadline", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)

		_, err := fx.r.DoWork(okNs + testReplicationProgressNs)
		require.ErrorIs(t, err, errLogReplicationNoProgress)

		// The replication has not ended, so the helper starts a stop.
		require.Equal(t, logReplicationStopping, fx.r.phase)
		require.Equal(t, []int64{testReplicationID}, fx.control.stopBuilds)
		require.False(t, fx.r.IsClosed())
	})

	t.Run("failed to stop at the stop position at the deadline", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.signalAt(archivewire.RecordingSignal_REPLICATE, testReplicationStopPosition)

		_, err := fx.r.DoWork(okNs + testReplicationProgressNs)
		require.ErrorIs(t, err, errLogReplicationStopMissed)
		require.Equal(t, logReplicationStopping, fx.r.phase)
	})
}

func TestLogReplication_signals(t *testing.T) {
	t.Parallel()

	const okNs = testReplicationBuildNs + 40

	t.Run("STOP stores the stop and end makes the helper done", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		require.Equal(t, testReplicationDestinationID, fx.r.RecordingID())

		// A foreign replication id does not fold.
		fx.r.OnSignal(testReplicationID+1, 9, 9, archivewire.RecordingSignal_STOP)
		require.False(t, fx.control.request.stopped)

		fx.signalAt(archivewire.RecordingSignal_STOP, testReplicationStopPosition)
		require.Equal(t, testReplicationDestinationID, fx.recording.latchedRecordingID)
		require.Equal(t, testReplicationStopPosition, fx.recording.stopPosition)
		require.False(t, fx.r.IsDone())

		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)
		require.True(t, fx.r.IsDone())
		require.Equal(t, testReplicationStopPosition, fx.r.Position())
		require.Zero(t, fx.doWork(t, okNs+testReplicationProgressNs))
		require.True(t, fx.r.IsDone())

		// Close after the end closes at once. It does not build a stop.
		fx.r.Close()
		require.True(t, fx.r.IsClosed())
		require.True(t, fx.r.IsDone())
		require.Empty(t, fx.control.stopBuilds)

		// A closed helper drops signals.
		signals := fx.control.request.signals
		fx.signal(archivewire.RecordingSignal_EXTEND, 1, 1)
		require.Equal(t, signals, fx.control.request.signals)
	})

	t.Run("STOP below the stop position fails after the end", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.signalAt(archivewire.RecordingSignal_STOP, testReplicationStopPosition-1)
		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)
		require.True(t, fx.r.IsDone())

		// The replication has ended, so the failure closes without a stop.
		_, err := fx.r.DoWork(okNs + 1)
		require.ErrorIs(t, err, errLogReplicationStoppedShort)
		require.True(t, fx.r.IsClosed())
		require.Empty(t, fx.control.stopBuilds)
		require.Equal(t, testReplicationStopPosition-1, fx.recording.stopPosition)
	})

	t.Run("REPLICATE and SYNC before EXTEND report nothing", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toRunning(t, okNs)

		fx.signalAt(archivewire.RecordingSignal_REPLICATE, testReplicationStartPosition)
		fx.signalAt(archivewire.RecordingSignal_SYNC, testReplicationStopPosition)
		require.Equal(t, 2, fx.control.request.signals)
		require.Equal(t, cnc.NullValue, fx.r.RecordingID())
		require.Equal(t, archive.NullPosition, fx.r.Position())

		fx.signal(
			archivewire.RecordingSignal_EXTEND,
			testReplicationDestinationID+1,
			testReplicationStartPosition,
		)
		require.Equal(t, testReplicationDestinationID+1, fx.r.RecordingID())
		require.Equal(t, testReplicationStartPosition, fx.r.Position())
	})

	t.Run("in-sync end fails before extend", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toRunning(t, okNs)
		fx.signalAt(archivewire.RecordingSignal_SYNC, testReplicationStopPosition)
		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)

		_, err := fx.r.DoWork(okNs + 1)
		require.ErrorIs(t, err, errLogReplicationEndedEarly)
		require.True(t, fx.r.IsClosed())
		require.False(t, fx.r.IsDone())
		require.Equal(t, cnc.NullValue, fx.r.RecordingID())
		require.Equal(t, archive.NullPosition, fx.r.Position())
		require.Equal(t, recordingIDUnknown, fx.recording.latchedRecordingID)
		require.Empty(t, fx.control.stopBuilds)
	})

	t.Run("DELETE fails and stops a running replication", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.signalAt(archivewire.RecordingSignal_DELETE, archive.NullPosition)

		_, err := fx.r.DoWork(okNs + 1)
		require.ErrorIs(t, err, errLogReplicationRecordingDeleted)
		require.Equal(t, logReplicationStopping, fx.r.phase)
	})
}

func TestLogReplication_close(t *testing.T) {
	t.Parallel()

	const okNs = testReplicationBuildNs + 40

	t.Run("offering closes at once", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.r.Close()
		require.True(t, fx.r.IsClosed())
		require.True(t, fx.control.request.closed)
		require.Empty(t, fx.control.stopBuilds)
		require.Zero(t, fx.doWork(t, testReplicationBuildNs+1))
		require.Zero(t, fx.control.request.tryOfferCalls)
	})

	t.Run("awaiting response stops after the OK", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, testReplicationBuildNs)
		fx.r.Close()
		require.False(t, fx.r.IsClosed())

		fx.control.request.responded = true
		require.Equal(t, 1, fx.doWork(t, testReplicationBuildNs+1))
		require.Equal(t, logReplicationStopping, fx.r.phase)
		require.Equal(t, []int64{testReplicationID}, fx.control.stopBuilds)
	})

	t.Run("awaiting response closes on an error", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, testReplicationBuildNs)
		fx.r.Close()
		fx.control.request.responseErr = errors.New("refused")

		_, err := fx.r.DoWork(testReplicationBuildNs + 1)
		require.Error(t, err)
		require.True(t, fx.r.IsClosed())
		require.Empty(t, fx.control.stopBuilds)
	})

	t.Run("stopping and closed do not change", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.r.Close()
		require.Equal(t, logReplicationStopping, fx.r.phase)
		fx.r.Close()
		require.Equal(t, logReplicationStopping, fx.r.phase)
		require.Len(t, fx.control.stopBuilds, 1)
	})

	stopCases := []struct {
		name     string
		script   func(stop *fakeStopRequest)
		ticks    []int64
		wantErr  string
		wantDone bool
	}{
		{
			name: "stop OK or unknown replication closes",
			script: func(stop *fakeStopRequest) {
				stop.offerAccepted = true
				stop.responded = true
			},
			ticks:    []int64{okNs + 1, okNs + 2},
			wantDone: true,
		},
		{
			name: "stop response error closes",
			script: func(stop *fakeStopRequest) {
				stop.offerAccepted = true
				stop.responseErr = errors.New("refused")
			},
			ticks:   []int64{okNs + 1, okNs + 2},
			wantErr: "cluster: log replication stop: refused",
		},
		{
			name: "stop offer error closes",
			script: func(stop *fakeStopRequest) {
				stop.offerErr = errors.New("not connected")
			},
			ticks:   []int64{okNs + 1},
			wantErr: "cluster: log replication stop: not connected",
		},
		{
			name:   "stop offer deadline runs from the first stopping tick",
			script: func(*fakeStopRequest) {},
			ticks: []int64{
				okNs + 7,
				okNs + 7 + testReplicationOfferNs - 1,
				okNs + 7 + testReplicationOfferNs,
			},
			wantErr: "cluster: log replication stop: no offer within 100ns",
		},
		{
			name: "stop response deadline runs from the true stop offer",
			script: func(stop *fakeStopRequest) {
				stop.offerAccepted = true
			},
			ticks: []int64{
				okNs + 3,
				okNs + 3 + testReplicationResponseNs - 1,
				okNs + 3 + testReplicationResponseNs,
			},
			wantErr: "cluster: log replication stop: no response within 200ns",
		},
	}
	for _, tc := range stopCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newReplicationFixture(t)
			fx.toExtended(t, okNs)
			tc.script(fx.control.stop)
			fx.r.Close()
			require.Equal(t, logReplicationStopping, fx.r.phase)

			last := len(tc.ticks) - 1
			for i, nowNs := range tc.ticks[:last] {
				_, err := fx.r.DoWork(nowNs)
				require.NoError(t, err, "tick %d", i)
				require.False(t, fx.r.IsClosed(), "tick %d", i)
			}
			_, err := fx.r.DoWork(tc.ticks[last])
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.True(t, fx.r.IsClosed())
			require.True(t, fx.control.stop.closed)
			require.True(t, fx.control.request.closed)
			require.Equal(t, tc.wantDone, fx.r.stopResolved)
			require.Zero(t, fx.doWork(t, tc.ticks[last]+1))
		})
	}

	t.Run("a stop reply on the deadline tick closes as stopped", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.control.stop.offerAccepted = true
		fx.r.Close()
		require.Equal(t, 1, fx.doWork(t, okNs+1))

		fx.control.stop.responded = true
		require.Equal(t, 1, fx.doWork(t, okNs+1+testReplicationResponseNs))
		require.True(t, fx.r.IsClosed())
		require.True(t, fx.r.stopResolved)
	})
}

func TestLogReplication_stopReplication(t *testing.T) {
	t.Parallel()

	const okNs = testReplicationBuildNs + 40

	t.Run("running stops once with the replication id", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)

		require.NoError(t, fx.r.stopReplication())
		require.Equal(t, []int64{testReplicationID}, fx.control.blockingStops)
		require.Equal(t, []string{"replicate close", "blocking stop"}, fx.control.log)
		require.True(t, fx.r.IsClosed())

		require.NoError(t, fx.r.stopReplication())
		require.Len(t, fx.control.blockingStops, 1)
	})

	t.Run("OK latched before PollResponse closes the handle first", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toAwaitingResponse(t, testReplicationBuildNs)
		fx.control.request.latched = true

		require.NoError(t, fx.r.stopReplication())
		require.Equal(t, []string{"replicate close", "blocking stop"}, fx.control.log)
		require.Equal(t, []int64{testReplicationID}, fx.control.blockingStops)
	})

	t.Run("stopping closes the stop handle first", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.r.Close()
		fx.control.stop.offerAccepted = true
		require.Equal(t, 1, fx.doWork(t, okNs+1))

		require.NoError(t, fx.r.stopReplication())
		require.Equal(t, []string{"stop close", "replicate close", "blocking stop"}, fx.control.log)
	})

	t.Run("ended replication does not stop", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.signal(archivewire.RecordingSignal_REPLICATE_END, cnc.NullValue, archive.NullPosition)

		require.NoError(t, fx.r.stopReplication())
		require.Empty(t, fx.control.blockingStops)
	})

	t.Run("resolved stop does not stop again", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		fx.r.Close()
		fx.control.stop.offerAccepted = true
		fx.control.stop.responded = true
		require.Equal(t, 1, fx.doWork(t, okNs+1))
		require.Equal(t, 1, fx.doWork(t, okNs+2))
		require.True(t, fx.r.IsClosed())

		require.NoError(t, fx.r.stopReplication())
		require.Empty(t, fx.control.blockingStops)
	})

	t.Run("unknown id does not stop", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		require.NoError(t, fx.r.stopReplication())
		require.Empty(t, fx.control.blockingStops)
		require.True(t, fx.r.IsClosed())
	})

	t.Run("stop error wraps", func(t *testing.T) {
		t.Parallel()

		fx := newReplicationFixture(t)
		fx.toExtended(t, okNs)
		stopErr := errors.New("response timeout")
		fx.control.blockingErr = stopErr

		err := fx.r.stopReplication()
		require.ErrorIs(t, err, stopErr)
		require.ErrorContains(t, err, "cluster: log replication stop: ")
	})
}

func TestLogReplication_running_zeroAllocs(t *testing.T) {
	fx := newReplicationFixture(t)
	fx.toExtended(t, testReplicationBuildNs)
	meta := make([]byte, cnc.CounterMetadataLength)
	values := make([]byte, cnc.CounterValueLength)
	require.NoError(t, cnc.WriteRecordingPositionCounterRecord(
		meta,
		values,
		0,
		cnc.CounterRecordAllocated,
		cnc.ArchiveRecordingPositionCounterTypeID,
		testReplicationDestinationID,
		1,
		"aeron:ipc",
		cnc.NullValue,
		testReplicationStartPosition,
	))
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(t, err)
	fx.r.deps.Positions = cnc.NewRecordingPositionCache(reader, cnc.NullValue)
	valuesBuffer := atomicbuffer.Wrap(values)
	nowNs := testReplicationBuildNs
	position := testReplicationStartPosition

	// Each run reaches a progress check and raises the counter. The raise
	// moves the progress deadline.
	allocs := testing.AllocsPerRun(100, func() {
		nowNs += testReplicationCheckNs
		position++
		valuesBuffer.PutInt64(cnc.CounterOffset(0), position)
		_, _ = fx.r.DoWork(nowNs)
		fx.signalAt(archivewire.RecordingSignal_REPLICATE, testReplicationStartPosition)
	})
	require.Zero(t, allocs)
	require.Equal(t, logReplicationRunning, fx.r.phase)
	require.Equal(t, position, fx.r.Position())
	require.Equal(t, nowNs+testReplicationProgressNs, fx.r.progressDeadlineNs)
}

func TestConsensusModule_Close_stopsLogReplication(t *testing.T) {
	t.Parallel()

	fx := newReplicationFixture(t)
	fx.toExtended(t, testReplicationBuildNs)
	stopErr := errors.New("stop timed out")
	fx.control.blockingErr = stopErr
	module := &ConsensusModule{agent: fx.agent, logReplication: fx.r}

	err := module.Close()
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, []int64{testReplicationID}, fx.control.blockingStops)
	require.Nil(t, module.logReplication)

	require.NoError(t, module.Close())
	require.Len(t, fx.control.blockingStops, 1)
}

func TestConsensusModule_Close_stopsReplacedLogReplication(t *testing.T) {
	t.Parallel()

	const okNs = testReplicationBuildNs + 40

	// Phase: the first helper's stop offer times out, so its stop never
	// resolves and the archive may still run its replication.
	unresolved := newReplicationFixture(t)
	unresolved.toExtended(t, okNs)
	unresolved.r.Close()
	require.Zero(t, unresolved.doWork(t, okNs+1))
	_, err := unresolved.r.DoWork(okNs + 1 + testReplicationOfferNs)
	require.ErrorContains(t, err, "cluster: log replication stop: no offer within ")
	require.True(t, unresolved.r.IsClosed())
	require.True(t, unresolved.r.hasUnresolvedStop())

	// Phase: the second helper's stop resolves.
	resolved := newReplicationFixture(t)
	resolved.toExtended(t, okNs)
	resolved.r.Close()
	resolved.control.stop.offerAccepted = true
	resolved.control.stop.responded = true
	require.Equal(t, 1, resolved.doWork(t, okNs+1))
	require.Equal(t, 1, resolved.doWork(t, okNs+2))
	require.True(t, resolved.r.IsClosed())
	require.False(t, resolved.r.hasUnresolvedStop())

	// Phase: the election replaces both. Only the unresolved helper is kept.
	current := newReplicationFixture(t)
	current.toExtended(t, okNs)
	module := &ConsensusModule{agent: current.agent}
	module.trackLogReplication(unresolved.r)
	module.trackLogReplication(resolved.r)
	module.trackLogReplication(current.r)
	require.Equal(t, []*logReplication{unresolved.r}, module.unresolvedReplications)
	require.Same(t, current.r, module.logReplication)

	// Phase: Close stops the unresolved and the current helper once each.
	require.NoError(t, module.Close())
	require.Equal(t, []int64{testReplicationID}, unresolved.control.blockingStops)
	require.Empty(t, resolved.control.blockingStops)
	require.Equal(t, []int64{testReplicationID}, current.control.blockingStops)
	require.Nil(t, module.unresolvedReplications)
	require.Nil(t, module.logReplication)

	require.NoError(t, module.Close())
	require.Len(t, unresolved.control.blockingStops, 1)
	require.Len(t, current.control.blockingStops, 1)
}

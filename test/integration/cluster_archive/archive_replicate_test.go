//go:build integration && archive_live

package cluster_archive_test

import (
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/test/integration/harness"
)

const (
	archiveReplicateStreamID int32 = 4101

	// archiveReplicateChannel overrides the archive replication channel per
	// request. The Archive default names localhost.
	archiveReplicateChannel = "aeron:udp?endpoint=127.0.0.1:0"

	archiveReplicateSessionID int32 = 777

	archiveReplicateUnknownRecordingID int64 = 1_000_000
)

// archiveReplicatePeer is one media driver with its Archive, transport
// client, and archive client.
type archiveReplicatePeer struct {
	aeron          *aeron.Client
	hadClientError *atomic.Bool
	archive        *archive.Client
}

func startArchiveReplicatePeer(t *testing.T, controlChannel string) archiveReplicatePeer {
	t.Helper()

	md := harness.Start(t)
	harness.StartArchiveNode(t, harness.ArchiveNodeConfig{
		AeronDir:       md.Dir,
		ControlChannel: controlChannel,
	})
	aeronClient, hadClientError := harness.ConnectIPCClient(t, md)
	return archiveReplicatePeer{
		aeron:          aeronClient,
		hadClientError: hadClientError,
		archive:        connectArchiveClient(t, aeronClient),
	}
}

// archiveReplicateSignalLog records every destination session signal. It
// routes each signal to every AsyncReplicate the test built. A handle folds
// only the signals of its own replication.
type archiveReplicateSignalLog struct {
	events  []codecs.RecordingSignalEventFields
	handles []*archive.AsyncReplicate
}

func (l *archiveReplicateSignalLog) RecordingSignal(event codecs.RecordingSignalEventFields) {
	l.events = append(l.events, event)
	for _, handle := range l.handles {
		handle.RecordingSignal(event)
	}
}

// signalsFor returns the signal kinds for replicationID in arrival order.
func (l *archiveReplicateSignalLog) signalsFor(replicationID int64) []archivewire.RecordingSignal {
	var signals []archivewire.RecordingSignal
	for _, event := range l.events {
		if event.CorrelationID == replicationID {
			signals = append(signals, event.Signal)
		}
	}
	return signals
}

// eventFor returns the first event of kind signal for replicationID.
func (l *archiveReplicateSignalLog) eventFor(
	replicationID int64,
	signal archivewire.RecordingSignal,
) (codecs.RecordingSignalEventFields, bool) {
	for _, event := range l.events {
		if event.CorrelationID == replicationID && event.Signal == signal {
			return event, true
		}
	}
	return codecs.RecordingSignalEventFields{}, false
}

// archiveReplicateEnv holds the source and destination peers of one test and
// the destination signal log.
type archiveReplicateEnv struct {
	source      archiveReplicatePeer
	destination archiveReplicatePeer
	signals     *archiveReplicateSignalLog
}

// pollUntil pumps both transport clients and polls both archive clients
// until done reports true. name labels failures.
func (env *archiveReplicateEnv) pollUntil(t *testing.T, name string, done func() bool) {
	t.Helper()

	sourceError := "source transport error: " + name
	destinationError := "destination transport error: " + name
	deadline := time.Now().Add(harness.ConnectedPollTimeout)
	for time.Now().Before(deadline) {
		require.False(t, env.source.hadClientError.Load(), sourceError)
		require.False(t, env.destination.hadClientError.Load(), destinationError)
		env.source.aeron.DoWork()
		env.destination.aeron.DoWork()
		_ = env.source.archive.Poll()
		_ = env.destination.archive.Poll()
		if done() {
			return
		}
		time.Sleep(aeron.DefaultIdleSleepDuration)
	}
	require.FailNow(t, name+" not complete within "+harness.ConnectedPollTimeout.String())
}

// runBlocking runs call on its own goroutine. The test goroutine pumps the
// transport client of other meanwhile. call blocks on the opposite peer.
func (env *archiveReplicateEnv) runBlocking(
	t *testing.T,
	other archiveReplicatePeer,
	name string,
	call func() error,
) {
	t.Helper()

	harness.PollClientDuring(
		t,
		other.aeron,
		other.hadClientError,
		harness.ConnectedPollTimeout,
		"transport error during "+name,
		func() string { return name + " not complete within " + harness.ConnectedPollTimeout.String() },
		call,
	)
}

// offerUntil offers messages on pub, then waits until the source recording
// position reaches the last offer position. Returns that position. Each
// retry pumps both transport clients.
func (env *archiveReplicateEnv) offerUntil(
	t *testing.T,
	pub *aeron.Publication,
	recordingID int64,
	messages int,
) int64 {
	t.Helper()

	var position int64
	for range messages {
		env.pollUntil(t, "source offer", func() bool {
			position = pub.Offer([]byte("replicated"))
			if position > 0 {
				return true
			}
			require.True(
				t,
				aeron.IsOfferRetriable(position),
				"source offer: "+aeron.PositionCodeName(position),
			)
			return false
		})
	}
	counters, err := env.source.aeron.CountersReader()
	require.NoError(t, err)
	env.pollUntil(t, "source recording position "+strconv.FormatInt(position, 10), func() bool {
		slot, ok := counters.FindRecordingPositionCounter(recordingID, cnc.NullValue)
		return ok && slot.Get() >= position
	})
	return position
}

// offerThenPoll calls tryOffer until it accepts, then poll until it reports
// done. An error from either fails the test.
func (env *archiveReplicateEnv) offerThenPoll(
	t *testing.T,
	name string,
	tryOffer func() (bool, error),
	poll func() (bool, error),
) {
	t.Helper()

	offered := false
	env.pollUntil(t, name, func() bool {
		if !offered {
			var err error
			offered, err = tryOffer()
			require.NoError(t, err)
			return false
		}
		done, err := poll()
		require.NoError(t, err)
		return done
	})
}

// listRecording lists recordingID on the destination archive.
func (env *archiveReplicateEnv) listRecording(
	t *testing.T,
	recordingID int64,
) codecs.RecordingDescriptorFields {
	t.Helper()

	handle := env.destination.archive.NewAsyncListRecording(recordingID)
	defer handle.Close()
	var descriptor codecs.RecordingDescriptorFields
	env.offerThenPoll(t, "destination list recording", handle.TryOffer, func() (bool, error) {
		done, got, err := handle.PollDescriptor()
		descriptor = got
		return done, err
	})
	return descriptor
}

// asyncReplicate routes destination signals to handle, drives it until
// PollResponse returns true, and returns the replication id.
func (env *archiveReplicateEnv) asyncReplicate(t *testing.T, handle *archive.AsyncReplicate) int64 {
	t.Helper()

	env.signals.handles = append(env.signals.handles, handle)
	env.offerThenPoll(t, "async replicate response", handle.TryOffer, handle.PollResponse)
	replicationID := handle.ReplicationID()
	require.NotEqual(t, cnc.NullValue, replicationID)
	return replicationID
}

// awaitReplicationEnd polls until handle reports STOP and REPLICATE_END, then
// closes it. name labels failures.
func (env *archiveReplicateEnv) awaitReplicationEnd(
	t *testing.T,
	name string,
	handle *archive.AsyncReplicate,
) {
	t.Helper()

	env.pollUntil(t, name, func() bool {
		return handle.HasStopped() && handle.HasReplicationEnded()
	})
	handle.Close()
}

func newArchiveReplicateParams() archive.ReplicationParams {
	params := archive.NewReplicationParams()
	params.ReplicationChannel = []byte(archiveReplicateChannel)
	return params
}

// TestArchiveClient_replicate runs, extends and stops replications between
// two Java Archives.
func TestArchiveClient_replicate(t *testing.T) {
	t.Parallel()

	signals := &archiveReplicateSignalLog{}
	env := &archiveReplicateEnv{
		source:      startArchiveReplicatePeer(t, harness.ArchiveReplicateSourceControl),
		destination: startArchiveReplicatePeer(t, harness.ArchiveReplicateDestinationControl),
		signals:     signals,
	}
	env.destination.archive.SetRecordingSignalHandler(signals)
	sourceControlChannel := []byte(harness.ArchiveReplicateSourceControl)
	sourceControlStreamID := int32(archive.DefaultControlRequestStreamID)

	// Phase: the source records with autoStop true up to P1.
	pub, err := env.source.aeron.AddPublication(
		archiveReplicateStreamID,
		archiveClientRecordingChannel,
	)
	require.NoError(t, err)
	harness.CleanupClose(t, "close source publication", pub.Close)
	var sourceRecordingID int64
	env.runBlocking(t, env.destination, "source start recording", func() error {
		var startErr error
		sourceRecordingID, startErr = env.source.archive.StartRecordingAwaitSignal(
			t.Context(),
			archiveReplicateStreamID,
			archivewire.SourceLocation_LOCAL,
			true,
			[]byte(archiveClientRecordingChannel),
			archive.NewStartSignalTracker(env.source.archive.ControlSessionID()),
		)
		return startErr
	})
	p1 := env.offerUntil(t, pub, sourceRecordingID, 4)

	// Phase: blocking Replicate into a new recording stops at P1.
	params := newArchiveReplicateParams()
	params.StopPosition = p1
	var firstID int64
	env.runBlocking(t, env.source, "replicate to P1", func() error {
		var replicateErr error
		firstID, replicateErr = env.destination.archive.Replicate(
			t.Context(),
			sourceRecordingID,
			sourceControlStreamID,
			sourceControlChannel,
			params,
		)
		return replicateErr
	})
	// STOP can arrive on a later poll than REPLICATE_END.
	var stop codecs.RecordingSignalEventFields
	env.pollUntil(t, "first replication end", func() bool {
		_, ended := signals.eventFor(firstID, archivewire.RecordingSignal_REPLICATE_END)
		var stopped bool
		stop, stopped = signals.eventFor(firstID, archivewire.RecordingSignal_STOP)
		return ended && stopped
	})
	firstSignals := signals.signalsFor(firstID)
	replicateAt := slices.Index(firstSignals, archivewire.RecordingSignal_REPLICATE)
	extendAt := slices.Index(firstSignals, archivewire.RecordingSignal_EXTEND)
	require.GreaterOrEqual(t, replicateAt, 0, "signals: %v", firstSignals)
	require.Greater(t, extendAt, replicateAt, "signals: %v", firstSignals)
	stopAt := slices.Index(firstSignals, archivewire.RecordingSignal_STOP)
	require.Greater(t, stopAt, extendAt, "signals: %v", firstSignals)
	require.NotContains(t, firstSignals, archivewire.RecordingSignal_SYNC)
	require.Equal(t, p1, stop.Position)
	destinationRecordingID := stop.RecordingID
	descriptor := env.listRecording(t, destinationRecordingID)
	require.Equal(t, int64(0), descriptor.StartPosition)
	require.Equal(t, p1, descriptor.StopPosition)

	// Phase: AsyncReplicate extends the same recording to P2.
	p2 := env.offerUntil(t, pub, sourceRecordingID, 4)
	params = newArchiveReplicateParams()
	params.DestinationRecordingID = destinationRecordingID
	params.StopPosition = p2
	params.ReplicationSessionID = archiveReplicateSessionID
	extend := env.destination.archive.NewAsyncReplicate(
		sourceRecordingID,
		sourceControlStreamID,
		sourceControlChannel,
		params,
	)
	extendID := env.asyncReplicate(t, extend)
	env.awaitReplicationEnd(t, "extend replication end", extend)
	extendSignals := signals.signalsFor(extendID)
	require.True(t, extend.HasExtended(), "signals: %v", extendSignals)
	require.NotContains(t, extendSignals, archivewire.RecordingSignal_REPLICATE)
	require.Equal(t, destinationRecordingID, extend.RecordingID())
	require.Equal(t, p2, extend.Position())
	extendStop, ok := signals.eventFor(extendID, archivewire.RecordingSignal_STOP)
	require.True(t, ok, "signals: %v", extendSignals)
	require.Equal(t, p2, extendStop.Position)
	descriptor = env.listRecording(t, destinationRecordingID)
	require.Equal(t, int64(0), descriptor.StartPosition)
	require.Equal(t, p2, descriptor.StopPosition)

	// Phase: AsyncStopReplication stops a live replication.
	live := env.destination.archive.NewAsyncReplicate(
		sourceRecordingID,
		sourceControlStreamID,
		sourceControlChannel,
		newArchiveReplicateParams(),
	)
	liveID := env.asyncReplicate(t, live)
	env.pollUntil(t, "live replication extend", live.HasExtended)
	stopHandle := env.destination.archive.NewAsyncStopReplication(liveID)
	env.offerThenPoll(t, "async stop replication", stopHandle.TryOffer, stopHandle.PollResponse)
	require.True(t, stopHandle.IsStopped())
	stopHandle.Close()
	env.awaitReplicationEnd(t, "live replication end", live)
	descriptor = env.listRecording(t, live.RecordingID())
	require.Equal(t, live.Position(), descriptor.StopPosition)

	// Phase: a second stop finds no replication.
	var stopped bool
	env.runBlocking(t, env.source, "stop ended replication", func() error {
		var stopErr error
		stopped, stopErr = env.destination.archive.StopReplication(t.Context(), liveID)
		return stopErr
	})
	require.False(t, stopped)

	// Phase: an unknown destination recording fails the request.
	params = newArchiveReplicateParams()
	params.DestinationRecordingID = archiveReplicateUnknownRecordingID
	var unknownErr error
	env.runBlocking(t, env.source, "replicate to unknown recording", func() error {
		_, unknownErr = env.destination.archive.Replicate(
			t.Context(),
			sourceRecordingID,
			sourceControlStreamID,
			sourceControlChannel,
			params,
		)
		return nil
	})
	code, ok := archive.ExceptionCode(unknownErr)
	require.True(t, ok, "replicate error: %v", unknownErr)
	require.Equal(t, int32(archive.ErrorUnknownRecording), code)

	// Phase: a stopped source replicates in full and ends with SYNC.
	sourceStopped := false
	var p3 int64
	env.source.archive.SetRecordingSignalHandler(archive.RecordingSignalHandlerFunc(
		func(event codecs.RecordingSignalEventFields) {
			if event.Signal == archivewire.RecordingSignal_STOP && event.RecordingID == sourceRecordingID {
				sourceStopped = true
				p3 = event.Position
			}
		},
	))
	require.NoError(t, pub.Close())
	env.pollUntil(t, "source recording STOP", func() bool { return sourceStopped })
	require.GreaterOrEqual(t, p3, p2)
	stoppedSource := env.destination.archive.NewAsyncReplicate(
		sourceRecordingID,
		sourceControlStreamID,
		sourceControlChannel,
		newArchiveReplicateParams(),
	)
	stoppedSourceID := env.asyncReplicate(t, stoppedSource)
	env.awaitReplicationEnd(t, "stopped source replication end", stoppedSource)
	stoppedSignals := signals.signalsFor(stoppedSourceID)
	require.Contains(t, stoppedSignals, archivewire.RecordingSignal_REPLICATE)
	require.Contains(t, stoppedSignals, archivewire.RecordingSignal_REPLICATE_END)
	require.True(t, stoppedSource.HasExtended(), "signals: %v", stoppedSignals)
	require.True(t, stoppedSource.HasSynced(), "signals: %v", stoppedSignals)
	stoppedStop, ok := signals.eventFor(stoppedSourceID, archivewire.RecordingSignal_STOP)
	require.True(t, ok, "signals: %v", stoppedSignals)
	require.Equal(t, p3, stoppedStop.Position)
	require.Equal(t, p3, stoppedSource.Position())
	require.NotEqual(t, destinationRecordingID, stoppedSource.RecordingID())
	require.NotEqual(t, live.RecordingID(), stoppedSource.RecordingID())
	descriptor = env.listRecording(t, stoppedSource.RecordingID())
	require.Equal(t, int64(0), descriptor.StartPosition)
	require.Equal(t, p3, descriptor.StopPosition)
}

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
)

// recordingArchiveStub records which archive stop API logRecording calls.
type recordingArchiveStub struct {
	controlSessionID int64

	startErr error
	startID  int64

	stopErr      error
	stopAwaitErr error
	listErr      error
	listStopPos  int64

	stopCalls      int
	stopAwaitCalls int
	listCalls      int
	startCalls     int

	lastStopChannel      []byte
	lastStopStreamID     int32
	lastStopAwaitChannel []byte
}

func (s *recordingArchiveStub) ControlSessionID() int64 {
	return s.controlSessionID
}

func (s *recordingArchiveStub) StartRecordingAwaitSignal(
	_ context.Context,
	_ int32,
	_ archivewire.SourceLocation,
	_ []byte,
	_ *archive.StartSignalTracker,
) (int64, error) {
	s.startCalls++
	if s.startErr != nil {
		return 0, s.startErr
	}
	return s.startID, nil
}

func (s *recordingArchiveStub) StopRecording(
	_ context.Context,
	streamID int32,
	channel []byte,
) error {
	s.stopCalls++
	s.lastStopStreamID = streamID
	s.lastStopChannel = append([]byte(nil), channel...)
	return s.stopErr
}

func (s *recordingArchiveStub) StopRecordingAwaitSignal(
	_ context.Context,
	_ int32,
	channel []byte,
	_ *archive.StopSignalTracker,
) error {
	s.stopAwaitCalls++
	s.lastStopAwaitChannel = append([]byte(nil), channel...)
	return s.stopAwaitErr
}

func (s *recordingArchiveStub) ListRecording(
	_ context.Context,
	_ int64,
) (codecs.RecordingDescriptorFields, error) {
	s.listCalls++
	if s.listErr != nil {
		return codecs.RecordingDescriptorFields{}, s.listErr
	}
	return codecs.RecordingDescriptorFields{StopPosition: s.listStopPos}, nil
}

func TestSessionScopedLogChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logChannel   string
		logSessionID int32
		want         string
	}{
		{
			name:         "bare template with endpoint",
			logChannel:   "aeron:udp?endpoint=239.255.12.1:9004|term-length=64k",
			logSessionID: 12345,
			want:         "aeron:udp?endpoint=239.255.12.1:9004|term-length=64k|session-id=12345",
		},
		{
			name:         "template ending in query delimiter",
			logChannel:   "aeron:udp?",
			logSessionID: 7,
			want:         "aeron:udp?session-id=7",
		},
		{
			name:         "min int32 session id",
			logChannel:   "aeron:udp?endpoint=239.255.12.1:9004",
			logSessionID: -2147483648,
			want:         "aeron:udp?endpoint=239.255.12.1:9004|session-id=-2147483648",
		},
		{
			name:         "negative session id",
			logChannel:   "aeron:udp?endpoint=239.255.12.1:9004",
			logSessionID: -7,
			want:         "aeron:udp?endpoint=239.255.12.1:9004|session-id=-7",
		},
		{
			name:         "sentinel session id",
			logChannel:   "aeron:udp?endpoint=239.255.12.1:9004",
			logSessionID: -1,
			want:         "aeron:udp?endpoint=239.255.12.1:9004|session-id=-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sessionScopedLogChannel(tt.logChannel, tt.logSessionID)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLogRecording_StopLogRecording_noPriorStart(t *testing.T) {
	t.Parallel()

	r := &logRecording{}
	stopPosition, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, int64(0), stopPosition)
}

func TestLogRecording_StopLogRecording_secondStopIdempotent(t *testing.T) {
	t.Parallel()

	// A completed stop clears startChannel and retains stopPosition. A second
	// call must return the retained position and offer nothing (nil client is
	// safe on that path).
	r := &logRecording{
		recordingID:  42,
		stopPosition: 1001,
	}

	stopPosition, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, int64(1001), stopPosition)

	stopPosition, err = r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, int64(1001), stopPosition)
}

func TestLogRecording_dirtyStopUsesOKBoundPath(t *testing.T) {
	t.Parallel()

	// Unknown catalog id must call StopRecording only. Must not use
	// StopRecordingAwaitSignal (match-any STOP for negative id).
	const (
		streamID     = int32(100)
		stopPosition = int64(7)
	)
	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")
	stub := &recordingArchiveStub{}
	r := &logRecording{
		client:       stub,
		logStreamID:  streamID,
		startChannel: channel,
		recordingID:  recordingIDUnknown,
		stopPosition: stopPosition,
	}

	got, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, stopPosition, got)
	require.Equal(t, 1, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
	require.Equal(t, 0, stub.listCalls)
	require.Equal(t, streamID, stub.lastStopStreamID)
	require.Equal(t, channel, stub.lastStopChannel)
	require.False(t, r.needsStop())
	require.Empty(t, r.startChannel)
}

func TestLogRecording_dirtyStopUnknownSubscriptionIsClean(t *testing.T) {
	t.Parallel()

	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")
	stub := &recordingArchiveStub{
		stopErr: &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription},
	}
	r := &logRecording{
		client:       stub,
		logStreamID:  100,
		startChannel: channel,
		recordingID:  recordingIDUnknown,
		stopPosition: 11,
	}

	got, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, int64(11), got)
	require.Equal(t, 1, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
	require.False(t, r.needsStop())
}

func TestLogRecording_dirtyStopRealErrorRetainsChannel(t *testing.T) {
	t.Parallel()

	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")
	stopErr := &archive.Exception{ErrorCode: archive.ErrorGeneric}
	stub := &recordingArchiveStub{stopErr: stopErr}
	r := &logRecording{
		client:       stub,
		logStreamID:  100,
		startChannel: channel,
		recordingID:  recordingIDUnknown,
	}

	_, err := r.StopLogRecording()
	require.ErrorIs(t, err, stopErr)
	require.Equal(t, 1, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
	require.True(t, r.needsStop())
	require.Equal(t, channel, r.startChannel)
}

func TestLogRecording_knownStopUsesAwaitSignal(t *testing.T) {
	t.Parallel()

	const (
		recordingID = int64(42)
		streamID    = int32(100)
	)
	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")
	stub := &recordingArchiveStub{}
	r := &logRecording{
		client:       stub,
		logStreamID:  streamID,
		startChannel: channel,
		recordingID:  recordingID,
	}

	// Stub does not latch STOP; position is zero. Method choice is the assert.
	got, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 1, stub.stopAwaitCalls)
	require.Equal(t, 0, stub.listCalls)
	require.Equal(t, channel, stub.lastStopAwaitChannel)
	require.False(t, r.needsStop())
}

func TestLogRecording_knownStopUnknownSubscriptionListsCatalog(t *testing.T) {
	t.Parallel()

	const (
		recordingID = int64(42)
		listStopPos = int64(900)
	)
	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")
	stub := &recordingArchiveStub{
		stopAwaitErr: &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription},
		listStopPos:  listStopPos,
	}
	r := &logRecording{
		client:       stub,
		logStreamID:  100,
		startChannel: channel,
		recordingID:  recordingID,
	}

	got, err := r.StopLogRecording()
	require.NoError(t, err)
	require.Equal(t, listStopPos, got)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 1, stub.stopAwaitCalls)
	require.Equal(t, 1, stub.listCalls)
	require.False(t, r.needsStop())
}

func TestLogRecording_startFailureDrainsViaStopLogRecording(t *testing.T) {
	t.Parallel()

	const (
		logChannel   = "aeron:udp?endpoint=239.255.12.1:9004"
		logSessionID = int32(9)
		streamID     = int32(100)
	)
	wantChannel := []byte(sessionScopedLogChannel(logChannel, logSessionID))

	t.Run("clean rollback clears channel", func(t *testing.T) {
		t.Parallel()

		startErr := errors.New("start failed")
		stub := &recordingArchiveStub{startErr: startErr}
		r := &logRecording{
			client:      stub,
			logChannel:  logChannel,
			logStreamID: streamID,
		}

		_, err := r.StartLogRecording(archivewire.SourceLocation_LOCAL, logSessionID)
		require.Error(t, err)
		require.ErrorIs(t, err, startErr)
		require.Equal(t, 1, stub.startCalls)
		require.Equal(t, 1, stub.stopCalls)
		require.Equal(t, 0, stub.stopAwaitCalls)
		require.Equal(t, wantChannel, stub.lastStopChannel)
		require.Equal(t, streamID, stub.lastStopStreamID)
		require.False(t, r.needsStop())
		require.Empty(t, r.startChannel)
		require.Equal(t, recordingIDUnknown, r.recordingID)
	})

	t.Run("dirty rollback keeps channel", func(t *testing.T) {
		t.Parallel()

		startErr := errors.New("start failed")
		stopErr := &archive.Exception{ErrorCode: archive.ErrorGeneric}
		stub := &recordingArchiveStub{
			startErr: startErr,
			stopErr:  stopErr,
		}
		r := &logRecording{
			client:      stub,
			logChannel:  logChannel,
			logStreamID: streamID,
		}

		_, err := r.StartLogRecording(archivewire.SourceLocation_LOCAL, logSessionID)
		require.Error(t, err)
		require.ErrorIs(t, err, startErr)
		require.ErrorIs(t, err, stopErr)
		require.Equal(t, 1, stub.startCalls)
		require.Equal(t, 1, stub.stopCalls)
		require.Equal(t, 0, stub.stopAwaitCalls)
		require.True(t, r.needsStop())
		require.Equal(t, wantChannel, r.startChannel)
		require.Equal(t, recordingIDUnknown, r.recordingID)
		require.False(t, r.IsActive())
	})
}

func TestIsUnknownSubscription(t *testing.T) {
	t.Parallel()

	require.True(t, isUnknownSubscription(&archive.Exception{
		ErrorCode: archive.ErrorUnknownSubscription,
	}))
	require.False(t, isUnknownSubscription(&archive.Exception{
		ErrorCode: archive.ErrorGeneric,
	}))
	require.False(t, isUnknownSubscription(errors.New("other")))
	require.False(t, isUnknownSubscription(nil))
}

func TestLogRecording_needsStop(t *testing.T) {
	t.Parallel()

	require.False(t, (&logRecording{}).needsStop())
	require.False(t, (&logRecording{
		stopPosition: 99,
	}).needsStop())
	require.True(t, (&logRecording{
		startChannel: []byte("ch"),
	}).needsStop())
}

func TestLogRecording_IsActive(t *testing.T) {
	t.Parallel()

	// Asserts IsActive is true only when a start channel is kept and the catalog
	// id is known from START.
	require.False(t, (&logRecording{}).IsActive())
	require.False(t, (&logRecording{
		startChannel: []byte("ch"),
		recordingID:  recordingIDUnknown,
	}).IsActive())
	require.False(t, (&logRecording{
		recordingID: 42,
	}).IsActive())
	require.True(t, (&logRecording{
		startChannel: []byte("ch"),
		recordingID:  42,
	}).IsActive())
	require.True(t, (&logRecording{
		startChannel: []byte("ch"),
		recordingID:  0,
	}).IsActive())
}

func TestNewLogRecording_detachesCancel(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	r := newLogRecording(parent, nil, "aeron:udp", 1)
	cancel()

	require.NoError(t, r.ctx.Err())
	require.NotNil(t, r.ctx)
}

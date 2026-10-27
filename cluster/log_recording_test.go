package cluster

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/archive"
	"github.com/warrenguy/heron/archive/codecs"
	"github.com/warrenguy/heron/archive/codecs/archivewire"
	"github.com/warrenguy/heron/cluster/consensus"
	"github.com/warrenguy/heron/cluster/recordinglog"
)

// recordingArchiveStub records which archive stop API logRecording calls.
type recordingArchiveStub struct {
	controlSessionID int64

	startErr  error
	startID   int64
	extendErr error

	stopErr      error
	stopAwaitErr error
	listErr      error
	listStopPos  int64

	truncateErr   error
	truncateCount int64

	stopCalls      int
	stopAwaitCalls int
	listCalls      int
	startCalls     int
	extendCalls    int
	truncateCalls  int

	lastStopChannel          []byte
	lastStopStreamID         int32
	lastStopAwaitChannel     []byte
	lastListRecordingID      int64
	lastTruncateRecordingID  int64
	lastTruncatePosition     int64
	lastStartAutoStop        bool
	lastExtendID             int64
	lastExtendChannel        []byte
	lastExtendAutoStop       bool
	lastExtendStreamID       int32
	lastExtendSourceLocation archivewire.SourceLocation

	// lastBranch records "start" or "extend" for branch assertions.
	lastBranch string
}

func (s *recordingArchiveStub) ControlSessionID() int64 {
	return s.controlSessionID
}

func (s *recordingArchiveStub) StartRecordingAwaitSignal(
	_ context.Context,
	_ int32,
	_ archivewire.SourceLocation,
	autoStop bool,
	_ []byte,
	_ *archive.StartSignalTracker,
) (int64, error) {
	s.startCalls++
	s.lastBranch = "start"
	s.lastStartAutoStop = autoStop
	if s.startErr != nil {
		return 0, s.startErr
	}
	return s.startID, nil
}

func (s *recordingArchiveStub) ExtendRecordingAwaitSignal(
	_ context.Context,
	recordingID int64,
	streamID int32,
	sourceLocation archivewire.SourceLocation,
	autoStop bool,
	channel []byte,
	_ *archive.ExtendSignalTracker,
) (int64, error) {
	s.extendCalls++
	s.lastBranch = "extend"
	s.lastExtendID = recordingID
	s.lastExtendStreamID = streamID
	s.lastExtendSourceLocation = sourceLocation
	s.lastExtendAutoStop = autoStop
	s.lastExtendChannel = append([]byte(nil), channel...)
	if s.extendErr != nil {
		return 0, s.extendErr
	}
	return recordingID, nil
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
	recordingID int64,
) (codecs.RecordingDescriptorFields, error) {
	s.listCalls++
	s.lastListRecordingID = recordingID
	if s.listErr != nil {
		return codecs.RecordingDescriptorFields{}, s.listErr
	}
	return codecs.RecordingDescriptorFields{StopPosition: s.listStopPos}, nil
}

func (s *recordingArchiveStub) TruncateRecording(
	_ context.Context,
	recordingID int64,
	position int64,
) (int64, error) {
	s.truncateCalls++
	s.lastTruncateRecordingID = recordingID
	s.lastTruncatePosition = position
	if s.truncateErr != nil {
		return 0, s.truncateErr
	}
	return s.truncateCount, nil
}

// fixedTermLookup is a TermRecordingLookup with a fixed shared id.
type fixedTermLookup int64

func (f fixedTermLookup) TermRecordingID() int64 {
	return int64(f)
}

// joinImageSource returns a fixed image for every session id.
type joinImageSource struct {
	image *aeron.Image
}

func (s *joinImageSource) Ready(logSessionID int32) (consensus.FollowerLogJoinParams, bool) {
	return consensus.FollowerLogJoinParams{LogSessionID: logSessionID}, true
}

func (s *joinImageSource) Image(int32) *aeron.Image {
	return s.image
}

func openTermLog(t *testing.T, recordingID, leadershipTermID int64) *recordinglog.Log {
	t.Helper()
	dir := t.TempDir()
	log, err := recordinglog.Open(dir, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	require.NoError(t, log.AppendTerm(recordingID, leadershipTermID, 0, 1000))
	return log
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
	require.NoError(t, r.StopLogRecording())
	require.Equal(t, int64(0), r.stopPosition)
}

func TestLogRecording_StopLogRecording_secondStopIdempotent(t *testing.T) {
	t.Parallel()

	// A completed stop clears startChannel and retains stopPosition. A second
	// call must keep the latched position and offer nothing (nil client is
	// safe on that path).
	r := &logRecording{
		recordingID:  42,
		stopPosition: 1001,
	}

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, int64(1001), r.stopPosition)

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, int64(1001), r.stopPosition)
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

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, stopPosition, r.stopPosition)
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

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, int64(11), r.stopPosition)
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

	require.ErrorIs(t, r.StopLogRecording(), stopErr)
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
	require.NoError(t, r.StopLogRecording())
	require.Equal(t, int64(0), r.stopPosition)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 1, stub.stopAwaitCalls)
	require.Equal(t, 0, stub.listCalls)
	require.Equal(t, channel, stub.lastStopAwaitChannel)
	require.False(t, r.needsStop())

	// A blocking stop sets no STOP mark.
	_, stopped := r.ConsumeStopSignal()
	require.False(t, stopped)
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

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, listStopPos, r.stopPosition)
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

		_, err := r.StartLogRecording(archivewire.SourceLocation_LOCAL, logSessionID, "")
		require.Error(t, err)
		require.ErrorIs(t, err, startErr)
		require.Equal(t, 1, stub.startCalls)
		require.True(t, stub.lastStartAutoStop)
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

		_, err := r.StartLogRecording(archivewire.SourceLocation_LOCAL, logSessionID, "")
		require.Error(t, err)
		require.ErrorIs(t, err, startErr)
		require.ErrorIs(t, err, stopErr)
		require.Equal(t, 1, stub.startCalls)
		require.True(t, stub.lastStartAutoStop)
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
	// id is known from START or EXTEND.
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

// TestLogRecording_handleRecordingSignal_stopAdoptsCleanStop asserts an
// unsolicited STOP for the known catalog id is a clean stop. The STOP sets a
// one-shot mark. A later StopLogRecording offers nothing.
func TestLogRecording_handleRecordingSignal_stopAdoptsCleanStop(t *testing.T) {
	t.Parallel()

	const (
		recordingID = int64(42)
		stopPos     = int64(1024)
	)
	channel := []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")

	active := func() *logRecording {
		return &logRecording{
			client:       &recordingArchiveStub{},
			startChannel: append([]byte(nil), channel...),
			recordingID:  recordingID,
		}
	}

	t.Run("matching stop", func(t *testing.T) {
		t.Parallel()

		r := active()
		stub := r.client.(*recordingArchiveStub)
		r.handleRecordingSignal(codecs.RecordingSignalEventFields{
			RecordingID: recordingID,
			Position:    stopPos,
			Signal:      archivewire.RecordingSignal_STOP,
		})

		require.False(t, r.IsActive())
		require.False(t, r.needsStop())
		require.Empty(t, r.startChannel)
		require.Equal(t, recordingID, r.latchedRecordingID)
		require.Equal(t, stopPos, r.stopPosition)

		markPosition, stopped := r.ConsumeStopSignal()
		require.True(t, stopped)
		require.Equal(t, stopPos, markPosition)
		markPosition, stopped = r.ConsumeStopSignal()
		require.False(t, stopped)
		require.Equal(t, recordinglog.NullPosition, markPosition)

		got, err := r.StopPositionFor(recordingID)
		require.NoError(t, err)
		require.Equal(t, stopPos, got)
		require.Equal(t, 0, stub.listCalls)

		require.NoError(t, r.StopLogRecording())
		require.Equal(t, 0, stub.stopCalls)
		require.Equal(t, 0, stub.stopAwaitCalls)
	})

	tests := []struct {
		name        string
		r           *logRecording
		event       codecs.RecordingSignalEventFields
		wantActive  bool
		wantChannel []byte
		wantStopPos int64
	}{
		{
			name: "foreign recording id",
			r:    active(),
			event: codecs.RecordingSignalEventFields{
				RecordingID: recordingID + 1,
				Position:    stopPos,
				Signal:      archivewire.RecordingSignal_STOP,
			},
			wantActive:  true,
			wantChannel: channel,
		},
		{
			name: "start signal",
			r:    active(),
			event: codecs.RecordingSignalEventFields{
				RecordingID: recordingID,
				Position:    stopPos,
				Signal:      archivewire.RecordingSignal_START,
			},
			wantActive:  true,
			wantChannel: channel,
		},
		{
			name: "inactive control",
			r: &logRecording{
				recordingID:        recordingID,
				latchedRecordingID: recordingID,
				stopPosition:       99,
			},
			event: codecs.RecordingSignalEventFields{
				RecordingID: recordingID,
				Position:    stopPos,
				Signal:      archivewire.RecordingSignal_STOP,
			},
			wantStopPos: 99,
		},
		{
			name: "dirty start",
			r: &logRecording{
				startChannel: append([]byte(nil), channel...),
				recordingID:  recordingIDUnknown,
			},
			event: codecs.RecordingSignalEventFields{
				RecordingID: 0,
				Position:    stopPos,
				Signal:      archivewire.RecordingSignal_STOP,
			},
			wantChannel: channel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.r.handleRecordingSignal(tt.event)
			require.Equal(t, tt.wantActive, tt.r.IsActive())
			require.Equal(t, tt.wantChannel, tt.r.startChannel)
			require.Equal(t, tt.wantStopPos, tt.r.stopPosition)
			_, stopped := tt.r.ConsumeStopSignal()
			require.False(t, stopped)
		})
	}
}

func TestNewLogRecording_detachesCancel(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	r := newLogRecording(parent, nil, "aeron:udp", 1, nil)
	cancel()

	require.NoError(t, r.ctx.Err())
	require.NotNil(t, r.ctx)
}

func TestNewLogRecording_seedsUnknownRecordingID(t *testing.T) {
	t.Parallel()

	// Zero is a valid catalog id. Constructor must seed unknown. StopPositionFor(0)
	// must not treat a never-started control as retained.
	r := newLogRecording(context.Background(), nil, "aeron:udp", 1, nil)
	require.Equal(t, recordingIDUnknown, r.recordingID)
	require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
}

func TestLogRecording_forgetStopPosition(t *testing.T) {
	t.Parallel()

	const (
		termRecordingID int64 = 42
		storedStop      int64 = 1001
		catalogStop     int64 = 7000
	)
	tests := []struct {
		name             string
		forgetID         int64
		wantStop         int64
		wantListCalls    int
		wantIdentityStop int64
	}{
		{
			name:             "matching id reads the catalog and stops by identity",
			forgetID:         termRecordingID,
			wantStop:         catalogStop,
			wantListCalls:    1,
			wantIdentityStop: termRecordingID,
		},
		{
			name:             "foreign id keeps the stored stop",
			forgetID:         termRecordingID + 1,
			wantStop:         storedStop,
			wantIdentityStop: recordinglog.NullRecordingID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := &recordingArchiveStub{listStopPos: catalogStop}
			r := &logRecording{
				client:             stub,
				terms:              fixedTermLookup(termRecordingID),
				recordingID:        recordingIDUnknown,
				latchedRecordingID: recordingIDUnknown,
			}
			r.storeStopPosition(termRecordingID, storedStop)
			require.Equal(t, recordinglog.NullRecordingID, r.identityStopTarget())

			r.forgetStopPosition(tt.forgetID)

			got, err := r.StopPositionFor(termRecordingID)
			require.NoError(t, err)
			require.Equal(t, tt.wantStop, got)
			require.Equal(t, tt.wantListCalls, stub.listCalls)
			require.Equal(t, tt.wantIdentityStop, r.identityStopTarget())
		})
	}
}

func TestLogRecording_StopPositionFor(t *testing.T) {
	t.Parallel()

	t.Run("retained id answers with no catalog read", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{}
		r := &logRecording{
			client:             stub,
			recordingID:        42,
			latchedRecordingID: 42,
			stopPosition:       1001,
		}

		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(1001), got)
		require.Equal(t, 0, stub.listCalls)
	})

	t.Run("retained id with pending stop reads catalog", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{listStopPos: 7000}
		r := &logRecording{
			client:             stub,
			recordingID:        42,
			latchedRecordingID: 42,
			stopPosition:       1001,
			startChannel:       []byte("aeron:udp?endpoint=239.255.12.1:9004"),
		}

		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(7000), got)
		require.Equal(t, 1, stub.listCalls)
		require.Equal(t, int64(42), stub.lastListRecordingID)

		// The catalog read does not store. The pending stop still stores later.
		require.Equal(t, int64(1001), r.stopPosition)
		require.True(t, r.needsStop())
	})

	t.Run("foreign id reads catalog and does not latch", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{listStopPos: 7000}
		startChannel := []byte("aeron:udp?endpoint=239.255.12.1:9004")
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
			stopPosition:       11,
			startChannel:       startChannel,
		}

		got, err := r.StopPositionFor(99)
		require.NoError(t, err)
		require.Equal(t, int64(7000), got)
		require.Equal(t, 1, stub.listCalls)
		require.Equal(t, int64(99), stub.lastListRecordingID)
		require.Equal(t, int64(11), r.stopPosition)
		require.Equal(t, startChannel, r.startChannel)
	})

	t.Run("catalog no stop position refuses", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{listStopPos: recordinglog.NullPosition}
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		got, err := r.StopPositionFor(42)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reports no stop position")
		require.Equal(t, recordinglog.NullPosition, got)
		require.Equal(t, int64(42), stub.lastListRecordingID)
	})

	t.Run("catalog error propagates", func(t *testing.T) {
		t.Parallel()

		listErr := errors.New("catalog refused")
		stub := &recordingArchiveStub{listErr: listErr}
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		got, err := r.StopPositionFor(42)
		require.ErrorIs(t, err, listErr)
		require.Equal(t, recordinglog.NullPosition, got)
		require.Equal(t, int64(42), stub.lastListRecordingID)
	})

	t.Run("latched id survives failed re-start dirty drain", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{}
		r := &logRecording{
			client:             stub,
			recordingID:        42,
			latchedRecordingID: recordingIDUnknown,
			stopPosition:       0,
		}
		// Clean stop stores id and position.
		r.markStopped(1001)
		require.Equal(t, int64(42), r.latchedRecordingID)
		require.Equal(t, int64(1001), r.stopPosition)

		// Arm for re-start, then dirty drain: live id unknown. Stored id and
		// position stay.
		r.startChannel = []byte("aeron:udp?endpoint=239.255.12.1:9004")
		r.recordingID = recordingIDUnknown
		r.markStopped(r.stopPosition)

		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(1001), got)
		require.Equal(t, 0, stub.listCalls)
	})

	t.Run("negative id refuses without catalog read", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{}
		r := &logRecording{
			client:             stub,
			recordingID:        42,
			latchedRecordingID: 42,
		}

		got, err := r.StopPositionFor(-1)
		require.Error(t, err)
		require.Equal(t, recordinglog.NullPosition, got)
		require.Equal(t, 0, stub.listCalls)
	})
}

func TestLogRecording_TruncateLogRecording(t *testing.T) {
	t.Parallel()

	t.Run("success stores the truncate stop for StopPositionFor", func(t *testing.T) {
		t.Parallel()

		stub := &recordingArchiveStub{truncateCount: 7, listStopPos: 9999}
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		count, err := r.TruncateLogRecording(42, 128)
		require.NoError(t, err)
		require.Equal(t, int64(7), count)
		require.Equal(t, 1, stub.truncateCalls)
		require.Equal(t, int64(42), stub.lastTruncateRecordingID)
		require.Equal(t, int64(128), stub.lastTruncatePosition)

		// StopPositionFor returns the truncate position with no catalog read.
		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(128), got)
		require.Equal(t, 0, stub.listCalls)
	})

	t.Run("archive error wraps and stores nothing", func(t *testing.T) {
		t.Parallel()

		cause := &archive.Exception{ErrorCode: archive.ErrorInvalidPosition}
		stub := &recordingArchiveStub{truncateErr: cause}
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
		}

		count, err := r.TruncateLogRecording(42, 128)
		require.Error(t, err)
		require.Equal(t, int64(0), count)
		require.ErrorContains(t, err, "cluster: truncate log recording 42 to 128")
		got, ok := archive.AsException(err)
		require.True(t, ok)
		require.Equal(t, cause, got)
		require.ErrorIs(t, err, cause)
		require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
		require.Equal(t, int64(0), r.stopPosition)
	})

	t.Run("dirty pending keeps the channel and reads catalog", func(t *testing.T) {
		t.Parallel()

		startChannel := []byte("aeron:udp?endpoint=239.255.12.1:9004")
		stub := &recordingArchiveStub{truncateCount: 1, listStopPos: 128}
		r := &logRecording{
			client:             stub,
			recordingID:        recordingIDUnknown,
			latchedRecordingID: recordingIDUnknown,
			startChannel:       startChannel,
		}

		_, err := r.TruncateLogRecording(42, 128)
		require.NoError(t, err)
		require.Equal(t, startChannel, r.startChannel)

		// A pending channel keeps StopPositionFor on the catalog path. The catalog
		// reports the same lowered stop.
		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(128), got)
		require.Equal(t, 1, stub.listCalls)
	})
}

func TestLogRecording_startVsExtend_branchSourceIsRecordingLog(t *testing.T) {
	t.Parallel()

	// Branch source is TermRecordingLookup only. Recording log holds TERM id S;
	// StartLogRecording must extend S.
	const (
		S            int64 = 100
		logChannel         = "aeron:udp?endpoint=239.255.12.1:9004"
		logSessionID       = int32(9)
		streamID           = int32(100)
	)
	termLog := openTermLog(t, S, 1)
	stub := &recordingArchiveStub{}
	r := &logRecording{
		client:      stub,
		terms:       termLog,
		logChannel:  logChannel,
		logStreamID: streamID,
	}

	got, err := r.StartLogRecording(archivewire.SourceLocation_REMOTE, logSessionID, "")
	require.NoError(t, err)
	require.Equal(t, S, got)
	require.Equal(t, 0, stub.startCalls)
	require.Equal(t, 1, stub.extendCalls)
	require.Equal(t, S, stub.lastExtendID)
	require.True(t, stub.lastExtendAutoStop)
	require.Equal(t, streamID, stub.lastExtendStreamID)
	require.Equal(t, archivewire.SourceLocation_REMOTE, stub.lastExtendSourceLocation)
	require.Equal(t, []byte(sessionScopedLogChannel(logChannel, logSessionID)), stub.lastExtendChannel)
	require.True(t, r.IsActive())
	require.Equal(t, S, r.recordingID)
}

func TestLogRecording_refusedExtendFailsClosedNoStartFallback(t *testing.T) {
	t.Parallel()

	const (
		established  int64 = 55
		logChannel         = "aeron:udp?endpoint=239.255.12.1:9004"
		logSessionID       = int32(9)
		streamID           = int32(100)
	)
	extendErr := errors.New("extend refused")
	stub := &recordingArchiveStub{extendErr: extendErr}
	r := &logRecording{
		client:      stub,
		terms:       fixedTermLookup(established),
		logChannel:  logChannel,
		logStreamID: streamID,
	}

	_, err := r.StartLogRecording(archivewire.SourceLocation_REMOTE, logSessionID, "")
	require.Error(t, err)
	require.ErrorIs(t, err, extendErr)
	require.Contains(t, err.Error(), "extend log recording 55")
	require.Equal(t, 0, stub.startCalls)
	require.Equal(t, 1, stub.extendCalls)
	require.Equal(t, 1, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
	require.False(t, r.needsStop())
	require.Empty(t, r.startChannel)
	require.Equal(t, recordingIDUnknown, r.recordingID)
	require.False(t, r.IsActive())
}

func TestLogRecording_agentStoresReturnedRecordingID(t *testing.T) {
	t.Parallel()

	const (
		established  int64 = 77
		logChannel         = "aeron:udp?endpoint=239.255.12.1:9004"
		logSessionID       = int32(10)
		streamID           = int32(100)
	)
	termLog := openTermLog(t, established, 2)
	stub := &recordingArchiveStub{}
	r := &logRecording{
		client:      stub,
		terms:       termLog,
		logChannel:  logChannel,
		logStreamID: streamID,
	}

	image := new(aeron.Image)
	src := &joinImageSource{image: image}
	adapter := consensus.NewLogAdapter(0)
	agent := consensus.NewAgent(consensus.AgentConfig{
		LogJoinSource: src,
		LogAdapter:    adapter,
	})
	// Seed a different local id so the post-join equality asserts the store.
	agent.SetLogRecordingID(established + 1)
	agent.SetLogRecordingControl(r)

	joined, err := agent.JoinLogAsFollower(
		consensus.FollowerLogJoinParams{LogSessionID: logSessionID},
		1,
	)
	require.NoError(t, err)
	require.True(t, joined)
	require.Equal(t, established, agent.LocalLogRecordingID())
	require.Equal(t, termLog.TermRecordingID(), agent.LocalLogRecordingID())
	require.True(t, agent.IsRecordingActive())
	require.Equal(t, 1, stub.extendCalls)
	require.Equal(t, 0, stub.startCalls)
	require.Equal(t, archivewire.SourceLocation_REMOTE, stub.lastExtendSourceLocation)
	require.Same(t, image, adapter.Image())
}

func TestLogRecording_agentPrepareAfterUnknownSubscription(t *testing.T) {
	t.Parallel()

	const catalogStop = int64(4096)
	unk := &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription}
	h := &stopRequestHarness{offerOK: true, responseErr: unk, listOfferOK: true}
	r := newPollStopRecording(pollStopRecordingID, 0, h)
	stub := r.client.(*recordingArchiveStub)

	agent := consensus.NewAgent(consensus.AgentConfig{})
	// Set the id first. The setter refuses while a control is active.
	agent.SetLogRecordingID(pollStopRecordingID)
	agent.SetLogRecordingControl(r)
	require.True(t, agent.IsRecordingActive())

	// Stop reply: the catalog read is in flight.
	position, prepared, err := agent.PrepareForNewLeadership(0, pollStopNowNs)
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, position)
	require.True(t, agent.IsPrepareInProgress())
	require.Len(t, h.listBuilt, 1)
	require.Equal(t, pollStopRecordingID, h.lastList().recordingID)

	// Catalog reply: prepare stores the catalog stop.
	h.deliverDescriptor(catalogStop)
	position, prepared, err = agent.PrepareForNewLeadership(0, pollStopNowNs+1)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, catalogStop, position)
	require.False(t, agent.IsPrepareInProgress())
	require.False(t, agent.IsRecordingActive())
	require.Zero(t, h.inFlight())
	require.Equal(t, 0, stub.listCalls)
}

func TestLogRecording_agentPrepareStopsRecoveredRecording(t *testing.T) {
	t.Parallel()

	const catalogStop = int64(4096)
	h := &stopRequestHarness{offerOK: true, listOfferOK: true}
	r := newIdentityStopRecording(h)
	stub := r.client.(*recordingArchiveStub)

	agent := consensus.NewAgent(consensus.AgentConfig{})
	agent.SetLogRecordingID(pollStopRecordingID)
	agent.SetLogRecordingControl(r)
	require.False(t, agent.IsRecordingActive())

	// Identity stop offered: no reply yet.
	position, prepared, err := agent.PrepareForNewLeadership(0, pollStopNowNs)
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, position)
	require.True(t, agent.IsPrepareInProgress())
	require.Equal(t, []int64{pollStopRecordingID}, h.identityBuilt)
	require.Equal(t, 1, h.last().tryOfferCalls)
	require.Empty(t, h.listBuilt)

	// Identity reply: the catalog read is in flight.
	h.responded = true
	position, prepared, err = agent.PrepareForNewLeadership(0, pollStopNowNs+1)
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(t, recordinglog.NullPosition, position)
	require.True(t, h.last().closed)
	require.Len(t, h.listBuilt, 1)
	require.Equal(t, pollStopRecordingID, h.lastList().recordingID)

	// Catalog reply: prepare stores the catalog stop.
	h.deliverDescriptor(catalogStop)
	position, prepared, err = agent.PrepareForNewLeadership(0, pollStopNowNs+2)
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, catalogStop, position)
	require.False(t, agent.IsPrepareInProgress())
	require.False(t, agent.IsRecordingActive())
	require.Zero(t, h.inFlight())
	require.Len(t, h.built, 1)
	require.Equal(t, 0, stub.listCalls)
}

const (
	pollStopOfferTimeoutNs    = int64(1000)
	pollStopResponseTimeoutNs = int64(500)
	pollStopNowNs             = int64(10_000)
	pollStopStreamID          = int32(100)
	pollStopRecordingID       = int64(42)
)

var pollStopChannel = []byte("aeron:udp?endpoint=239.255.12.1:9004|session-id=9")

// stopRequestHarness scripts TryOffer and PollResponse for one or more
// scriptedStopRequest values built by newRequest or newIdentityRequest. The
// list fields script scriptedListRequest values built by newListRequest.
type stopRequestHarness struct {
	built []*scriptedStopRequest
	// identityBuilt holds each identity request's recording id. Identity
	// requests also join built.
	identityBuilt []int64
	offerOK       bool
	offerErr      error
	responded     bool
	responseErr   error
	lastStream    int32
	lastChannel   []byte

	listBuilt    []*scriptedListRequest
	listOfferOK  bool
	listOfferErr error
	// listDelivered hands listStopPosition to the next PollDescriptor once.
	listDelivered    bool
	listStopPosition int64
	// listReplyErr hands a non-OK reply to the next PollDescriptor once.
	listReplyErr error
}

func (h *stopRequestHarness) newRequest(
	streamID int32,
	channel []byte,
) logStopRequest {
	h.lastStream = streamID
	h.lastChannel = append([]byte(nil), channel...)
	req := &scriptedStopRequest{h: h}
	h.built = append(h.built, req)
	return req
}

func (h *stopRequestHarness) newIdentityRequest(recordingID int64) logStopRequest {
	h.identityBuilt = append(h.identityBuilt, recordingID)
	req := &scriptedStopRequest{h: h}
	h.built = append(h.built, req)
	return req
}

func (h *stopRequestHarness) last() *scriptedStopRequest {
	if len(h.built) == 0 {
		return nil
	}
	return h.built[len(h.built)-1]
}

func (h *stopRequestHarness) newListRequest(recordingID int64) logListRequest {
	stop := h.last()
	req := &scriptedListRequest{
		h:                 h,
		recordingID:       recordingID,
		stopClosedAtBuild: stop != nil && stop.closed,
	}
	h.listBuilt = append(h.listBuilt, req)
	return req
}

func (h *stopRequestHarness) lastList() *scriptedListRequest {
	if len(h.listBuilt) == 0 {
		return nil
	}
	return h.listBuilt[len(h.listBuilt)-1]
}

func (h *stopRequestHarness) deliverDescriptor(stopPosition int64) {
	h.listDelivered = true
	h.listStopPosition = stopPosition
}

func (h *stopRequestHarness) inFlight() int {
	n := 0
	for _, req := range h.built {
		if !req.closed {
			n++
		}
	}
	for _, req := range h.listBuilt {
		if !req.closed {
			n++
		}
	}
	return n
}

// scriptedStopRequest is a logStopRequest double. Offer and response
// outcomes are read from the harness on each call.
type scriptedStopRequest struct {
	h             *stopRequestHarness
	tryOfferCalls int
	pollCalls     int
	closed        bool
	latchedOK     bool
}

func (s *scriptedStopRequest) TryOffer() (bool, error) {
	s.tryOfferCalls++
	if s.closed {
		return false, nil
	}
	return s.h.offerOK, s.h.offerErr
}

func (s *scriptedStopRequest) PollResponse() (bool, error) {
	s.pollCalls++
	if s.closed {
		return false, nil
	}
	err := s.h.responseErr
	responded := s.h.responded
	s.h.responseErr = nil
	s.h.responded = false
	if err != nil {
		return true, err
	}
	if responded {
		s.latchedOK = true
		return true, nil
	}
	return false, nil
}

func (s *scriptedStopRequest) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.latchedOK = false
}

// scriptedListRequest is a logListRequest double. Each call reads its
// outcome from the harness.
type scriptedListRequest struct {
	h           *stopRequestHarness
	recordingID int64
	// stopClosedAtBuild is true when newListRequest found the last stop
	// handle closed.
	stopClosedAtBuild bool
	tryOfferCalls     int
	pollCalls         int
	closed            bool
}

func (s *scriptedListRequest) TryOffer() (bool, error) {
	s.tryOfferCalls++
	if s.closed {
		return false, nil
	}
	return s.h.listOfferOK, s.h.listOfferErr
}

func (s *scriptedListRequest) PollDescriptor() (
	bool,
	codecs.RecordingDescriptorFields,
	error,
) {
	s.pollCalls++
	if s.closed {
		return false, codecs.RecordingDescriptorFields{}, nil
	}
	err := s.h.listReplyErr
	delivered := s.h.listDelivered
	s.h.listReplyErr = nil
	s.h.listDelivered = false
	if err != nil {
		return true, codecs.RecordingDescriptorFields{}, err
	}
	if delivered {
		return true, codecs.RecordingDescriptorFields{
			RecordingID:  s.recordingID,
			StopPosition: s.h.listStopPosition,
		}, nil
	}
	return false, codecs.RecordingDescriptorFields{}, nil
}

func (s *scriptedListRequest) Close() {
	s.closed = true
}

func requireFailClosedPoll(
	t *testing.T,
	r *logRecording,
	req *scriptedStopRequest,
	done bool,
	err, cause error,
) {
	t.Helper()
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "stream "+strconv.Itoa(int(pollStopStreamID)))
	require.ErrorContains(t, err, string(pollStopChannel))
	require.False(t, done)
	require.True(t, r.needsStop())
	require.Equal(t, pollStopChannel, r.startChannel)
	require.Nil(t, r.pendingStop)
	require.True(t, req.closed)
}

func requirePollStopPending(t *testing.T, r *logRecording, nowNs int64) {
	t.Helper()
	done, err := r.PollStopLogRecording(nowNs)
	require.NoError(t, err)
	require.False(t, done)
}

func adoptPollStop(r *logRecording, pos int64) {
	r.handleRecordingSignal(codecs.RecordingSignalEventFields{
		RecordingID: pollStopRecordingID,
		Position:    pos,
		Signal:      archivewire.RecordingSignal_STOP,
	})
}

func newPollStopRecording(
	recordingID int64,
	stopPosition int64,
	h *stopRequestHarness,
) *logRecording {
	return &logRecording{
		client:              &recordingArchiveStub{},
		logStreamID:         pollStopStreamID,
		startChannel:        append([]byte(nil), pollStopChannel...),
		recordingID:         recordingID,
		stopPosition:        stopPosition,
		latchedRecordingID:  recordingIDUnknown,
		offerRetryTimeoutNs: pollStopOfferTimeoutNs,
		responseTimeoutNs:   pollStopResponseTimeoutNs,
		newStopRequest:      h.newRequest,
		newListRequest:      h.newListRequest,
	}
}

// newIdentityStopRecording returns a control after a restart. The term lookup
// names pollStopRecordingID. The control stores no stop for it. The stub's
// ListRecording reports a null stop.
func newIdentityStopRecording(h *stopRequestHarness) *logRecording {
	return &logRecording{
		client:                 &recordingArchiveStub{listStopPos: recordinglog.NullPosition},
		terms:                  fixedTermLookup(pollStopRecordingID),
		logStreamID:            pollStopStreamID,
		recordingID:            recordingIDUnknown,
		latchedRecordingID:     recordingIDUnknown,
		offerRetryTimeoutNs:    pollStopOfferTimeoutNs,
		responseTimeoutNs:      pollStopResponseTimeoutNs,
		newStopRequest:         h.newRequest,
		newListRequest:         h.newListRequest,
		newIdentityStopRequest: h.newIdentityRequest,
	}
}

func newSeedStopControl() (*logRecording, *recordingArchiveStub) {
	stub := &recordingArchiveStub{listStopPos: 9999}
	r := &logRecording{
		client:             stub,
		recordingID:        recordingIDUnknown,
		latchedRecordingID: recordingIDUnknown,
	}
	return r, stub
}

func TestLogRecording_PollStopLogRecording(t *testing.T) {
	t.Parallel()

	t.Run("nothing to stop", func(t *testing.T) {
		t.Parallel()

		h := &stopRequestHarness{offerOK: true}
		r := &logRecording{newStopRequest: h.newRequest}

		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Empty(t, h.built)
		require.False(t, r.needsStop())
	})

	t.Run("known id ordered happy path", func(t *testing.T) {
		t.Parallel()

		const stopPos = int64(1024)
		h := &stopRequestHarness{offerOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)
		stub := r.client.(*recordingArchiveStub)

		requirePollStopPending(t, r, pollStopNowNs)
		require.Len(t, h.built, 1)
		req := h.last()
		require.Equal(t, 1, req.tryOfferCalls)
		require.Equal(t, 1, req.pollCalls)
		require.True(t, r.needsStop())
		require.Equal(t, pollStopStreamID, h.lastStream)
		require.Equal(t, pollStopChannel, h.lastChannel)

		h.responded = true
		requirePollStopPending(t, r, pollStopNowNs)
		require.True(t, r.needsStop())
		require.Equal(t, 2, req.pollCalls)
		require.True(t, req.latchedOK)
		require.Equal(t, 1, req.tryOfferCalls)

		adoptPollStop(r, stopPos)
		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.False(t, r.needsStop())
		require.False(t, r.IsActive())
		require.Equal(t, stopPos, r.stopPosition)
		require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
		require.True(t, req.closed)
		require.Equal(t, 0, stub.stopCalls)
		require.Equal(t, 0, stub.stopAwaitCalls)
		require.Equal(t, 0, stub.listCalls)

		got, posErr := r.StopPositionFor(pollStopRecordingID)
		require.NoError(t, posErr)
		require.Equal(t, stopPos, got)
		require.Equal(t, 0, stub.listCalls)

		done, err = r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Len(t, h.built, 1)
	})

	t.Run("stop before ok", func(t *testing.T) {
		t.Parallel()

		const stopPos = int64(2048)
		h := &stopRequestHarness{offerOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		requirePollStopPending(t, r, pollStopNowNs)
		req := h.last()
		require.Equal(t, 1, req.tryOfferCalls)

		adoptPollStop(r, stopPos)
		require.True(t, req.closed)

		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, 1, req.tryOfferCalls)
		require.Len(t, h.built, 1)
		require.Equal(t, stopPos, r.stopPosition)
		require.False(t, r.needsStop())
	})

	t.Run("back pressured offer then accept", func(t *testing.T) {
		t.Parallel()

		h := &stopRequestHarness{}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		requirePollStopPending(t, r, pollStopNowNs)
		req := h.last()
		require.Equal(t, 1, req.tryOfferCalls)
		require.Equal(t, 0, req.pollCalls)
		require.False(t, r.pendingStop.offered)

		requirePollStopPending(t, r, pollStopNowNs+1)
		require.Equal(t, 2, req.tryOfferCalls)
		require.Equal(t, 0, req.pollCalls)

		h.offerOK = true
		acceptAt := pollStopNowNs + 2
		requirePollStopPending(t, r, acceptAt)
		require.Equal(t, 3, req.tryOfferCalls)
		require.Equal(t, 1, req.pollCalls)
		require.True(t, r.pendingStop.offered)
		require.Equal(t, acceptAt+pollStopResponseTimeoutNs, r.pendingStop.deadlineNs)
	})

	timeoutCases := []struct {
		name      string
		offerOK   bool
		configure func(*logRecording)
		cause     error
	}{
		{
			name:      "offer bound expiry keeps channel",
			configure: func(r *logRecording) { r.offerRetryTimeoutNs = 10 },
			cause:     errStopOfferTimeout,
		},
		{
			name:      "response bound expiry then fresh request",
			offerOK:   true,
			configure: func(r *logRecording) { r.responseTimeoutNs = 10 },
			cause:     errStopResponseTimeout,
		},
	}
	for _, tt := range timeoutCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &stopRequestHarness{offerOK: tt.offerOK}
			r := newPollStopRecording(pollStopRecordingID, 0, h)
			tt.configure(r)

			requirePollStopPending(t, r, 0)
			first := h.last()
			require.Equal(t, 1, first.tryOfferCalls)

			done, err := r.PollStopLogRecording(10)
			requireFailClosedPoll(t, r, first, done, err, tt.cause)
			require.Len(t, h.built, 1)

			requirePollStopPending(t, r, 10)
			require.Len(t, h.built, 2)
			second := h.last()
			require.NotSame(t, first, second)
			require.False(t, second.closed)
			require.Equal(t, 1, second.tryOfferCalls)
		})
	}

	t.Run("dirty success", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name        string
			priorPos    int64
			latchID     int64
			responded   bool
			responseErr error
		}{
			{
				name:      "ok keeps prior position",
				priorPos:  77,
				latchID:   9,
				responded: true,
			},
			{
				name:     "unknown subscription is clean",
				priorPos: 11,
				latchID:  9,
				responseErr: &archive.Exception{
					ErrorCode: archive.ErrorUnknownSubscription,
				},
			},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				h := &stopRequestHarness{
					offerOK:     true,
					responded:   tt.responded,
					responseErr: tt.responseErr,
				}
				r := newPollStopRecording(recordingIDUnknown, tt.priorPos, h)
				r.latchedRecordingID = tt.latchID

				done, err := r.PollStopLogRecording(pollStopNowNs)
				require.NoError(t, err)
				require.True(t, done)
				require.Equal(t, tt.priorPos, r.stopPosition)
				require.False(t, r.needsStop())
				require.Equal(t, tt.latchID, r.latchedRecordingID)
				require.True(t, h.last().closed)
			})
		}
	})

	t.Run("known id unknown subscription stores the catalog stop", func(t *testing.T) {
		t.Parallel()

		const catalogStop = int64(900)
		unk := &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription}
		h := &stopRequestHarness{offerOK: true, responseErr: unk, listOfferOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)
		stub := r.client.(*recordingArchiveStub)

		// Stop reply: ErrorUnknownSubscription enters the catalog read.
		requirePollStopPending(t, r, pollStopNowNs)
		req := h.last()
		require.True(t, req.closed)
		require.Len(t, h.listBuilt, 1)
		list := h.lastList()
		require.Equal(t, pollStopRecordingID, list.recordingID)
		require.True(t, list.stopClosedAtBuild)
		require.Equal(t, 1, list.tryOfferCalls)
		require.Equal(t, 1, list.pollCalls)
		require.False(t, list.closed)
		require.Equal(t, 1, h.inFlight())
		require.True(t, r.needsStop())

		// Catalog reply: the descriptor carries the stop.
		h.deliverDescriptor(catalogStop)
		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, catalogStop, r.stopPosition)
		require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
		require.False(t, r.needsStop())
		require.True(t, req.closed)
		require.True(t, list.closed)
		require.Zero(t, h.inFlight())
		require.Equal(t, 1, list.tryOfferCalls)
		require.Len(t, h.built, 1)

		got, posErr := r.StopPositionFor(pollStopRecordingID)
		require.NoError(t, posErr)
		require.Equal(t, catalogStop, got)
		require.Equal(t, 0, stub.listCalls)
		require.Equal(t, 0, stub.stopCalls)
		require.Equal(t, 0, stub.stopAwaitCalls)
	})

	t.Run("known id list back pressured then accepted", func(t *testing.T) {
		t.Parallel()

		unk := &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription}
		h := &stopRequestHarness{offerOK: true, responseErr: unk}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		requirePollStopPending(t, r, pollStopNowNs)
		list := h.lastList()
		require.Equal(t, 1, list.tryOfferCalls)
		require.Equal(t, 0, list.pollCalls)
		require.False(t, r.pendingStop.offered)
		require.Equal(t, pollStopNowNs+pollStopOfferTimeoutNs, r.pendingStop.deadlineNs)

		requirePollStopPending(t, r, pollStopNowNs+1)
		require.Equal(t, 2, list.tryOfferCalls)
		require.Equal(t, 0, list.pollCalls)

		h.listOfferOK = true
		acceptAt := pollStopNowNs + 2
		requirePollStopPending(t, r, acceptAt)
		require.Equal(t, 3, list.tryOfferCalls)
		require.Equal(t, 1, list.pollCalls)
		require.True(t, r.pendingStop.offered)
		require.Equal(t, acceptAt+pollStopResponseTimeoutNs, r.pendingStop.deadlineNs)
		require.Len(t, h.built, 1)
		require.Len(t, h.listBuilt, 1)
		require.Equal(t, 1, h.last().tryOfferCalls)
		require.Equal(t, 1, h.inFlight())
	})

	listOfferErr := errors.New("publication closed")
	unknownRecording := &archive.Exception{ErrorCode: archive.ErrorUnknownRecording}
	listFailureCases := []struct {
		name string
		// setup runs before the first poll.
		setup func(*stopRequestHarness, *logRecording)
		// firstPending adds one pending poll before the failing poll.
		firstPending bool
		// deliver runs between the pending poll and the failing poll.
		deliver func(*stopRequestHarness)
		failNs  int64
		cause   error
	}{
		{
			name:   "terminal list offer error",
			setup:  func(h *stopRequestHarness, _ *logRecording) { h.listOfferErr = listOfferErr },
			failNs: 0,
			cause:  listOfferErr,
		},
		{
			name: "list offer bound expiry",
			setup: func(_ *stopRequestHarness, r *logRecording) {
				r.offerRetryTimeoutNs = 10
			},
			firstPending: true,
			failNs:       10,
			cause:        errStopOfferTimeout,
		},
		{
			name: "list response bound expiry",
			setup: func(h *stopRequestHarness, r *logRecording) {
				h.listOfferOK = true
				r.responseTimeoutNs = 10
			},
			firstPending: true,
			failNs:       10,
			cause:        errStopResponseTimeout,
		},
		{
			name:         "unknown recording reply",
			setup:        func(h *stopRequestHarness, _ *logRecording) { h.listOfferOK = true },
			firstPending: true,
			deliver:      func(h *stopRequestHarness) { h.listReplyErr = unknownRecording },
			failNs:       1,
			cause:        unknownRecording,
		},
		{
			name:         "null catalog stop",
			setup:        func(h *stopRequestHarness, _ *logRecording) { h.listOfferOK = true },
			firstPending: true,
			deliver: func(h *stopRequestHarness) {
				h.deliverDescriptor(recordinglog.NullPosition)
			},
			failNs: 1,
			cause:  errNoCatalogStopPosition,
		},
	}
	for _, tt := range listFailureCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			unk := &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription}
			h := &stopRequestHarness{offerOK: true, responseErr: unk}
			r := newPollStopRecording(pollStopRecordingID, 0, h)
			tt.setup(h, r)

			if tt.firstPending {
				requirePollStopPending(t, r, 0)
				if tt.deliver != nil {
					tt.deliver(h)
				}
			}
			done, err := r.PollStopLogRecording(tt.failNs)
			first := h.last()
			requireFailClosedPoll(t, r, first, done, err, tt.cause)
			listedID := strconv.FormatInt(pollStopRecordingID, 10)
			require.ErrorContains(t, err, "list recording "+listedID)
			require.Len(t, h.listBuilt, 1)
			require.True(t, h.lastList().closed)
			require.Zero(t, h.inFlight())
			require.Equal(t, pollStopRecordingID, r.recordingID)

			// Next attempt: a fresh stop request, no list request yet.
			requirePollStopPending(t, r, tt.failNs)
			require.Len(t, h.built, 2)
			second := h.last()
			require.NotSame(t, first, second)
			require.False(t, second.closed)
			require.Len(t, h.listBuilt, 1)
		})
	}

	t.Run("stop signal during the catalog read", func(t *testing.T) {
		t.Parallel()

		const stopPos = int64(777)
		unk := &archive.Exception{ErrorCode: archive.ErrorUnknownSubscription}
		h := &stopRequestHarness{offerOK: true, responseErr: unk, listOfferOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		requirePollStopPending(t, r, pollStopNowNs)
		list := h.lastList()
		require.False(t, list.closed)

		adoptPollStop(r, stopPos)
		require.True(t, list.closed)
		require.Nil(t, r.pendingStop)
		require.Zero(t, h.inFlight())

		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, stopPos, r.stopPosition)
		require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
		require.Equal(t, 1, list.pollCalls)
		require.Len(t, h.built, 1)
		require.Len(t, h.listBuilt, 1)
	})

	t.Run("known id other error fails closed", func(t *testing.T) {
		t.Parallel()

		generic := &archive.Exception{ErrorCode: archive.ErrorGeneric}
		h := &stopRequestHarness{offerOK: true, responseErr: generic, listOfferOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		done, err := r.PollStopLogRecording(pollStopNowNs)
		requireFailClosedPoll(t, r, h.last(), done, err, generic)
		require.Empty(t, h.listBuilt)
	})

	t.Run("known id adoption without ok reply is done", func(t *testing.T) {
		t.Parallel()

		const stopPos = int64(333)
		h := &stopRequestHarness{offerOK: true}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		requirePollStopPending(t, r, pollStopNowNs)
		adoptPollStop(r, stopPos)

		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, stopPos, r.stopPosition)
		require.False(t, r.needsStop())
		require.True(t, h.last().closed)
	})

	t.Run("terminal offer fails closed", func(t *testing.T) {
		t.Parallel()

		offerErr := errors.New("publication closed")
		h := &stopRequestHarness{offerErr: offerErr}
		r := newPollStopRecording(pollStopRecordingID, 0, h)

		done, err := r.PollStopLogRecording(pollStopNowNs)
		requireFailClosedPoll(t, r, h.last(), done, err, offerErr)
	})

	t.Run("identity reply stores the catalog stop", func(t *testing.T) {
		t.Parallel()

		// logStopRequest carries no IsStopped. A stopped reply and a
		// not-active reply take the same path.
		const catalogStop = int64(2048)
		h := &stopRequestHarness{offerOK: true, listOfferOK: true}
		r := newIdentityStopRecording(h)
		stub := r.client.(*recordingArchiveStub)

		// Identity stop offered: no reply yet.
		requirePollStopPending(t, r, pollStopNowNs)
		require.Equal(t, []int64{pollStopRecordingID}, h.identityBuilt)
		req := h.last()
		require.Equal(t, 1, req.tryOfferCalls)
		require.Equal(t, 1, req.pollCalls)
		require.Equal(t, pollStopNowNs+pollStopResponseTimeoutNs, r.pendingStop.deadlineNs)

		// Identity reply: the catalog read is in flight.
		h.responded = true
		requirePollStopPending(t, r, pollStopNowNs+1)
		require.True(t, req.closed)
		require.Len(t, h.listBuilt, 1)
		list := h.lastList()
		require.Equal(t, pollStopRecordingID, list.recordingID)
		require.True(t, list.stopClosedAtBuild)
		require.Equal(t, 1, h.inFlight())

		// Catalog reply: the control stores the stop for the term id.
		h.deliverDescriptor(catalogStop)
		done, err := r.PollStopLogRecording(pollStopNowNs + 2)
		require.NoError(t, err)
		require.True(t, done)
		require.Nil(t, r.pendingStop)
		require.Zero(t, h.inFlight())
		require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
		require.Equal(t, catalogStop, r.stopPosition)
		require.False(t, r.IsActive())
		require.Equal(t, recordingIDUnknown, r.recordingID)

		got, posErr := r.StopPositionFor(pollStopRecordingID)
		require.NoError(t, posErr)
		require.Equal(t, catalogStop, got)
		require.Equal(t, 0, stub.listCalls)

		// The stored stop ends the identity path.
		done, err = r.PollStopLogRecording(pollStopNowNs + 3)
		require.NoError(t, err)
		require.True(t, done)
		require.Len(t, h.identityBuilt, 1)
	})

	storedStopCases := []struct {
		name  string
		store func(*testing.T, *logRecording)
	}{
		{
			name: "plan seed",
			store: func(_ *testing.T, r *logRecording) {
				r.seedStopPosition(pollStopRecordingID, 512)
			},
		},
		{
			name: "truncate",
			store: func(t *testing.T, r *logRecording) {
				_, err := r.TruncateLogRecording(pollStopRecordingID, 512)
				require.NoError(t, err)
			},
		},
		{
			name: "clean stop",
			store: func(_ *testing.T, r *logRecording) {
				r.startChannel = append([]byte(nil), pollStopChannel...)
				r.recordingID = pollStopRecordingID
				adoptPollStop(r, 512)
			},
		},
	}
	for _, tt := range storedStopCases {
		t.Run("identity skipped after "+tt.name, func(t *testing.T) {
			t.Parallel()

			h := &stopRequestHarness{offerOK: true, listOfferOK: true}
			r := newIdentityStopRecording(h)
			tt.store(t, r)

			done, err := r.PollStopLogRecording(pollStopNowNs)
			require.NoError(t, err)
			require.True(t, done)
			require.Empty(t, h.built)
			require.Empty(t, h.listBuilt)
		})
	}

	// Plan seed and truncate store the stop without a stop request.
	for _, tt := range storedStopCases[:2] {
		for _, listing := range []bool{false, true} {
			name := "identity offer ends after " + tt.name
			if listing {
				name = "identity catalog read ends after " + tt.name
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				h := &stopRequestHarness{offerOK: true, listOfferOK: true}
				r := newIdentityStopRecording(h)

				requirePollStopPending(t, r, pollStopNowNs)
				req := h.last()
				if listing {
					h.responded = true
					requirePollStopPending(t, r, pollStopNowNs+1)
					require.NotNil(t, r.pendingStop.list)
				}
				require.Equal(t, 1, h.inFlight())

				tt.store(t, r)
				done, err := r.PollStopLogRecording(pollStopNowNs + 2)
				require.NoError(t, err)
				require.True(t, done)
				require.Nil(t, r.pendingStop)
				require.True(t, req.closed)
				require.Zero(t, h.inFlight())
				require.Equal(t, []int64{pollStopRecordingID}, h.identityBuilt)
				require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
				require.Equal(t, int64(512), r.stopPosition)
			})
		}
	}

	t.Run("identity skipped without a term id", func(t *testing.T) {
		t.Parallel()

		h := &stopRequestHarness{offerOK: true}
		r := newIdentityStopRecording(h)
		r.terms = fixedTermLookup(recordinglog.NullRecordingID)

		done, err := r.PollStopLogRecording(pollStopNowNs)
		require.NoError(t, err)
		require.True(t, done)
		require.Empty(t, h.built)
	})

	t.Run("start channel takes the channel attempt", func(t *testing.T) {
		t.Parallel()

		h := &stopRequestHarness{offerOK: true}
		r := newIdentityStopRecording(h)
		r.startChannel = append([]byte(nil), pollStopChannel...)
		r.recordingID = pollStopRecordingID

		requirePollStopPending(t, r, pollStopNowNs)
		require.Len(t, h.built, 1)
		require.Empty(t, h.identityBuilt)
		require.Equal(t, pollStopChannel, h.lastChannel)
		require.False(t, r.pendingStop.isIdentity())
	})

	identityOfferErr := errors.New("publication closed")
	identityListErr := errors.New("list publication closed")
	identityGeneric := &archive.Exception{ErrorCode: archive.ErrorGeneric}
	identityUnknown := &archive.Exception{ErrorCode: archive.ErrorUnknownRecording}
	identityFailureCases := []struct {
		name string
		// setup runs before the first poll.
		setup func(*stopRequestHarness, *logRecording)
		// firstPending adds one pending poll before the failing poll.
		firstPending bool
		// deliver runs between the pending poll and the failing poll.
		deliver func(*stopRequestHarness)
		failNs  int64
		cause   error
		// listed is true when the catalog read fails.
		listed bool
	}{
		{
			name:  "terminal offer",
			setup: func(h *stopRequestHarness, _ *logRecording) { h.offerErr = identityOfferErr },
			cause: identityOfferErr,
		},
		{
			name: "offer bound expiry",
			setup: func(h *stopRequestHarness, r *logRecording) {
				h.offerOK = false
				r.offerRetryTimeoutNs = 10
			},
			firstPending: true,
			failNs:       10,
			cause:        errStopOfferTimeout,
		},
		{
			name:         "response bound expiry",
			setup:        func(_ *stopRequestHarness, r *logRecording) { r.responseTimeoutNs = 10 },
			firstPending: true,
			failNs:       10,
			cause:        errStopResponseTimeout,
		},
		{
			name:         "unknown recording reply",
			firstPending: true,
			deliver:      func(h *stopRequestHarness) { h.responseErr = identityUnknown },
			failNs:       1,
			cause:        identityUnknown,
		},
		{
			name:         "generic error reply",
			firstPending: true,
			deliver:      func(h *stopRequestHarness) { h.responseErr = identityGeneric },
			failNs:       1,
			cause:        identityGeneric,
		},
		{
			name:         "list error",
			firstPending: true,
			deliver: func(h *stopRequestHarness) {
				h.responded = true
				h.listReplyErr = identityListErr
			},
			failNs: 1,
			cause:  identityListErr,
			listed: true,
		},
		{
			name:         "null catalog stop",
			firstPending: true,
			deliver: func(h *stopRequestHarness) {
				h.responded = true
				h.deliverDescriptor(recordinglog.NullPosition)
			},
			failNs: 1,
			cause:  errNoCatalogStopPosition,
			listed: true,
		},
	}
	for _, tt := range identityFailureCases {
		t.Run("identity "+tt.name+" fails closed", func(t *testing.T) {
			t.Parallel()

			h := &stopRequestHarness{offerOK: true, listOfferOK: true}
			r := newIdentityStopRecording(h)
			if tt.setup != nil {
				tt.setup(h, r)
			}

			if tt.firstPending {
				requirePollStopPending(t, r, 0)
				if tt.deliver != nil {
					tt.deliver(h)
				}
			}
			done, err := r.PollStopLogRecording(tt.failNs)
			first := h.last()
			require.ErrorIs(t, err, tt.cause)
			listedID := strconv.FormatInt(pollStopRecordingID, 10)
			require.ErrorContains(t, err, "stop log recording "+listedID+" by identity")
			require.False(t, done)
			require.False(t, r.needsStop())
			require.Nil(t, r.pendingStop)
			require.True(t, first.closed)
			require.Zero(t, h.inFlight())
			require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
			if tt.listed {
				require.ErrorContains(t, err, "list recording "+listedID)
				require.Len(t, h.listBuilt, 1)
			} else {
				require.Empty(t, h.listBuilt)
			}

			// Next attempt: a fresh identity request.
			h.offerErr = nil
			h.offerOK = true
			requirePollStopPending(t, r, tt.failNs)
			require.Equal(t, []int64{pollStopRecordingID, pollStopRecordingID}, h.identityBuilt)
			second := h.last()
			require.NotSame(t, first, second)
			require.False(t, second.closed)
		})
	}

	t.Run("stop signal during the identity attempt", func(t *testing.T) {
		t.Parallel()

		const catalogStop = int64(800)
		h := &stopRequestHarness{offerOK: true, listOfferOK: true}
		r := newIdentityStopRecording(h)

		requirePollStopPending(t, r, pollStopNowNs)
		p := r.pendingStop
		req := h.last()

		// The control is inactive. The STOP stores nothing and sets no mark.
		adoptPollStop(r, 500)
		require.Same(t, p, r.pendingStop)
		require.False(t, req.closed)
		require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
		_, stopped := r.ConsumeStopSignal()
		require.False(t, stopped)

		h.responded = true
		h.deliverDescriptor(catalogStop)
		done, err := r.PollStopLogRecording(pollStopNowNs + 1)
		require.NoError(t, err)
		require.True(t, done)
		require.Equal(t, catalogStop, r.stopPosition)
		require.Equal(t, pollStopRecordingID, r.latchedRecordingID)
		_, stopped = r.ConsumeStopSignal()
		require.False(t, stopped)
	})

	abandonCases := []struct {
		name    string
		abandon func(*testing.T, *logRecording)
	}{
		{
			name: "StopLogRecording",
			abandon: func(t *testing.T, r *logRecording) {
				require.NoError(t, r.StopLogRecording())
				require.False(t, r.needsStop())
			},
		},
		{
			name: "StartLogRecording",
			abandon: func(t *testing.T, r *logRecording) {
				id, err := r.StartLogRecording(
					archivewire.SourceLocation_REMOTE,
					9,
					"aeron:udp?endpoint=239.255.12.1:9004",
				)
				require.NoError(t, err)
				require.Equal(t, pollStopRecordingID, id)
				require.True(t, r.IsActive())
			},
		},
	}
	for _, tt := range abandonCases {
		for _, inCatalogRead := range []bool{false, true} {
			name := tt.name + " abandons the identity attempt awaiting its reply"
			if inCatalogRead {
				name = tt.name + " abandons the identity attempt in its catalog read"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				h := &stopRequestHarness{offerOK: true, listOfferOK: true}
				r := newIdentityStopRecording(h)

				requirePollStopPending(t, r, pollStopNowNs)
				identity := h.last()
				require.False(t, identity.closed)
				if inCatalogRead {
					h.responded = true
					requirePollStopPending(t, r, pollStopNowNs+1)
					require.True(t, identity.closed)
					require.False(t, h.lastList().closed)
				}
				require.Equal(t, 1, h.inFlight())

				tt.abandon(t, r)
				require.True(t, identity.closed)
				if inCatalogRead {
					require.True(t, h.lastList().closed)
				} else {
					require.Empty(t, h.listBuilt)
				}
				require.Nil(t, r.pendingStop)
				require.Zero(t, h.inFlight())
				require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
			})
		}
	}
}

func TestLogRecording_StopLogRecording_abandonsPending(t *testing.T) {
	t.Parallel()

	h := &stopRequestHarness{offerOK: true}
	r := newPollStopRecording(pollStopRecordingID, 0, h)
	stub := r.client.(*recordingArchiveStub)

	requirePollStopPending(t, r, pollStopNowNs)
	req := h.last()
	require.False(t, req.closed)

	require.NoError(t, r.StopLogRecording())
	require.True(t, req.closed)
	require.Equal(t, 1, stub.stopAwaitCalls)
	require.Equal(t, 0, stub.stopCalls)
	require.False(t, r.needsStop())
	require.Nil(t, r.pendingStop)

	require.NoError(t, r.StopLogRecording())
	require.Equal(t, 1, stub.stopAwaitCalls)
	require.True(t, req.closed)
	require.False(t, r.needsStop())
}

func TestLogRecording_StartLogRecording_afterAdoptedStop(t *testing.T) {
	t.Parallel()

	const (
		logChannel   = "aeron:udp?endpoint=239.255.12.1:9004"
		logSessionID = int32(9)
		newID        = int64(99)
	)
	h := &stopRequestHarness{offerOK: true}
	r := newPollStopRecording(pollStopRecordingID, 0, h)
	r.logChannel = logChannel
	stub := r.client.(*recordingArchiveStub)
	stub.startID = newID

	requirePollStopPending(t, r, pollStopNowNs)
	first := h.last()

	adoptPollStop(r, 1024)
	require.True(t, first.closed)
	require.False(t, r.needsStop())
	require.Nil(t, r.pendingStop)
	require.True(t, r.stopSignalled)

	got, err := r.StartLogRecording(archivewire.SourceLocation_LOCAL, logSessionID, "")
	require.NoError(t, err)
	require.Equal(t, newID, got)

	// The restart clears the STOP mark of the prior recording.
	_, stopped := r.ConsumeStopSignal()
	require.False(t, stopped)
	wantChannel := []byte(sessionScopedLogChannel(logChannel, logSessionID))
	require.Equal(t, wantChannel, r.startChannel)
	require.True(t, r.needsStop())
	require.Equal(t, 1, stub.startCalls)
	require.True(t, stub.lastStartAutoStop)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)

	requirePollStopPending(t, r, pollStopNowNs)
	require.Len(t, h.built, 2)
	second := h.last()
	require.NotSame(t, first, second)
	require.Equal(t, wantChannel, h.lastChannel)
	require.Equal(t, 1, second.tryOfferCalls)
	require.False(t, second.closed)
}

func TestLogRecording_StopLogRecording_afterAdoptedStop(t *testing.T) {
	t.Parallel()

	h := &stopRequestHarness{offerOK: true}
	r := newPollStopRecording(pollStopRecordingID, 0, h)
	stub := r.client.(*recordingArchiveStub)

	requirePollStopPending(t, r, pollStopNowNs)
	req := h.last()

	adoptPollStop(r, 2048)
	require.True(t, req.closed)
	require.False(t, r.needsStop())
	require.Nil(t, r.pendingStop)

	require.NoError(t, r.StopLogRecording())
	require.True(t, req.closed)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
	require.False(t, r.needsStop())

	require.NoError(t, r.StopLogRecording())
	require.True(t, req.closed)
	require.Equal(t, 0, stub.stopCalls)
	require.Equal(t, 0, stub.stopAwaitCalls)
}

func TestLogRecording_seedStopPosition(t *testing.T) {
	t.Parallel()

	storeCases := []struct {
		name    string
		id, pos int64
	}{
		{name: "seeded id takes stored path", id: 42, pos: 1001},
		{name: "zero id and zero position seed stored path", id: 0, pos: 0},
	}
	for _, tt := range storeCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, stub := newSeedStopControl()
			r.seedStopPosition(tt.id, tt.pos)
			got, err := r.StopPositionFor(tt.id)
			require.NoError(t, err)
			require.Equal(t, tt.pos, got)
			require.Equal(t, 0, stub.listCalls)
			require.Equal(t, tt.id, r.latchedRecordingID)
			require.Equal(t, tt.pos, r.stopPosition)
		})
	}

	skipCases := []struct {
		name    string
		id, pos int64
	}{
		{name: "null id does not seed", id: recordinglog.NullRecordingID, pos: 1001},
		{name: "null position does not seed", id: 42, pos: recordinglog.NullPosition},
	}
	for _, tt := range skipCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := &logRecording{
				latchedRecordingID: recordingIDUnknown,
				stopPosition:       7,
			}
			r.seedStopPosition(tt.id, tt.pos)
			require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
			require.Equal(t, int64(7), r.stopPosition)
		})
	}
}

func TestSeedLogRecordingStopFromPlan(t *testing.T) {
	t.Parallel()

	t.Run("concrete extent seeds stored path", func(t *testing.T) {
		t.Parallel()

		r, stub := newSeedStopControl()
		plan := recordinglog.RecoveryPlan{
			Log: &recordinglog.RecoveryPlanLog{
				Term:   recordinglog.TermRef{RecordingID: 42},
				Extent: recordinglog.LogRecordingExtents{StopPosition: 1001},
			},
		}
		seedLogRecordingStopFromPlan(r, plan)
		got, err := r.StopPositionFor(42)
		require.NoError(t, err)
		require.Equal(t, int64(1001), got)
		require.Equal(t, 0, stub.listCalls)
	})

	skipCases := []struct {
		name         string
		plan         recordinglog.RecoveryPlan
		checkCatalog bool
	}{
		{
			name:         "nil recovery log skips seed",
			plan:         recordinglog.RecoveryPlan{},
			checkCatalog: true,
		},
		{
			name: "null term id skips seed",
			plan: recordinglog.RecoveryPlan{
				Log: &recordinglog.RecoveryPlanLog{
					Term:   recordinglog.TermRef{RecordingID: recordinglog.NullRecordingID},
					Extent: recordinglog.LogRecordingExtents{StopPosition: 1001},
				},
			},
		},
		{
			name: "null extent stop skips seed",
			plan: recordinglog.RecoveryPlan{
				Log: &recordinglog.RecoveryPlanLog{
					Term:   recordinglog.TermRef{RecordingID: 42},
					Extent: recordinglog.LogRecordingExtents{StopPosition: recordinglog.NullPosition},
				},
			},
		},
	}
	for _, tt := range skipCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, stub := newSeedStopControl()
			seedLogRecordingStopFromPlan(r, tt.plan)
			require.Equal(t, recordingIDUnknown, r.latchedRecordingID)
			require.Equal(t, int64(0), r.stopPosition)
			if tt.checkCatalog {
				got, err := r.StopPositionFor(42)
				require.NoError(t, err)
				require.Equal(t, int64(9999), got)
				require.Equal(t, 1, stub.listCalls)
			}
		})
	}
}

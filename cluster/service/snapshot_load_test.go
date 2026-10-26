//go:build testhooks

package service

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

// snapshotLoadLogPosition is the log position the loader expects on each marker.
const snapshotLoadLogPosition int64 = 512

// fakeSnapshotReplay records each snapshotReplayControl call. It returns the
// configured session id or error.
type fakeSnapshotReplay struct {
	startCalls []fakeStartReplayCall
	stopCalls  []int64
	pollCalls  int
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

var _ snapshotReplayControl = (*fakeSnapshotReplay)(nil)

func (f *fakeSnapshotReplay) StartReplay(
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

func (f *fakeSnapshotReplay) Poll() int {
	f.pollCalls++
	return 0
}

func (f *fakeSnapshotReplay) StopReplay(_ context.Context, replaySessionID int64) error {
	f.stopCalls = append(f.stopCalls, replaySessionID)
	return f.stopErr
}

// serviceMarkerFields returns a service marker at logPosition with mark,
// time unit NANOS and app version 3.
func serviceMarkerFields(
	logPosition int64,
	mark clusterwire.SnapshotMark,
) codecs.SnapshotMarkerFields {
	return codecs.SnapshotMarkerFields{
		TypeID:           codecs.ServiceSnapshotTypeID,
		LogPosition:      logPosition,
		LeadershipTermID: 5,
		Mark:             mark,
		TimeUnit:         clusterwire.ClusterTimeUnit_NANOS,
		AppVersion:       3,
	}
}

func encodeSnapshotMarkerFrame(tb assertTB, fields codecs.SnapshotMarkerFields) []byte {
	tb.Helper()
	buf := make([]byte, codecs.SnapshotMarkerEncodedLen())
	end, ok := codecs.EncodeSnapshotMarker(
		buf, 0, clusterwire.SnapshotMarkerSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

func encodeClientSessionFrame(tb assertTB, fields codecs.ClientSessionFields) []byte {
	tb.Helper()
	length, ok := codecs.ClusterWireMessageLen(
		clusterwire.ClientSessionSbeBlockLength,
		len(fields.ResponseChannel),
		len(fields.EncodedPrincipal),
	)
	require.True(tb, ok)
	buf := make([]byte, length)
	end, ok := codecs.EncodeClientSession(
		buf, 0, clusterwire.ClientSessionSbeSchemaVersion, fields,
	)
	require.True(tb, ok)
	return buf[:end]
}

func encodeTimerFrame(tb testing.TB) []byte {
	tb.Helper()
	buf := make([]byte, codecs.TimerEncodedLen())
	end, ok := codecs.EncodeTimer(
		buf, 0, clusterwire.TimerSbeSchemaVersion, codecs.TimerFields{CorrelationID: 1},
	)
	require.True(tb, ok)
	return buf[:end]
}

// sampleClientSession returns session id on channel. The stream id is id+10.
// The principal is "p" plus channel.
func sampleClientSession(id int64, channel string) codecs.ClientSessionFields {
	return codecs.ClientSessionFields{
		ClusterSessionID: id,
		ResponseStreamID: int32(id) + 10,
		ResponseChannel:  []byte(channel),
		EncodedPrincipal: []byte("p" + channel),
	}
}

func wholeFrames(payloads ...[]byte) []aeron.FragmentedFrameForTest {
	frames := make([]aeron.FragmentedFrameForTest, len(payloads))
	for i, payload := range payloads {
		frames[i] = aeron.FragmentedFrameForTest{Flags: logbuffer.Unfragmented, Payload: payload}
	}
	return frames
}

// fragmentPayload splits payload into a BEGIN, middle, END frame train. Each
// frame holds at most chunk bytes.
func fragmentPayload(payload []byte, chunk int) []aeron.FragmentedFrameForTest {
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

// writeSnapshotFrameTrain writes frames onto image from offset 0. Returns
// the end offset.
func writeSnapshotFrameTrain(
	tb assertTB,
	image *aeron.Image,
	frames []aeron.FragmentedFrameForTest,
) int64 {
	tb.Helper()
	offsets := aeron.WriteCommittedDataFramesForStreamForTest(
		aeron.ImageTermBufferForHooks(image),
		0,
		image.SessionID(),
		image.StreamID(),
		hookTermID,
		frames,
	)
	if len(frames) == 0 {
		return 0
	}
	last := frames[len(frames)-1].Payload
	return int64(offsets[len(offsets)-1] + logbuffer.AlignFragmentChunkLength(len(last)))
}

// snapshotFramesSpan returns the aligned term span of payloads as whole
// frames.
func snapshotFramesSpan(payloads [][]byte) int64 {
	span := 0
	for _, payload := range payloads {
		span += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return int64(span)
}

func writeSnapshotFrames(tb assertTB, image *aeron.Image, payloads [][]byte) {
	tb.Helper()
	writeSnapshotFrameTrain(tb, image, wholeFrames(payloads...))
}

// pollSnapshotLoader polls image into loader until an error, done, or an
// empty poll.
func pollSnapshotLoader(image *aeron.Image, loader *serviceSnapshotLoader) {
	for range 64 {
		fragments := image.ControlledPoll(loader.handler, snapshotLoadFragmentLimit)
		if loader.err != nil || loader.done || fragments == 0 {
			return
		}
	}
}

// restoredSession is one registry entry, compared by value.
type restoredSession struct {
	id        int64
	streamID  int32
	channel   string
	principal string
}

func restoredSessions(agent *Agent) []restoredSession {
	var sessions []restoredSession
	for _, session := range agent.sessionList {
		sessions = append(sessions, restoredSession{
			id:        session.id,
			streamID:  session.responseStreamID,
			channel:   session.responseChannel,
			principal: string(session.encodedPrincipal),
		})
	}
	return sessions
}

func TestSnapshotLoader_dispatch(t *testing.T) {
	t.Parallel()

	marker := func(mark clusterwire.SnapshotMark) []byte {
		return encodeSnapshotMarkerFrame(t, serviceMarkerFields(snapshotLoadLogPosition, mark))
	}
	begin := marker(clusterwire.SnapshotMark_BEGIN)
	end := marker(clusterwire.SnapshotMark_END)
	session := func(id int64, channel string) []byte {
		return encodeClientSessionFrame(t, sampleClientSession(id, channel))
	}
	withMarker := func(edit func(*codecs.SnapshotMarkerFields)) []byte {
		fields := serviceMarkerFields(snapshotLoadLogPosition, clusterwire.SnapshotMark_BEGIN)
		edit(&fields)
		return encodeSnapshotMarkerFrame(t, fields)
	}
	longChannel := "aeron:udp?endpoint=" + strings.Repeat("c", 300)
	overCap := session(4, "aeron:ipc")
	// The response channel length follows the header and the block.
	varLengthOffset := codecs.MessageHeaderEncodedLen() + int(clusterwire.ClientSessionSbeBlockLength)
	binary.LittleEndian.PutUint32(overCap[varLengthOffset:], 0x7fff_ffff)

	tests := []struct {
		name         string
		frames       []aeron.FragmentedFrameForTest
		wantErr      string
		wantSessions []restoredSession
		wantUnit     clusterwire.ClusterTimeUnit
		wantVersion  int32
	}{
		// Phase: accept.
		{
			name:   "begin two sessions end",
			frames: wholeFrames(begin, session(2, "aeron:ipc"), session(9, "aeron:udp"), end),
			wantSessions: []restoredSession{
				{id: 2, streamID: 12, channel: "aeron:ipc", principal: "paeron:ipc"},
				{id: 9, streamID: 19, channel: "aeron:udp", principal: "paeron:udp"},
			},
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 3,
		},
		{
			name:   "descending sessions restore ascending",
			frames: wholeFrames(begin, session(9, "aeron:udp"), session(2, "aeron:ipc"), end),
			wantSessions: []restoredSession{
				{id: 2, streamID: 12, channel: "aeron:ipc", principal: "paeron:ipc"},
				{id: 9, streamID: 19, channel: "aeron:udp", principal: "paeron:udp"},
			},
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 3,
		},
		{
			name: "section and null marks continue",
			frames: wholeFrames(
				begin,
				marker(clusterwire.SnapshotMark_SECTION),
				marker(clusterwire.SnapshotMark_NULL_VALUE),
				end,
			),
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 3,
		},
		{
			name: "null time unit stores millis",
			frames: wholeFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TimeUnit = clusterwire.ClusterTimeUnit_NULL_VALUE
				f.AppVersion = 8
			}), end),
			wantUnit:    clusterwire.ClusterTimeUnit_MILLIS,
			wantVersion: 8,
		},
		{
			name: "fragmented session row assembles",
			frames: append(append(
				wholeFrames(begin),
				fragmentPayload(session(3, longChannel), 64)...,
			), wholeFrames(end)...),
			wantSessions: []restoredSession{
				{id: 3, streamID: 13, channel: longChannel, principal: "p" + longChannel},
			},
			wantUnit:    clusterwire.ClusterTimeUnit_NANOS,
			wantVersion: 3,
		},
		// Phase: refuse.
		{
			name:    "session row first",
			frames:  wholeFrames(session(2, "aeron:ipc"), end),
			wantErr: "service: load snapshot: template 102 before the begin marker",
		},
		{
			name:    "end first",
			frames:  wholeFrames(end),
			wantErr: "service: load snapshot: end marker before the begin marker",
		},
		{
			name:    "template 24 first",
			frames:  wholeFrames(encodeNLT(t, codecs.NewLeadershipTermEventFields{}), begin),
			wantErr: "service: load snapshot: template 24 before the begin marker",
		},
		{
			name:    "begin twice",
			frames:  wholeFrames(begin, begin, end),
			wantErr: "service: load snapshot: begin marker repeated",
		},
		{
			name: "module snapshot type id",
			frames: wholeFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TypeID = codecs.ConsensusModuleSnapshotTypeID
			}), end),
			wantErr: "service: load snapshot: snapshot type id 1, want 2",
		},
		{
			name: "marker log position off the key",
			frames: wholeFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.LogPosition = 640
			}), end),
			wantErr: "service: load snapshot: marker log position 640, want 512",
		},
		{
			name:    "repeated session id",
			frames:  wholeFrames(begin, session(4, "aeron:ipc"), session(4, "aeron:udp"), end),
			wantErr: "service: load snapshot: client session 4 repeated",
			wantSessions: []restoredSession{
				{id: 4, streamID: 14, channel: "aeron:ipc", principal: "paeron:ipc"},
			},
		},
		{
			name:    "timer inside the snapshot",
			frames:  wholeFrames(begin, encodeTimerFrame(t), end),
			wantErr: "service: load snapshot: template 104 inside the snapshot",
		},
		{
			name: "schema id 112",
			frames: wholeFrames(encodeMessageHeader(
				t, clusterwire.SnapshotMarkerSbeBlockLength, 100, 112, 17,
			)),
			wantErr: "service: load snapshot: schema id 112, want 111",
		},
		{
			name:    "short header",
			frames:  wholeFrames([]byte{1, 2, 3, 4}),
			wantErr: "service: load snapshot: message header short, length 4",
		},
		{
			name: "header-only marker",
			frames: wholeFrames(encodeMessageHeader(
				t,
				clusterwire.SnapshotMarkerSbeBlockLength,
				clusterwire.SnapshotMarkerSbeTemplateID,
				clusterwire.MessageHeaderSbeSchemaID,
				clusterwire.SnapshotMarkerSbeSchemaVersion,
			)),
			wantErr: "service: load snapshot: template 100 does not decode",
		},
		{
			name:    "session row over-cap var length",
			frames:  wholeFrames(begin, overCap, end),
			wantErr: "service: load snapshot: client session row does not decode",
		},
		// Phase: undefined enum values format as integers.
		{
			name: "mark 7",
			frames: wholeFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.Mark = clusterwire.SnapshotMark(7)
			})),
			wantErr: "service: load snapshot: snapshot mark 7 unknown",
		},
		{
			name: "time unit 9",
			frames: wholeFrames(withMarker(func(f *codecs.SnapshotMarkerFields) {
				f.TimeUnit = clusterwire.ClusterTimeUnit(9)
			}), end),
			wantErr: "service: load snapshot: time unit 9 unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &recordingService{}
			agent := NewAgent(AgentConfig{
				Service:              svc,
				ResponsePublications: newFakeTransport(t),
			})
			_, image := controlIngressFixtures(t)
			writeSnapshotFrameTrain(t, image, tt.frames)
			loader := newServiceSnapshotLoader(agent, snapshotLoadLogPosition)

			require.NotPanics(t, func() { pollSnapshotLoader(image, loader) })

			require.Equal(t, tt.wantSessions, restoredSessions(agent), "registry")
			require.Empty(t, svc.opens, "SessionOpen calls")
			require.Empty(t, agent.responseAdds, "response adds")
			if tt.wantErr != "" {
				require.ErrorIs(t, loader.err, errSnapshotLoad)
				require.EqualError(t, loader.err, tt.wantErr)
				require.False(t, loader.done, "done")
				require.Equal(t, clusterwire.ClusterTimeUnit_NULL_VALUE, agent.timeUnit)
				require.Zero(t, agent.appVersion, "app version")
				return
			}
			require.NoError(t, loader.err)
			require.True(t, loader.done, "done")
			require.Equal(t, tt.wantUnit, agent.timeUnit)
			require.Equal(t, tt.wantVersion, agent.appVersion)
		})
	}
}

// snapshotLoadAdder records each replay subscription add. Each add builds a
// fresh hook subscription. onAdd runs on it before the add returns.
type snapshotLoadAdder struct {
	calls []transportAdd
	subs  []*aeron.Subscription
	err   error
	onAdd func(sub *aeron.Subscription)
}

func (a *snapshotLoadAdder) AddSubscription(
	streamID int32,
	channel string,
) (*aeron.Subscription, error) {
	a.calls = append(a.calls, transportAdd{streamID: streamID, channel: channel})
	if a.err != nil {
		return nil, a.err
	}
	sub := aeron.NewSubscriptionForHooks(9_500, streamID, channel)
	a.subs = append(a.subs, sub)
	if a.onAdd != nil {
		a.onAdd(sub)
	}
	return sub, nil
}

func TestSnapshotLoad_run(t *testing.T) {
	t.Parallel()

	const replaySessionID int64 = 2<<32 | 31
	startErr := errors.New("unknown recording")
	addErr := errors.New("add failed")
	stopErr := errors.New("stop failed")
	loadErr := errors.New("bad state")
	begin := encodeSnapshotMarkerFrame(
		t, serviceMarkerFields(snapshotLoadLogPosition, clusterwire.SnapshotMark_BEGIN),
	)
	end := encodeSnapshotMarkerFrame(
		t, serviceMarkerFields(snapshotLoadLogPosition, clusterwire.SnapshotMark_END),
	)
	userBytes := []byte("user-state")
	userStart := snapshotFramesSpan([][]byte{begin, end})

	tests := []struct {
		name string
		// frames lands on the replay image. Nil frames do not attach an image.
		frames     [][]byte
		closeAfter int // pump that closes the image. Zero never closes it.
		startErr   error
		addErr     error
		stopErr    error
		loadErr    error
		timeout    time.Duration
		wantErr    []error
		wantText   string
		wantLoad   bool
	}{
		{
			name:     "start replay error",
			startErr: startErr,
			wantErr:  []error{errSnapshotLoad, startErr},
			wantText: "service: load snapshot: start replay: unknown recording",
		},
		{
			name:    "subscription add error",
			addErr:  addErr,
			wantErr: []error{errSnapshotLoad, addErr},
		},
		{
			name:     "image absent past the timeout",
			timeout:  20 * time.Millisecond,
			wantErr:  []error{errSnapshotLoad},
			wantText: "service: load snapshot: replay image not found within 20ms",
		},
		{
			name:     "corrupt first frame",
			frames:   [][]byte{encodeTimerFrame(t)},
			wantErr:  []error{errSnapshotLoad},
			wantText: "service: load snapshot: template 104 before the begin marker",
		},
		{
			name:       "image closed before end",
			frames:     [][]byte{begin},
			closeAfter: 2,
			wantErr:    []error{errSnapshotLoad},
			wantText:   "service: load snapshot: replay ended before the end marker",
		},
		{
			name:     "no fragment within the gap timeout",
			frames:   [][]byte{begin},
			timeout:  20 * time.Millisecond,
			wantErr:  []error{errSnapshotLoad},
			wantText: "service: load snapshot: no replay fragment within 20ms",
		},
		{
			name:     "LoadSnapshot error",
			frames:   [][]byte{begin, end, userBytes},
			loadErr:  loadErr,
			wantErr:  []error{errSnapshotLoad, loadErr},
			wantText: "service: load snapshot: LoadSnapshot: bad state",
			wantLoad: true,
		},
		{
			name:     "clean load",
			frames:   [][]byte{begin, end, userBytes},
			wantLoad: true,
		},
		{
			name:     "clean load with a stop error",
			frames:   [][]byte{begin, end, userBytes},
			stopErr:  stopErr,
			wantErr:  []error{errSnapshotLoad, stopErr},
			wantText: "service: load snapshot: stop replay: stop failed",
			wantLoad: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			replay := &fakeSnapshotReplay{
				sessionID: replaySessionID,
				startErr:  tt.startErr,
				stopErr:   tt.stopErr,
			}
			var image *aeron.Image
			adder := &snapshotLoadAdder{err: tt.addErr}
			if tt.frames != nil {
				adder.onAdd = func(sub *aeron.Subscription) {
					image = aeron.NewTestImageForSubscriptionHooksWithSessionID(t, sub, 31, 9_501, 0)
					writeSnapshotFrames(t, image, tt.frames)
					require.True(t, aeron.SubscriptionAddImageForHooks(sub, image))
				}
			}
			pumps := 0
			svc := &loadRecordingService{loadErr: tt.loadErr}
			timeout := tt.timeout
			if timeout == 0 {
				timeout = time.Second
			}
			load := snapshotLoad{
				control:       replay,
				subscriptions: adder,
				pump: func() int {
					pumps++
					if pumps == tt.closeAfter {
						aeron.ImageInternalCloseForHooks(image)
					}
					return 0
				},
				channel:  DefaultReplayChannel,
				streamID: DefaultReplayStreamID,
				timeout:  timeout,
				handler:  svc,
			}
			agent := NewAgent(AgentConfig{})

			err := load.run(agent, 42, snapshotLoadLogPosition)

			for _, want := range tt.wantErr {
				require.ErrorIs(t, err, want)
			}
			if tt.wantErr == nil {
				require.NoError(t, err)
			}
			if tt.wantText != "" {
				require.EqualError(t, err, tt.wantText)
			}
			require.Len(t, replay.startCalls, 1)
			started := 0
			if tt.startErr == nil {
				started = 1
			}
			require.Len(t, adder.calls, started, "subscription adds")
			if started > 0 {
				require.Equal(t, "aeron:ipc?session-id=31", adder.calls[0].channel)
			}
			require.Len(t, replay.stopCalls, started, "StopReplay calls")
			require.Equal(t, pumps, replay.pollCalls, "one archive Poll per pump")
			for _, stop := range replay.stopCalls {
				require.Equal(t, replaySessionID, stop)
			}
			for _, sub := range adder.subs {
				require.True(t, sub.IsClosed(), "replay subscription closed")
			}
			if !tt.wantLoad {
				require.Empty(t, svc.loadPositions, "LoadSnapshot calls")
				return
			}
			require.Equal(t, []int64{userStart}, svc.loadPositions, "image position at LoadSnapshot")
			require.Equal(t, [][]byte{userBytes}, svc.loadBytes, "user bytes")
		})
	}
}

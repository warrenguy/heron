//go:build testhooks

package consensus

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func boundLogPublisher(t testing.TB) (*LogPublisher, *aeron.ExclusivePublication) {
	t.Helper()
	pub := aeron.NewTestExclusivePublicationForHooks(t)
	p := NewLogPublisher()
	p.SetPublication(pub)
	return p, pub
}

func boundLogPublisherWithLimit(
	t testing.TB,
	limitPosition int64,
	connected bool,
) *LogPublisher {
	t.Helper()
	pub := aeron.NewTestExclusivePublicationForHooksWithLimit(t, limitPosition, connected)
	p := NewLogPublisher()
	p.SetPublication(pub)
	return p
}

// termBufferSource is any publication exposing term buffers for test decode.
type termBufferSource interface {
	TermBuffer(termIndex int) atomicbuffer.AtomicBuffer
}

// termWireAt returns the DATA frame payload at termOffset in term 0 and the
// next aligned term offset.
func termWireAt(
	t *testing.T,
	pub termBufferSource,
	termOffset int,
) ([]byte, int) {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, termOffset))
	require.Positive(t, frameLength)
	wire := termBuf.Bytes()[termOffset+logbuffer.OffsetFramePayload : termOffset+frameLength]
	return wire, termOffset + logbuffer.AlignFrameLength(frameLength)
}

func termStartWire(t *testing.T, pub *aeron.ExclusivePublication) []byte {
	t.Helper()
	wire, _ := termWireAt(t, pub, 0)
	return wire
}

func decodeNLTAtTermStart(
	t *testing.T,
	pub *aeron.ExclusivePublication,
) codecs.NewLeadershipTermEventFields {
	t.Helper()
	fields, decoded := codecs.TryDecodeNewLeadershipTermEventAt(termStartWire(t, pub), 0)
	require.True(t, decoded)
	return fields
}

func TestLogPublisher_unbound(t *testing.T) {
	p := NewLogPublisher()
	require.NotNil(t, p)
	require.Nil(t, p.Publication())
	require.Equal(t, int64(0), p.Position())
	require.Equal(t, int32(0), p.SessionID())
	require.Empty(t, p.scratch)

	require.Equal(t, int64(0), (*LogPublisher)(nil).Position())
	require.Equal(t, int32(0), (*LogPublisher)(nil).SessionID())
}

func TestLogPublisher_SetPublication_scratchSize(t *testing.T) {
	p, pub := boundLogPublisher(t)
	need := pub.MaxPayloadLength()
	require.Positive(t, need)

	require.Same(t, pub, p.Publication())
	require.Len(t, p.scratch, need)
	require.GreaterOrEqual(t, cap(p.scratch), need)
	require.Equal(t, pub.Position(), p.Position())
	require.Equal(t, pub.SessionID(), p.SessionID())
}

func TestLogPublisher_SetPublication_nilClearsBind(t *testing.T) {
	p, pub := boundLogPublisher(t)
	require.Len(t, p.scratch, pub.MaxPayloadLength())

	p.SetPublication(nil)
	require.Nil(t, p.Publication())
	require.Equal(t, int64(0), p.Position())
	require.Equal(t, int32(0), p.SessionID())

	// Scratch capacity retained; only publication binding cleared.
	require.Len(t, p.scratch, pub.MaxPayloadLength())
}

func TestLogPublisher_SetPublication_rebindReusesScratch(t *testing.T) {
	p, pub := boundLogPublisher(t)
	need := pub.MaxPayloadLength()
	first := p.scratch

	p.SetPublication(pub)
	require.Len(t, p.scratch, need)
	require.Equal(t, cap(first), cap(p.scratch))

	// Same backing array when capacity already sufficient.
	require.Equal(t, &first[0], &p.scratch[0])
}

func TestLogPublisher_AppendMessage_oversize(t *testing.T) {
	p, pub := boundLogPublisher(t)

	headerLen := codecs.SessionMessageHeaderEncodedLen()
	// Above MaxMessageLength: fail-closed UnsupportedMessageLength.
	oversize := make([]byte, pub.MaxMessageLength()-headerLen+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.AppendMessage(1, 2, 3, oversize))

	// Position stays 0: oversize rejected before Offer.
	require.Equal(t, int64(0), p.Position())
}

func TestLogPublisher_AppendMessage_abovePayloadCap(t *testing.T) {
	p, pub := boundLogPublisher(t)

	headerLen := codecs.SessionMessageHeaderEncodedLen()
	// One byte past MaxPayloadLength total: fragmented offer when MaxMessageLength allows.
	total := pub.MaxPayloadLength() + 1
	if total > pub.MaxMessageLength() {
		t.Skip("test publication MaxMessageLength too small for mid-band payload")
	}
	position := p.AppendMessage(1, 2, 3, make([]byte, total-headerLen))
	require.Positive(t, position)

	// Multi-frame train: first frame BEGIN-only; walk to END frame.
	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	first := logbuffer.FrameFlags(termBuf, 0)
	require.Equal(t, uint8(logbuffer.BeginFlag), first&logbuffer.BeginFlag)
	require.Equal(t, uint8(0), first&logbuffer.EndFlag)

	off := logbuffer.AlignFrameLength(frameLength)
	var last uint8
	for range 15 {
		frameLength = int(logbuffer.FrameLengthVolatile(termBuf, off))
		require.Positive(t, frameLength)
		last = logbuffer.FrameFlags(termBuf, off)
		if last&logbuffer.EndFlag != 0 {
			break
		}
		off += logbuffer.AlignFrameLength(frameLength)
	}
	require.Equal(t, uint8(logbuffer.EndFlag), last&logbuffer.EndFlag)
	require.Equal(t, uint8(0), last&logbuffer.BeginFlag)
}

func TestLogPublisher_AppendMessage_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 11
		clusterSessionID int64 = 22
		timestamp        int64 = 33
	)
	payload := []byte("log-app-payload")
	position := p.AppendMessage(leadershipTermID, clusterSessionID, timestamp, payload)
	require.Positive(t, position)

	wire := termStartWire(t, pub)
	fields, ok := codecs.TryDecodeSessionMessageHeaderAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, clusterSessionID, fields.ClusterSessionID)
	require.Equal(t, timestamp, fields.Timestamp)

	headerLen := codecs.SessionMessageHeaderEncodedLen()
	require.Equal(t, payload, wire[headerLen:])
}

// Asserts AppendMessage grows scratch when the bound buffer cannot hold the
// total. Asserts the position advances on success.
func TestLogPublisher_AppendMessage_staleScratchGrows(t *testing.T) {
	p, _ := boundLogPublisher(t)
	p.scratch = nil

	require.Positive(t, p.AppendMessage(1, 2, 3, []byte("payload")))
	require.GreaterOrEqual(
		t,
		cap(p.scratch),
		len("payload")+codecs.SessionMessageHeaderEncodedLen(),
	)
}

// TestLogPublisher_appendFailurePaths asserts every append method against the
// shared failure matrix: nil receiver, unbound publisher, retriable exhaustion,
// and terminal close. Failed appends do not mutate publication state, so one
// setup serves every append method.
func TestLogPublisher_appendFailurePaths(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T) *LogPublisher
		wantCode int64
	}{
		{
			name:     "nil receiver",
			setup:    func(t *testing.T) *LogPublisher { return nil },
			wantCode: aeron.Closed,
		},
		{
			name:     "unbound publisher",
			setup:    func(t *testing.T) *LogPublisher { return NewLogPublisher() },
			wantCode: aeron.Closed,
		},
		{
			name: "not connected exhausts",
			setup: func(t *testing.T) *LogPublisher {
				return boundLogPublisherWithLimit(t, 0, false)
			},
			wantCode: aeron.NotConnected,
		},
		{
			name: "back pressured exhausts",
			setup: func(t *testing.T) *LogPublisher {
				return boundLogPublisherWithLimit(t, 0, true)
			},
			wantCode: aeron.BackPressured,
		},
		{
			name: "closed terminal",
			setup: func(t *testing.T) *LogPublisher {
				p, pub := boundLogPublisher(t)
				require.NoError(t, pub.Close())
				return p
			},
			wantCode: aeron.Closed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setup(t)
			require.Equal(t, tt.wantCode, p.AppendMessage(1, 2, 3, []byte("x")))
			require.False(t, p.AppendNewLeadershipTermEvent(
				1, 2, 3, 4, 5, clusterwire.ClusterTimeUnit_NANOS, 6,
			))
			require.Equal(t, tt.wantCode, p.AppendSessionOpen(
				1, 2, 3, 4, 5, []byte("ch"), []byte("pr"),
			))
			require.False(t, p.AppendSessionClose(
				1, 2, 3, clusterwire.CloseReason_CLIENT_ACTION,
			))
			require.False(t, p.AppendTimer(1, 2, 3))
			actionPos, actionOK := p.AppendClusterAction(
				1, 2, clusterwire.ClusterAction_SNAPSHOT, 0,
			)
			require.False(t, actionOK)
			require.Equal(t, int64(0), actionPos)
		})
	}
}

func TestLogPublisher_AppendSessionOpen_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 11
		correlationID    int64 = 22
		clusterSessionID int64 = 33
		timestamp        int64 = 44
		responseStreamID int32 = 55
	)
	responseChannel := []byte("aeron:udp?endpoint=localhost:0")
	encodedPrincipal := []byte{0x01, 0x02, 0x03}

	position := p.AppendSessionOpen(
		leadershipTermID,
		correlationID,
		clusterSessionID,
		timestamp,
		responseStreamID,
		responseChannel,
		encodedPrincipal,
	)
	require.Positive(t, position)

	wire := termStartWire(t, pub)
	fields, ok := codecs.TryDecodeSessionOpenEventAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, correlationID, fields.CorrelationID)
	require.Equal(t, clusterSessionID, fields.ClusterSessionID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, responseStreamID, fields.ResponseStreamID)
	require.Equal(t, responseChannel, fields.ResponseChannel)
	require.Equal(t, encodedPrincipal, fields.EncodedPrincipal)
}

func TestLogPublisher_AppendSessionOpen_emptyPrincipal(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 1
		correlationID    int64 = 2
		clusterSessionID int64 = 3
		timestamp        int64 = 4
		responseStreamID int32 = 5
	)
	responseChannel := []byte("ch")

	position := p.AppendSessionOpen(
		leadershipTermID,
		correlationID,
		clusterSessionID,
		timestamp,
		responseStreamID,
		responseChannel,
		nil,
	)
	require.Positive(t, position)

	fields, ok := codecs.TryDecodeSessionOpenEventAt(termStartWire(t, pub), 0)
	require.True(t, ok)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, correlationID, fields.CorrelationID)
	require.Equal(t, clusterSessionID, fields.ClusterSessionID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, responseStreamID, fields.ResponseStreamID)
	require.Equal(t, responseChannel, fields.ResponseChannel)
	require.Empty(t, fields.EncodedPrincipal)
}

func TestLogPublisher_AppendSessionOpen_oversize(t *testing.T) {
	p, pub := boundLogPublisher(t)

	emptyFixed, ok := codecs.ClusterWireMessageLen(
		clusterwire.SessionOpenEventSbeBlockLength, 0, 0,
	)
	require.True(t, ok)
	// Above MaxMessageLength: fail-closed UnsupportedMessageLength.
	oversize := make([]byte, pub.MaxMessageLength()-emptyFixed+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.AppendSessionOpen(
		1, 2, 3, 4, 5, oversize, nil,
	))
	require.Equal(t, int64(0), p.Position())

	overVar := make([]byte, codecs.ClusterEncodeMaxVarLen+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.AppendSessionOpen(
		1, 2, 3, 4, 5, overVar, nil,
	))
	require.Equal(t, int64(0), p.Position())
}

// Asserts AppendSessionOpen grows scratch when the bound buffer cannot hold
// the encoded total.
func TestLogPublisher_AppendSessionOpen_staleScratchGrows(t *testing.T) {
	p, _ := boundLogPublisher(t)
	p.scratch = nil

	require.Positive(t, p.AppendSessionOpen(
		1, 2, 3, 4, 5, []byte("ch"), []byte("pr"),
	))
	require.Positive(t, cap(p.scratch))
}

func TestLogPublisher_AppendSessionClose_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 7
		clusterSessionID int64 = 8
		timestamp        int64 = 9
	)
	closeReason := clusterwire.CloseReason_SERVICE_ACTION
	pre := p.Position()
	claimLen := codecs.SessionCloseEventEncodedLen()
	aligned := int64(logbuffer.AlignFragmentChunkLength(claimLen))

	ok := p.AppendSessionClose(leadershipTermID, clusterSessionID, timestamp, closeReason)
	require.True(t, ok)

	wire := termStartWire(t, pub)
	fields, decoded := codecs.TryDecodeSessionCloseEventAt(wire, 0)
	require.True(t, decoded)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, clusterSessionID, fields.ClusterSessionID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, closeReason, fields.CloseReason)
	require.Equal(t, pre+aligned, p.Position())
}

func TestLogPublisher_AppendTimer_happy(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 10
		correlationID    int64 = 20
		timestamp        int64 = 30
	)
	pre := p.Position()
	claimLen := codecs.TimerEventEncodedLen()
	aligned := int64(logbuffer.AlignFragmentChunkLength(claimLen))

	ok := p.AppendTimer(leadershipTermID, correlationID, timestamp)
	require.True(t, ok)

	wire := termStartWire(t, pub)
	fields, decoded := codecs.TryDecodeTimerEventAt(wire, 0)
	require.True(t, decoded)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, correlationID, fields.CorrelationID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, pre+aligned, p.Position())
}

func TestLogPublisher_AppendNLT_success(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID    int64 = 7
		timestamp           int64 = 1001
		termBaseLogPosition int64 = 42
		leaderMemberID      int32 = 3
		logSessionID        int32 = 9
		appVersion          int32 = 1
	)
	timeUnit := clusterwire.ClusterTimeUnit_NANOS
	pre := p.Position()
	claimLen := codecs.NewLeadershipTermEventEncodedLen()
	aligned := int64(logbuffer.AlignFragmentChunkLength(claimLen))

	ok := p.AppendNewLeadershipTermEvent(
		leadershipTermID,
		timestamp,
		termBaseLogPosition,
		leaderMemberID,
		logSessionID,
		timeUnit,
		appVersion,
	)
	require.True(t, ok)

	fields := decodeNLTAtTermStart(t, pub)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, termBaseLogPosition, fields.TermBaseLogPosition)
	require.Equal(t, leaderMemberID, fields.LeaderMemberID)
	require.Equal(t, logSessionID, fields.LogSessionID)
	require.Equal(t, timeUnit, fields.TimeUnit)
	require.Equal(t, appVersion, fields.AppVersion)

	// LogPosition on the wire is the successful claim end position (pre + aligned).
	require.Equal(t, pre+aligned, fields.LogPosition)
	require.Equal(t, pre+aligned, p.Position())
}

func TestLogPublisher_AppendClusterAction_success(t *testing.T) {
	p, pub := boundLogPublisher(t)

	const (
		leadershipTermID int64 = 7
		timestamp        int64 = 1001
		flags            int32 = 4
	)
	action := clusterwire.ClusterAction_SNAPSHOT
	pre := p.Position()
	claimLen := codecs.ClusterActionRequestEncodedLen()
	aligned := int64(logbuffer.AlignFragmentChunkLength(claimLen))

	pos, ok := p.AppendClusterAction(leadershipTermID, timestamp, action, flags)
	require.True(t, ok)

	fields := decodeClusterActionAtTermStart(t, pub)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, timestamp, fields.Timestamp)
	require.Equal(t, action, fields.Action)
	require.Equal(t, flags, fields.Flags)

	// LogPosition on the wire is the successful claim end position (pre + aligned).
	require.Equal(t, pre+aligned, fields.LogPosition)
	require.Equal(t, pre+aligned, pos)
	require.Equal(t, pre+aligned, p.Position())
}

func TestAgent_AppendNewLeadershipTermEvent_nilPublisher(t *testing.T) {
	a := NewAgent(AgentConfig{})
	require.False(t, a.AppendNewLeadershipTermEvent(1000))

	// Non-nil publisher without bound publication also fails closed.
	a.SetLogPublisher(NewLogPublisher())
	require.False(t, a.AppendNewLeadershipTermEvent(1000))
}

func TestAgent_AppendNewLeadershipTermEvent_viaJoinLogAsLeader(t *testing.T) {
	lp, pub := boundLogPublisher(t)

	const (
		thisMemberID     int32 = 5
		leadershipTermID int64 = 3
		joinLogPos       int64 = 777
		nowNs            int64 = 5000
		appVersion       int32 = 2
	)
	a := NewAgent(AgentConfig{
		ThisMemberID:     thisMemberID,
		LeadershipTermID: 0,
		AppVersion:       appVersion,
	})
	a.SetLogPublisher(lp)
	require.NoError(t, a.JoinLogAsLeader(leadershipTermID, joinLogPos, pub.SessionID(), false))

	require.True(t, a.AppendNewLeadershipTermEvent(nowNs))

	fields := decodeNLTAtTermStart(t, pub)
	require.Equal(t, leadershipTermID, fields.LeadershipTermID)
	require.Equal(t, nowNs, fields.Timestamp)
	require.Equal(t, joinLogPos, fields.TermBaseLogPosition)
	require.Equal(t, thisMemberID, fields.LeaderMemberID)
	require.Equal(t, pub.SessionID(), fields.LogSessionID)
	require.Equal(t, clusterwire.ClusterTimeUnit_NANOS, fields.TimeUnit)
	require.Equal(t, appVersion, fields.AppVersion)
	require.Equal(t, lp.Position(), fields.LogPosition)
	require.Positive(t, fields.LogPosition)
}

func TestAgent_AppendNewLeadershipTermEvent_termBaseSources(t *testing.T) {
	tests := []struct {
		name     string
		newAgent func(lp *LogPublisher) *Agent
		wantBase int64
	}{
		{
			name: "default zero without join or setter",
			newAgent: func(lp *LogPublisher) *Agent {
				a := NewAgent(AgentConfig{ThisMemberID: 1, LeadershipTermID: 2})
				a.SetLogPublisher(lp)
				return a
			},
			wantBase: 0,
		},
		{
			name: "setter",
			newAgent: func(lp *LogPublisher) *Agent {
				a := NewAgent(AgentConfig{ThisMemberID: 1, LeadershipTermID: 2})
				a.SetLogPublisher(lp)
				a.SetTermBaseLogPosition(42)
				return a
			},
			wantBase: 42,
		},
		{
			name: "config seed",
			newAgent: func(lp *LogPublisher) *Agent {
				return NewAgent(AgentConfig{
					ThisMemberID:        1,
					LeadershipTermID:    2,
					TermBaseLogPosition: 99,
					LogPublisher:        lp,
				})
			},
			wantBase: 99,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp, pub := boundLogPublisher(t)
			a := tt.newAgent(lp)
			require.Equal(t, tt.wantBase, a.termBaseLogPosition)
			require.True(t, a.AppendNewLeadershipTermEvent(10))
			fields := decodeNLTAtTermStart(t, pub)
			require.Equal(t, tt.wantBase, fields.TermBaseLogPosition)
		})
	}
}

func TestAgent_AddLogPublication_inject(t *testing.T) {
	a := NewAgent(AgentConfig{})
	sessionID, err := a.AddLogPublication(0)
	require.NoError(t, err)
	require.Equal(t, int32(-1), sessionID)

	// Publisher without publication still unbound.
	a.SetLogPublisher(NewLogPublisher())
	sessionID, err = a.AddLogPublication(0)
	require.NoError(t, err)
	require.Equal(t, int32(-1), sessionID)

	lp, pub := boundLogPublisher(t)

	// Required inject order: SetLogPublisher + SetPublication before call.
	a.SetLogPublisher(lp)
	got, err := a.AddLogPublication(100)
	require.NoError(t, err)
	require.Equal(t, pub.SessionID(), got)
	require.Equal(t, lp.SessionID(), got)

	// Unbind returns to -1.
	lp.SetPublication(nil)
	sessionID, err = a.AddLogPublication(0)
	require.NoError(t, err)
	require.Equal(t, int32(-1), sessionID)
}

// Asserts reseed failure reports the session the publisher still holds.
// Reseed closes the old publication before opening the new one. A failure
// after that close must not report the pre-reseed session id. A later call
// reseeds from the unbound state.
func TestAddLogPublication_reseedFailureReportsBoundSession(t *testing.T) {
	t.Run("failure after unbind returns null session", func(t *testing.T) {
		publisher, _ := boundLogPublisher(t)
		a := NewAgent(AgentConfig{})
		a.SetLogPublisher(publisher)
		a.SetLogRecordingID(1)
		cause := errors.New("add exclusive publication failed")
		a.SetLogPublicationReseed(func(int64) (int32, error) {
			publisher.SetPublication(nil)
			return 0, cause
		})

		sessionID, err := a.AddLogPublication(100)
		require.ErrorIs(t, err, cause)
		require.Equal(t, int32(nullValue), sessionID)
	})

	// The test publication sits at 0, so the first call targets 100 to reach
	// the hook.
	retries := []struct {
		name   string
		target int64
	}{
		{name: "retry after unbind returns the rebuilt session", target: 100},
		{name: "retry after unbind rebuilds at target 0", target: 0},
	}
	for _, tt := range retries {
		t.Run(tt.name, func(t *testing.T) {
			const rebuilt int32 = 4242

			publisher, _ := boundLogPublisher(t)
			a := NewAgent(AgentConfig{})
			a.SetLogPublisher(publisher)
			a.SetLogRecordingID(1)
			cause := errors.New("add exclusive publication failed")
			var positions []int64
			a.SetLogPublicationReseed(func(position int64) (int32, error) {
				positions = append(positions, position)
				if len(positions) == 1 {
					publisher.SetPublication(nil)
					return 0, cause
				}
				publisher.SetPublication(aeron.NewTestExclusivePublicationForHooks(t))
				return rebuilt, nil
			})

			// Phase: the add fails after the unbind.
			sessionID, err := a.AddLogPublication(100)
			require.ErrorIs(t, err, cause)
			require.Equal(t, int32(nullValue), sessionID)

			// Phase: the retry rebuilds from the unbound state.
			sessionID, err = a.AddLogPublication(tt.target)
			require.NoError(t, err)
			require.Equal(t, rebuilt, sessionID)
			require.Equal(t, []int64{100, tt.target}, positions)
		})
	}

	t.Run("failure before unbind returns the live session", func(t *testing.T) {
		publisher, _ := boundLogPublisher(t)
		a := NewAgent(AgentConfig{})
		a.SetLogPublisher(publisher)
		a.SetLogRecordingID(1)
		cause := errors.New("list recording failed")
		a.SetLogPublicationReseed(func(int64) (int32, error) { return 0, cause })

		sessionID, err := a.AddLogPublication(100)
		require.ErrorIs(t, err, cause)
		require.Equal(t, publisher.SessionID(), sessionID)
	})

	t.Run("success returns the reseeded session", func(t *testing.T) {
		const reseeded int32 = 4242

		publisher, _ := boundLogPublisher(t)
		a := NewAgent(AgentConfig{})
		a.SetLogPublisher(publisher)
		a.SetLogRecordingID(1)
		a.SetLogPublicationReseed(func(int64) (int32, error) { return reseeded, nil })

		sessionID, err := a.AddLogPublication(100)
		require.NoError(t, err)
		require.Equal(t, reseeded, sessionID)
	})
}

// Asserts AddLogPublication reseeds only with a publisher and a known
// recording id, when the publication is unbound or off target.
func TestAddLogPublication_reseedsOnPositionMismatch(t *testing.T) {
	const reseeded int32 = 4242

	tests := []struct {
		name          string
		nilPublisher  bool
		noPublication bool
		recordingID   int64
		appendFirst   bool
		target        int64
		wantReseed    bool
	}{
		{
			name:        "ahead of target reseeds",
			recordingID: 1,
			appendFirst: true,
			target:      0,
			wantReseed:  true,
		},
		{
			name:        "behind target reseeds",
			recordingID: 1,
			target:      100,
			wantReseed:  true,
		},
		{
			name:        "equal to target keeps the publication",
			recordingID: 1,
			target:      0,
		},
		{
			name:        "negative recording id keeps the publication",
			recordingID: nullValue,
			appendFirst: true,
			target:      0,
		},
		{
			name:         "nil publisher returns null session",
			nilPublisher: true,
			recordingID:  1,
			target:       100,
		},
		{
			name:          "unbound publication reseeds at target 0",
			noPublication: true,
			recordingID:   1,
			target:        0,
			wantReseed:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAgent(AgentConfig{})
			a.SetLogRecordingID(tt.recordingID)
			want := int32(nullValue)
			switch {
			case tt.noPublication:
				a.SetLogPublisher(NewLogPublisher())
			case !tt.nilPublisher:
				publisher, _ := boundLogPublisher(t)
				if tt.appendFirst {
					require.Positive(t, publisher.AppendMessage(1, 2, 3, []byte("entry")))
				}
				a.SetLogPublisher(publisher)
				want = publisher.SessionID()
			}
			called := false
			a.SetLogPublicationReseed(func(position int64) (int32, error) {
				if !tt.wantReseed {
					t.Fatalf("unexpected reseed at %d", position)
				}
				require.Equal(t, tt.target, position)
				called = true
				return reseeded, nil
			})
			if tt.wantReseed {
				want = reseeded
			}

			sessionID, err := a.AddLogPublication(tt.target)
			require.NoError(t, err)
			require.Equal(t, want, sessionID)
			require.Equal(t, tt.wantReseed, called)
		})
	}
}

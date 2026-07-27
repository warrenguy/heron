//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
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

func termStartWire(t *testing.T, pub *aeron.ExclusivePublication) []byte {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	require.Positive(t, frameLength)
	return termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLength]
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
	oversize := make([]byte, pub.MaxPayloadLength()-headerLen+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.AppendMessage(1, 2, 3, oversize))

	// Position stays 0: oversize rejected before Offer.
	require.Equal(t, int64(0), p.Position())
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

// TestLogPublisher_appendFailurePaths drives AppendMessage and
// AppendNewLeadershipTermEvent through the shared failure matrix: nil receiver,
// unbound publisher, retriable exhaustion, and terminal close. Failed appends
// do not mutate publication state, so one setup serves both calls.
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
		})
	}
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

	// LogPosition on wire is successful claim end position (pre + aligned).
	require.Equal(t, pre+aligned, fields.LogPosition)
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
	a.JoinLogAsLeader(leadershipTermID, joinLogPos, pub.SessionID(), false)

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
	require.Equal(t, int32(-1), a.AddLogPublication(0))

	// Publisher without publication still unbound.
	a.SetLogPublisher(NewLogPublisher())
	require.Equal(t, int32(-1), a.AddLogPublication(0))

	lp, pub := boundLogPublisher(t)

	// Required inject order: SetLogPublisher + SetPublication before call.
	a.SetLogPublisher(lp)
	got := a.AddLogPublication(100)
	require.Equal(t, pub.SessionID(), got)
	require.Equal(t, lp.SessionID(), got)

	// Unbind returns to -1.
	lp.SetPublication(nil)
	require.Equal(t, int32(-1), a.AddLogPublication(0))
}

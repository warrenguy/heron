//go:build testhooks

package consensus

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
	"github.com/warrenguy/heron/cluster/codecs/clusterwire"
)

func boundServiceProxy(t testing.TB) (*ServiceProxy, *aeron.Publication) {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	p := NewServiceProxy()
	p.SetPublication(pub)
	return p, pub
}

func boundServiceProxyWithLimit(
	t testing.TB,
	limitPosition int64,
	connected bool,
) *ServiceProxy {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithLimit(t, limitPosition, connected)
	p := NewServiceProxy()
	p.SetPublication(pub)
	return p
}

func sampleJoinLogFields() codecs.JoinLogFields {
	return codecs.JoinLogFields{
		LogPosition:    100,
		MaxLogPosition: math.MaxInt64,
		MemberID:       1,
		LogSessionID:   7,
		LogStreamID:    100,
		IsStartup:      clusterwire.BooleanType_TRUE,
		Role:           int32(RoleLeader),
		IsStandby:      clusterwire.BooleanType_FALSE,
		LogChannel:     []byte("aeron:ipc"),
	}
}

func TestServiceProxy_unbound(t *testing.T) {
	p := NewServiceProxy()
	require.NotNil(t, p)
	require.Nil(t, p.Publication())
	require.Empty(t, p.scratch)
}

func TestServiceProxy_SetPublication_scratchSize(t *testing.T) {
	p, pub := boundServiceProxy(t)
	need := pub.MaxPayloadLength()
	require.Positive(t, need)

	require.Same(t, pub, p.Publication())
	require.Len(t, p.scratch, need)
	require.GreaterOrEqual(t, cap(p.scratch), need)
}

func TestServiceProxy_SetPublication_nilClearsBind(t *testing.T) {
	p, pub := boundServiceProxy(t)
	require.Len(t, p.scratch, pub.MaxPayloadLength())

	p.SetPublication(nil)
	require.Nil(t, p.Publication())
	require.Equal(t, aeron.Closed, p.PublishJoinLog(sampleJoinLogFields()))

	// Scratch capacity retained; only publication binding cleared.
	require.Len(t, p.scratch, pub.MaxPayloadLength())
}

func TestServiceProxy_SetPublication_rebindReusesScratch(t *testing.T) {
	p, pub := boundServiceProxy(t)
	need := pub.MaxPayloadLength()
	first := p.scratch

	p.SetPublication(pub)
	require.Len(t, p.scratch, need)
	require.Equal(t, cap(first), cap(p.scratch))

	// Same backing array when capacity already sufficient.
	require.Equal(t, &first[0], &p.scratch[0])
}

func TestServiceProxy_PublishJoinLog_happy(t *testing.T) {
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)

	emptyChannel := sampleJoinLogFields()
	emptyChannel.LogChannel = nil

	notStartupFollower := sampleJoinLogFields()
	notStartupFollower.IsStartup = clusterwire.BooleanType_FALSE
	notStartupFollower.Role = int32(RoleFollower)

	tests := []struct {
		name   string
		fields codecs.JoinLogFields
	}{
		{name: "startup_leader", fields: sampleJoinLogFields()},
		{name: "empty_channel", fields: emptyChannel},
		{name: "not_startup_follower", fields: notStartupFollower},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, pub := boundServiceProxy(t)

			pos := p.PublishJoinLog(tt.fields)
			require.Positive(t, pos)

			wire, _ := termWireAt(t, pub, 0)
			wantLen, ok := codecs.ClusterWireMessageLen(
				codecs.JoinLogBlockLength(actingVersion),
				len(tt.fields.LogChannel),
			)
			require.True(t, ok)
			require.Len(t, wire, wantLen)

			got, decOK := codecs.TryDecodeJoinLogAt(wire, 0)
			require.True(t, decOK)
			require.Equal(t, codecs.JoinLogOracleFields(tt.fields, actingVersion), got)
		})
	}
}

func TestServiceProxy_PublishJoinLog_oversize(t *testing.T) {
	p, pub := boundServiceProxy(t)
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)

	emptyFixed, ok := codecs.ClusterWireMessageLen(
		codecs.JoinLogBlockLength(actingVersion),
		0,
	)
	require.True(t, ok)

	// Above MaxMessageLength: fail-closed UnsupportedMessageLength.
	fields := sampleJoinLogFields()
	fields.LogChannel = make([]byte, pub.MaxMessageLength()-emptyFixed+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.PublishJoinLog(fields))
	require.Equal(t, int64(0), pub.Position())

	// Per-field var length above ClusterEncodeMaxVarLen.
	overVar := sampleJoinLogFields()
	overVar.LogChannel = make([]byte, codecs.ClusterEncodeMaxVarLen+1)
	require.Equal(t, aeron.UnsupportedMessageLength, p.PublishJoinLog(overVar))
	require.Equal(t, int64(0), pub.Position())
}

// Asserts PublishJoinLog returns Closed for nil or unbound proxy when
// LogChannel is oversize, not UnsupportedMessageLength.
func TestServiceProxy_PublishJoinLog_unboundOversizeClosed(t *testing.T) {
	overVar := sampleJoinLogFields()
	overVar.LogChannel = make([]byte, codecs.ClusterEncodeMaxVarLen+1)

	require.Equal(t, aeron.Closed, (*ServiceProxy)(nil).PublishJoinLog(overVar))
	require.Equal(t, aeron.Closed, NewServiceProxy().PublishJoinLog(overVar))
}

// Asserts PublishJoinLog fragments when total is above MaxPayloadLength and
// within MaxMessageLength.
func TestServiceProxy_PublishJoinLog_abovePayloadCap(t *testing.T) {
	p, pub := boundServiceProxy(t)
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)

	emptyFixed, ok := codecs.ClusterWireMessageLen(
		codecs.JoinLogBlockLength(actingVersion),
		0,
	)
	require.True(t, ok)

	// One byte past MaxPayloadLength: fragments when MaxMessageLength allows.
	total := pub.MaxPayloadLength() + 1
	if total > pub.MaxMessageLength() {
		t.Skip("test publication MaxMessageLength too small for mid-band JoinLog")
	}
	channelLen := total - emptyFixed
	require.Positive(t, channelLen)

	fields := sampleJoinLogFields()
	fields.LogChannel = make([]byte, channelLen)
	pos := p.PublishJoinLog(fields)
	require.Positive(t, pos)
	require.GreaterOrEqual(t, cap(p.scratch), total)

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

// Asserts PublishJoinLog grows scratch when the bound buffer cannot hold
// the encoded total.
func TestServiceProxy_PublishJoinLog_staleScratchGrows(t *testing.T) {
	p, _ := boundServiceProxy(t)
	p.scratch = nil

	fields := sampleJoinLogFields()
	actingVersion := uint16(clusterwire.JoinLogSbeSchemaVersion)
	need, ok := codecs.ClusterWireMessageLen(
		codecs.JoinLogBlockLength(actingVersion),
		len(fields.LogChannel),
	)
	require.True(t, ok)

	require.Positive(t, p.PublishJoinLog(fields))
	require.GreaterOrEqual(t, cap(p.scratch), need)
}

func TestServiceProxy_PublishRequestServiceAck_happy(t *testing.T) {
	p, pub := boundServiceProxy(t)
	const logPos int64 = 4242

	pos := p.PublishRequestServiceAck(logPos)
	require.Positive(t, pos)

	wire, _ := termWireAt(t, pub, 0)
	require.Len(t, wire, codecs.RequestServiceAckEncodedLen())

	got, ok := codecs.TryDecodeRequestServiceAckAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, logPos, got.LogPosition)
}

// Asserts every publish method against the shared failure matrix: nil receiver,
// unbound publisher, retriable exhaustion, and terminal close. Failed publishes
// do not mutate publication state, so one setup serves both calls.
func TestServiceProxy_publishFailurePaths(t *testing.T) {
	fields := sampleJoinLogFields()
	tests := []struct {
		name     string
		setup    func(t *testing.T) *ServiceProxy
		wantCode int64
	}{
		{
			name:     "nil receiver",
			setup:    func(t *testing.T) *ServiceProxy { return nil },
			wantCode: aeron.Closed,
		},
		{
			name:     "unbound publisher",
			setup:    func(t *testing.T) *ServiceProxy { return NewServiceProxy() },
			wantCode: aeron.Closed,
		},
		{
			name: "not connected exhausts",
			setup: func(t *testing.T) *ServiceProxy {
				return boundServiceProxyWithLimit(t, 0, false)
			},
			wantCode: aeron.NotConnected,
		},
		{
			name: "back pressured exhausts",
			setup: func(t *testing.T) *ServiceProxy {
				return boundServiceProxyWithLimit(t, 0, true)
			},
			wantCode: aeron.BackPressured,
		},
		{
			name: "closed terminal",
			setup: func(t *testing.T) *ServiceProxy {
				p, pub := boundServiceProxy(t)
				require.NoError(t, pub.Close())
				return p
			},
			wantCode: aeron.Closed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setup(t)
			require.Equal(t, tt.wantCode, p.PublishJoinLog(fields))
			require.Equal(t, tt.wantCode, p.PublishRequestServiceAck(99))
		})
	}
}

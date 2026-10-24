//go:build testhooks

package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/cluster/codecs"
)

func boundModuleProxy(t testing.TB) (*ConsensusModuleProxy, *aeron.Publication) {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithConnected(t, true)
	p := NewConsensusModuleProxy()
	p.SetPublication(pub)
	return p, pub
}

func boundModuleProxyWithLimit(
	t testing.TB,
	limitPosition int64,
	connected bool,
) *ConsensusModuleProxy {
	t.Helper()
	pub := aeron.NewTestPublicationForHooksWithLimit(t, limitPosition, connected)
	p := NewConsensusModuleProxy()
	p.SetPublication(pub)
	return p
}

func sampleServiceAckFields() codecs.ServiceAckFields {
	return codecs.ServiceAckFields{
		LogPosition: 100,
		Timestamp:   200,
		AckID:       3,
		RelevantID:  -1,
		ServiceID:   1,
	}
}

func sampleScheduleTimerFields() codecs.ScheduleTimerFields {
	return codecs.ScheduleTimerFields{
		CorrelationID: 11,
		Deadline:      22,
	}
}

func sampleCancelTimerFields() codecs.CancelTimerFields {
	return codecs.CancelTimerFields{CorrelationID: 11}
}

func sampleCloseSessionFields() codecs.CloseSessionFields {
	return codecs.CloseSessionFields{ClusterSessionID: 7}
}

// termWireAt returns the DATA frame payload at termOffset in term 0.
func termWireAt(t *testing.T, pub *aeron.Publication, termOffset int) []byte {
	t.Helper()
	termBuf := pub.TermBuffer(0)
	frameLength := int(logbuffer.FrameLengthVolatile(termBuf, termOffset))
	require.Positive(t, frameLength)
	return termBuf.Bytes()[termOffset+logbuffer.OffsetFramePayload : termOffset+frameLength]
}

// requireDecodedFrame asserts the first offered frame on pub has encodedLen
// and decodes to want.
func requireDecodedFrame[F any](
	t *testing.T,
	pub *aeron.Publication,
	encodedLen int,
	tryDecodeAt func([]byte, int) (F, bool),
	want F,
) {
	t.Helper()
	wire := termWireAt(t, pub, 0)
	require.Len(t, wire, encodedLen)

	got, ok := tryDecodeAt(wire, 0)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestConsensusModuleProxy_unbound(t *testing.T) {
	p := NewConsensusModuleProxy()
	require.NotNil(t, p)
	require.Nil(t, p.Publication())
	require.Empty(t, p.scratch)
}

func TestConsensusModuleProxy_PublishServiceAck_happy(t *testing.T) {
	p, pub := boundModuleProxy(t)
	fields := sampleServiceAckFields()

	require.Positive(t, p.PublishServiceAck(fields))
	requireDecodedFrame(t, pub, codecs.ServiceAckEncodedLen(), codecs.TryDecodeServiceAckAt, fields)
}

func TestConsensusModuleProxy_PublishScheduleTimer_happy(t *testing.T) {
	p, pub := boundModuleProxy(t)
	fields := sampleScheduleTimerFields()

	require.Positive(t, p.PublishScheduleTimer(fields))
	requireDecodedFrame(
		t,
		pub,
		codecs.ScheduleTimerEncodedLen(),
		codecs.TryDecodeScheduleTimerAt,
		fields,
	)
}

func TestConsensusModuleProxy_PublishCancelTimer_happy(t *testing.T) {
	p, pub := boundModuleProxy(t)
	fields := sampleCancelTimerFields()

	require.Positive(t, p.PublishCancelTimer(fields))
	requireDecodedFrame(t, pub, codecs.CancelTimerEncodedLen(), codecs.TryDecodeCancelTimerAt, fields)
}

func TestConsensusModuleProxy_PublishCloseSession_happy(t *testing.T) {
	p, pub := boundModuleProxy(t)
	fields := sampleCloseSessionFields()

	require.Positive(t, p.PublishCloseSession(fields))
	requireDecodedFrame(
		t,
		pub,
		codecs.CloseSessionEncodedLen(),
		codecs.TryDecodeCloseSessionAt,
		fields,
	)
}

// Asserts PublishServiceAck, PublishScheduleTimer, PublishCancelTimer, and
// PublishCloseSession against the shared failure matrix:
//   - nil receiver
//   - unbound publisher
//   - retriable exhaustion
//   - terminal close
//
// A failed publish does not mutate publication state. One setup serves all
// four calls. Test pubs lack an offer count hook. Retry budget is
// [aeron.SendAttempts].
func TestConsensusModuleProxy_publishFailurePaths(t *testing.T) {
	ack := sampleServiceAckFields()
	schedule := sampleScheduleTimerFields()
	cancel := sampleCancelTimerFields()
	closeSession := sampleCloseSessionFields()
	tests := []struct {
		name     string
		setup    func(t *testing.T) *ConsensusModuleProxy
		wantCode int64
	}{
		{
			name:     "nil receiver",
			setup:    func(t *testing.T) *ConsensusModuleProxy { return nil },
			wantCode: aeron.Closed,
		},
		{
			name:     "unbound publisher",
			setup:    func(t *testing.T) *ConsensusModuleProxy { return NewConsensusModuleProxy() },
			wantCode: aeron.Closed,
		},
		{
			name: "not connected exhausts",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, false)
			},
			wantCode: aeron.NotConnected,
		},
		{
			name: "back pressured exhausts",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				return boundModuleProxyWithLimit(t, 0, true)
			},
			wantCode: aeron.BackPressured,
		},
		{
			name: "closed terminal",
			setup: func(t *testing.T) *ConsensusModuleProxy {
				p, pub := boundModuleProxy(t)
				require.NoError(t, pub.Close())
				return p
			},
			wantCode: aeron.Closed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setup(t)
			require.Equal(t, tt.wantCode, p.PublishServiceAck(ack))
			require.Equal(t, tt.wantCode, p.PublishScheduleTimer(schedule))
			require.Equal(t, tt.wantCode, p.PublishCancelTimer(cancel))
			require.Equal(t, tt.wantCode, p.PublishCloseSession(closeSession))
		})
	}
}

func TestConsensusModuleProxy_SetPublication_nilClearsBind(t *testing.T) {
	p, pub := boundModuleProxy(t)
	require.Len(t, p.scratch, pub.MaxPayloadLength())

	p.SetPublication(nil)
	require.Nil(t, p.Publication())
	require.Equal(t, aeron.Closed, p.PublishServiceAck(sampleServiceAckFields()))
	require.Len(t, p.scratch, pub.MaxPayloadLength())
}

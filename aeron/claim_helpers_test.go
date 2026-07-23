//go:build testhooks

package aeron

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestClaimFillCommit_success(t *testing.T) {
	t.Parallel()

	ep := NewTestExclusivePublicationForHooks(t)
	payload := []byte("claim-fill-ok")
	pre := ep.Position()
	aligned := int64(logbuffer.AlignFragmentChunkLength(len(payload)))

	var sawPos int64
	ok := ep.ClaimFillCommit(
		len(payload),
		3,
		func(term []byte, payloadOff int, claimPos int64) bool {
			copy(term[payloadOff:], payload)
			sawPos = claimPos
			return true
		},
	)
	require.True(t, ok)
	require.Equal(t, pre+aligned, sawPos)
	require.Equal(t, pre+aligned, ep.Position())

	termBuf := ep.TermBuffer(0)
	require.EqualValues(t, logbuffer.HdrTypeData, logbuffer.FrameType(termBuf, 0))
	frameLen := int(logbuffer.FrameLengthVolatile(termBuf, 0))
	got := termBuf.Bytes()[logbuffer.OffsetFramePayload:frameLen]
	require.Equal(t, payload, got)
}

// TestClaimFillCommit_retriableThenSuccess drives the retry branch to a
// commit: attempt 1 hits term end (padding, rotation, AdminAction), attempt 2
// claims at the next term start with the last attempt of budget.
func TestClaimFillCommit_retriableThenSuccess(t *testing.T) {
	t.Parallel()

	const margin = 64
	ep := NewTestExclusivePublication(
		t,
		LogConfigNearTermEndForTest(DefaultTestExclusivePublicationLogConfig, margin),
		int64(1)<<62,
	)
	payload := make([]byte, margin)
	for i := range payload {
		payload[i] = byte(i)
	}

	calls := 0
	var sawPos int64
	ok := ep.ClaimFillCommit(
		len(payload),
		2,
		func(term []byte, payloadOff int, claimPos int64) bool {
			calls++
			sawPos = claimPos
			copy(term[payloadOff:], payload)
			return true
		},
	)
	require.True(t, ok)
	require.Equal(t, 1, calls)
	require.Equal(t, sawPos, ep.Position())

	nextTermBuf := ep.TermBuffer(logbuffer.IndexByTermCount(1))
	require.EqualValues(t, logbuffer.HdrTypeData, logbuffer.FrameType(nextTermBuf, 0))
	frameLen := int(logbuffer.FrameLengthVolatile(nextTermBuf, 0))
	got := nextTermBuf.Bytes()[logbuffer.OffsetFramePayload:frameLen]
	require.Equal(t, payload, got)
}

func TestClaimFillCommit_nilAndAttempts(t *testing.T) {
	t.Parallel()

	ep := NewTestExclusivePublicationForHooks(t)
	fill := func([]byte, int, int64) bool { return true }
	var nilPub *ExclusivePublication
	require.False(t, nilPub.ClaimFillCommit(8, 3, fill))
	require.False(t, ep.ClaimFillCommit(8, 0, fill))
	require.False(t, ep.ClaimFillCommit(8, -1, fill))
	require.False(t, ep.ClaimFillCommit(8, 3, nil))
}

func TestClaimFillCommit_invalidLength(t *testing.T) {
	t.Parallel()

	ep := NewTestExclusivePublicationForHooks(t)
	require.False(t, ep.ClaimFillCommit(-1, 3, func([]byte, int, int64) bool {
		t.Fatal("fill must not run on invalid length")
		return true
	}))
	require.False(t, ep.ClaimFillCommit(ep.MaxPayloadLength()+1, 3, func([]byte, int, int64) bool {
		t.Fatal("fill must not run on oversize length")
		return true
	}))
}

func TestClaimFillCommit_fillFalseAborts(t *testing.T) {
	t.Parallel()

	ep := NewTestExclusivePublicationForHooks(t)
	pre := ep.Position()
	aligned := int64(logbuffer.AlignFragmentChunkLength(16))

	ok := ep.ClaimFillCommit(16, 3, func(term []byte, payloadOff int, _ int64) bool {
		copy(term[payloadOff:payloadOff+4], []byte("dead"))
		return false
	})
	require.False(t, ok)
	require.Equal(t, pre+aligned, ep.Position())
	termBuf := ep.TermBuffer(0)
	require.EqualValues(t, logbuffer.HdrTypePad, logbuffer.FrameType(termBuf, 0))
}

func TestClaimFillCommit_noFillOnClaimFail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T) *ExclusivePublication
	}{
		{
			name: "back pressured",
			setup: func(t *testing.T) *ExclusivePublication {
				return NewTestExclusivePublicationForHooksWithLimit(t, 0, true)
			},
		},
		{
			name: "not connected",
			setup: func(t *testing.T) *ExclusivePublication {
				return NewTestExclusivePublicationForHooksWithLimit(t, 0, false)
			},
		},
		{
			name: "closed",
			setup: func(t *testing.T) *ExclusivePublication {
				ep := NewTestExclusivePublicationForHooks(t)
				require.NoError(t, ep.Close())
				return ep
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ep := tt.setup(t)
			calls := 0
			ok := ep.ClaimFillCommit(8, 3, func([]byte, int, int64) bool {
				calls++
				return true
			})
			require.False(t, ok)
			require.Zero(t, calls)
		})
	}
}

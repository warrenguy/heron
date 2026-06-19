package aeron_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func tryClaimForExclusiveTest(t *testing.T, ep *aeron.ExclusivePublication, length int) (int, int) {
	t.Helper()

	termIndex, frameOffset, _, err := ep.TryClaim(length)
	require.NoError(t, err)
	return termIndex, frameOffset
}

func committedExclusiveTestClaim(
	t *testing.T,
	ep *aeron.ExclusivePublication,
	length int,
) (int, int) {
	t.Helper()

	termIndex, frameOffset := tryClaimForExclusiveTest(t, ep, length)
	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
	require.NoError(t, ep.Commit(termIndex, frameOffset))
	return termIndex, frameOffset
}

func newTestExclusivePublicationNearTermEnd(t *testing.T, margin int) *aeron.ExclusivePublication {
	t.Helper()

	return aeron.NewTestExclusivePublication(
		t,
		aeron.LogConfigNearTermEndForTest(aeron.DefaultTestExclusivePublicationLogConfig, margin),
		int64(1)<<62,
	)
}

func TestExclusivePublication_ClaimFinalize_errors(t *testing.T) {
	cases := []struct {
		name    string
		op      func(*aeron.ExclusivePublication) error
		setup   func(*aeron.ExclusivePublication)
		wantErr string
	}{
		{
			name:    "Commit not in flight",
			op:      func(ep *aeron.ExclusivePublication) error { return ep.Commit(0, 0) },
			wantErr: "aeron: claimed space not in flight",
		},
		{
			name:    "Abort not in flight",
			op:      func(ep *aeron.ExclusivePublication) error { return ep.Abort(0, 0) },
			wantErr: "aeron: claimed space not in flight",
		},
		{
			name:    "Commit invalid term index",
			op:      func(ep *aeron.ExclusivePublication) error { return ep.Commit(3, 0) },
			wantErr: "aeron: invalid term index 3",
		},
		{
			name:    "Abort invalid term index",
			op:      func(ep *aeron.ExclusivePublication) error { return ep.Abort(3, 0) },
			wantErr: "aeron: invalid term index 3",
		},
		{
			name:    "Commit invalid frame offset",
			op:      func(ep *aeron.ExclusivePublication) error { return ep.Commit(0, -1) },
			wantErr: "aeron: invalid frame offset -1",
		},
		{
			name: "Commit frame offset past term",
			op: func(ep *aeron.ExclusivePublication) error {
				capacity := aeron.ExclusivePublicationTermBufferForTest(ep, 0).Capacity()
				return ep.Commit(0, capacity-logbuffer.DataHeaderLength+1)
			},
			wantErr: fmt.Sprintf(
				"aeron: invalid frame offset %d",
				aeron.TestPublicationTermLength-logbuffer.DataHeaderLength+1,
			),
		},
		{
			name: "Commit closed",
			op:   func(ep *aeron.ExclusivePublication) error { return ep.Commit(0, 0) },
			setup: func(ep *aeron.ExclusivePublication) {
				require.NoError(t, ep.Close())
			},
			wantErr: "aeron: exclusive publication is closed",
		},
		{
			name: "Abort closed",
			op:   func(ep *aeron.ExclusivePublication) error { return ep.Abort(0, 0) },
			setup: func(ep *aeron.ExclusivePublication) {
				require.NoError(t, ep.Close())
			},
			wantErr: "aeron: exclusive publication is closed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := aeron.NewTestExclusivePublication(
				t,
				aeron.DefaultTestExclusivePublicationLogConfig(),
				1_000_000,
			)
			if tc.setup != nil {
				tc.setup(ep)
			}
			require.EqualError(t, tc.op(ep), tc.wantErr)
		})
	}
}

func TestExclusivePublication_ClaimThenClose_commitAndAbort(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset := tryClaimForExclusiveTest(t, ep, 8)
	require.NoError(t, ep.Close())

	require.EqualError(t, ep.Commit(termIndex, frameOffset), "aeron: exclusive publication is closed")
	require.EqualError(t, ep.Abort(termIndex, frameOffset), "aeron: exclusive publication is closed")
}

func TestExclusivePublication_Commit_afterRotationOnWrongTermIndex(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset := tryClaimForExclusiveTest(t, ep, 16)

	aeron.RotateExclusivePublicationForTest(ep)

	err := ep.Commit((termIndex+1)%logbuffer.PartitionCount, frameOffset)
	require.EqualError(t, err, "aeron: claimed space not in flight")
	require.NoError(t, ep.Commit(termIndex, frameOffset))
}

func TestExclusivePublication_Commit_twice(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset := committedExclusiveTestClaim(t, ep, 8)
	require.EqualError(t, ep.Commit(termIndex, frameOffset), "aeron: claimed space not in flight")
}

func TestExclusivePublication_Abort_afterCommit(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)

	termIndex, frameOffset := committedExclusiveTestClaim(t, ep, 8)
	require.EqualError(t, ep.Abort(termIndex, frameOffset), "aeron: claimed space not in flight")
}

func TestExclusivePublication_handleEndOfLog_subHeaderPadding(t *testing.T) {
	const margin = 65
	termLength := aeron.TestPublicationTermLength
	paddingOffset := termLength - 1

	ep := newTestExclusivePublicationNearTermEnd(t, margin)

	first := ep.Offer(make([]byte, 16))
	require.Positive(t, first)

	second := ep.Offer([]byte{1})
	require.Equal(t, aeron.AdminAction, second)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, 0)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))

	require.Equal(t, int32(1), aeron.ExclusivePublicationActiveTermCountForTest(ep))

	nextIndex := logbuffer.IndexByTermCount(1)
	require.Positive(t, aeron.OfferRetryForTest(ep, []byte{2, 3}))

	nextTermBuf := aeron.ExclusivePublicationTermBufferForTest(ep, nextIndex)
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(nextTermBuf, 0))
}

func TestExclusivePublication_handleEndOfLog_minimalPadWhenPaddingFitsFrameLength(t *testing.T) {
	const margin = 40
	termLength := aeron.TestPublicationTermLength
	paddingOffset := termLength - 8

	ep := newTestExclusivePublicationNearTermEnd(t, margin)

	require.Positive(t, ep.Offer(nil))

	got := ep.Offer([]byte{1})
	require.Equal(t, aeron.AdminAction, got)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, 0)
	require.Equal(t, int32(8), termBuf.GetInt32(paddingOffset))
	require.Equal(
		t,
		int16(logbuffer.HdrTypePad),
		logbuffer.FrameType(termBuf, paddingOffset),
	)

	require.Equal(t, int32(1), aeron.ExclusivePublicationActiveTermCountForTest(ep))
	require.Positive(t, aeron.OfferRetryForTest(ep, []byte{9}))
}

func TestExclusivePublication_handleEndOfLog_noPadWhenAtTermEnd(t *testing.T) {
	termLength := aeron.TestPublicationTermLength

	ep := newTestExclusivePublicationNearTermEnd(t, 0)
	got := ep.Offer([]byte{1, 2, 3})
	require.Equal(t, aeron.AdminAction, got)

	termBuf := aeron.ExclusivePublicationTermBufferForTest(ep, 0)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))
	require.Equal(t, int32(1), aeron.ExclusivePublicationActiveTermCountForTest(ep))
}

func TestExclusivePublication_handleEndOfLog_reservedPositionAtMax_noRotate(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	beforeCount := aeron.ExclusivePublicationActiveTermCountForTest(ep)
	_, termBuf := aeron.ExclusivePublicationActiveTermBufferForTest(ep)
	maxPossible := ep.MaxPossiblePositionForTest()

	got := ep.HandleEndOfLogForTest(
		termBuf,
		aeron.TestPublicationTermLength,
		aeron.TestPublicationInitialTermID,
		aeron.TestPublicationTermLength-64,
		maxPossible,
	)
	require.Equal(t, aeron.MaxPositionExceeded, got)
	require.Equal(t, beforeCount, aeron.ExclusivePublicationActiveTermCountForTest(ep))
}

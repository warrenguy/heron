package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

// claimingPublication is the TryClaim term-buffer surface shared by
// Publication and ExclusivePublication.
type claimingPublication interface {
	TermBuffer(termIndex int) atomicbuffer.AtomicBuffer
	TryClaim(length int) (termIndex, frameOffset int, position int64, err error)
	Commit(termIndex, frameOffset int) error
}

var termBufferInvalidIndexCases = []struct {
	name      string
	termIndex int
}{
	{"negative", -1},
	{"partition count", logbuffer.PartitionCount},
	{"past partition count", logbuffer.PartitionCount + 1},
}

func requireTermBufferSameBacking(
	tb testing.TB,
	want atomicbuffer.AtomicBuffer,
	got atomicbuffer.AtomicBuffer,
) {
	tb.Helper()
	require.Equal(tb, want.Capacity(), got.Capacity())

	wantBytes := want.Bytes()
	gotBytes := got.Bytes()
	if len(wantBytes) > 0 {
		require.Same(tb, &wantBytes[0], &gotBytes[0])
	}
}

func requireTermBufferMatchesOracle(
	t *testing.T,
	pub claimingPublication,
	oracle func(termIndex int) atomicbuffer.AtomicBuffer,
) {
	t.Helper()
	for termIndex := range logbuffer.PartitionCount {
		requireTermBufferSameBacking(
			t,
			oracle(termIndex),
			pub.TermBuffer(termIndex),
		)
	}
}

func requireTermBufferInvalidIndexPanics(
	t *testing.T,
	pub claimingPublication,
	oracle func(termIndex int) atomicbuffer.AtomicBuffer,
) {
	t.Helper()
	for _, tc := range termBufferInvalidIndexCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Panics(t, func() { pub.TermBuffer(tc.termIndex) })
			require.Panics(t, func() { oracle(tc.termIndex) })
		})
	}
}

func requireTermBufferTryClaimCommit(t *testing.T, pub claimingPublication) {
	t.Helper()
	termIndex, frameOffset, position, err := pub.TryClaim(16)
	require.NoError(t, err)
	require.Positive(t, position)
	require.GreaterOrEqual(t, frameOffset, 0)

	termBuf := pub.TermBuffer(termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("claim-payload-16b"))
	require.NoError(t, pub.Commit(termIndex, frameOffset))
	require.Equal(
		t,
		int32(16+logbuffer.DataHeaderLength),
		logbuffer.FrameLengthVolatile(termBuf, frameOffset),
	)
}

func TestPublication_TermBuffer(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	requireTermBufferMatchesOracle(t, pub, func(termIndex int) atomicbuffer.AtomicBuffer {
		return aeron.PublicationTermBufferForTest(pub, termIndex)
	})
}

func TestPublication_TermBuffer_invalidIndex(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	requireTermBufferInvalidIndexPanics(t, pub, func(termIndex int) atomicbuffer.AtomicBuffer {
		return aeron.PublicationTermBufferForTest(pub, termIndex)
	})
}

func TestPublication_TermBuffer_TryClaimCommit(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)
	requireTermBufferTryClaimCommit(t, pub)
}

func TestExclusivePublication_TermBuffer(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	requireTermBufferMatchesOracle(t, ep, func(termIndex int) atomicbuffer.AtomicBuffer {
		return aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	})
}

func TestExclusivePublication_TermBuffer_invalidIndex(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	requireTermBufferInvalidIndexPanics(t, ep, func(termIndex int) atomicbuffer.AtomicBuffer {
		return aeron.ExclusivePublicationTermBufferForTest(ep, termIndex)
	})
}

func TestExclusivePublication_TermBuffer_TryClaimCommit(t *testing.T) {
	ep := aeron.NewTestExclusivePublication(
		t,
		aeron.DefaultTestExclusivePublicationLogConfig(),
		1_000_000,
	)
	requireTermBufferTryClaimCommit(t, ep)
}

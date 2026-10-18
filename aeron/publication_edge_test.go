package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func tryClaimForTest(t *testing.T, pub *aeron.Publication, length int) (int, int) {
	t.Helper()

	termIndex, frameOffset, _, err := pub.TryClaim(length)
	require.NoError(t, err)
	return termIndex, frameOffset
}

func committedTestClaim(t *testing.T, pub *aeron.Publication, length int) (int, int) {
	t.Helper()

	termIndex, frameOffset := tryClaimForTest(t, pub, length)
	termBuf := aeron.PublicationTermBufferForTest(pub, termIndex)
	termBuf.PutBytes(frameOffset+logbuffer.OffsetFramePayload, []byte("12345678"))
	require.NoError(t, pub.Commit(termIndex, frameOffset))
	return termIndex, frameOffset
}

func newTestPublicationNearTermEnd(t *testing.T, margin int) *aeron.Publication {
	t.Helper()

	return aeron.NewTestPublication(t, publicationLogConfigNearTermEnd(margin), int64(1)<<62)
}

func TestPublication_Commit_afterRotationOnWrongTermIndex(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset := tryClaimForTest(t, pub, 16)

	aeron.RotatePublicationLogForTest(pub, 0, aeron.TestPublicationInitialTermID)

	err := pub.Commit((termIndex+1)%logbuffer.PartitionCount, frameOffset)
	require.EqualError(t, err, "aeron: claimed space not in flight")
	require.NoError(t, pub.Commit(termIndex, frameOffset))
}

func TestPublication_ClaimThenClose_commitAndAbort(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset := tryClaimForTest(t, pub, 8)
	require.NoError(t, pub.Close())

	require.EqualError(t, pub.Commit(termIndex, frameOffset), "aeron: publication is closed")
	require.EqualError(t, pub.Abort(termIndex, frameOffset), "aeron: publication is closed")
}

func TestPublication_Commit_twice(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset := committedTestClaim(t, pub, 8)
	require.EqualError(t, pub.Commit(termIndex, frameOffset), "aeron: claimed space not in flight")
}

func TestPublication_Abort_afterCommit(t *testing.T) {
	pub := aeron.NewTestPublication(t, aeron.DefaultTestPublicationLogConfig(), 1_000_000)

	termIndex, frameOffset := committedTestClaim(t, pub, 8)
	require.EqualError(t, pub.Abort(termIndex, frameOffset), "aeron: claimed space not in flight")
}

func TestPublication_handleEndOfLog_subHeaderPadding(t *testing.T) {
	const margin = 72
	termLength := aeron.TestPublicationTermLength
	paddingOffset := termLength - 8

	pub := newTestPublicationNearTermEnd(t, margin)

	first := pub.Offer(make([]byte, 16))
	require.Positive(t, first)

	second := pub.Offer([]byte{1})
	require.Equal(t, aeron.AdminAction, second)

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, paddingOffset))

	require.Equal(t, int32(1), aeron.PublicationActiveTermCountForTest(pub))

	nextIndex := logbuffer.IndexByTermCount(1)
	third := aeron.OfferRetryForTest(pub, []byte{2, 3})
	require.Positive(t, third)

	nextTermBuf := aeron.PublicationTermBufferForTest(pub, nextIndex)
	require.Equal(t, int16(logbuffer.HdrTypeData), logbuffer.FrameType(nextTermBuf, 0))
}

func TestPublication_handleEndOfLog_minimalPadWhenPaddingFitsFrameLength(t *testing.T) {
	const margin = 40
	termLength := aeron.TestPublicationTermLength
	paddingOffset := termLength - 8

	pub := newTestPublicationNearTermEnd(t, margin)

	require.Positive(t, pub.Offer(nil))

	got := pub.Offer([]byte{1})
	require.Equal(t, aeron.AdminAction, got)

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	require.Equal(t, int32(8), termBuf.GetInt32(paddingOffset))
	require.Equal(
		t,
		int16(logbuffer.HdrTypePad),
		logbuffer.FrameType(termBuf, paddingOffset),
	)

	require.Equal(t, int32(1), aeron.PublicationActiveTermCountForTest(pub))
	require.Positive(t, aeron.OfferRetryForTest(pub, []byte{9}))
}

func TestPublication_handleEndOfLog_noPadWhenAtTermEnd(t *testing.T) {
	termLength := aeron.TestPublicationTermLength

	pub := newTestPublicationNearTermEnd(t, 0)
	got := pub.Offer([]byte{1, 2, 3})
	require.Equal(t, aeron.AdminAction, got)

	termBuf := aeron.PublicationTermBufferForTest(pub, 0)
	require.Equal(t, int32(0), logbuffer.FrameLengthVolatile(termBuf, termLength))
	require.Equal(t, int32(1), aeron.PublicationActiveTermCountForTest(pub))
}

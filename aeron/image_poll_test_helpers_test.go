package aeron_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

const (
	pollTestZeroAllocPayload     = aeron.PollTestZeroAllocPayload
	pollTestPaddingLength        = int32(2 * logbuffer.FrameAlignment)
	pollTestWidePaddingLength    = int32(3 * logbuffer.FrameAlignment)
	pollTestNonZeroFrameOffset   = logbuffer.FrameAlignment
	pollTestTailFrameLength      = 2 * logbuffer.FrameAlignment
	pollTestMinimalPadLength     = int32(8)
	pollTestTermEndFullPadLength = int32(40)
)

type pollCapture struct {
	payloads [][]byte
	flags    []byte
	headers  []aeron.Header
}

func (capture *pollCapture) reset() {
	capture.payloads = capture.payloads[:0]
	capture.flags = capture.flags[:0]
	capture.headers = capture.headers[:0]
}

func (capture *pollCapture) handler(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *aeron.Header,
) {
	capture.payloads = append(capture.payloads, slices.Clone(buffer.Bytes()[offset:offset+length]))
	capture.flags = append(capture.flags, header.Flags())
	headerCopy := *header
	capture.headers = append(capture.headers, headerCopy)
}

func newPollImage(tb testing.TB, joinPosition int64) (*aeron.Image, *pollCapture) {
	tb.Helper()

	sub := testSubscription(tb)
	image := aeron.NewImageForTest(tb, sub, joinPosition)
	return image, &pollCapture{}
}

func newPollImageWithErrorHandler(
	tb testing.TB,
	joinPosition int64,
) (*aeron.Image, *pollCapture, func() error) {
	tb.Helper()

	var handled error
	sub := testSubscription(tb)
	image := aeron.NewImageForTestWithErrorHandler(tb, sub, joinPosition, func(err error) {
		handled = err
	})
	return image, &pollCapture{}, func() error { return handled }
}

func writePollTestDataFrame(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	aeron.WriteCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		aeron.TestPublicationSessionID,
		aeron.TestPublicationStreamID,
		aeron.TestPublicationInitialTermID,
		termOffset,
		flags,
		payload,
	)
}

func writePollTestPaddingThenData(
	termBuffer atomic.AtomicBuffer,
	paddingLength int32,
	payload []byte,
) {
	aeron.WriteCommittedPaddingFrameForTest(
		termBuffer,
		0,
		aeron.TestPublicationInitialTermID,
		0,
		paddingLength,
	)
	writePollTestDataFrame(
		termBuffer,
		int(paddingLength),
		int32(paddingLength),
		logbuffer.Unfragmented,
		payload,
	)
}

func writePollTestCommittedThenCorrupt(
	termBuffer atomic.AtomicBuffer,
	payload []byte,
	corruptOffset int,
) {
	writePollTestDataFrame(termBuffer, 0, 0, logbuffer.Unfragmented, payload)
	logbuffer.PutFrameType(termBuffer, corruptOffset, logbuffer.HdrTypeData)
	logbuffer.PutFrameLengthOrdered(
		termBuffer,
		corruptOffset,
		int32(logbuffer.DataHeaderLength-1),
	)
}

func writeDataFrameChain(
	tb testing.TB,
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	frameCount int,
	payload []byte,
) int {
	tb.Helper()

	offset := startOffset
	for range frameCount {
		writePollTestDataFrame(termBuffer, offset, int32(offset), logbuffer.Unfragmented, payload)
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	return offset
}

var imagePollSink int

func imagePollZeroAllocHandler(
	_ atomic.AtomicBuffer,
	_ int,
	length int,
	_ *aeron.Header,
) {
	imagePollSink = length
}

type subscriptionPollStep struct {
	want     int
	payloads [][]byte
}

func addSubscriptionPollImage(
	tb testing.TB,
	sub *aeron.Subscription,
	correlationID int64,
	payloads ...[]byte,
) *aeron.Image {
	tb.Helper()

	image := aeron.NewImageForTestWithCorrelationID(tb, sub, 0, correlationID)
	termBuffer := aeron.ImageTermBuffer0ForTest(image)
	offset := 0
	for _, payload := range payloads {
		writePollTestDataFrame(
			termBuffer,
			offset,
			int32(offset),
			logbuffer.Unfragmented,
			payload,
		)
		offset += logbuffer.AlignFragmentChunkLength(len(payload))
	}
	aeron.SubscriptionAddImageForTest(sub, image)
	return image
}

func addTwoStandardPollImages(tb testing.TB, sub *aeron.Subscription) {
	tb.Helper()

	addSubscriptionPollImage(
		tb,
		sub,
		aeron.TestImageCorrelationID,
		[]byte("img0-a"),
		[]byte("img0-b"),
	)
	addSubscriptionPollImage(
		tb,
		sub,
		aeron.TestImageCorrelationID+1,
		[]byte("img1-a"),
		[]byte("img1-b"),
	)
}

func requireSubscriptionPoll(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *pollCapture,
	limit int,
	want int,
	wantPayloads [][]byte,
) {
	tb.Helper()

	require.Equal(tb, want, sub.Poll(capture.handler, limit))
	require.Equal(tb, wantPayloads, capture.payloads)
}

func requireSubscriptionPollSteps(
	tb testing.TB,
	sub *aeron.Subscription,
	capture *pollCapture,
	limit int,
	steps ...subscriptionPollStep,
) {
	tb.Helper()

	for _, step := range steps {
		capture.reset()
		requireSubscriptionPoll(tb, sub, capture, limit, step.want, step.payloads)
	}
}

func subscriptionPollOnFirstFragment(
	tb testing.TB,
	capture *pollCapture,
	onFirst func(tb testing.TB),
) aeron.FragmentHandler {
	tb.Helper()

	var handlerCalls int
	return func(
		buffer atomic.AtomicBuffer,
		offset int,
		length int,
		header *aeron.Header,
	) {
		handlerCalls++
		if handlerCalls == 1 {
			onFirst(tb)
		}
		capture.handler(buffer, offset, length, header)
	}
}

func setupZeroAllocPollImage(tb testing.TB) *aeron.Image {
	tb.Helper()

	image, _ := newPollImage(tb, 0)
	writeDataFrameChain(
		tb,
		aeron.ImageTermBuffer0ForTest(image),
		0,
		1,
		aeron.PollTestZeroAllocPayloadBytes(),
	)
	imagePollSink = image.Poll(imagePollZeroAllocHandler, 1)
	return image
}

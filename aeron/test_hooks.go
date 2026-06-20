//go:build testhooks

package aeron

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

// HeaderSetFragmentedFrameLenForTest seeds fragmentedFrameLen on a poll flyweight.
func HeaderSetFragmentedFrameLenForTest(header *Header, fragmentedFrameLen int32) {
	header.fragmentedFrameLen = fragmentedFrameLen
}

// HeaderFragmentedFrameLenForTest returns fragmentedFrameLen on a completed header.
func HeaderFragmentedFrameLenForTest(header *Header) int32 {
	return header.fragmentedFrameLen
}

// Cross-package test hooks. Publication-related IDs align with in-mem Test* fixtures;
// subscription/image/counter IDs use separate namespace for cross-package hook tests
// (not full mirror of export_test). Helpers live here because _test.go is not visible
// to external test packages.
const (
	testHookIPCChannel = "aeron:ipc"

	testHookSubscriptionRegistrationID int64 = 9002
	testHookSubscriptionStreamID       int32 = 2003

	testHookPublicationRegistrationID int64 = 9001
	testHookPublicationLimitCounterID int32 = 1
	testHookPublicationSessionID      int32 = 1001
	testHookPublicationStreamID       int32 = 2002
	testHookPublicationInitialTermID  int32 = 7
	testHookPublicationMTULength      int32 = 1408
	testHookPublicationLimitPosition  int64 = 1 << 40

	testHookMappedLogFileName = "test-hook.log"

	testHookImageCorrelationID          int64 = 9102
	testHookImageSessionID              int32 = 1002
	testHookSubscriberPositionCounterID int32 = 4
)

// NewTestSubscriptionForHooks builds a subscription for cross-package close tests.
func NewTestSubscriptionForHooks(tb testing.TB) *Subscription {
	tb.Helper()

	return newSubscription(
		testHookSubscriptionRegistrationID,
		testHookSubscriptionStreamID,
		testHookIPCChannel,
	)
}

// NewFileBackedPublicationForHooks builds a file-backed publication for linger close tests.
func NewFileBackedPublicationForHooks(tb testing.TB, logPath string) *Publication {
	tb.Helper()

	writeTestHookLogFile(tb, logPath)

	log, err := logbuffer.MapLogFile(logPath)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = log.Close() })

	pub, err := newPublicationFromReady(
		testHookPublicationBuffersReady(logPath),
		log,
		newTestHookCountersReader(tb),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

func NewTestExclusivePublicationForHooks(tb testing.TB) *ExclusivePublication {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeExclusivePublication)
	pub, err := newExclusivePublicationFromReady(
		testHookPublicationBuffersReady(testHookMappedLogFileName),
		log,
		newTestHookCountersReader(tb),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

// NewTestPublicationForHooks builds a publication for cross-package close tests.
func NewTestPublicationForHooks(tb testing.TB) *Publication {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeConcurrentPublication)
	pub, err := newPublicationFromReady(
		testHookPublicationBuffersReady(testHookMappedLogFileName),
		log,
		newTestHookCountersReader(tb),
		testHookIPCChannel,
	)
	require.NoError(tb, err)
	return pub
}

func testHookPublicationBuffersReady(logFileName string) conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		RegistrationID:            testHookPublicationRegistrationID,
		SessionID:                 testHookPublicationSessionID,
		StreamID:                  testHookPublicationStreamID,
		PublicationLimitCounterID: testHookPublicationLimitCounterID,
		ChannelStatusCounterID:    cnc.NullCounterID,
		LogFileName:               logFileName,
	}
}

func allocTestHookLogBuffer(tb testing.TB) ([]byte, atomic.AtomicBuffer) {
	tb.Helper()

	computed, err := logbuffer.ComputeLogLength(logbuffer.TermMinLength, logbuffer.PageMinSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	metaStart := logbuffer.MetaDataOffset(logbuffer.TermMinLength)
	return buf, atomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
}

func initTestHookLogMetadata(meta atomic.AtomicBuffer) {
	meta.PutInt32Release(logbuffer.OffsetTermLength, logbuffer.TermMinLength)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.PageMinSize)
	meta.PutInt32(logbuffer.OffsetMTULength, testHookPublicationMTULength)
	meta.PutInt32(logbuffer.OffsetInitialTermID, testHookPublicationInitialTermID)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
}

func seedTestHookPartitionRawTails(meta atomic.AtomicBuffer) {
	rawTails := [logbuffer.PartitionCount]int64{
		logbuffer.PackRawTail(testHookPublicationInitialTermID, 0),
		logbuffer.PackRawTail(testHookPublicationInitialTermID-2, 0),
		logbuffer.PackRawTail(testHookPublicationInitialTermID-1, 0),
	}
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, partition, rawTails[partition])
	}
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)
}

func prepareTestHookLogBuffer(tb testing.TB, logType byte) []byte {
	tb.Helper()

	buf, meta := allocTestHookLogBuffer(tb)
	initTestHookLogMetadata(meta)
	seedTestHookPartitionRawTails(meta)
	meta.Bytes()[logbuffer.OffsetLogType] = logType
	return buf
}

func writeTestHookLogFile(tb testing.TB, path string) {
	tb.Helper()

	buf := prepareTestHookLogBuffer(tb, logbuffer.LogBufferTypeConcurrentPublication)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func newTestHookMappedLog(tb testing.TB, logType byte) *logbuffer.MappedLogBuffer {
	tb.Helper()

	buf := prepareTestHookLogBuffer(tb, logType)

	log, err := logbuffer.MapLogBufferFromBytes(buf)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = log.Close() })
	return log
}

// NewTestImageForHooks builds an in-memory image wired to sub for cross-package poll tests.
// Caller must register the image via SubscriptionAddImageForHooks before Subscription.Poll.
func NewTestImageForHooks(tb testing.TB, sub *Subscription, joinPosition int64) *Image {
	tb.Helper()

	log := newTestHookMappedLog(tb, logbuffer.LogBufferTypeConcurrentPublication)
	logBuffers, err := logbuffer.NewLogBuffers(log)
	require.NoError(tb, err)

	image, err := newImage(
		sub,
		testHookAvailableImage(),
		logBuffers,
		newTestHookCountersReaderWithSubscriberPosition(tb, joinPosition),
		nil,
	)
	require.NoError(tb, err)
	return image
}

// SubscriptionAddImageForHooks registers image on sub. False when image is nil
// or correlation ID is already registered.
func SubscriptionAddImageForHooks(sub *Subscription, image *Image) bool {
	return sub.addImage(image)
}

// ImageTermBufferForHooks returns partition 0 term buffer for frame writes.
func ImageTermBufferForHooks(image *Image) atomic.AtomicBuffer {
	return image.termBuffers[0]
}

// WriteCommittedDataFrameForHooks commits a DATA frame at frameOffset. Unfragmented
// flags use WriteDataHeaderUnfragmented; other flags use WriteDataHeaderFragmented.
func WriteCommittedDataFrameForHooks(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	frameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	writeHeader := logbuffer.WriteDataHeaderUnfragmented
	if flags&logbuffer.Unfragmented != logbuffer.Unfragmented {
		writeHeader = logbuffer.WriteDataHeaderFragmented
	}
	if err := writeHeader(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
	); err != nil {
		panic(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
}

func testHookAvailableImage() conductor.AvailableImage {
	return conductor.AvailableImage{
		CorrelationID:              testHookImageCorrelationID,
		SessionID:                  testHookImageSessionID,
		StreamID:                   testHookSubscriptionStreamID,
		SubscriptionRegistrationID: testHookSubscriptionRegistrationID,
		SubscriberPositionID:       testHookSubscriberPositionCounterID,
		LogFileName:                testHookMappedLogFileName,
		SourceIdentity:             testHookIPCChannel,
	}
}

func newTestHookCountersReaderFromRecords(
	tb testing.TB,
	counterCount int,
	writeRecords func(meta, values []byte),
) *cnc.CountersReader {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)
	writeRecords(meta, values)

	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}

func newTestHookCountersReader(tb testing.TB) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderFromRecords(
		tb,
		int(testHookPublicationLimitCounterID)+1,
		func(meta, values []byte) {
			cnc.WriteCounterRecord(
				meta,
				values,
				testHookPublicationLimitCounterID,
				cnc.CounterRecordAllocated,
				cnc.DriverPublisherLimitTypeID,
				testHookPublicationRegistrationID,
				testHookPublicationLimitPosition,
			)
		},
	)
}

func newTestHookCountersReaderWithSubscriberPosition(
	tb testing.TB,
	joinPosition int64,
) *cnc.CountersReader {
	tb.Helper()

	return newTestHookCountersReaderFromRecords(
		tb,
		int(testHookSubscriberPositionCounterID)+1,
		func(meta, values []byte) {
			cnc.WriteCounterRecord(
				meta,
				values,
				testHookSubscriberPositionCounterID,
				cnc.CounterRecordAllocated,
				cnc.DriverSubscriberPositionTypeID,
				testHookSubscriptionRegistrationID,
				joinPosition,
			)
		},
	)
}

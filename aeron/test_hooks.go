//go:build testhooks

package aeron

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// Cross-package test hooks. Values mirror export_test.go Test* constants but live
// here because _test.go helpers are not visible to external test packages.
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

func writeTestHookLogFile(tb testing.TB, path string) {
	tb.Helper()

	buf, meta := allocTestHookLogBuffer(tb)
	initTestHookLogMetadata(meta)
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func newTestHookMappedLog(tb testing.TB, logType byte) *logbuffer.MappedLogBuffer {
	tb.Helper()

	buf, meta := allocTestHookLogBuffer(tb)
	initTestHookLogMetadata(meta)
	seedTestHookPartitionRawTails(meta)
	meta.Bytes()[logbuffer.OffsetLogType] = logType

	log, err := logbuffer.MapLogBufferFromBytes(buf)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = log.Close() })
	return log
}

func newTestHookCountersReader(tb testing.TB) *cnc.CountersReader {
	tb.Helper()

	meta := make([]byte, cnc.CounterMetadataLength*3)
	values := make([]byte, cnc.CounterValueLength*3)
	cnc.WriteCounterRecord(
		meta,
		values,
		testHookPublicationLimitCounterID,
		cnc.CounterRecordAllocated,
		cnc.DriverPublisherLimitTypeID,
		testHookPublicationRegistrationID,
		testHookPublicationLimitPosition,
	)
	reader, err := cnc.NewCountersReader(meta, values)
	require.NoError(tb, err)
	return reader
}

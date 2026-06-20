package aeron

import (
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
	"github.com/warrenguy/heron/agrona/ringbuffer"
)

const (
	TestPublicationRegistrationID   int64 = inMemPublicationRegistrationID
	TestPublicationLimitCounterID   int32 = inMemPublicationLimitCounterID
	TestPublicationChannelCounterID int32 = inMemPublicationChannelCounterID
	TestPublicationSessionID        int32 = inMemPublicationSessionID
	TestPublicationStreamID         int32 = inMemPublicationStreamID
	TestPublicationInitialTermID    int32 = inMemPublicationInitialTermID
	TestPublicationMTULength        int32 = inMemPublicationMTULength
	TestPublicationTermLength             = inMemPublicationTermLength

	TestSubscriptionRegistrationID  int64 = inMemSubscriptionRegistrationID
	TestImageCorrelationID          int64 = inMemImageCorrelationID
	TestSubscriberPositionCounterID int32 = inMemSubscriberPositionCounterID
	TestImageSourceIdentity               = inMemImageSourceIdentity
	TestSubscriberPositionAtJoin    int64 = 128

	PollTestZeroAllocPayload = inMemPollZeroAllocPayload
)

func PollTestZeroAllocPayloadBytes() []byte {
	return inMemPollZeroAllocPayloadBytes
}

func PollTestZeroAllocFrameBytes() int64 {
	return inMemPollZeroAllocFrameBytes
}

type TestPublicationLogConfig = inMemPublicationLogConfig

func DefaultTestPublicationLogConfig() TestPublicationLogConfig {
	return defaultInMemPublicationLogConfig()
}

func DefaultTestExclusivePublicationLogConfig() TestPublicationLogConfig {
	return defaultInMemExclusivePublicationLogConfig()
}

const (
	NearMaxTermCountForTest                   = int32(1<<31 - 1)
	nearMaxPositionTermRemainingForTest int32 = 32
)

func LogTermOffsetAtPositionForTest(position int64) int32 {
	shift := logbuffer.PositionBitsToShift(TestPublicationTermLength)
	return logbuffer.ComputeTermOffsetFromPosition(position, shift)
}

func LogConfigAtPositionForTest(
	base func() TestPublicationLogConfig,
	position int64,
) TestPublicationLogConfig {
	shift := logbuffer.PositionBitsToShift(TestPublicationTermLength)
	termID := logbuffer.ComputeTermIDFromPosition(
		position,
		shift,
		TestPublicationInitialTermID,
	)
	termOffset := logbuffer.ComputeTermOffsetFromPosition(position, shift)
	index := logbuffer.IndexByPosition(position, shift)
	termCount := logbuffer.ComputeTermCount(termID, TestPublicationInitialTermID)

	cfg := base()
	cfg.ActiveTermCount = termCount
	cfg.RawTails[index] = logbuffer.PackRawTail(termID, termOffset)
	return cfg
}

func LogConfigNearTermEndForTest(
	base func() TestPublicationLogConfig,
	margin int,
) TestPublicationLogConfig {
	cfg := base()
	cfg.RawTails[0] = logbuffer.PackRawTail(
		TestPublicationInitialTermID,
		int32(TestPublicationTermLength-margin),
	)
	return cfg
}

func nearMaxTermTailForTest(remaining int32) (termID int32, index int, termOffset int32) {
	termID = logbuffer.AddWrapI32(TestPublicationInitialTermID, NearMaxTermCountForTest)
	index = logbuffer.IndexByTermCount(NearMaxTermCountForTest)
	termOffset = int32(TestPublicationTermLength) - remaining
	return termID, index, termOffset
}

func LogConfigWithTermRemainingForTest(
	base func() TestPublicationLogConfig,
	remaining int32,
) TestPublicationLogConfig {
	termID, index, termOffset := nearMaxTermTailForTest(remaining)

	cfg := base()
	cfg.ActiveTermCount = NearMaxTermCountForTest
	cfg.RawTails[index] = logbuffer.PackRawTail(termID, termOffset)
	return cfg
}

func PositionWithTermRemainingForTest(remaining int32) int64 {
	termID, _, termOffset := nearMaxTermTailForTest(remaining)
	shift := logbuffer.PositionBitsToShift(TestPublicationTermLength)
	return logbuffer.ComputePosition(
		termID,
		termOffset,
		shift,
		TestPublicationInitialTermID,
	)
}

func LogConfigNearMaxPositionForTest(
	base func() TestPublicationLogConfig,
) TestPublicationLogConfig {
	return LogConfigWithTermRemainingForTest(base, nearMaxPositionTermRemainingForTest)
}

func PositionNearMaxPositionForTest() int64 {
	return PositionWithTermRemainingForTest(nearMaxPositionTermRemainingForTest)
}

func NewExclusivePublicationFromReadyForTest(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
	channel string,
) (*ExclusivePublication, error) {
	return newExclusivePublicationFromReady(ready, log, counters, channel)
}

func NewPublicationFromReadyForTest(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters *cnc.CountersReader,
	channel string,
) (*Publication, error) {
	return newPublicationFromReady(ready, log, counters, channel)
}

func PublicationBuffersReadyForTest() conductor.PublicationBuffersReady {
	return testPublicationBuffersReady()
}

func OfferRetryForTest(pub offerWithOffsetRetrySource, payload []byte) int64 {
	return OfferWithOffsetRetryForTest(pub, payload, 0, len(payload))
}

type fragmentedWriteSlot struct {
	termIndex   int
	frameOffset int
}

type inflightSlotTracker struct {
	mu         sync.Mutex
	inflight   map[fragmentedWriteSlot]int
	overlapped map[fragmentedWriteSlot]struct{}
}

func (tracker *inflightSlotTracker) reset() {
	tracker.mu.Lock()
	tracker.inflight = make(map[fragmentedWriteSlot]int)
	tracker.overlapped = make(map[fragmentedWriteSlot]struct{})
	tracker.mu.Unlock()
}

func (tracker *inflightSlotTracker) track(key fragmentedWriteSlot, begin bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if begin {
		tracker.inflight[key]++
		if tracker.inflight[key] > 1 {
			tracker.overlapped[key] = struct{}{}
		}
		return
	}
	tracker.inflight[key]--
	if tracker.inflight[key] <= 0 {
		delete(tracker.inflight, key)
	}
}

func (tracker *inflightSlotTracker) report() []fragmentedWriteSlot {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	result := make([]fragmentedWriteSlot, 0, len(tracker.inflight)+len(tracker.overlapped))
	seen := make(map[fragmentedWriteSlot]struct{}, len(tracker.inflight)+len(tracker.overlapped))
	for key := range tracker.overlapped {
		seen[key] = struct{}{}
		result = append(result, key)
	}
	for key, count := range tracker.inflight {
		if count > 0 {
			if _, ok := seen[key]; !ok {
				result = append(result, key)
			}
		}
	}
	return result
}

var (
	fragmentedWriteTracker       inflightSlotTracker
	fragmentedReservationTracker inflightSlotTracker
)

func trackFragmentedSlot(
	tracker *inflightSlotTracker,
	termIndex int,
	frameOffset int,
	begin bool,
) {
	tracker.track(
		fragmentedWriteSlot{termIndex: termIndex, frameOffset: frameOffset},
		begin,
	)
}

func EnableFragmentedWriteTrackingForTest() {
	fragmentedWriteTracker.reset()
	fragmentedReservationTracker.reset()
	setFragmentedReservationTrackHook(func(termIndex, termOffset int, begin bool) {
		trackFragmentedSlot(&fragmentedReservationTracker, termIndex, termOffset, begin)
	})
	setFragmentedWriteTrackHook(func(termIndex, frameOffset int, begin bool) {
		trackFragmentedSlot(&fragmentedWriteTracker, termIndex, frameOffset, begin)
	})
}

func DisableFragmentedWriteTrackingForTest() {
	setFragmentedWriteTrackHook(nil)
	setFragmentedReservationTrackHook(nil)
	fragmentedWriteTracker.reset()
	fragmentedReservationTracker.reset()
}

func SetFragmentedWriteTrackForTest(fn func(termIndex, frameOffset int, begin bool)) {
	setFragmentedWriteTrackHook(fn)
}

func ReportFragmentedWriteTrackerForTest() []fragmentedWriteSlot {
	return fragmentedWriteTracker.report()
}

func ReportFragmentedReservationTrackerForTest() []fragmentedWriteSlot {
	return fragmentedReservationTracker.report()
}

type offerWithOffsetRetrySource interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
}

func offerWithOffsetRetry(
	pub offerWithOffsetRetrySource,
	buffer []byte,
	offset int,
	length int,
) int64 {
	for {
		position := pub.OfferWithOffset(buffer, offset, length)
		if position != AdminAction {
			return position
		}
	}
}

func OfferWithOffsetRetryForTest(
	pub offerWithOffsetRetrySource,
	buffer []byte,
	offset int,
	length int,
) int64 {
	return offerWithOffsetRetry(pub, buffer, offset, length)
}

type tryClaimRetrySource interface {
	TryClaim(length int) (termIndex, frameOffset int, position int64, err error)
}

func tryClaimRetry(
	pub tryClaimRetrySource,
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	for {
		termIndex, frameOffset, position, err = pub.TryClaim(length)
		if err != nil {
			return 0, 0, 0, err
		}
		if position != AdminAction {
			return termIndex, frameOffset, position, nil
		}
	}
}

func TryClaimRetryForTest(
	pub tryClaimRetrySource,
	length int,
) (termIndex, frameOffset int, position int64, err error) {
	return tryClaimRetry(pub, length)
}

func NewTestPublication(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *Publication {
	tb.Helper()
	return newTestPublication(tb, logCfg, newTestCountersReaderWithoutTB(limitPosition))
}

func (pub *Publication) MaxPayloadLengthForTest() int {
	return pub.maxPayloadLength
}

func (pub *Publication) MaxMessageLengthForTest() int {
	return pub.maxMessageLength
}

const maxFragmentedReadIterations = 64

type FragmentedFrameForTest struct {
	Flags   uint8
	Payload []byte
}

func FragmentedMessageStartOffsetForTest(position int64, shift, totalAligned int) int {
	termOffset := int(logbuffer.ComputeTermOffsetFromPosition(position, shift))
	return termOffset - totalAligned
}

func ReadFragmentedMessageFramesForTest(
	termBuf atomic.AtomicBuffer,
	startOffset int,
) ([]FragmentedFrameForTest, bool) {
	var frames []FragmentedFrameForTest
	offset := startOffset
	for range maxFragmentedReadIterations {
		frameLength := int(logbuffer.FrameLengthVolatile(termBuf, offset))
		if frameLength <= 0 {
			return nil, false
		}

		payloadStart := offset + logbuffer.OffsetFramePayload
		payloadEnd := offset + frameLength
		frames = append(frames, FragmentedFrameForTest{
			Flags: logbuffer.FrameFlags(termBuf, offset),
			Payload: append(
				[]byte(nil),
				termBuf.Bytes()[payloadStart:payloadEnd]...,
			),
		})

		if frames[len(frames)-1].Flags&logbuffer.EndFlag != 0 {
			return frames, true
		}
		offset += logbuffer.AlignFrameLength(frameLength)
	}

	return nil, false
}

func ReadFragmentedMessageForTest(
	tb testing.TB,
	termBuf atomic.AtomicBuffer,
	startOffset int,
) []FragmentedFrameForTest {
	tb.Helper()

	frames, ok := ReadFragmentedMessageFramesForTest(termBuf, startOffset)
	require.True(tb, ok, "fragmented message missing END flag within iteration budget")
	return frames
}

func (pub *Publication) MetaDataBufferForTest() atomic.AtomicBuffer {
	return pub.meta
}

func PublicationActiveTermCountForTest(pub *Publication) int32 {
	return publicationActiveTermCountForTest(pub.meta)
}

func PublicationLimitSlotForTest(pub *Publication) int64 {
	return publicationLimitSlotForTest(pub.publicationLimit)
}

func PublicationConductorForTest(pub *Publication) *conductor.Conductor {
	return pub.conductor
}

func NewTestPublicationWithoutTBForRapid(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *Publication {
	return newTestPublicationWithoutTB(logCfg, limitPosition)
}

func NewTestExclusivePublication(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *ExclusivePublication {
	tb.Helper()
	return newTestExclusivePublication(tb, logCfg, newTestCountersReaderWithoutTB(limitPosition))
}

func NewTestExclusivePublicationWithoutTBForRapid(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *ExclusivePublication {
	return newTestExclusivePublicationWithoutTB(logCfg, limitPosition)
}

func (ep *ExclusivePublication) MaxPayloadLengthForTest() int {
	return ep.maxPayloadLength
}

func (ep *ExclusivePublication) MaxMessageLengthForTest() int {
	return ep.maxMessageLength
}

func (ep *ExclusivePublication) MaxPossiblePositionForTest() int64 {
	return ep.maxPossiblePosition
}

func (ep *ExclusivePublication) MetaDataBufferForTest() atomic.AtomicBuffer {
	return ep.meta
}

func publicationActiveTermCountForTest(meta atomic.AtomicBuffer) int32 {
	return logbuffer.ActiveTermCount(meta)
}

func publicationLimitSlotForTest(limit cnc.CounterSlot) int64 {
	return limit.Get()
}

func publicationActiveTermBufferForTest(
	meta atomic.AtomicBuffer,
	termBuffers [logbuffer.PartitionCount]atomic.AtomicBuffer,
) (int, atomic.AtomicBuffer) {
	index := logbuffer.IndexByTermCount(publicationActiveTermCountForTest(meta))
	return index, termBuffers[index]
}

func publicationTermBufferForTest(
	termBuffers [logbuffer.PartitionCount]atomic.AtomicBuffer,
	index int,
) atomic.AtomicBuffer {
	return termBuffers[index]
}

func setPublicationRawTailForTest(
	meta atomic.AtomicBuffer,
	partitionIndex int,
	rawTail int64,
) {
	_ = logbuffer.PutRawTail(meta, partitionIndex, rawTail)
}

func setPublicationActiveTermCountForTest(meta atomic.AtomicBuffer, termCount int32) {
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, termCount)
}

func ExclusivePublicationActiveTermCountForTest(ep *ExclusivePublication) int32 {
	return publicationActiveTermCountForTest(ep.meta)
}

func ExclusivePublicationLimitSlotForTest(ep *ExclusivePublication) int64 {
	return publicationLimitSlotForTest(ep.publicationLimit)
}

func ExclusivePublicationConductorForTest(ep *ExclusivePublication) *conductor.Conductor {
	return ep.conductor
}

func ExclusivePublicationActiveTermBufferForTest(
	ep *ExclusivePublication,
) (int, atomic.AtomicBuffer) {
	return publicationActiveTermBufferForTest(ep.meta, ep.termBuffers)
}

func ExclusivePublicationTermBufferForTest(
	ep *ExclusivePublication,
	index int,
) atomic.AtomicBuffer {
	return publicationTermBufferForTest(ep.termBuffers, index)
}

func SetExclusivePublicationRawTailForTest(
	ep *ExclusivePublication,
	partitionIndex int,
	rawTail int64,
) {
	setPublicationRawTailForTest(ep.meta, partitionIndex, rawTail)
}

func SetExclusivePublicationActiveTermCountForTest(ep *ExclusivePublication, termCount int32) {
	setPublicationActiveTermCountForTest(ep.meta, termCount)
}

func RotateExclusivePublicationForTest(ep *ExclusivePublication) {
	ep.rotateTerm()
}

func (ep *ExclusivePublication) HandleEndOfLogForTest(
	termBuf atomic.AtomicBuffer,
	termLength int,
	termID int32,
	termOffset int,
	reservedPosition int64,
) int64 {
	return ep.handleEndOfLog(termBuf, termLength, termID, termOffset, reservedPosition)
}

func NewTestExclusivePublicationWithCounters(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters *cnc.CountersReader,
) *ExclusivePublication {
	tb.Helper()
	return newTestExclusivePublication(tb, logCfg, counters)
}

func NewTestMappedLogForTest(
	tb testing.TB,
	cfg TestPublicationLogConfig,
) *logbuffer.MappedLogBuffer {
	return newTestMappedLog(tb, cfg)
}

func NewTestCountersReaderForTest(tb testing.TB, limitPosition int64) *cnc.CountersReader {
	tb.Helper()
	return newTestCountersReaderWithoutTB(limitPosition)
}

func NewTestCountersReaderWithSubscriberPositionForTest(
	tb testing.TB,
	joinPosition int64,
	registrationID ...int64,
) *cnc.CountersReader {
	tb.Helper()
	regID := TestSubscriptionRegistrationID
	if len(registrationID) > 0 {
		regID = registrationID[0]
	}
	return newTestCountersReaderWithSubscriberPositionRegistration(joinPosition, regID)
}

func NewTestCountersReaderLimitOnlyForTest(tb testing.TB, limitPosition int64) *cnc.CountersReader {
	tb.Helper()
	return newTestCountersReaderLimitOnlyWithoutTB(limitPosition)
}

func PublicationActiveTermBufferForTest(pub *Publication) (int, atomic.AtomicBuffer) {
	return publicationActiveTermBufferForTest(pub.meta, pub.termBuffers)
}

func PublicationTermBufferForTest(pub *Publication, index int) atomic.AtomicBuffer {
	return publicationTermBufferForTest(pub.termBuffers, index)
}

func SetPublicationRawTailForTest(
	pub *Publication,
	partitionIndex int,
	rawTail int64,
) {
	setPublicationRawTailForTest(pub.meta, partitionIndex, rawTail)
}

func SetPublicationActiveTermCountForTest(pub *Publication, termCount int32) {
	setPublicationActiveTermCountForTest(pub.meta, termCount)
}

func RotatePublicationLogForTest(pub *Publication, termCount, termID int32) {
	_ = logbuffer.RotateLog(pub.meta, termCount, termID)
}

func VerifyPublicationLogIntegrityForTest(tb testing.TB, pub *Publication) {
	tb.Helper()

	termLength := pub.termBufferLength
	for partition := range logbuffer.PartitionCount {
		rawTail, err := logbuffer.GetRawTail(pub.meta, partition)
		require.NoError(tb, err)

		termEnd := int(logbuffer.TermOffset(rawTail, int32(termLength)))
		termBuf := pub.termBuffers[partition]
		offset := 0
		for offset < termEnd && termBuf.GetInt32Volatile(offset) == 0 {
			offset += logbuffer.FrameAlignment
		}
		for offset < termEnd {
			frameLength := termBuf.GetInt32Volatile(offset)
			require.NotZero(
				tb,
				frameLength,
				"gap at partition=%d offset=%d termEnd=%d",
				partition,
				offset,
				termEnd,
			)
			absLength := frameLength
			if absLength < 0 {
				absLength = -absLength
			}
			aligned := logbuffer.AlignFrameLength(int(absLength))
			require.Positive(tb, aligned)
			offset += aligned
		}
	}
}

func NewTestPublicationWithCounters(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters *cnc.CountersReader,
) *Publication {
	tb.Helper()
	return newTestPublication(tb, logCfg, counters)
}

func newTestMappedLog(tb testing.TB, cfg TestPublicationLogConfig) *logbuffer.MappedLogBuffer {
	if tb != nil {
		tb.Helper()
	}
	log := newInMemMappedLog(cfg)
	if tb != nil {
		tb.Cleanup(func() { _ = log.Close() })
	}
	return log
}

func testPublicationBuffersReady() conductor.PublicationBuffersReady {
	return inMemPublicationBuffersReady()
}

func newTestPublication(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters *cnc.CountersReader,
) *Publication {
	if tb != nil {
		tb.Helper()
	}
	log := newTestMappedLog(tb, logCfg)
	return newInMemPublicationFromLog(log, counters)
}

func newTestPublicationWithoutTB(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *Publication {
	return newTestPublication(nil, logCfg, newTestCountersReaderWithoutTB(limitPosition))
}

func newTestExclusivePublication(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters *cnc.CountersReader,
) *ExclusivePublication {
	if tb != nil {
		tb.Helper()
	}
	log := newTestMappedLog(tb, logCfg)
	return newInMemExclusivePublicationFromLog(log, counters)
}

func newTestExclusivePublicationWithoutTB(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *ExclusivePublication {
	return newTestExclusivePublication(nil, logCfg, newTestCountersReaderWithoutTB(limitPosition))
}

func writeTestLogFileForTest(tb testing.TB, path string, cfg TestPublicationLogConfig) {
	tb.Helper()
	require.NoError(tb, os.WriteFile(path, inMemPublicationLogBytes(cfg), 0o644))
}

func WriteTestLogFileForTest(tb testing.TB, path string) {
	writeTestLogFileForTest(tb, path, DefaultTestPublicationLogConfig())
}

func WriteTestExclusiveLogFileForTest(tb testing.TB, path string) {
	writeTestLogFileForTest(tb, path, DefaultTestExclusivePublicationLogConfig())
}

func WriteTestKindLogFileForTest(tb testing.TB, path string, exclusive bool) {
	if exclusive {
		WriteTestExclusiveLogFileForTest(tb, path)
		return
	}
	WriteTestLogFileForTest(tb, path)
}

func newTestCountersReaderFromRecords(
	counterCount int,
	writeRecords func(meta, values []byte),
) *cnc.CountersReader {
	return inMemCountersReaderFromRecords(counterCount, writeRecords)
}

func newTestCountersReaderLimitOnlyWithoutTB(limitPosition int64) *cnc.CountersReader {
	return newTestCountersReaderFromRecords(3, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			TestPublicationLimitCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverPublisherLimitTypeID,
			TestPublicationRegistrationID,
			limitPosition,
		)
	})
}

func newTestCountersReaderWithoutTB(limitPosition int64) *cnc.CountersReader {
	return inMemCountersReaderWithoutTB(limitPosition)
}

func MapConductorErrorForTest(err error) error {
	return mapConductorError(err)
}

func MapAwaitErrorForTest(err error) error {
	return mapAwaitError(err)
}

func MapPollPendingErrorForTest(
	correlationID int64,
	timeout time.Duration,
	err error,
) error {
	return mapPollPendingError(correlationID, timeout, err)
}

func MapRegistrationErrorForTest(err error) error {
	return mapRegistrationError(err)
}

func MapAddOfferErrorForTest(registrationKind string, correlationID int64, err error) error {
	return mapAddOfferError(registrationKind, correlationID, err)
}

func MapAddPollPendingErrorForTest(
	registrationKind string,
	correlationID int64,
	streamID int32,
	channel string,
	err error,
) error {
	return mapAddPollPendingError(registrationKind, correlationID, streamID, channel, err)
}

func NewRegistrationTimeoutErrorForTest(
	registrationKind string,
	correlationID int64,
	streamID int32,
	channel string,
) *RegistrationTimeoutError {
	return newRegistrationTimeoutError(registrationKind, correlationID, streamID, channel)
}

func (client *Client) GuardClosedBeforeOfferForTest(
	driverProxy *conductor.DriverProxy,
	cncMappedAtOfferStart bool,
) error {
	return client.guardClosedBeforeOffer(driverProxy, cncMappedAtOfferStart)
}

// SetAwaitPollSleepingHookForTest installs a hook invoked under clientLock
// immediately before each await-poll sleep. Set from the test goroutine before
// starting a blocking await; clear after the await goroutine has finished.
func (client *Client) SetAwaitPollSleepingHookForTest(hook func()) {
	setAwaitPollSleepingHook(client, hook)
}

// PublishClosedForTest sets closed without teardown; for guard tests only.
func (client *Client) PublishClosedForTest() {
	// CAS mirrors Close phase-1 publish; teardown is intentionally omitted.
	client.closed.CompareAndSwap(false, true)
}

func (client *Client) mapPollPendingErrorForTest(correlationID int64) func(error) error {
	return func(err error) error {
		return mapPollPendingError(correlationID, client.driverTimeout, err)
	}
}

// TryLockClientForTest non-blocking acquire of clientLock for test goroutines
// that hammer DoWorkLockedForTest while blocking await holds the lock.
//
// UnlockClientForTest required after true; false means clientLock held elsewhere.
func (client *Client) TryLockClientForTest() bool {
	if client == nil {
		return false
	}
	return client.clientLock.TryLock()
}

// UnlockClientForTest releases clientLock from a successful TryLockClientForTest.
func (client *Client) UnlockClientForTest() {
	client.unlockClient()
}

// DoWorkLockedForTest runs one conductor service tick without acquiring clientLock.
//
// Caller must already hold clientLock, as in blocking await/offer poll loops, Close
// teardown, or ErrorHandler and image handlers invoked during doWorkLocked; see
// Client godoc.
func (client *Client) DoWorkLockedForTest() int {
	return client.doWorkLocked()
}

// AwaitPendingPollOneTickForTest runs one awaitPendingPollTickForTest tick under clientLock.
//
// skipDoWork omits doWorkLocked when poll completion is pre-latched; afterPollPending
// runs after PollPending for closed-guard branch tests.
func (client *Client) AwaitPendingPollOneTickForTest(
	correlationID int64,
	skipDoWork bool,
	afterPollPending func(),
) (conductor.PendingRegistration, bool, error) {
	client.lockClient()
	defer client.unlockClient()

	return client.awaitPendingPollTickForTest(
		client.conductor,
		correlationID,
		client.mapPollPendingErrorForTest(correlationID),
		skipDoWork,
		afterPollPending,
	)
}

func (client *Client) conductorForTest() *conductor.Conductor {
	if client == nil || client.conductor == nil {
		return nil
	}
	return client.conductor
}

type stubPublicationForTest struct {
	registrationID int64
}

func (publication stubPublicationForTest) RegistrationID() int64 {
	return publication.registrationID
}

func InsertPublicationForTest(
	conductorInstance *conductor.Conductor,
	registrationID int64,
) {
	conductor.RegisterPublicationForTest(
		conductorInstance,
		registrationID,
		stubPublicationForTest{registrationID: registrationID},
	)
}

func NewFileBackedPublicationBoundForCloseTest(
	tb testing.TB,
	conductorInstance *conductor.Conductor,
	logPath string,
) *Publication {
	tb.Helper()

	pub := NewFileBackedPublicationForHooks(tb, logPath)

	_, err := conductor.AcquireLogBuffersForTest(conductorInstance, logPath)
	require.NoError(tb, err)

	pub.conductor = conductorInstance
	conductor.RegisterPublicationForTest(conductorInstance, pub.RegistrationID(), pub)
	return pub
}

var errLingerStubDoesNotBuildPublications = errors.New(
	"aeron: linger stub lifecycle does not build publications",
)

type lingerStubPublicationLifecycle struct {
	conductorResourceLifecycle
	registryKey string
}

func NewLingerStubPublicationLifecycle(registryKey string) conductor.ResourceLifecycle {
	return lingerStubPublicationLifecycle{registryKey: registryKey}
}

func (lingerStubPublicationLifecycle) NewPublication(
	conductor.PublicationBuffersReady,
	*logbuffer.LogBuffers,
	*cnc.CountersReader,
	string,
) (conductor.PublicationHandle, error) {
	return nil, errLingerStubDoesNotBuildPublications
}

func (l lingerStubPublicationLifecycle) NewExclusivePublication(
	event conductor.PublicationBuffersReady,
	logBuffers *logbuffer.LogBuffers,
	countersReader *cnc.CountersReader,
	channel string,
) (conductor.PublicationHandle, error) {
	return l.NewPublication(event, logBuffers, countersReader, channel)
}

func (l lingerStubPublicationLifecycle) RetirePublication(
	_ conductor.PublicationHandle,
) (string, error) {
	return l.registryKey, nil
}

func (client *Client) PublicationCountForTest() int {
	conductorInstance := client.conductorForTest()
	if conductorInstance == nil {
		return 0
	}
	return conductor.PublicationCountForTest(conductorInstance)
}

func (client *Client) SubscriptionCountForTest() int {
	conductorInstance := client.conductorForTest()
	if conductorInstance == nil {
		return 0
	}
	return conductor.SubscriptionCountForTest(conductorInstance)
}

func (client *Client) drainedForTest() bool {
	conductorInstance := client.conductorForTest()
	if conductorInstance == nil {
		return true
	}
	registry := conductor.LogBuffersRegistryForTest(conductorInstance)
	return conductor.PublicationCountForTest(conductorInstance) == 0 &&
		conductor.SubscriptionCountForTest(conductorInstance) == 0 &&
		!registry.HasScheduledLinger()
}

// AwaitDrainForTest polls DoWork until publications, subscriptions, and
// scheduled linger are drained or timeout elapses.
//
// NanoClock must advance between ticks when linger is scheduled; a frozen
// clock causes await-drain timeout even if wall time passes.
func (client *Client) AwaitDrainForTest(timeout time.Duration) error {
	if err := client.requireReadyWithConductor(); err != nil {
		return err
	}
	conductorInstance := client.conductor
	deadlineNs := client.nanoClock() + timeout.Nanoseconds()
	for client.nanoClock() < deadlineNs {
		if conductorInstance.IsTerminating() {
			return mapConductorTerminationError(conductorInstance)
		}
		client.DoWork()
		if client.drainedForTest() {
			return nil
		}
		time.Sleep(client.idleSleepDuration)
	}
	return errors.New("aeron: await drain timeout")
}

func (client *Client) AwaitResponseForTest(
	correlationID int64,
	kind conductor.PendingKind,
	timeout time.Duration,
	offer func() error,
) (conductor.PendingRegistration, error) {
	return client.awaitResponse(correlationID, kind, timeout, offer)
}

// PollAwaitForTest probes a single DoWork tick: one PollPending, DoWork, then
// PollPending again.
//
// It is not the full awaitResponse loop.
func (client *Client) PollAwaitForTest(correlationID int64) error {
	if err := client.requireReadyWithConductor(); err != nil {
		return err
	}
	mapErr := client.mapPollPendingErrorForTest(correlationID)
	done, _, err := client.conductor.PollPending(correlationID)
	if done {
		return mapErr(err)
	}
	client.DoWork()
	done, _, err = client.conductor.PollPending(correlationID)
	if !done {
		return ErrClientNotReady
	}
	return mapErr(err)
}

func NormalizeContextForTest(ctx Context) Context {
	return normalizeContext(ctx)
}

func ValidateConnectContextForTest(ctx Context) error {
	return validateConnectContext(ctx)
}

func (client *Client) SetCloseLingerDurationForTest(duration time.Duration) {
	conductorInstance := client.conductorForTest()
	if conductorInstance == nil {
		return
	}
	conductor.SetCloseLingerNsForTest(conductorInstance, duration.Nanoseconds())
}

func (ctx Context) UseConductorInvokerEnabledForTest() bool {
	return ctx.useConductorInvokerEnabled()
}

func WaitForDriverHeartbeatForTest(
	toDriver *ringbuffer.ManyToOneRingBuffer,
	timeout time.Duration,
	epochClock conductor.EpochClock,
) error {
	return waitForDriverHeartbeat(toDriver, timeout, epochClock)
}

func ValidateControlProtocolVersionForTest(reader *cnc.CountersReader) error {
	return validateControlProtocolVersion(reader)
}

func ValidateClientIDForTest(clientID int64) error {
	return validateClientID(clientID)
}

func NewAccessorTestClient(tb testing.TB) *Client {
	tb.Helper()

	mapped := cnctest.OpenAccessorTestMappedCncFile(tb)

	countersReader, err := cnc.NewCountersReader(
		mapped.CountersMetaDataBuffer(),
		mapped.CountersValuesBuffer(),
	)
	require.NoError(tb, err)

	return &Client{
		mapped:         mapped,
		countersReader: countersReader,
	}
}

func NewClientForTest(
	conductorInstance *conductor.Conductor,
	nanoClock conductor.NanoClock,
	clientID ...int64,
) *Client {
	var id int64
	if len(clientID) > 0 {
		id = clientID[0]
	}

	return NewClientForTestWithDriverProxy(
		conductorInstance,
		nil,
		nanoClock,
		id,
		defaultDriverTimeout,
		defaultIdleSleepDuration,
	)
}

func NewClientForTestWithDriverProxy(
	conductorInstance *conductor.Conductor,
	driverProxy *conductor.DriverProxy,
	nanoClock conductor.NanoClock,
	clientID int64,
	driverTimeout time.Duration,
	idleSleepDuration time.Duration,
) *Client {
	return &Client{
		clientID:          clientID,
		conductor:         conductorInstance,
		driverProxy:       driverProxy,
		nanoClock:         nanoClock,
		driverTimeout:     driverTimeout,
		idleSleepDuration: idleSleepDuration,
	}
}

func NewSubscriptionForTest(
	registrationID int64,
	streamID int32,
	channel string,
) *Subscription {
	return newSubscription(registrationID, streamID, channel)
}

func AvailableImageForTest() conductor.AvailableImage {
	return inMemAvailableImage()
}

func AvailableImageWithSubscriberPositionIDForTest(
	subscriberPositionID int32,
) conductor.AvailableImage {
	event := AvailableImageForTest()
	event.SubscriberPositionID = subscriberPositionID
	return event
}

func NewTestLogBuffersForTest(
	tb testing.TB,
	cfg TestPublicationLogConfig,
) *logbuffer.LogBuffers {
	tb.Helper()
	log := newTestMappedLog(tb, cfg)
	logBuffers, err := logbuffer.NewLogBuffers(log)
	require.NoError(tb, err)
	return logBuffers
}

func NewImageWithCommittedDataFrameForTest(
	tb testing.TB,
	joinPosition int64,
	frameOffset int,
) (*Image, int32) {
	tb.Helper()

	sub := newSubscription(
		TestSubscriptionRegistrationID,
		TestPublicationStreamID,
		TestImageSourceIdentity,
	)
	image := NewImageForTest(tb, sub, joinPosition)
	termBuffer := ImageTermBuffer0ForTest(image)
	payload := []byte("payload")
	WriteCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		TestPublicationSessionID,
		TestPublicationStreamID,
		TestPublicationInitialTermID,
		int32(frameOffset),
		logbuffer.Unfragmented,
		payload,
	)
	return image, int32(len(payload) + logbuffer.DataHeaderLength)
}

func NewImageDirectForTest(
	sub *Subscription,
	event conductor.AvailableImage,
	logBuffers *logbuffer.LogBuffers,
	counters *cnc.CountersReader,
) (*Image, error) {
	return newImage(sub, event, logBuffers, counters, nil)
}

func NewImageForTest(
	tb testing.TB,
	sub *Subscription,
	joinPosition int64,
) *Image {
	tb.Helper()
	return NewImageForTestWithCorrelationID(tb, sub, joinPosition, TestImageCorrelationID)
}

func NewImageForTestWithCorrelationID(
	tb testing.TB,
	sub *Subscription,
	joinPosition int64,
	correlationID int64,
) *Image {
	tb.Helper()
	event := AvailableImageForTest()
	event.CorrelationID = correlationID
	return newTestImage(tb, sub, joinPosition, event, nil)
}

func ImageHeaderForTest(image *Image) *Header {
	return &image.header
}

func ImageInternalCloseForTest(image *Image) {
	image.internalClose()
}

func ImageSubscriberErrorHandlerForTest(image *Image) func(error) {
	if image == nil {
		return nil
	}
	return image.errorHandler
}

func ImageInvokeSubscriberErrorHandlerForTest(image *Image, err error) {
	image.invokeSubscriberErrorHandler(err)
}

func SetPollAfterPaddingAdvanceForTest(hook func()) {
	setPollAfterPaddingAdvanceHook(hook)
}

func writeCommittedDataFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
	fragmented bool,
) {
	if !fragmented &&
		sessionID == inMemPublicationSessionID &&
		streamID == inMemPublicationStreamID {
		writeInMemCommittedDataFrame(
			termBuffer,
			frameOffset,
			termID,
			termOffset,
			flags,
			payload,
		)
		return
	}

	frameLength := int32(len(payload) + logbuffer.DataHeaderLength)
	var err error
	if fragmented {
		err = logbuffer.WriteDataHeaderFragmented(
			termBuffer,
			frameOffset,
			sessionID,
			streamID,
			termID,
			termOffset,
			flags,
		)
	} else {
		err = logbuffer.WriteDataHeaderUnfragmented(
			termBuffer,
			frameOffset,
			sessionID,
			streamID,
			termID,
			termOffset,
			flags,
		)
	}
	if err != nil {
		panic(err)
	}
	termBuffer.PutBytes(frameOffset+logbuffer.OffsetFramePayload, payload)
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, frameLength)
}

func WriteCommittedDataFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	writeCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
		false,
	)
}

func ImageTermBuffer0ForTest(image *Image) atomic.AtomicBuffer {
	return ImageTermBufferForTest(image, 0)
}

func ImageTermBufferForTest(image *Image, index int) atomic.AtomicBuffer {
	return image.termBuffers[index]
}

func WriteCommittedFragmentedDataFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	sessionID int32,
	streamID int32,
	termID int32,
	termOffset int32,
	flags uint8,
	payload []byte,
) {
	writeCommittedDataFrameForTest(
		termBuffer,
		frameOffset,
		sessionID,
		streamID,
		termID,
		termOffset,
		flags,
		payload,
		true,
	)
}

func WriteCommittedFragmentedDataFramesForTest(
	termBuffer atomic.AtomicBuffer,
	startOffset int,
	fragments []FragmentedFrameForTest,
) []int {
	offsets := make([]int, len(fragments))
	offset := startOffset
	for i, fragment := range fragments {
		offsets[i] = offset
		WriteCommittedFragmentedDataFrameForTest(
			termBuffer,
			offset,
			TestPublicationSessionID,
			TestPublicationStreamID,
			TestPublicationInitialTermID,
			int32(offset),
			fragment.Flags,
			fragment.Payload,
		)
		offset += logbuffer.AlignFragmentChunkLength(len(fragment.Payload))
	}
	return offsets
}

func WriteMinimalTermEndPaddingForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	paddingLength int32,
) {
	logbuffer.CommitMinimalTermEndPadding(termBuffer, frameOffset, paddingLength)
}

func WriteCommittedPaddingFrameForTest(
	termBuffer atomic.AtomicBuffer,
	frameOffset int,
	termID int32,
	termOffset int32,
	paddingLength int32,
) {
	if err := logbuffer.WritePaddingFrame(termBuffer, frameOffset, termID, termOffset); err != nil {
		panic(err)
	}
	logbuffer.PutFrameLengthOrdered(termBuffer, frameOffset, paddingLength)
}

func NewImageForTestWithErrorHandler(
	tb testing.TB,
	sub *Subscription,
	joinPosition int64,
	errorHandler func(error),
) *Image {
	tb.Helper()
	return newTestImage(tb, sub, joinPosition, AvailableImageForTest(), errorHandler)
}

func ImageSetSubscriberPositionForTest(image *Image, value int64) {
	setInMemImageSubscriberPosition(image, value)
}

func ImageTermBuffer0CapacityForTest(image *Image) int {
	return ImageTermBufferForTest(image, 0).Capacity()
}

func ImageHeaderBufferCapacityForTest(image *Image) int {
	return image.header.buffer.Capacity()
}

func SubscriptionAddImageForTest(sub *Subscription, image *Image) {
	sub.addImage(image)
}

func SubscriptionRemoveImageForTest(sub *Subscription, correlationID int64) *Image {
	return sub.removeImage(correlationID)
}

func SubscriptionImageAtIndexForTest(sub *Subscription, index int) (*Image, bool) {
	if uint(index) >= uint(len(sub.images)) {
		return nil, false
	}
	image := sub.images[index]
	return image, image != nil
}

func SubscriptionInjectNilImageAtIndexForTest(sub *Subscription, index int) {
	if index < 0 || index > len(sub.images) {
		return
	}
	sub.images = slices.Insert(sub.images, index, nil)
}

func SubscriptionSetRoundRobinIndexForTest(sub *Subscription, index uint32) {
	sub.roundRobinIndex = index
}

func SubscriptionSetAvailableImageHandlerForTest(
	sub *Subscription,
	handler func(*Image),
) {
	sub.availableImageHandler = handler
}

func SubscriptionSetUnavailableImageHandlerForTest(
	sub *Subscription,
	handler func(*Image),
) {
	sub.unavailableImageHandler = handler
}

func SubscriptionRoundRobinIndexForTest(sub *Subscription) uint32 {
	return sub.roundRobinIndex
}

func SubscriptionChannelStatusCounterIDForTest(sub *Subscription) int32 {
	return sub.channelStatusCounterID
}

func newTestImage(
	tb testing.TB,
	sub *Subscription,
	joinPosition int64,
	event conductor.AvailableImage,
	errorHandler func(error),
) *Image {
	tb.Helper()

	logBuffers := NewTestLogBuffersForTest(tb, DefaultTestPublicationLogConfig())
	counters := newTestCountersReaderWithSubscriberPositionRegistration(
		joinPosition,
		event.SubscriptionRegistrationID,
	)
	image, err := newImage(sub, event, logBuffers, counters, errorHandler)
	require.NoError(tb, err)
	return image
}

func newTestCountersReaderWithSubscriberPositionRegistration(
	joinPosition int64,
	registrationID int64,
) *cnc.CountersReader {
	return inMemCountersReaderWithSubscriberPosition(joinPosition, registrationID)
}

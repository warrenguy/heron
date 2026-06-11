package aeron

import (
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/cnc/cnctest"
	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

const (
	TestPublicationRegistrationID   int64 = 9001
	TestPublicationLimitCounterID   int32 = 1
	TestPublicationChannelCounterID int32 = 2
	TestPublicationSessionID        int32 = 1001
	TestPublicationStreamID         int32 = 2002
	TestPublicationInitialTermID    int32 = 7
	TestPublicationMTULength        int32 = 1408
	TestPublicationTermLength             = logbuffer.TermMinLength

	TestSubscriptionRegistrationID  int64 = 8001
	TestImageCorrelationID          int64 = 8101
	TestSubscriberPositionCounterID int32 = 3
	TestImageSourceIdentity               = "aeron:ipc"
	TestSubscriberPositionAtJoin    int64 = 128
)

type TestPublicationLogConfig struct {
	InitialTermID   int32
	ActiveTermCount int32
	RawTails        [logbuffer.PartitionCount]int64
	IsConnected     int32
}

func DefaultTestPublicationLogConfig() TestPublicationLogConfig {
	return TestPublicationLogConfig{
		InitialTermID:   TestPublicationInitialTermID,
		ActiveTermCount: 0,
		RawTails: [logbuffer.PartitionCount]int64{
			logbuffer.PackRawTail(TestPublicationInitialTermID, 0),
			logbuffer.PackRawTail(TestPublicationInitialTermID-2, 0),
			logbuffer.PackRawTail(TestPublicationInitialTermID-1, 0),
		},
		IsConnected: 1,
	}
}

func NewPublicationFromReadyForTest(
	ready conductor.PublicationBuffersReady,
	log *logbuffer.MappedLogBuffer,
	counters cnc.CountersView,
	channel string,
) (*Publication, error) {
	return newPublicationFromReady(ready, log, counters, channel)
}

func PublicationBuffersReadyForTest() conductor.PublicationBuffersReady {
	return testPublicationBuffersReady()
}

func OfferRetryForTest(pub *Publication, payload []byte) int64 {
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
	reported := make(map[fragmentedWriteSlot]struct{}, len(tracker.inflight)+len(tracker.overlapped))
	for key := range tracker.overlapped {
		if _, ok := reported[key]; ok {
			continue
		}
		reported[key] = struct{}{}
		result = append(result, key)
	}
	for key, count := range tracker.inflight {
		if count <= 0 {
			continue
		}
		if _, ok := reported[key]; ok {
			continue
		}
		reported[key] = struct{}{}
		result = append(result, key)
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
	fragmentedReservationTrack = func(termIndex, termOffset int, begin bool) {
		trackFragmentedSlot(&fragmentedReservationTracker, termIndex, termOffset, begin)
	}
	fragmentedWriteTrack = func(termIndex, frameOffset int, begin bool) {
		trackFragmentedSlot(&fragmentedWriteTracker, termIndex, frameOffset, begin)
	}
}

func DisableFragmentedWriteTrackingForTest() {
	fragmentedWriteTrack = nil
	fragmentedReservationTrack = nil
	fragmentedWriteTracker.reset()
	fragmentedReservationTracker.reset()
}

func SetFragmentedWriteTrackForTest(fn func(termIndex, frameOffset int, begin bool)) {
	fragmentedWriteTrack = fn
}

func ReportFragmentedWriteTrackerForTest() []fragmentedWriteSlot {
	return fragmentedWriteTracker.report()
}

func ReportFragmentedReservationTrackerForTest() []fragmentedWriteSlot {
	return fragmentedReservationTracker.report()
}

func OfferWithOffsetRetryForTest(
	pub *Publication,
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

func TryClaimRetryForTest(
	pub *Publication,
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
	return logbuffer.ActiveTermCount(pub.meta)
}

func NewTestPublicationWithoutTBForRapid(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *Publication {
	return newTestPublicationWithoutTB(logCfg, limitPosition)
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
	termCount := logbuffer.ActiveTermCount(pub.meta)
	index := logbuffer.IndexByTermCount(termCount)
	return index, pub.termBuffers[index]
}

func PublicationTermBufferForTest(pub *Publication, index int) atomic.AtomicBuffer {
	return pub.termBuffers[index]
}

func SetPublicationRawTailForTest(
	pub *Publication,
	partitionIndex int,
	rawTail int64,
) {
	_ = logbuffer.PutRawTail(pub.meta, partitionIndex, rawTail)
}

func SetPublicationActiveTermCountForTest(pub *Publication, termCount int32) {
	pub.meta.PutInt32Release(logbuffer.OffsetActiveTermCount, termCount)
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

type ErrCountersView struct {
	inner       cnc.CountersView
	failLimit   bool
	failChannel bool
}

func (view *ErrCountersView) CounterValue(counterID int32) (int64, error) {
	if view == nil || view.inner == nil {
		return 0, errors.New("aeron: counters view is nil")
	}
	if view.failLimit && counterID == TestPublicationLimitCounterID {
		return 0, errors.New("aeron: injected limit counter failure")
	}
	if view.failChannel && counterID == TestPublicationChannelCounterID {
		return 0, errors.New("aeron: injected channel counter failure")
	}
	return view.inner.CounterValue(counterID)
}

func (view *ErrCountersView) FindHeartbeatCounter(
	registrationID int64,
	typeID int32,
) (counterID int32, ok bool) {
	return view.inner.FindHeartbeatCounter(registrationID, typeID)
}

func (view *ErrCountersView) IsCounterActive(
	counterID int32,
	registrationID int64,
	typeID int32,
) bool {
	return view.inner.IsCounterActive(counterID, registrationID, typeID)
}

func NewTestPublicationWithCounters(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters cnc.CountersView,
) *Publication {
	tb.Helper()
	return newTestPublication(tb, logCfg, counters)
}

func NewErrCountersView(
	tb testing.TB,
	limitPosition int64,
	failLimit bool,
	failChannel bool,
) *ErrCountersView {
	tb.Helper()
	return &ErrCountersView{
		inner:       newTestCountersReaderWithoutTB(limitPosition),
		failLimit:   failLimit,
		failChannel: failChannel,
	}
}

func newTestMappedLog(tb testing.TB, cfg TestPublicationLogConfig) *logbuffer.MappedLogBuffer {
	if tb != nil {
		tb.Helper()
	}

	computed, err := logbuffer.ComputeLogLength(TestPublicationTermLength, logbuffer.PageMinSize)
	if tb != nil {
		require.NoError(tb, err)
	} else if err != nil {
		panic(err)
	}

	buf := make([]byte, computed)
	writeTestPublicationMetadata(buf, cfg)

	log, err := logbuffer.MapLogBufferFromBytes(buf)
	if tb != nil {
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = log.Close() })
		return log
	}
	if err != nil {
		panic(err)
	}
	return log
}

func testPublicationBuffersReady() conductor.PublicationBuffersReady {
	return conductor.PublicationBuffersReady{
		RegistrationID:            TestPublicationRegistrationID,
		SessionID:                 TestPublicationSessionID,
		StreamID:                  TestPublicationStreamID,
		PublicationLimitCounterID: TestPublicationLimitCounterID,
		ChannelStatusCounterID:    TestPublicationChannelCounterID,
		LogFileName:               "test.log",
	}
}

func newTestPublication(
	tb testing.TB,
	logCfg TestPublicationLogConfig,
	counters cnc.CountersView,
) *Publication {
	log := newTestMappedLog(tb, logCfg)
	pub, err := newPublicationFromReady(testPublicationBuffersReady(), log, counters, "aeron:ipc")
	if tb != nil {
		require.NoError(tb, err)
	} else if err != nil {
		panic(err)
	}
	return pub
}

func newTestPublicationWithoutTB(
	logCfg TestPublicationLogConfig,
	limitPosition int64,
) *Publication {
	return newTestPublication(nil, logCfg, newTestCountersReaderWithoutTB(limitPosition))
}

func WriteTestLogFileForTest(tb testing.TB, path string) {
	tb.Helper()

	computed, err := logbuffer.ComputeLogLength(TestPublicationTermLength, logbuffer.PageMinSize)
	require.NoError(tb, err)

	buf := make([]byte, computed)
	writeTestPublicationMetadata(buf, DefaultTestPublicationLogConfig())
	require.NoError(tb, os.WriteFile(path, buf, 0o644))
}

func writeTestPublicationMetadata(buf []byte, cfg TestPublicationLogConfig) {
	metaStart := logbuffer.MetaDataOffset(TestPublicationTermLength)
	meta := atomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])

	for i := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(meta, i, cfg.RawTails[i])
	}
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, cfg.ActiveTermCount)
	meta.PutInt64(logbuffer.OffsetCorrelationID, TestPublicationRegistrationID)
	meta.PutInt32(logbuffer.OffsetInitialTermID, cfg.InitialTermID)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	meta.PutInt32(logbuffer.OffsetTermLength, TestPublicationTermLength)
	meta.PutInt32(logbuffer.OffsetPageSize, logbuffer.PageMinSize)
	meta.PutInt32(logbuffer.OffsetMTULength, TestPublicationMTULength)
	meta.PutInt32Release(logbuffer.OffsetIsConnected, cfg.IsConnected)
	meta.Bytes()[logbuffer.OffsetLogType] = logbuffer.LogBufferTypeConcurrentPublication

	if err := logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		TestPublicationSessionID,
		TestPublicationStreamID,
		cfg.InitialTermID,
		0,
		logbuffer.Unfragmented,
	); err != nil {
		panic(err)
	}
}

func newTestCountersReaderFromRecords(
	counterCount int,
	writeRecords func(meta, values []byte),
) *cnc.CountersReader {
	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)
	writeRecords(meta, values)

	reader, err := cnc.NewCountersReader(meta, values)
	if err != nil {
		panic(err)
	}
	return reader
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
		err := cnc.WriteChannelStatusCounterRecord(
			meta,
			values,
			TestPublicationChannelCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSendChannelStatusTypeID,
			"aeron:ipc",
			TestPublicationRegistrationID,
			1,
		)
		if err != nil {
			panic(err)
		}
	})
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

func (client *Client) conductorForTest() *conductor.Conductor {
	if client == nil || client.conductor == nil {
		return nil
	}
	return client.conductor
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
	return client.PublicationCountForTest() == 0 &&
		client.SubscriptionCountForTest() == 0 &&
		!registry.HasScheduledLinger()
}

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
	done, _, err := client.conductor.PollPending(correlationID)
	if done {
		return mapPollPendingError(correlationID, client.driverTimeout, err)
	}
	client.DoWork()
	done, _, err = client.conductor.PollPending(correlationID)
	if !done {
		return ErrClientNotReady
	}
	return mapPollPendingError(correlationID, client.driverTimeout, err)
}

func NormalizeContextForTest(ctx Context) Context {
	return normalizeContext(ctx)
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

func ValidateControlProtocolVersionForTest(reader cnc.CountersView) error {
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
	return conductor.AvailableImage{
		CorrelationID:              TestImageCorrelationID,
		SessionID:                  TestPublicationSessionID,
		StreamID:                   TestPublicationStreamID,
		SubscriptionRegistrationID: TestSubscriptionRegistrationID,
		SubscriberPositionID:       TestSubscriberPositionCounterID,
		LogFileName:                "test-image.log",
		SourceIdentity:             TestImageSourceIdentity,
	}
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
	pollAfterPaddingAdvanceForTest = hook
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
	image.subscriberPos.SetRelease(value)
}

func HeaderSetFragmentedFrameLenForTest(header *Header, fragmentedFrameLen int32) {
	header.fragmentedFrameLen = fragmentedFrameLen
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
	return newTestCountersReaderFromRecords(4, func(meta, values []byte) {
		cnc.WriteCounterRecord(
			meta,
			values,
			TestSubscriberPositionCounterID,
			cnc.CounterRecordAllocated,
			cnc.DriverSubscriberPositionTypeID,
			registrationID,
			joinPosition,
		)
	})
}

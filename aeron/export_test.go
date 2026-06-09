package aeron

import (
	"errors"
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

func (view *ErrCountersView) GetCounterValue(counterID int32) (int64, error) {
	if view == nil || view.inner == nil {
		return 0, errors.New("aeron: counters view is nil")
	}
	if view.failLimit && counterID == TestPublicationLimitCounterID {
		return 0, errors.New("aeron: injected limit counter failure")
	}
	if view.failChannel && counterID == TestPublicationChannelCounterID {
		return 0, errors.New("aeron: injected channel counter failure")
	}
	return view.inner.GetCounterValue(counterID)
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

func newTestCountersReaderLimitOnlyWithoutTB(limitPosition int64) *cnc.CountersReader {
	const counterCount = 3
	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)

	cnc.WriteCounterRecord(
		meta,
		values,
		TestPublicationLimitCounterID,
		cnc.CounterRecordAllocated,
		cnc.DriverPublisherLimitTypeID,
		TestPublicationRegistrationID,
		limitPosition,
	)

	reader, err := cnc.NewCountersReader(meta, values)
	if err != nil {
		panic(err)
	}
	return reader
}

func newTestCountersReaderWithoutTB(limitPosition int64) *cnc.CountersReader {
	const counterCount = 3
	meta := make([]byte, cnc.CounterMetadataLength*counterCount)
	values := make([]byte, cnc.CounterValueLength*counterCount)

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

	reader, err := cnc.NewCountersReader(meta, values)
	if err != nil {
		panic(err)
	}
	return reader
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

func (client *Client) AwaitResponseForTest(
	correlationID int64,
	kind conductor.PendingKind,
	timeout time.Duration,
	offer func() error,
) (conductor.PendingRegistration, error) {
	return client.awaitResponse(correlationID, kind, timeout, offer)
}

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

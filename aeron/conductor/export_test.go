package conductor

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/broadcast"
	"github.com/warrenguy/heron/aeron/cnc"
	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/aeron/ringbuffer"
)

func (conductor *Conductor) DriverProxyForTest() *DriverProxy {
	return conductor.driverProxy
}

func (conductor *Conductor) CountersReaderForTest() *cnc.CountersReader {
	return conductor.countersReader
}

func (conductor *Conductor) SetHeartbeatCounterIDForTest(counterID int32) {
	conductor.heartbeatCounterID = counterID
}

func (conductor *Conductor) TimeOfLastKeepAliveNsForTest() int64 {
	return conductor.timeOfLastKeepAliveNs
}

func (conductor *Conductor) StoreClientHeartbeatForTest(nowNs, nowMs int64) int {
	return conductor.storeClientHeartbeat(nowNs, nowMs)
}

func (conductor *Conductor) SetTerminatingForTest(terminating bool) {
	conductor.isTerminating = terminating
}

func (proxy *DriverProxy) Ring() *ringbuffer.ManyToOneRingBuffer {
	return proxy.ring
}

func NewCopyBroadcastReceiverWithScratchLength(
	receiver *broadcast.BroadcastReceiver,
	scratchLength int,
) *CopyBroadcastReceiver {
	return newCopyBroadcastReceiver(receiver, scratchLength)
}

func (proxy *DriverProxy) OfferCorrelatedForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, false)
}

func (proxy *DriverProxy) OfferCorrelatedSilentForTest(msgTypeID int) error {
	return proxy.offerCorrelated(msgTypeID, true)
}

func (proxy *DriverProxy) OfferCorrelatedBodyForTest(
	msgTypeID int,
	bodyLength int,
	correlationID int64,
	silent bool,
	afterEncode func(encodedOffset int) error,
) error {
	return proxy.offerCorrelatedBody(msgTypeID, bodyLength, correlationID, silent, afterEncode)
}

func (proxy *DriverProxy) OfferBodyForTest(
	msgTypeID int,
	bodyLength int,
	silent bool,
	encode func(encodedOffset int) error,
) error {
	return proxy.offerBody(msgTypeID, bodyLength, silent, encode)
}

func NewConductorForTest(
	tb testing.TB,
	config Config,
	countersReader *cnc.CountersReader,
) *Conductor {
	tb.Helper()

	conductor, err := NewConductorUnchecked(config, countersReader)
	require.NoError(tb, err)
	return conductor
}

func DecodeTrailingStringASCIIForTest(
	buf aeronatomic.AtomicBuffer,
	offset, bodyLength, stringOffset int,
	wrapped error,
) (string, error) {
	return decodeTrailingStringASCII(buf, offset, bodyLength, stringOffset, wrapped)
}

func SetLogBuffersRegistryRefCountForTest(
	registry *LogBuffersRegistry,
	logFileName string,
	refCount int32,
) error {
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return err
	}
	entry := registry.entries[key]
	if entry == nil {
		return fmt.Errorf("conductor: log file %q not in registry", logFileName)
	}
	entry.refCount = refCount
	return nil
}

func ResolveLogFileKeyForTest(
	registry *LogBuffersRegistry,
	logFileName string,
) (string, error) {
	return registry.resolveLogFileKey(logFileName)
}

func ReleaseResolvedKeyForTest(
	registry *LogBuffersRegistry,
	key string,
	lingerNs int64,
) {
	registry.releaseResolvedKey(key, lingerNs)
}

func LogBuffersRegistryRefCountByResolvedKeyForTest(
	registry *LogBuffersRegistry,
	key string,
) int32 {
	if registry == nil {
		return 0
	}
	entry := registry.entries[key]
	if entry == nil {
		return 0
	}
	return entry.refCount
}

func SeedLogBuffersRegistryForTest(
	registry *LogBuffersRegistry,
	logFileName string,
	buffers *logbuffer.LogBuffers,
) error {
	if registry == nil {
		return errors.New("conductor: log buffers registry is nil")
	}
	if logFileName == "" {
		return errors.New("conductor: log file name is empty")
	}
	if buffers == nil {
		return errors.New("conductor: log buffers is nil")
	}
	key, err := registry.resolveLogFileKey(logFileName)
	if err != nil {
		return err
	}
	if registry.entries[key] != nil {
		return fmt.Errorf("conductor: log file %q already registered", logFileName)
	}
	registry.entries[key] = &logBufferEntry{
		buffers:  buffers,
		refCount: 1,
	}
	return nil
}

func ValidateLogFileNameForTest(logFileName, mediaDriverDir string) error {
	return validateLogFileName(logFileName, mediaDriverDir)
}

func lastOptionalForTest[T any](ptr *T) (T, bool) {
	if ptr == nil {
		var zero T
		return zero, false
	}
	return *ptr, true
}

func LastPublicationReadyForTest(conductor *Conductor) (PublicationBuffersReady, bool) {
	return lastOptionalForTest(conductor.lastPublicationReady)
}

func LastSubscriptionReadyForTest(conductor *Conductor) (SubscriptionReady, bool) {
	return lastOptionalForTest(conductor.lastSubscriptionReady)
}

func PublicationErrorsForTest(conductor *Conductor) []PublicationError {
	return slices.Clone(conductor.publicationErrors)
}

func (conductor *Conductor) OnPublicationReadyForTest(event PublicationBuffersReady) {
	conductor.onPublicationReady(event)
}

func (conductor *Conductor) OnSubscriptionReadyForTest(event SubscriptionReady) {
	conductor.onSubscriptionReady(event)
}

func (conductor *Conductor) OnAvailableImageForTest(event AvailableImage) {
	conductor.onAvailableImage(event)
}

func NewSyntheticLogBuffersForTest(tb testing.TB) *logbuffer.LogBuffers {
	tb.Helper()

	const termLength = 65536
	const pageSize = 4096
	computed, err := logbuffer.ComputeLogLength(termLength, pageSize)
	if err != nil {
		tb.Fatal(err)
	}

	buf := make([]byte, computed)
	metaStart := logbuffer.MetaDataOffset(termLength)
	meta := aeronatomic.Wrap(buf[metaStart : metaStart+logbuffer.MetaDataLength])
	for partition := range logbuffer.PartitionCount {
		_ = logbuffer.PutRawTail(
			meta,
			partition,
			logbuffer.PackRawTail(7, termLength),
		)
	}
	meta.PutInt32Release(logbuffer.OffsetTermLength, termLength)
	meta.PutInt32(logbuffer.OffsetPageSize, pageSize)
	meta.PutInt32(logbuffer.OffsetMTULength, 1408)
	meta.PutInt32(logbuffer.OffsetInitialTermID, 7)
	meta.PutInt32(logbuffer.OffsetDefaultFrameHeaderLength, logbuffer.DataHeaderLength)
	if err := logbuffer.WriteDataHeaderUnfragmented(
		meta,
		logbuffer.OffsetDefaultFrameHeader,
		1001,
		2002,
		7,
		0,
		logbuffer.Unfragmented,
	); err != nil {
		tb.Fatal(err)
	}

	mapped, err := logbuffer.MapLogBufferFromBytes(buf)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = mapped.Close() })

	logBuffers, err := logbuffer.NewLogBuffers(mapped)
	if err != nil {
		tb.Fatal(err)
	}
	return logBuffers
}

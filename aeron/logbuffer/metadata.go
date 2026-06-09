package logbuffer

import (
	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	OffsetTermTailCounters               = 0
	OffsetActiveTermCount                = 24
	OffsetEndOfStreamPosition            = 128
	OffsetIsConnected                    = 136
	OffsetActiveTransportCount           = 140
	OffsetCorrelationID                  = 256
	OffsetInitialTermID                  = 264
	OffsetDefaultFrameHeaderLength       = 268
	OffsetMTULength                      = 272
	OffsetTermLength                     = 276
	OffsetPageSize                       = 280
	OffsetPublicationWindowLength        = 284
	OffsetReceiverWindowLength           = 288
	OffsetSocketSndbufLength             = 292
	OffsetOSDefaultSocketSndbufLength    = 296
	OffsetOSMaxSocketSndbufLength        = 300
	OffsetSocketRcvbufLength             = 304
	OffsetOSDefaultSocketRcvbufLength    = 308
	OffsetOSMaxSocketRcvbufLength        = 312
	OffsetMaxResend                      = 316
	OffsetDefaultFrameHeader             = 320
	OffsetEntityTag                      = 448
	OffsetResponseCorrelationID          = 456
	OffsetLingerTimeoutNs                = 464
	OffsetUntetheredWindowLimitTimeoutNs = 472
	OffsetUntetheredRestingTimeoutNs     = 480
	OffsetGroup                          = 488
	OffsetIsResponse                     = 489
	OffsetRejoin                         = 490
	OffsetReliable                       = 491
	OffsetSparse                         = 492
	OffsetSignalEOS                      = 493
	OffsetSpiesSimulateConnection        = 494
	OffsetTether                         = 495
	OffsetIsPublicationRevoked           = 496
	OffsetLogType                        = 497 // Aeron: LOG_TYPE_OFFSET (OffsetType)
	OffsetUntetheredLingerTimeoutNs      = 500
)

// partitionIndex must be in [0, PartitionCount); out of range yields wrong
// metadata offsets with no bounds check.
func OffsetTermTailCounter(partitionIndex int) int {
	return OffsetTermTailCounters + partitionIndex*8
}

type MetadataView struct {
	InitialTermID int32
	TermLength    int32
	PageSize      int32
	MTULength     int32
	SessionID     int32
	StreamID      int32
	CorrelationID int64
	IsConnected   int32
	Type          uint8
}

// Point-in-time snapshot; not for hot-path volatile polling - use
// IsConnectedVolatile etc. on the live buffer instead.
func ParseMetadataView(meta atomic.AtomicBuffer) (MetadataView, error) {
	termLength, pageSize, err := readTermLengthAndPageSize(meta)
	if err != nil {
		return MetadataView{}, err
	}

	defaultHeaderOffset := OffsetDefaultFrameHeader
	return MetadataView{
		InitialTermID: meta.GetInt32(OffsetInitialTermID),
		TermLength:    termLength,
		PageSize:      pageSize,
		MTULength:     MTULength(meta),
		SessionID:     meta.GetInt32(defaultHeaderOffset + OffsetSessionID),
		StreamID:      meta.GetInt32(defaultHeaderOffset + OffsetStreamID),
		CorrelationID: meta.GetInt64(OffsetCorrelationID),
		IsConnected:   meta.GetInt32(OffsetIsConnected),
		Type:          meta.Bytes()[OffsetLogType],
	}, nil
}

func MTULength(meta atomic.AtomicBuffer) int32 {
	return meta.GetInt32(OffsetMTULength)
}

func IsConnectedVolatile(meta atomic.AtomicBuffer) int32 {
	atomic.AcquireFence()
	return meta.GetInt32Volatile(OffsetIsConnected)
}

// Returns the stored default header slice. Values in (0, DataHeaderLength)
// pass through unchanged; driver init writes 32. Callers copying the template
// must ensure length is at least DataHeaderLength.
func DefaultFrameHeaderBuffer(meta atomic.AtomicBuffer) atomic.AtomicBuffer {
	headerLength := meta.GetInt32(OffsetDefaultFrameHeaderLength)
	if headerLength <= 0 {
		headerLength = DataHeaderLength
	}
	if headerLength > DefaultFrameHeaderMaxLength {
		headerLength = DefaultFrameHeaderMaxLength
	}
	return meta.Slice(OffsetDefaultFrameHeader, int(headerLength))
}

func readTermLengthAndPageSize(meta atomic.AtomicBuffer) (int32, int32, error) {
	if meta.Capacity() < MetaDataLength {
		return 0, 0, errSpanTooShort("buffer", meta.Capacity(), MetaDataLength)
	}
	termLength := meta.GetInt32(OffsetTermLength)
	pageSize := meta.GetInt32(OffsetPageSize)
	if err := validateDimensions(termLength, pageSize); err != nil {
		return 0, 0, err
	}
	return termLength, pageSize, nil
}

func validateDimensions(termLength, pageSize int32) error {
	if err := CheckTermLength(int(termLength)); err != nil {
		return err
	}
	return CheckPageSize(int(pageSize))
}

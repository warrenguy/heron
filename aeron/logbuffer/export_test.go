package logbuffer

import (
	"os"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
)

const (
	TestTermLength     = 65536
	TestPageSize       = 4096
	TestComputedLength = 200704
	TestMTULength      = 1408
)

type SyntheticMetadataConfig struct {
	InitialTermID   int32
	ActiveTermCount int32
	RawTails        [PartitionCount]int64
	SessionID       int32
	StreamID        int32
	CorrelationID   int64
	IsConnected     int32
	MTULength       int32
	LogType         uint8
}

func DefaultSyntheticMetadataConfig() SyntheticMetadataConfig {
	return SyntheticMetadataConfig{
		InitialTermID:   7,
		ActiveTermCount: 0,
		RawTails: [PartitionCount]int64{
			PackRawTail(7, int32(TestTermLength)),
			PackRawTail(5, 0),
			PackRawTail(6, 0),
		},
		SessionID:     1001,
		StreamID:      2002,
		CorrelationID: 3003,
		IsConnected:   1,
		MTULength:     TestMTULength,
		LogType:       LogBufferTypeConcurrentPublication,
	}
}

func WriteSyntheticMetadata(buf []byte, cfg SyntheticMetadataConfig) {
	metaStart := MetaDataOffset(TestTermLength)
	meta := atomic.Wrap(buf[metaStart : metaStart+MetaDataLength])

	for i := range PartitionCount {
		_ = PutRawTail(meta, i, cfg.RawTails[i])
	}
	meta.PutInt32Release(OffsetActiveTermCount, cfg.ActiveTermCount)
	meta.PutInt64(OffsetCorrelationID, cfg.CorrelationID)
	meta.PutInt32(OffsetInitialTermID, cfg.InitialTermID)
	meta.PutInt32(OffsetDefaultFrameHeaderLength, DataHeaderLength)
	meta.PutInt32(OffsetTermLength, TestTermLength)
	meta.PutInt32(OffsetPageSize, TestPageSize)
	meta.PutInt32(OffsetMTULength, cfg.MTULength)
	meta.PutInt32(OffsetIsConnected, cfg.IsConnected)
	meta.Bytes()[OffsetLogType] = cfg.LogType

	if err := WriteDataHeaderUnfragmented(
		meta,
		OffsetDefaultFrameHeader,
		cfg.SessionID,
		cfg.StreamID,
		cfg.InitialTermID,
		0,
		Unfragmented,
	); err != nil {
		panic(err)
	}
}

func WriteTestLogFile(tb testing.TB, path string) {
	tb.Helper()
	buf := make([]byte, TestComputedLength)
	WriteSyntheticMetadata(buf, DefaultSyntheticMetadataConfig())
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		tb.Fatal(err)
	}
}

func MapLogBuffer(data []byte) (*MappedLogBuffer, error) {
	return mapLogBuffer(data)
}

func MapLogFileAfterDiscover(
	file *os.File,
	path string,
	termLength int32,
	pageSize int32,
	computed int,
) (*MappedLogBuffer, error) {
	return mapLogFileAfterDiscover(file, path, layoutDimensions{
		termLength: termLength,
		pageSize:   pageSize,
		computed:   computed,
	}, 0)
}

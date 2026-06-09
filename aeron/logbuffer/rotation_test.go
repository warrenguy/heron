package logbuffer_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

func TestRotateLog_synthetic(t *testing.T) {
	full := make([]byte, logbuffer.TestComputedLength)
	logbuffer.WriteSyntheticMetadata(full, logbuffer.DefaultSyntheticMetadataConfig())
	metaStart := logbuffer.MetaDataOffset(logbuffer.TestTermLength)
	meta := atomic.Wrap(full[metaStart : metaStart+logbuffer.MetaDataLength])

	tail0Before, err := logbuffer.GetRawTail(meta, 0)
	require.NoError(t, err)
	tail2Before, err := logbuffer.GetRawTail(meta, 2)
	require.NoError(t, err)

	require.True(t, logbuffer.RotateLog(meta, 0, 7))
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(meta))
	rawTail, err := logbuffer.GetRawTail(meta, 1)
	require.NoError(t, err)
	require.Equal(t, int32(8), logbuffer.TermID(rawTail))
	require.Equal(
		t,
		int32(0),
		logbuffer.TermOffset(rawTail, int32(logbuffer.TestTermLength)),
	)

	tail0After, err := logbuffer.GetRawTail(meta, 0)
	require.NoError(t, err)
	tail2After, err := logbuffer.GetRawTail(meta, 2)
	require.NoError(t, err)
	require.Equal(t, tail0Before, tail0After)
	require.Equal(t, tail2Before, tail2After)
}

func TestCasRawTail(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)

	initial := logbuffer.PackRawTail(5, 0)
	require.NoError(t, logbuffer.PutRawTail(meta, 1, initial))

	ok, err := logbuffer.CasRawTail(meta, 1, initial, logbuffer.PackRawTail(8, 0))
	require.NoError(t, err)
	require.True(t, ok)
	rawTail, err := logbuffer.GetRawTail(meta, 1)
	require.NoError(t, err)
	require.Equal(t, logbuffer.PackRawTail(8, 0), rawTail)

	ok, err = logbuffer.CasRawTail(meta, 1, initial, logbuffer.PackRawTail(9, 0))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCasRawTail_invalidPartition(t *testing.T) {
	meta := atomic.Wrap(make([]byte, logbuffer.MetaDataLength))
	_, err := logbuffer.GetRawTail(meta, 3)
	require.EqualError(t, err, "logbuffer: invalid partition index: 3")
	require.EqualError(t, logbuffer.PutRawTail(meta, -1, 0), "logbuffer: invalid partition index: -1")
	_, err = logbuffer.CasRawTail(meta, 3, 0, 1)
	require.EqualError(t, err, "logbuffer: invalid partition index: 3")
	_, err = logbuffer.CasRawTail(meta, -1, 0, 1)
	require.EqualError(t, err, "logbuffer: invalid partition index: -1")
}

func TestCasActiveTermCount(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)

	require.True(t, logbuffer.CasActiveTermCount(meta, 0, 1))
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(meta))
	require.False(t, logbuffer.CasActiveTermCount(meta, 0, 2))
}

func TestRotateLog_preconditionMismatchStillAdvancesCount(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)
	require.NoError(t, logbuffer.PutRawTail(meta, 1, logbuffer.PackRawTail(99, 0)))
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)

	require.True(t, logbuffer.RotateLog(meta, 0, 7))
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(meta))
	rawTail, err := logbuffer.GetRawTail(meta, 1)
	require.NoError(t, err)
	require.Equal(t, logbuffer.PackRawTail(99, 0), rawTail)
}

func TestRotateLog_staleCurrentTermCountCASFails(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 2)

	require.False(t, logbuffer.RotateLog(meta, 0, 7))
	require.Equal(t, int32(2), logbuffer.ActiveTermCount(meta))
}

func TestRotateLog_casRawTailRetry(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)
	require.NoError(t, logbuffer.PutRawTail(meta, 1, logbuffer.PackRawTail(5, 0)))
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	alternate := false
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				offset := int32(0)
				if alternate {
					offset = 1024
				}
				alternate = !alternate
				_ = logbuffer.PutRawTail(meta, 1, logbuffer.PackRawTail(5, offset))
			}
		}
	})

	require.True(t, logbuffer.RotateLog(meta, 0, 7))
	require.Equal(t, int32(1), logbuffer.ActiveTermCount(meta))
	close(stop)
	wg.Wait()
	rawTail, err := logbuffer.GetRawTail(meta, 1)
	require.NoError(t, err)
	require.Equal(t, int32(8), logbuffer.TermID(rawTail))
	require.Equal(
		t,
		int32(0),
		logbuffer.TermOffset(rawTail, int32(logbuffer.TestTermLength)),
	)
}

func TestRawTailVolatile_activeTermCountZero(t *testing.T) {
	buf := make([]byte, logbuffer.MetaDataLength)
	meta := atomic.Wrap(buf)
	require.NoError(t, logbuffer.PutRawTail(meta, 0, logbuffer.PackRawTail(7, 128)))
	meta.PutInt32Release(logbuffer.OffsetActiveTermCount, 0)

	rawTail, err := logbuffer.RawTailVolatile(meta)
	require.NoError(t, err)
	require.Equal(t, logbuffer.PackRawTail(7, 128), rawTail)
}

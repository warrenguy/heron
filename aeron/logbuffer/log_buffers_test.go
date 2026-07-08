package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/atomicbuffer"
)

func TestNewLogBuffers_nilMapped(t *testing.T) {
	_, err := logbuffer.NewLogBuffers(nil)
	require.ErrorContains(t, err, "mapped log buffer is nil")
}

func TestLogBuffers_happyPath(t *testing.T) {
	mapped := logbuffer.MapSyntheticLogBufferForTest(t)
	t.Cleanup(func() { _ = mapped.Close() })

	logBuffers, err := logbuffer.NewLogBuffers(mapped)
	require.NoError(t, err)

	meta, err := logBuffers.MetaDataBuffer()
	require.NoError(t, err)
	require.Equal(t, logbuffer.MetaDataLength, meta.Capacity())

	termBuffers, err := logBuffers.TermBuffers()
	require.NoError(t, err)
	for index := range logbuffer.PartitionCount {
		require.Equal(t, logbuffer.TestTermLength, termBuffers[index].Capacity())
	}

	require.Empty(t, logBuffers.Path())
}

func TestLogBuffers_termBuffersMatchIndividual(t *testing.T) {
	mapped := logbuffer.MapSyntheticLogBufferForTest(t)
	t.Cleanup(func() { _ = mapped.Close() })

	logBuffers, err := logbuffer.NewLogBuffers(mapped)
	require.NoError(t, err)

	batch, err := logBuffers.TermBuffers()
	require.NoError(t, err)

	for index := range logbuffer.PartitionCount {
		individual, err := mapped.TermBuffer(index)
		require.NoError(t, err)
		requireTermBufferSameBacking(t, batch[index], individual)
	}
}

func requireTermBufferSameBacking(
	tb testing.TB,
	batch atomicbuffer.AtomicBuffer,
	individual atomicbuffer.AtomicBuffer,
) {
	tb.Helper()
	require.Equal(tb, individual.Capacity(), batch.Capacity())

	batchBytes := batch.Bytes()
	individualBytes := individual.Bytes()
	if len(batchBytes) > 0 {
		require.Equal(tb, &batchBytes[0], &individualBytes[0])
	}
}

func TestLogBuffers_closedMappedTermBuffersError(t *testing.T) {
	mapped := logbuffer.MapSyntheticLogBufferForTest(t)

	logBuffers, err := logbuffer.NewLogBuffers(mapped)
	require.NoError(t, err)
	require.NoError(t, mapped.Close())

	for _, getErr := range []func() error{
		func() error { _, err := logBuffers.TermBuffers(); return err },
		func() error { _, err := logBuffers.MetaDataBuffer(); return err },
	} {
		require.ErrorContains(t, getErr(), "closed mapped log buffer")
	}
}

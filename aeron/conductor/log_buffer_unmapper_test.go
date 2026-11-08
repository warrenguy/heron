package conductor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/luas/aeron/logbuffer"
)

func TestLogBufferUnmapper_fullQueueUnmapsInline(t *testing.T) {
	t.Parallel()

	// The unmapper goroutine does not run, so each queued log stays mapped.
	queue := make(chan *logbuffer.MappedLogBuffer, logBufferUnmapQueueLength)
	done := make(chan struct{})
	unmapper := &logBufferUnmapper{queue: queue, done: done}

	queued := make([]*logbuffer.MappedLogBuffer, 0, logBufferUnmapQueueLength)
	for range logBufferUnmapQueueLength {
		mapped := NewSyntheticLogBuffersForTest(t).MappedLogBuffer()
		unmapper.unmap(mapped)
		queued = append(queued, mapped)
	}
	require.Len(t, queue, logBufferUnmapQueueLength)
	for _, mapped := range queued {
		require.NotNil(t, mapped.Bytes())
	}

	// A full queue unmaps on the caller goroutine.
	overflow := NewSyntheticLogBuffersForTest(t).MappedLogBuffer()
	unmapper.unmap(overflow)
	require.Nil(t, overflow.Bytes())

	// stop returns after the goroutine unmaps each queued log.
	go unmapQueuedLogBuffers(queue, done)
	unmapper.stop()
	for _, mapped := range queued {
		require.Nil(t, mapped.Bytes())
	}
	require.Nil(t, unmapper.queue)
}

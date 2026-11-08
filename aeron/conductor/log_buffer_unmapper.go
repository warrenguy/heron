package conductor

import "luas.fast/aeron/logbuffer"

// logBufferUnmapQueueLength caps the unmap queue. A full queue unmaps on the
// caller goroutine.
const logBufferUnmapQueueLength = 64

// logBufferUnmapper unmaps removed log buffers on its own goroutine.
//
// Behaviour: MappedLogBuffer.Close frees the pages of a log file that the
// driver has deleted. For a large log, the free takes milliseconds. The
// goroutine keeps that cost off the DoWork goroutine.
//
// Lifetime: start runs the goroutine. stop ends the goroutine after it drains
// the queue. start and stop are idempotent. A nil or stopped unmapper unmaps
// on the caller goroutine.
//
// Concurrency: callers must hold the resourceMu write lock for start, unmap
// and stop.
//
// Ownership: unmap passes ownership of the log buffer to the unmapper.
type logBufferUnmapper struct {
	// queue is nil while the goroutine is stopped.
	queue chan *logbuffer.MappedLogBuffer
	done  chan struct{}
}

func (unmapper *logBufferUnmapper) start() {
	if unmapper == nil || unmapper.queue != nil {
		return
	}
	unmapper.queue = make(chan *logbuffer.MappedLogBuffer, logBufferUnmapQueueLength)
	unmapper.done = make(chan struct{})
	go unmapQueuedLogBuffers(unmapper.queue, unmapper.done)
}

func unmapQueuedLogBuffers(
	queue <-chan *logbuffer.MappedLogBuffer,
	done chan<- struct{},
) {
	defer close(done)
	for mapped := range queue {
		_ = mapped.Close()
	}
}

func (unmapper *logBufferUnmapper) unmap(mapped *logbuffer.MappedLogBuffer) {
	if unmapper != nil && unmapper.queue != nil {
		select {
		case unmapper.queue <- mapped:
			return
		default:
		}
	}
	_ = mapped.Close()
}

func (unmapper *logBufferUnmapper) stop() {
	if unmapper == nil || unmapper.queue == nil {
		return
	}
	close(unmapper.queue)
	<-unmapper.done
	unmapper.queue = nil
	unmapper.done = nil
}

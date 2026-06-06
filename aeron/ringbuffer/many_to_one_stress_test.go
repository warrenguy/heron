//go:build stress

package ringbuffer_test

import (
	"bytes"
	"runtime"
	"sync"
	stdatomic "sync/atomic"
	"testing"

	aeronatomic "github.com/warrenguy/heron/aeron/atomic"
)

// MPSC stress uses plain stores; do not run under -race (false positives). See Unblock godoc.
// Run: make test-stress
func TestManyToOneRingBuffer_MPSCStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const (
		dataCapacity = 64 * 1024
		writers      = 4
		perWriter    = 2000
		msgLen       = 16
		total        = writers * perWriter
	)

	ring, _ := newTestRingBuffer(t, dataCapacity)
	payload := bytes.Repeat([]byte{0xBE}, msgLen)
	src := aeronatomic.Wrap(payload)

	var written stdatomic.Uint64
	var read stdatomic.Uint64
	done := make(chan struct{})

	go func() {
		defer close(done)
		for read.Load() < total {
			n := ring.Read(func(int, aeronatomic.AtomicBuffer, int, int) {
				read.Add(1)
			})
			if n == 0 {
				runtime.Gosched()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			for range perWriter {
				for {
					ok, err := ring.Write(testMsgTypeID, src, 0, msgLen)
					if err != nil {
						panic(err)
					}
					if ok {
						written.Add(1)
						break
					}
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	<-done

	if got := written.Load(); got != total {
		t.Fatalf("written=%d want %d", got, total)
	}
	if read.Load() != total {
		t.Fatalf("read=%d want %d", read.Load(), total)
	}
	assertConsumerCaughtUp(t, ring)
}

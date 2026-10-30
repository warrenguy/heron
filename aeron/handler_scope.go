package aeron

import (
	"bytes"
	"runtime"
	"sync/atomic"
)

// handlerScope stores the id of the goroutine that runs a user handler
// under clientLock.
type handlerScope struct {
	goroutine atomic.Int64
	// reporting is invoker-only. A reentrant call does not report
	// ErrReentrantCall again while ErrorHandler runs for that report.
	reporting bool
}

func (scope *handlerScope) EnterHandler() bool {
	if scope.goroutine.Load() != 0 {
		return false
	}
	scope.goroutine.Store(currentGoroutineID())
	return true
}

func (scope *handlerScope) ExitHandler() {
	scope.goroutine.Store(0)
}

// A zero id returns false before the stack read.
func (scope *handlerScope) isHandlerGoroutine() bool {
	id := scope.goroutine.Load()
	if id == 0 {
		return false
	}
	return id == currentGoroutineID()
}

// A malformed header returns 0.
func currentGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	header := buf[:n]
	if !bytes.HasPrefix(header, []byte(prefix)) {
		return 0
	}
	var id int64
	for _, c := range header[len(prefix):] {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + int64(c-'0')
	}
	return id
}

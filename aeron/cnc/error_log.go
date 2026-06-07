package cnc

import "github.com/warrenguy/heron/aeron/atomic"

const ErrorLogEntryHeaderLength = 24

func ErrorLogExists(buffer []byte) bool {
	if len(buffer) < 4 {
		return false
	}
	return atomic.Wrap(buffer).GetInt32Volatile(0) > 0
}

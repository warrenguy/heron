package cnc

import "github.com/warrenguy/heron/atomicbuffer"

const ErrorLogEntryHeaderLength = 24

func ErrorLogExists(buffer []byte) bool {
	if len(buffer) < 4 {
		return false
	}
	return atomicbuffer.Wrap(buffer).GetInt32Volatile(0) > 0
}

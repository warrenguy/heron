package mmap

import "golang.org/x/sys/unix"

// A failed populate leaves the pages to fault on first use.
func preTouch(data []byte) {
	_ = unix.Madvise(data, unix.MADV_POPULATE_WRITE)
}

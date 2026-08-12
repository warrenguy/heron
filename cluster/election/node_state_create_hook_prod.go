//go:build !testhooks

package election

import (
	"os"

	"github.com/warrenguy/heron/mmap"
)

func callInitialiseOnCreate(_ string, buf []byte, version int32) (int, int, error) {
	return initialiseOnCreate(buf, version)
}

func truncateNodeStateFile(file *os.File, size int64) error {
	return file.Truncate(size)
}

func mapNodeStateFile(file *os.File, length int) (*mmap.Mapping, error) {
	return mmap.MapOpenedFile(file, length)
}

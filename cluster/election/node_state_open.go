package election

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func openNodeStateFileCreateExclusive(path string) (*os.File, error) {
	fd, err := unix.Open(
		path,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600,
	)
	if err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return nil, os.ErrExist
		}
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openNodeStateFileExisting(path string) (*os.File, fs.FileInfo, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}

	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		return nil, nil, ErrCorruptNodeState
	}
	return file, info, nil
}

func tightenNodeStateFileMode(file *os.File, info fs.FileInfo) error {
	if info.Mode().Perm() == 0o600 {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return ErrCorruptNodeState
	}

	return unix.Fchmod(int(file.Fd()), 0o600)
}

func removePartialNodeStateFile(path string) {
	_ = os.Remove(path)
}

func abortCreateNodeStateFile(path string, file *os.File) {
	if file != nil {
		_ = file.Close()
	}
	removePartialNodeStateFile(path)
}

func validateNodeStateFileSize(size int64) error {
	if size != int64(MinimumNodeStateFileLength) {
		return ErrCorruptNodeState
	}
	return nil
}

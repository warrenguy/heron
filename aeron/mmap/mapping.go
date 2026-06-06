package mmap

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type Mapping struct {
	Data []byte
	file *os.File
}

func requirePositiveLength(length int) error {
	if length <= 0 {
		return errors.New("mmap: length must be positive")
	}
	return nil
}

// MapCreate truncates path to length before mmap; unsafe on live files larger than length.
func MapCreate(path string, length int) (*Mapping, error) {
	if err := requirePositiveLength(length); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(int64(length)); err != nil {
		_ = file.Close()
		return nil, err
	}
	return mmapOpenedFile(file, length, false)
}

// MapExisting rejects files shorter than length; does not truncate.
func MapExisting(path string, length int) (*Mapping, error) {
	if err := requirePositiveLength(length); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return mmapOpenedFile(file, length, true)
}

func mmapOpenedFile(file *os.File, length int, validateMinSize bool) (*Mapping, error) {
	if validateMinSize {
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if info.Size() < int64(length) {
			_ = file.Close()
			return nil, fmt.Errorf("mmap: file too short: %d < %d", info.Size(), length)
		}
	}
	data, err := unix.Mmap(int(file.Fd()), 0, length, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Mapping{Data: data, file: file}, nil
}

func PreTouch(data []byte, pageSize int) {
	if pageSize <= 0 {
		pageSize = os.Getpagesize()
	}
	for offset := 0; offset < len(data); offset += pageSize {
		_ = data[offset]
	}
}

func (m *Mapping) Close() error {
	if m == nil {
		return nil
	}
	err := unix.Munmap(m.Data)
	m.Data = nil
	if m.file != nil {
		if closeErr := m.file.Close(); err == nil {
			err = closeErr
		}
		m.file = nil
	}
	return err
}

package mmap

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type Mapping struct {
	Data []byte
	file *os.File
}

func Map(path string, length int) (*Mapping, error) {
	if length <= 0 {
		return nil, errors.New("mmap: length must be positive")
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(int64(length)); err != nil {
		_ = file.Close()
		return nil, err
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

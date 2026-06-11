package logbuffer

import (
	"errors"

	"github.com/warrenguy/heron/aeron/atomic"
)

// LogBuffers wraps a MappedLogBuffer for shared pub/image IPC lifetime.
//
// Conductor refcount acquire/release deduplicates mmap by path.
type LogBuffers struct {
	mapped *MappedLogBuffer
}

func NewLogBuffers(mapped *MappedLogBuffer) (*LogBuffers, error) {
	if mapped == nil {
		return nil, errors.New("logbuffer: mapped log buffer is nil")
	}
	return &LogBuffers{mapped: mapped}, nil
}

func (buffers *LogBuffers) mappedOrErr() (*MappedLogBuffer, error) {
	if buffers == nil || buffers.mapped == nil {
		return nil, errors.New("logbuffer: log buffers is nil")
	}
	return buffers.mapped, nil
}

func (buffers *LogBuffers) MetaDataBuffer() (atomic.AtomicBuffer, error) {
	mapped, err := buffers.mappedOrErr()
	if err != nil {
		return atomic.AtomicBuffer{}, err
	}
	return mapped.MetaDataBuffer()
}

func (buffers *LogBuffers) TermBuffers() ([PartitionCount]atomic.AtomicBuffer, error) {
	mapped, err := buffers.mappedOrErr()
	if err != nil {
		return [PartitionCount]atomic.AtomicBuffer{}, err
	}
	return mapped.TermBuffers()
}

// MappedLogBuffer returns the inner mapped buffer without a closed-state check.
//
// Prefer MetaDataBuffer and TermBuffers when validity matters.
func (buffers *LogBuffers) MappedLogBuffer() *MappedLogBuffer {
	if buffers == nil {
		return nil
	}
	return buffers.mapped
}

func (buffers *LogBuffers) Path() string {
	if buffers == nil {
		return ""
	}
	return buffers.mapped.Path()
}

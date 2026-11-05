// Package ringbuffer implements the Agrona ManyToOneRingBuffer wire layout.
//
// A buffer matches the Agrona and C driver memory layout for CnC and to-driver
// ring interop. The record headers and trailer counters are fixed. The package
// is not a generic MPSC queue.
package ringbuffer

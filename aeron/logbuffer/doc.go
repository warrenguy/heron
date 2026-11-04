// Package logbuffer implements the Aeron log buffer layout.
//
// A log holds PartitionCount term partitions from offset 0. The metadata fills
// the last MetaDataLength bytes. Page alignment can pad the gap between the
// terms and the metadata.
package logbuffer

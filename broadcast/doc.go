// Package broadcast implements the Agrona broadcast buffer wire layout.
//
// A buffer matches the Agrona and C driver broadcast descriptor layout for
// driver and CnC interop. The trailer and record layouts are fixed. The package
// is not a generic pub/sub queue.
package broadcast

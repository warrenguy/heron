package aeron

import (
	"github.com/warrenguy/heron/agrona/atomic"
)

// FragmentHandler receives one committed fragment during Poll. Offset points past
// the 32-byte frame header at payload start.
//
// The buffer view is valid only for the callback duration.
//
// Handlers must not retain the Header pointer beyond the callback; copy fields if
// needed after return.
type FragmentHandler func(
	buffer atomic.AtomicBuffer,
	offset int,
	length int,
	header *Header,
)

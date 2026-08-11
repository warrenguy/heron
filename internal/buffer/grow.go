// Package buffer provides small reusable byte-slice helpers for encode scratch
// and other retained buffers.
package buffer

// GrowScratch returns a slice of length total.
//
// When cap(scratch) >= total, re-slices scratch to that length. When capacity is
// short, allocates with capacity max(total, 2*cap(scratch)).
//
// The caller must retain the return as the next scratch. Does not clear old
// content. Must not use for credential or sensitive wire buffers that need a
// full clear on reuse.
func GrowScratch(scratch []byte, total int) []byte {
	if cap(scratch) < total {
		return make([]byte, total, max(total, cap(scratch)*2))
	}
	return scratch[:total]
}

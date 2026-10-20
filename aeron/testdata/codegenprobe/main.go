// Command codegenprobe links the OfferWithOffset, TryClaim, Commit, and Abort
// methods of both publication types for objdump.
package main

import (
	"os"

	"github.com/warrenguy/heron/aeron"
)

type publisher interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
	TryClaim(length int) (termIndex, frameOffset int, position int64, err error)
	Commit(termIndex, frameOffset int) error
	Abort(termIndex, frameOffset int) error
}

func main() {
	publishers := []publisher{&aeron.Publication{}, &aeron.ExclusivePublication{}}

	if len(os.Args) > len(publishers) {
		for _, p := range publishers {
			p.OfferWithOffset(nil, 0, 0)
			termIndex, frameOffset, _, _ := p.TryClaim(0)
			_ = p.Commit(termIndex, frameOffset)
			_ = p.Abort(termIndex, frameOffset)
		}
	}
}

// Command offerprobe links both OfferWithOffset methods for objdump.
package main

import (
	"os"

	"github.com/warrenguy/heron/aeron"
)

type offerer interface {
	OfferWithOffset(buffer []byte, offset, length int) int64
}

func main() {
	offerers := []offerer{&aeron.Publication{}, &aeron.ExclusivePublication{}}

	if len(os.Args) > len(offerers) {
		for _, o := range offerers {
			o.OfferWithOffset(nil, 0, 0)
		}
	}
}

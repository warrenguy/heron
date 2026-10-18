package main

import "github.com/warrenguy/heron/atomicbuffer"

func main() {
	buf := atomicbuffer.Wrap(make([]byte, 16))
	buf.PutInt32Release(0, -5)
	atomicbuffer.ReleaseFence()
	buf.PutInt64Release(8, -7)
	atomicbuffer.AcquireFence()
}

package main

import "github.com/warrenguy/heron/atomicbuffer"

func main() {
	atomicbuffer.AcquireFence()
	atomicbuffer.ReleaseFence()
}

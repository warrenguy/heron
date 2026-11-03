//go:build !linux

package mmap

func preTouch([]byte) {}

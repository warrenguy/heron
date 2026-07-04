//go:build integration && java_interop

package javainterop

import (
	"time"

	"github.com/warrenguy/heron/test/integration/harness"
)

// StreamJavaToGo is the IPC stream for Java publish / Go subscribe smoke.
const StreamJavaToGo int32 = 1100

// StreamGoToJava is the IPC stream for Go publish / Java subscribe smoke.
const StreamGoToJava int32 = 1101

// MessageCount is the smoke message count (K=32).
const MessageCount = 32

// PayloadSize is the smoke payload size in bytes.
const PayloadSize = 32

// OfferStallTimeout delegates to harness offer retry stall policy.
func OfferStallTimeout(timeout time.Duration) time.Duration {
	return harness.OfferStallTimeout(timeout)
}

// Payloads builds count fixed-size payloads for oracle parity with JavaPublisher.
//
// Index i is ASCII "java-" followed by decimal i (java-0 ... java-31),
// padded with 0x00 to size bytes.
func Payloads(count, size int) [][]byte {
	return harness.PayloadsWithPrefix("java-", count, size)
}

// GoPayloads builds count fixed-size payloads for oracle parity with JavaDuplex.
//
// Index i is ASCII "go-" followed by decimal i (go-0 ... go-31),
// padded with 0x00 to size bytes.
func GoPayloads(count, size int) [][]byte {
	return harness.PayloadsWithPrefix("go-", count, size)
}

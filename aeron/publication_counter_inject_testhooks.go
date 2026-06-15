//go:build testhooks

package aeron

import "testing"

// publicationCounterValueErrorHook is unsynchronized; tests must not mutate it
// concurrently without serializing set/clear with accessor calls.
var publicationCounterValueErrorHook func(*Publication, int32) error

// SetPublicationCounterValueErrorForTest injects CounterValue failures on cold accessors.
func SetPublicationCounterValueErrorForTest(
	tb testing.TB,
	pub *Publication,
	counterID int32,
	err error,
) {
	if pub == nil {
		panic("aeron: publication is nil")
	}
	tb.Helper()

	publicationCounterValueErrorHook = func(p *Publication, id int32) error {
		if p != pub || id != counterID {
			return nil
		}
		return err
	}
	tb.Cleanup(func() {
		publicationCounterValueErrorHook = nil
	})
}

func publicationCounterValueError(pub *Publication, counterID int32) error {
	if publicationCounterValueErrorHook == nil {
		return nil
	}
	return publicationCounterValueErrorHook(pub, counterID)
}

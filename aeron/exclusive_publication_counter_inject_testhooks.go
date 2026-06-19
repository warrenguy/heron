//go:build testhooks

package aeron

import "testing"

// exclusivePublicationCounterValueErrorHook is unsynchronized; tests must not mutate it
// concurrently without serializing set/clear with accessor calls.
var exclusivePublicationCounterValueErrorHook func(*ExclusivePublication, int32) error

// SetExclusivePublicationCounterValueErrorForTest injects CounterValue failures on cold accessors.
func SetExclusivePublicationCounterValueErrorForTest(
	tb testing.TB,
	ep *ExclusivePublication,
	counterID int32,
	err error,
) {
	if ep == nil {
		panic("aeron: exclusive publication is nil")
	}
	tb.Helper()

	exclusivePublicationCounterValueErrorHook = func(p *ExclusivePublication, id int32) error {
		if p != ep || id != counterID {
			return nil
		}
		return err
	}
	tb.Cleanup(func() {
		exclusivePublicationCounterValueErrorHook = nil
	})
}

func exclusivePublicationCounterValueError(ep *ExclusivePublication, counterID int32) error {
	if exclusivePublicationCounterValueErrorHook == nil {
		return nil
	}
	return exclusivePublicationCounterValueErrorHook(ep, counterID)
}

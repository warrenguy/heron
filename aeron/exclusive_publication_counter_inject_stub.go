//go:build !testhooks

package aeron

func exclusivePublicationCounterValueError(ep *ExclusivePublication, counterID int32) error {
	return nil
}

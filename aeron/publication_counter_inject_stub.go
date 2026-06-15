//go:build !testhooks

package aeron

func publicationCounterValueError(pub *Publication, counterID int32) error {
	return nil
}

//go:build !testhooks

package aeron

func trackFragmentedReservationBegin(termIndex, termOffset int) bool { return false }

func trackFragmentedReservationEnd(termIndex, termOffset int) {}

func trackFragmentedWrite(termIndex, frameOffset int, begin bool) {}

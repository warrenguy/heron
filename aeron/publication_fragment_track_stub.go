//go:build !testhooks

package aeron

func trackFragmentedReservationBegin(*Publication, int32, int) bool { return false }

func trackFragmentedReservationEnd(*Publication, int32, int) {}

func trackFragmentedWrite(*Publication, int32, int, bool) {}

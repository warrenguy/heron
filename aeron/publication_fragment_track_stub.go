//go:build !testhooks

package aeron

func trackFragmentedReservationBegin(*Publication, int, int) bool { return false }

func trackFragmentedReservationEnd(*Publication, int, int) {}

func trackFragmentedWrite(*Publication, int, int, bool) {}

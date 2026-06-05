package atomic_test

import (
	"errors"
	"testing"

	"github.com/warrenguy/heron/aeron/atomic"
)

func TestDataCapacity(t *testing.T) {
	const trailer, min = 128, 16
	got, err := atomic.DataCapacity(trailer+1024, trailer, min)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1024 {
		t.Fatalf("got %d want 1024", got)
	}
}

func TestDataCapacity_NotPowerOfTwo(t *testing.T) {
	_, err := atomic.DataCapacity(128+100, 128, 16)
	if !errors.Is(err, atomic.ErrDataCapacityNotPowerOfTwo) {
		t.Fatalf("got %v want ErrDataCapacityNotPowerOfTwo", err)
	}
}

func TestDataCapacity_BelowMinimum(t *testing.T) {
	_, err := atomic.DataCapacity(128+8, 128, 16)
	if !errors.Is(err, atomic.ErrDataCapacityBelowMinimum) {
		t.Fatalf("got %v want ErrDataCapacityBelowMinimum", err)
	}
}

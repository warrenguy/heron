package sbe_test

import (
	"testing"

	"github.com/warrenguy/heron/aeron/internal/testutil"
	"github.com/warrenguy/heron/aeron/sbe"
)

func TestEncodeIngressHeader_ZeroAllocs(t *testing.T) {
	buf, fields := ingressEncodeFixture(0)

	testutil.AssertZeroAllocs(t, "EncodeIngressHeader", 1000, func() {
		_, _ = sbe.EncodeIngressHeader(buf, 0, fields)
	})
}

package codecs_test

import (
	"testing"

	"luas.fast/cluster/codecs"
	"luas.fast/internal/testutil"
)

func TestEncodeIngressHeader_ZeroAllocs(t *testing.T) {
	buf, fields := ingressEncodeFixture(0)

	testutil.AssertZeroAllocs(t, "EncodeIngressHeader", 1000, func() {
		_, _ = codecs.EncodeIngressHeader(buf, 0, fields)
	})
}

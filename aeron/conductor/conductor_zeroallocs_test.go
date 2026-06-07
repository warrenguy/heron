package conductor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestConductor_DoWork_ZeroAllocs(t *testing.T) {
	conductorInstance := newConductorForTest(
		t,
		conductorTestConfig(t, func(error) {}, fixedClocks{nanoNs: 0}),
	)

	testutil.AssertZeroAllocs(t, "DoWork", 1000, func() {
		conductorInstance.DoWork()
	})
}

func TestConductor_DoWork_ZeroAllocsAfterKeepaliveTick(t *testing.T) {
	var nanoNs int64
	epochMs := int64(1_700_000_000_123)
	nanoNs = (600 * time.Millisecond).Nanoseconds()

	conductorInstance := newConductorForTest(
		t,
		conductorTestConfigMutableNano(t, func(error) {}, epochMs, &nanoNs),
		withHeartbeatCounter(t, conductorTestClientID, 0),
	)

	require.Equal(t, 1, conductorInstance.DoWork())

	got, err := conductorInstance.CountersReaderForTest().GetCounterValue(0)
	require.NoError(t, err)
	require.Equal(t, epochMs, got)

	tickStep := conductorTestKeepAliveInterval.Nanoseconds() + 1

	testutil.AssertZeroAllocs(t, "DoWorkAfterKeepalive", 1000, func() {
		nanoNs += tickStep
		conductorInstance.DoWork()
	})
}

package conductor

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConductor_lingerSweepBlocker_clearRemovesSideTableEntry(t *testing.T) {
	conductorInstance := NewConductorForTest(t, Config{ClientID: 42}, nil)

	var calls atomic.Int32
	conductorInstance.SetLingerSweepBlockerForTest(func() {
		calls.Add(1)
	})
	t.Cleanup(func() { conductorInstance.SetLingerSweepBlockerForTest(nil) })

	invokeLingerSweepBlocker(conductorInstance)
	require.Equal(t, int32(1), calls.Load())

	conductorInstance.SetLingerSweepBlockerForTest(nil)
	invokeLingerSweepBlocker(conductorInstance)
	require.Equal(t, int32(1), calls.Load())
}

func TestConductor_lingerSweepBlocker_nilReceiver(t *testing.T) {
	cases := []struct {
		name string
		run  func()
	}{
		{
			name: "set blocker",
			run:  func() { setLingerSweepBlockerHook(nil, func() {}) },
		},
		{
			name: "clear blocker",
			run:  func() { setLingerSweepBlockerHook(nil, nil) },
		},
		{
			name: "invoke",
			run:  func() { invokeLingerSweepBlocker(nil) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, tc.run)
		})
	}
}

package cnc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func TestLayout_goldenOffsets(t *testing.T) {
	tests := []struct {
		name string
		got  int
		want int
	}{
		{"CounterMetadataStateOffset", cnc.CounterMetadataStateOffset, 0},
		{"CounterMetadataTypeIDOffset", cnc.CounterMetadataTypeIDOffset, 4},
		{"CounterMetadataFreeForReuseDeadlineOffset", cnc.CounterMetadataFreeForReuseDeadlineOffset, 8},
		{"CounterMetadataKeyOffset", cnc.CounterMetadataKeyOffset, 16},
		{"CounterValueRegistrationIDOffset", cnc.CounterValueRegistrationIDOffset, 8},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.got)
		})
	}

	require.Equal(t, atomic.SizeInt32, cnc.CounterMetadataTypeIDOffset)
	require.Equal(
		t,
		cnc.CounterMetadataKeyOffset,
		cnc.CounterMetadataFreeForReuseDeadlineOffset+atomic.SizeInt64,
	)
	require.Equal(t, atomic.SizeInt64, cnc.CounterValueRegistrationIDOffset)
}

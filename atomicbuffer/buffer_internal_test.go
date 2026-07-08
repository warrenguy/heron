package atomicbuffer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceRangeInBounds(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		offset   int
		length   int
		want     bool
	}{
		{name: "mid slice", capacity: 8, offset: 0, length: 4, want: true},
		{name: "end aligned", capacity: 8, offset: 4, length: 4, want: true},
		{name: "zero at end", capacity: 8, offset: 8, length: 0, want: true},
		{name: "negative offset", capacity: 8, offset: -1, length: 0, want: false},
		{name: "negative length", capacity: 8, offset: 0, length: -1, want: false},
		{name: "past end", capacity: 8, offset: 5, length: 4, want: false},
		{name: "empty zero", capacity: 0, offset: 0, length: 0, want: true},
		{name: "empty nonzero", capacity: 0, offset: 0, length: 1, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(
				t,
				test.want,
				sliceRangeInBounds(test.capacity, test.offset, test.length),
			)
		})
	}
}

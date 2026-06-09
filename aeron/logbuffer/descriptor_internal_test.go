package logbuffer

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeLogLength_overflow(t *testing.T) {
	termLength := int(math.MaxInt64/int64(PartitionCount)) + 1
	_, err := rawLogLength(termLength)
	require.EqualError(t, err, "logbuffer: computed length overflow")

	_, err = ComputeLogLength(termLength, PageMinSize)
	require.EqualError(
		t,
		err,
		fmt.Sprintf(
			"logbuffer: term length above maximum: %d > %d",
			termLength,
			TermMaxLength,
		),
	)
}

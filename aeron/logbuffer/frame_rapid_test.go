//go:build rapid

package logbuffer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/warrenguy/heron/aeron/logbuffer"
	"github.com/warrenguy/heron/agrona/atomic"
)

func TestCommittedFrameLengthVolatile_termEndSlack_rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		paddingLength := rapid.Int32Range(1, 31).Draw(t, "paddingLength")

		var wantLength int32
		if paddingLength >= 4 {
			wantLength = paddingLength
		}

		buf := make([]byte, paddingLength)
		buffer := atomic.Wrap(buf)
		logbuffer.CommitMinimalTermEndPadding(buffer, 0, paddingLength)

		require.Zero(t, logbuffer.FrameLengthVolatile(buffer, 0))
		require.Equal(t, wantLength, logbuffer.CommittedFrameLengthVolatile(buffer, 0))
	})
}

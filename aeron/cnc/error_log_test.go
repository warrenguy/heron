package cnc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/atomic"
	"github.com/warrenguy/heron/aeron/cnc"
)

func errorLogBufferWithHeaderLength(length int32) []byte {
	buffer := make([]byte, cnc.ErrorLogEntryHeaderLength)
	atomic.Wrap(buffer).PutInt32Release(0, length)
	return buffer
}

func TestErrorLogExists(t *testing.T) {
	tests := []struct {
		name   string
		buffer []byte
		want   bool
	}{
		{
			name:   "empty buffer",
			buffer: nil,
			want:   false,
		},
		{
			name:   "short buffer",
			buffer: []byte{0x01, 0x02},
			want:   false,
		},
		{
			name:   "zero length entry",
			buffer: make([]byte, cnc.ErrorLogEntryHeaderLength),
			want:   false,
		},
		{
			name:   "non-zero length entry",
			buffer: errorLogBufferWithHeaderLength(32),
			want:   true,
		},
		{
			name:   "negative length treated as no entry",
			buffer: errorLogBufferWithHeaderLength(-2147483648),
			want:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cnc.ErrorLogExists(test.buffer))
		})
	}
}

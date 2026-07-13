package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAwaitListRecording_nilDescriptorPollerReturnsError(t *testing.T) {
	err := AwaitListRecording(
		t.Context(),
		nil,
		Context{},
		func() int { return 0 },
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

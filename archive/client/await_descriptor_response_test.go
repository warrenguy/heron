package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAwaitArchiveListRecording_nilDescriptorPollerReturnsError(t *testing.T) {
	err := AwaitArchiveListRecording(
		nil,
		ArchiveContext{},
		func() int { return 0 },
		nil,
		time.Second,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAwaitDescriptorControlResponse_nilDescriptorPollerReturnsError(t *testing.T) {
	_, err := AwaitDescriptorControlResponse(
		nil,
		ArchiveContext{},
		func() int { return 0 },
		nil,
		1,
		time.Second,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-nil")
}

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

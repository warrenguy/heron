package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewReplicationParams_upstreamDefaults(t *testing.T) {
	params := NewReplicationParams()

	require.Equal(t, ReplicationParams{
		DestinationRecordingID: -1,
		StopPosition:           -1,
		ChannelTagID:           -1,
		SubscriptionTagID:      -1,
		FileIOMaxLength:        -1,
		ReplicationSessionID:   -1,
	}, params)
}

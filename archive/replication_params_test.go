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

func TestReplicationParams_validate(t *testing.T) {
	cases := []struct {
		name    string
		edit    func(params *ReplicationParams)
		wantErr bool
	}{
		{name: "defaults", edit: func(*ReplicationParams) {}},
		{
			name: "live_destination_default_session_id",
			edit: func(params *ReplicationParams) {
				params.LiveDestination = []byte("aeron:udp?endpoint=127.0.0.1:0")
			},
		},
		{
			name: "live_destination_with_session_id",
			edit: func(params *ReplicationParams) {
				params.LiveDestination = []byte("aeron:udp?endpoint=127.0.0.1:0")
				params.ReplicationSessionID = 7
			},
			wantErr: true,
		},
		{
			name: "empty_live_destination_with_session_id",
			edit: func(params *ReplicationParams) {
				params.LiveDestination = []byte{}
				params.ReplicationSessionID = 7
			},
		},
		{
			name:    "file_io_max_length_zero",
			edit:    func(params *ReplicationParams) { params.FileIOMaxLength = 0 },
			wantErr: true,
		},
		{
			name:    "file_io_max_length_below_null",
			edit:    func(params *ReplicationParams) { params.FileIOMaxLength = -2 },
			wantErr: true,
		},
		{
			name: "file_io_max_length_positive",
			edit: func(params *ReplicationParams) { params.FileIOMaxLength = 4096 },
		},
		{
			name:    "zero_value",
			edit:    func(params *ReplicationParams) { *params = ReplicationParams{} },
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := NewReplicationParams()
			tc.edit(&params)

			err := params.validate()
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidReplicationParams)
				return
			}
			require.NoError(t, err)
		})
	}
}

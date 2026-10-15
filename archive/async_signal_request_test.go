//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestControlOfferResult(t *testing.T) {
	cases := []struct {
		name         string
		position     int64
		accepted     bool
		wantOffered  bool
		wantTerminal bool
	}{
		{name: "accepted", position: 128, accepted: true, wantOffered: true},
		{name: "back_pressured", position: aeron.BackPressured},
		{name: "admin_action", position: aeron.AdminAction},
		{name: "encode_failure", position: 0, wantTerminal: true},
		{name: "closed", position: aeron.Closed, wantTerminal: true},
		{name: "max_position_exceeded", position: aeron.MaxPositionExceeded, wantTerminal: true},
		{name: "not_connected", position: aeron.NotConnected, wantTerminal: true},
		{
			name:         "unsupported_message_length",
			position:     aeron.UnsupportedMessageLength,
			wantTerminal: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offered, err := controlOfferResult(tc.position, tc.accepted)
			require.Equal(t, tc.wantOffered, offered)
			if !tc.wantTerminal {
				require.NoError(t, err)
				return
			}
			requireControlTerminal(t, err, tc.position)
		})
	}
}

package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestPositionCodeName(t *testing.T) {
	tests := []struct {
		name     string
		position int64
		want     string
	}{
		{name: "notConnected", position: aeron.NotConnected, want: "NotConnected"},
		{name: "backPressured", position: aeron.BackPressured, want: "BackPressured"},
		{name: "adminAction", position: aeron.AdminAction, want: "AdminAction"},
		{name: "closed", position: aeron.Closed, want: "Closed"},
		{name: "maxPositionExceeded", position: aeron.MaxPositionExceeded, want: "MaxPositionExceeded"},
		{
			name:     "unsupportedMessageLength",
			position: aeron.UnsupportedMessageLength,
			want:     "UnsupportedMessageLength",
		},
		{name: "positiveStreamPosition", position: 128, want: "128"},
		{name: "zero", position: 0, want: "0"},
		{name: "unknownNegative", position: -99, want: "Unknown(-99)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.PositionCodeName(tc.position))
		})
	}
}

func TestPublicationPositionRetriability(t *testing.T) {
	tests := []struct {
		name        string
		position    int64
		wantOffer   bool
		wantIngress bool
	}{
		{name: "success", position: 1, wantOffer: false, wantIngress: false},
		{name: "zero", position: 0, wantOffer: false, wantIngress: false},
		{name: "notConnected", position: aeron.NotConnected, wantOffer: true, wantIngress: false},
		{name: "backPressured", position: aeron.BackPressured, wantOffer: true, wantIngress: true},
		{name: "adminAction", position: aeron.AdminAction, wantOffer: true, wantIngress: true},
		{name: "closed", position: aeron.Closed, wantOffer: false, wantIngress: false},
		{
			name:        "maxPositionExceeded",
			position:    aeron.MaxPositionExceeded,
			wantOffer:   false,
			wantIngress: false,
		},
		{
			name:        "unsupportedMessageLength",
			position:    aeron.UnsupportedMessageLength,
			wantOffer:   false,
			wantIngress: false,
		},
		{name: "unknownNegative", position: -99, wantOffer: false, wantIngress: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantOffer, aeron.IsOfferRetriable(tc.position))
			require.Equal(t, tc.wantIngress, aeron.IsIngressRetriable(tc.position))
		})
	}
}

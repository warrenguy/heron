package aeron_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// scriptedOfferPub returns OfferWithOffset results in script order. Calls past
// the script length return the last result again.
type scriptedOfferPub struct {
	results []int64
	calls   int
}

func (p *scriptedOfferPub) OfferWithOffset(
	_ []byte,
	_, _ int,
) int64 {
	i := p.calls
	p.calls++
	if i >= len(p.results) {
		return p.results[len(p.results)-1]
	}
	return p.results[i]
}

func TestOfferAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		results   []int64
		attempts  int
		want      int64
		wantCalls int
	}{
		{
			name:      "success first try",
			results:   []int64{42},
			attempts:  aeron.SendAttempts,
			want:      42,
			wantCalls: 1,
		},
		{
			name: "retries then success",
			results: []int64{
				aeron.BackPressured,
				aeron.AdminAction,
				99,
			},
			attempts:  aeron.SendAttempts,
			want:      99,
			wantCalls: 3,
		},
		{
			name: "exhausts retriable",
			results: []int64{
				aeron.NotConnected,
				aeron.BackPressured,
				aeron.AdminAction,
			},
			attempts:  aeron.SendAttempts,
			want:      aeron.AdminAction,
			wantCalls: 3,
		},
		{
			name:      "terminal stops immediately",
			results:   []int64{aeron.Closed, 50},
			attempts:  aeron.SendAttempts,
			want:      aeron.Closed,
			wantCalls: 1,
		},
		{
			name:      "zero attempts",
			results:   []int64{42},
			attempts:  0,
			want:      0,
			wantCalls: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &scriptedOfferPub{results: tt.results}
			got := aeron.OfferAttempts(pub, []byte("x"), tt.attempts)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantCalls, pub.calls)
		})
	}
}

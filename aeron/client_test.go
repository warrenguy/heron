package aeron_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestConnect_validationErrors(t *testing.T) {
	useInvoker := false

	tests := []struct {
		name        string
		cfg         aeron.Config
		wantErr     string
		fullConnect bool
	}{
		{
			name:    "requires aeron dir",
			cfg:     aeron.Config{},
			wantErr: "aeron: AeronDir is required",
		},
		{
			name: "rejects negative close linger duration",
			cfg: aeron.Config{
				AeronDir:            "unused",
				CloseLingerDuration: -time.Second,
			},
			wantErr: "aeron: CloseLingerDuration must not be negative",
		},
		{
			name: "rejects negative poll timeout",
			cfg: aeron.Config{
				AeronDir:    "unused",
				PollTimeout: -time.Second,
			},
			wantErr: "aeron: PollTimeout must not be negative",
		},
		{
			name: "rejects negative publication offer timeout",
			cfg: aeron.Config{
				AeronDir:                "unused",
				PublicationOfferTimeout: -time.Second,
			},
			wantErr: "aeron: PublicationOfferTimeout must not be negative",
		},
		{
			name: "rejects dedicated conductor invoker",
			cfg: aeron.Config{
				AeronDir:            "unused",
				UseConductorInvoker: &useInvoker,
			},
			wantErr: "aeron: dedicated conductor invoker is not supported",
		},
		{
			name: "wait cnc ready timeout",
			cfg: aeron.Config{
				AeronDir:      t.TempDir(),
				DriverTimeout: 10 * time.Millisecond,
			},
			wantErr:     "aeron: wait for cnc ready: cnc: timeout waiting for ready",
			fullConnect: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var err error
			if testCase.fullConnect {
				_, err = aeron.Connect(t.Context(), testCase.cfg)
			} else {
				err = aeron.ValidateConnectConfigForTest(
					aeron.NormalizeConfigForTest(testCase.cfg),
				)
			}
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

func TestConnect_contextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := aeron.Connect(ctx, aeron.Config{
		AeronDir:      t.TempDir(),
		DriverTimeout: time.Second,
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestConnect_contextDeadlineDuringCncWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()

	_, err := aeron.Connect(ctx, aeron.Config{
		AeronDir:      t.TempDir(),
		DriverTimeout: 5 * time.Second,
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

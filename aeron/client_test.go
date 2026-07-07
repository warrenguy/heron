package aeron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

func TestConnect_validationErrors(t *testing.T) {
	useInvoker := false

	tests := []struct {
		name        string
		ctx         aeron.Context
		wantErr     string
		fullConnect bool
	}{
		{
			name:    "requires aeron dir",
			ctx:     aeron.Context{},
			wantErr: "aeron: AeronDir is required",
		},
		{
			name: "rejects negative close linger duration",
			ctx: aeron.Context{
				AeronDir:            "unused",
				CloseLingerDuration: -time.Second,
			},
			wantErr: "aeron: CloseLingerDuration must not be negative",
		},
		{
			name: "rejects negative poll timeout",
			ctx: aeron.Context{
				AeronDir:    "unused",
				PollTimeout: -time.Second,
			},
			wantErr: "aeron: PollTimeout must not be negative",
		},
		{
			name: "rejects negative publication offer timeout",
			ctx: aeron.Context{
				AeronDir:                "unused",
				PublicationOfferTimeout: -time.Second,
			},
			wantErr: "aeron: PublicationOfferTimeout must not be negative",
		},
		{
			name: "rejects dedicated conductor invoker",
			ctx: aeron.Context{
				AeronDir:            "unused",
				UseConductorInvoker: &useInvoker,
			},
			wantErr: "aeron: dedicated conductor invoker is not supported",
		},
		{
			name: "wait cnc ready timeout",
			ctx: aeron.Context{
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
				_, err = aeron.Connect(testCase.ctx)
			} else {
				err = aeron.ValidateConnectContextForTest(
					aeron.NormalizeContextForTest(testCase.ctx),
				)
			}
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

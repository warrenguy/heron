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
		name    string
		ctx     aeron.Context
		wantErr string
	}{
		{
			name:    "requires aeron dir",
			ctx:     aeron.Context{},
			wantErr: "aeron: AeronDir is required",
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
			wantErr: "aeron: wait for cnc ready: cnc: timeout waiting for ready",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := aeron.Connect(testCase.ctx)
			require.EqualError(t, err, testCase.wantErr)
		})
	}
}

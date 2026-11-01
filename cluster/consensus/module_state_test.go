package consensus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var allModuleStates = []ModuleState{
	ModuleStateInit,
	ModuleStateActive,
	ModuleStateSuspended,
	ModuleStateSnapshot,
	ModuleStateQuitting,
	ModuleStateTerminating,
	ModuleStateClosed,
}

func TestModuleState_codes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state ModuleState
		code  int32
		name  string
	}{
		{state: ModuleStateInit, code: 0, name: "INIT"},
		{state: ModuleStateActive, code: 1, name: "ACTIVE"},
		{state: ModuleStateSuspended, code: 2, name: "SUSPENDED"},
		{state: ModuleStateSnapshot, code: 3, name: "SNAPSHOT"},
		{state: ModuleStateQuitting, code: 4, name: "QUITTING"},
		{state: ModuleStateTerminating, code: 5, name: "TERMINATING"},
		{state: ModuleStateClosed, code: 6, name: "CLOSED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.code, tt.state.Code())
			require.Equal(t, tt.name, tt.state.String())
			got, err := ModuleStateFromCode(int64(tt.code))
			require.NoError(t, err)
			require.Equal(t, tt.state, got)
		})
	}

	require.Equal(t, "ModuleState(7)", ModuleState(7).String())

	for _, code := range []int64{-1, 7} {
		_, err := ModuleStateFromCode(code)
		require.EqualError(
			t,
			err,
			fmt.Sprintf("consensus: invalid module state counter code: %d", code),
		)
	}
}

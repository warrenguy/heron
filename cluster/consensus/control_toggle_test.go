package consensus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var allControlToggleStates = []ControlToggleState{
	ControlToggleInactive,
	ControlToggleNeutral,
	ControlToggleSuspend,
	ControlToggleResume,
	ControlToggleSnapshot,
	ControlToggleShutdown,
	ControlToggleAbort,
	ControlToggleStandbySnapshot,
}

var allNodeControlToggleStates = []NodeControlToggleState{
	NodeControlToggleInactive,
	NodeControlToggleNeutral,
	NodeControlToggleReplicateStandbySnapshot,
	NodeControlToggleValidateRecordingLog,
}

// fakeToggle is a non-atomic ToggleCounter. Invoker-only.
type fakeToggle struct {
	value int64

	// afterLoad runs once after the next Load. It stands in for a tool write.
	afterLoad func()
}

func newFakeToggle(value int64) *fakeToggle { return &fakeToggle{value: value} }

func (f *fakeToggle) Load() int64 {
	value := f.value
	if hook := f.afterLoad; hook != nil {
		f.afterLoad = nil
		hook()
	}
	return value
}

func (f *fakeToggle) CompareAndSet(expect, update int64) bool {
	if f.value != expect {
		return false
	}
	f.value = update
	return true
}

func (f *fakeToggle) Store(value int64) { f.value = value }

func TestControlToggleState_codes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state ControlToggleState
		code  int32
		name  string
	}{
		{state: ControlToggleInactive, code: 0, name: "INACTIVE"},
		{state: ControlToggleNeutral, code: 1, name: "NEUTRAL"},
		{state: ControlToggleSuspend, code: 2, name: "SUSPEND"},
		{state: ControlToggleResume, code: 3, name: "RESUME"},
		{state: ControlToggleSnapshot, code: 4, name: "SNAPSHOT"},
		{state: ControlToggleShutdown, code: 5, name: "SHUTDOWN"},
		{state: ControlToggleAbort, code: 6, name: "ABORT"},
		{state: ControlToggleStandbySnapshot, code: 7, name: "STANDBY_SNAPSHOT"},
	}
	require.Len(t, tests, len(allControlToggleStates))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.code, tt.state.Code())
			require.Equal(t, tt.name, tt.state.String())
			got, err := ControlToggleStateFromCode(int64(tt.code))
			require.NoError(t, err)
			require.Equal(t, tt.state, got)
		})
	}

	require.Equal(t, "ControlToggleState(8)", ControlToggleState(8).String())
	require.Equal(t, "ControlToggleState(-1)", ControlToggleState(-1).String())
	for _, code := range []int64{-1, 8} {
		t.Run(fmt.Sprintf("code %d", code), func(t *testing.T) {
			t.Parallel()

			_, err := ControlToggleStateFromCode(code)
			require.EqualError(t, err, fmt.Sprintf("consensus: invalid control toggle value: %d", code))
		})
	}
}

func TestNodeControlToggleState_codes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state NodeControlToggleState
		code  int32
		name  string
	}{
		{state: NodeControlToggleInactive, code: 0, name: "INACTIVE"},
		{state: NodeControlToggleNeutral, code: 1, name: "NEUTRAL"},
		{
			state: NodeControlToggleReplicateStandbySnapshot,
			code:  2,
			name:  "REPLICATE_STANDBY_SNAPSHOT",
		},
		{state: NodeControlToggleValidateRecordingLog, code: 3, name: "VALIDATE_RECORDING_LOG"},
	}
	require.Len(t, tests, len(allNodeControlToggleStates))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.code, tt.state.Code())
			require.Equal(t, tt.name, tt.state.String())
			got, err := NodeControlToggleStateFromCode(int64(tt.code))
			require.NoError(t, err)
			require.Equal(t, tt.state, got)
		})
	}

	require.Equal(t, "NodeControlToggleState(4)", NodeControlToggleState(4).String())
	for _, code := range []int64{-1, 4} {
		t.Run(fmt.Sprintf("code %d", code), func(t *testing.T) {
			t.Parallel()

			_, err := NodeControlToggleStateFromCode(code)
			require.EqualError(
				t,
				err,
				fmt.Sprintf("consensus: invalid node control toggle value: %d", code),
			)
		})
	}
}

func TestControlToggleState_Toggle(t *testing.T) {
	t.Parallel()

	for _, state := range allControlToggleStates {
		for _, current := range allControlToggleStates {
			t.Run(state.String()+" on "+current.String(), func(t *testing.T) {
				t.Parallel()

				resumeOnSuspend := state == ControlToggleResume && current == ControlToggleSuspend
				wantMoved := current == ControlToggleNeutral || resumeOnSuspend
				want := int64(current)
				if wantMoved {
					want = int64(state)
				}
				counter := newFakeToggle(int64(current))

				require.Equal(t, wantMoved, state.Toggle(counter))
				require.Equal(t, want, counter.Load())
			})
		}
	}

	t.Run("nil counter", func(t *testing.T) {
		t.Parallel()

		for _, state := range allControlToggleStates {
			require.False(t, state.Toggle(nil), state.String())
		}
	})
}

func TestNodeControlToggleState_Toggle(t *testing.T) {
	t.Parallel()

	for _, state := range allNodeControlToggleStates {
		for _, current := range allNodeControlToggleStates {
			t.Run(state.String()+" on "+current.String(), func(t *testing.T) {
				t.Parallel()

				wantMoved := current == NodeControlToggleNeutral
				want := int64(current)
				if wantMoved {
					want = int64(state)
				}
				counter := newFakeToggle(int64(current))

				require.Equal(t, wantMoved, state.Toggle(counter))
				require.Equal(t, want, counter.Load())
			})
		}
	}

	t.Run("nil counter", func(t *testing.T) {
		t.Parallel()

		for _, state := range allNodeControlToggleStates {
			require.False(t, state.Toggle(nil), state.String())
		}
	})
}

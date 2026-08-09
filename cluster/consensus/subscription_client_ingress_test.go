//go:build testhooks

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// seedPendingBeginFragment polls only the BEGIN half of a BEGIN/END train so
// the controlled assembler holds a session builder. Returns the ingress and the
// session id of the seeded frames.
func seedPendingBeginFragment(t *testing.T) (*SubscriptionClientIngress, int32) {
	t.Helper()

	payload := []byte("pending-begin-reassembly-bytes")
	frames := buildBeginEndControlledFragments(t, payload)
	require.NotEmpty(t, frames)
	sessionID := frames[0].header.SessionID()
	poller := &fakeControlledFragmentPoller{frames: frames}
	ingress := NewSubscriptionClientIngress(poller)
	require.Equal(t, 1, ingress.Poll(1, func([]byte, int) aeron.ControlledAction {
		t.Fatal("BEGIN must not reach onFragment")
		return aeron.ControlledContinue
	}))
	return ingress, sessionID
}

func TestSubscriptionClientIngress_FreeSessionBuffer(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	require.True(t, ingress.FreeSessionBuffer(sessionID))
	require.False(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionClientIngress_FreeSessionBuffer_wrongSessionID(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	require.False(t, ingress.FreeSessionBuffer(sessionID+1))
	require.True(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionClientIngress_Clear(t *testing.T) {
	ingress, sessionID := seedPendingBeginFragment(t)

	ingress.Clear()

	require.False(t, ingress.FreeSessionBuffer(sessionID))
}

func TestSubscriptionClientIngress_teardownGuards_nilReceiverOrAssembler(t *testing.T) {
	cases := []struct {
		name    string
		ingress *SubscriptionClientIngress
	}{
		{name: "nil_receiver", ingress: nil},
		{name: "nil_assembler", ingress: &SubscriptionClientIngress{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, tc.ingress.FreeSessionBuffer(1))
			require.NotPanics(t, func() { tc.ingress.Clear() })
		})
	}
}

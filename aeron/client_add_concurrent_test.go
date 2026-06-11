//go:build !race

// Concurrent Close during blocking Add* races client lifecycle fields.
//
// Excluded from -race until clientLock serializes Close with awaitAdd on one client.

package aeron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/internal/testutil"
)

func TestClient_Add_concurrentCloseDuringPoll(t *testing.T) {
	tests := []struct {
		name             string
		addCall          func(*Client) (any, error)
		waitCmd          func(*testing.T, *testutil.ConductorFixture) int64
		conductorCountFn func(*testutil.ConductorFixture) int
		timeoutMsg       string
	}{
		{
			name: "publication",
			addCall: func(client *Client) (any, error) {
				return client.AddPublication(clientAddStreamID, clientAddChannel)
			},
			waitCmd: waitForAddPublicationCommand,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.PublicationCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddPublication client result",
		},
		{
			name: "subscription",
			addCall: func(client *Client) (any, error) {
				return client.AddSubscription(clientAddStreamID, clientAddChannel)
			},
			waitCmd: waitForAddSubscriptionCommand,
			conductorCountFn: func(fixture *testutil.ConductorFixture) int {
				return conductor.SubscriptionCountForTest(fixture.Conductor)
			},
			timeoutMsg: "timed out waiting for AddSubscription client result",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var nanoNs int64
			client, fixture := newClientAddFixture(
				t,
				func() int64 { return nanoNs },
				time.Second,
				time.Millisecond,
				t.TempDir(),
			)

			type result struct {
				handle any
				err    error
			}
			resultCh := make(chan result, 1)
			go func() {
				handle, err := testCase.addCall(client)
				resultCh <- result{handle: handle, err: err}
			}()

			correlationID := testCase.waitCmd(t, fixture)
			require.NoError(t, client.Close())

			got := receiveAddResult(
				t,
				resultCh,
				2*time.Second,
				testCase.timeoutMsg,
			)
			require.Nil(t, got.handle)
			require.ErrorIs(t, got.err, ErrClientClosed)
			_, ok := fixture.Conductor.PendingResult(correlationID)
			require.False(t, ok)
			require.Equal(t, 0, testCase.conductorCountFn(fixture))
		})
	}
}

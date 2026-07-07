package aeron_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

const pollTestShortTimeout = 5 * time.Millisecond

func pollTestClient(tb testing.TB) *aeron.Client {
	tb.Helper()
	return aeron.NewClientForTest(nil, func() int64 { return 0 })
}

func pollTestClientShort(tb testing.TB) *aeron.Client {
	tb.Helper()
	client := pollTestClient(tb)
	aeron.SetClientPollTimeoutsForTest(client, pollTestShortTimeout, pollTestShortTimeout)
	return client
}

func TestSubscriptionActiveImageCountAtLeast_emptySubscription(t *testing.T) {
	sub := aeron.NewSubscriptionForTest(77, 1001, "aeron:ipc")
	require.False(t, aeron.SubscriptionActiveImageCountAtLeast(sub, 1))
}

func TestSubscriptionActiveImageCountAtLeast_invalidWant(t *testing.T) {
	sub := aeron.NewSubscriptionForTest(77, 1001, "aeron:ipc")
	require.False(t, aeron.SubscriptionActiveImageCountAtLeast(sub, 0))
	require.False(t, aeron.SubscriptionActiveImageCountAtLeast(sub, -1))
}

func TestPollDeadline_usesClientPollTimeout(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)

	_, effective, err := aeron.PollDeadlineForTest(ctx, client)
	require.NoError(t, err)
	require.Equal(t, aeron.DefaultPollTimeout, effective)
}

func TestPollDeadline_contextDeadlineSooner_effectiveBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := pollTestClient(t)
	_, effective, err := aeron.PollDeadlineForTest(ctx, client)
	require.NoError(t, err)
	require.Less(t, effective, aeron.DefaultPollTimeout)
	require.InDelta(t, float64(50*time.Millisecond), float64(effective), float64(20*time.Millisecond))
}

func TestPreferContextError(t *testing.T) {
	sentinel := errors.New("sentinel")

	t.Run("activeContext", func(t *testing.T) {
		err := aeron.PreferContextError(t.Context(), sentinel)
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("canceled", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		err := aeron.PreferContextError(cancelCtx, sentinel)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("deadlineExceeded", func(t *testing.T) {
		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		err := aeron.PreferContextError(deadlineCtx, sentinel)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestPollUntilImageCount_invalidWant(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)
	sub := aeron.NewSubscriptionForTest(77, 1001, "aeron:ipc")

	tests := []struct {
		name string
		want int
	}{
		{name: "zero", want: 0},
		{name: "negative", want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := aeron.PollUntilImageCount(ctx, client, sub, tc.want)
			require.ErrorIs(t, err, aeron.ErrInvalidWant)
		})
	}
}

func TestPollUntilConnected_canceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := pollTestClient(t)
	pub := &fakePublicationOfferer{registrationID: 1, streamID: 100, sessionID: 200}

	err := aeron.PollUntilConnected(ctx, client, pub)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPollUntilConnected_success(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)
	pub := &fakePublicationOfferer{
		registrationID: 42,
		streamID:       1001,
		sessionID:      2002,
		connected:      true,
	}

	require.NoError(t, aeron.PollUntilConnected(ctx, client, pub))
}

func TestPollUntilConnected_successWinsOverConcurrentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := pollTestClient(t)
	pub := &fakePublicationOfferer{
		registrationID: 42,
		streamID:       1001,
		sessionID:      2002,
	}

	client.SetDoWorkAfterHookForTest(func() {
		pub.connected = true
		cancel()
	})
	t.Cleanup(func() { client.SetDoWorkAfterHookForTest(nil) })

	err := aeron.PollUntilConnected(ctx, client, pub)
	require.NoError(t, err)
}

func TestPollUntilConnected_timeoutError(t *testing.T) {
	ctx := context.Background()
	client := pollTestClientShort(t)
	pub := &fakePublicationOfferer{
		registrationID: 42,
		streamID:       1001,
		sessionID:      2002,
		position:       0,
	}

	err := aeron.PollUntilConnected(ctx, client, pub)
	var pollErr *aeron.PollUntilConnectedTimeoutError
	require.ErrorAs(t, err, &pollErr)
	require.EqualError(
		t,
		err,
		"aeron: publication 42 (stream 1001 session 2002) not connected within 5ms; position=0",
	)
}

func TestPollUntilImageCount_timeoutError(t *testing.T) {
	ctx := context.Background()
	client := pollTestClientShort(t)
	sub := aeron.NewSubscriptionForTest(77, 1001, "aeron:ipc")

	err := aeron.PollUntilImageCount(ctx, client, sub, 1)
	var pollErr *aeron.PollUntilImageCountTimeoutError
	require.ErrorAs(t, err, &pollErr)
	require.Contains(t, err.Error(), "active image count=0 want=1")
	require.Contains(t, err.Error(), "within 5ms")
}

func TestPollOneMessage_table(t *testing.T) {
	client := pollTestClient(t)
	shortClient := pollTestClientShort(t)

	tests := []struct {
		name    string
		setup   func(t *testing.T) (context.Context, *aeron.Subscription, *aeron.Client)
		want    []byte
		wantErr func(t *testing.T, err error)
	}{
		{
			name: "success",
			setup: func(t *testing.T) (context.Context, *aeron.Subscription, *aeron.Client) {
				sub := testSubscription(t)
				addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID, []byte("hello"))
				return context.Background(), sub, client
			},
			want: []byte("hello"),
		},
		{
			name: "firstFragmentOnly",
			setup: func(t *testing.T) (context.Context, *aeron.Subscription, *aeron.Client) {
				sub := testSubscription(t)
				addSubscriptionPollImage(
					t,
					sub,
					aeron.TestImageCorrelationID,
					[]byte("first"),
					[]byte("second"),
				)
				return context.Background(), sub, client
			},
			want: []byte("first"),
		},
		{
			name: "loopTimeout",
			setup: func(t *testing.T) (context.Context, *aeron.Subscription, *aeron.Client) {
				sub := testSubscription(t)
				addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID)
				return context.Background(), sub, shortClient
			},
			wantErr: func(t *testing.T, err error) {
				t.Helper()
				var pollErr *aeron.PollOneMessageTimeoutError
				require.ErrorAs(t, err, &pollErr)
				require.Contains(t, err.Error(), "within 5ms")
			},
		},
		{
			name: "canceledContext",
			setup: func(t *testing.T) (context.Context, *aeron.Subscription, *aeron.Client) {
				sub := testSubscription(t)
				addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, sub, client
			},
			wantErr: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, context.Canceled)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, sub, pollClient := tc.setup(t)
			got, err := aeron.PollOneMessage(ctx, pollClient, sub)
			if tc.wantErr != nil {
				tc.wantErr(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPollReassembled_preconditionNoActiveImages(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)
	sub := aeron.NewSubscriptionForTest(77, 1001, "aeron:ipc")

	_, err := aeron.PollReassembled(ctx, client, sub, make([]byte, 64))
	var pollErr *aeron.PollReassembledPreconditionError
	require.ErrorAs(t, err, &pollErr)
	require.Equal(t, "no active images", pollErr.Detail)
	require.EqualError(
		t,
		err,
		"aeron: subscription 77 reassembled 0/1 messages; fragments=0; no active images",
	)
}

func TestPollReassembled_bufferTooSmall(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)
	shortClient := pollTestClientShort(t)
	sub := testSubscription(t)
	payload := []byte("fragmented-payload-data")

	image := aeron.NewImageForTest(t, sub, 0)
	split := len(payload) / 2
	if split == 0 {
		split = 1
	}
	aeron.WriteCommittedDataFramesForTest(
		aeron.ImageTermBuffer0ForTest(image),
		0,
		[]aeron.FragmentedFrameForTest{
			{Flags: logbuffer.BeginFlag, Payload: payload[:split]},
			{Flags: logbuffer.EndFlag, Payload: payload[split:]},
		},
	)
	aeron.SubscriptionAddImageForTest(sub, image)

	_, err := aeron.PollReassembled(ctx, client, sub, make([]byte, 4))
	var bufErr *aeron.PollBufferTooSmallError
	require.ErrorAs(t, err, &bufErr)
	require.ErrorIs(t, err, aeron.ErrPollBufferTooSmall)
	require.Equal(t, len(payload), bufErr.Required)
	require.Equal(t, 4, bufErr.Have)

	// Message was consumed; retry with adequate buffer cannot recover it.
	retryBuf := make([]byte, len(payload))
	n, retryErr := aeron.PollReassembled(ctx, shortClient, sub, retryBuf)
	require.Zero(t, n)
	var pollErr *aeron.PollReassembledTimeoutError
	require.ErrorAs(t, retryErr, &pollErr)
	require.Contains(t, retryErr.Error(), "within 5ms")
}

func TestPollReassembled_loopTimeout(t *testing.T) {
	ctx := context.Background()
	client := pollTestClientShort(t)
	sub := testSubscription(t)
	addSubscriptionPollImage(t, sub, aeron.TestImageCorrelationID)

	_, err := aeron.PollReassembled(ctx, client, sub, make([]byte, 64))
	var pollErr *aeron.PollReassembledTimeoutError
	require.ErrorAs(t, err, &pollErr)
	require.NotEqual(t, "no active images", pollErr.Detail)
	require.Contains(t, pollErr.Detail, "active=1")
	require.Contains(t, err.Error(), "within 5ms")
}

func TestRunDuringClientWork_fnErrorPrecedence(t *testing.T) {
	ctx := context.Background()
	client := pollTestClient(t)
	wantErr := errors.New("fn failed")

	err := aeron.RunDuringClientWork(ctx, client, func() error {
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestRunDuringClientWork_canceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := pollTestClient(t)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := aeron.RunDuringClientWork(ctx, client, func() error {
		<-ctx.Done()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunDuringClientWork_timeoutError(t *testing.T) {
	ctx := context.Background()
	client := pollTestClientShort(t)

	err := aeron.RunDuringClientWork(ctx, client, func() error {
		time.Sleep(time.Second)
		return nil
	})
	var pollErr *aeron.RunDuringClientWorkTimeoutError
	require.ErrorAs(t, err, &pollErr)
	require.EqualError(t, err, "aeron: RunDuringClientWork timed out within 5ms")
}

func TestRunDuringClientWork_fnContinuesAfterTimeout(t *testing.T) {
	ctx := context.Background()
	client := pollTestClientShort(t)
	fnDone := make(chan struct{})

	err := aeron.RunDuringClientWork(ctx, client, func() error {
		time.Sleep(50 * time.Millisecond)
		close(fnDone)
		return nil
	})
	var pollErr *aeron.RunDuringClientWorkTimeoutError
	require.ErrorAs(t, err, &pollErr)

	select {
	case <-fnDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fn did not complete after helper returned timeout")
	}
}

func TestPollDeadline_contextDeadlineSooner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	client := pollTestClient(t)
	pub := &fakePublicationOfferer{registrationID: 1, streamID: 100, sessionID: 200}

	start := time.Now()
	err := aeron.PollUntilConnected(ctx, client, pub)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 200*time.Millisecond)
}

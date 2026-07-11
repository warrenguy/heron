package aeron_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

var (
	_ aeron.PublicationOfferer = (*aeron.Publication)(nil)
	_ aeron.PublicationOfferer = (*aeron.ExclusivePublication)(nil)
)

type fakePublicationOfferer struct {
	registrationID int64
	streamID       int32
	sessionID      int32
	position       int64
	connected      bool
	results        []int64
	exhausted      int64
	call           int
}

func (fake *fakePublicationOfferer) IsConnected() bool { return fake.connected }

func (fake *fakePublicationOfferer) RegistrationID() int64 { return fake.registrationID }

func (fake *fakePublicationOfferer) StreamID() int32 { return fake.streamID }

func (fake *fakePublicationOfferer) SessionID() int32 { return fake.sessionID }

func (fake *fakePublicationOfferer) Position() int64 { return fake.position }

func (fake *fakePublicationOfferer) Offer(_ []byte) int64 {
	var result int64
	switch {
	case fake.call < len(fake.results):
		result = fake.results[fake.call]
	case fake.exhausted != 0:
		result = fake.exhausted
	default:
		result = aeron.BackPressured
	}
	fake.call++
	if result > 0 {
		fake.position = result
	}
	return result
}

func offerRetryTestPub(mutate func(*fakePublicationOfferer)) *fakePublicationOfferer {
	const (
		regID    int64 = 42
		streamID int32 = 1001
		sessID   int32 = 2002
	)

	pub := &fakePublicationOfferer{
		registrationID: regID,
		streamID:       streamID,
		sessionID:      sessID,
	}
	if mutate != nil {
		mutate(pub)
	}
	return pub
}

func requireOfferTerminalStatus(tb testing.TB, err error, want int64) {
	tb.Helper()

	var terminal *aeron.OfferTerminalError
	require.ErrorAs(tb, err, &terminal)
	require.Equal(tb, want, terminal.Status)
}

func requireOfferStallStatus(tb testing.TB, err error, want int64) {
	tb.Helper()

	var stallErr *aeron.OfferStallError
	require.ErrorAs(tb, err, &stallErr)
	require.Equal(tb, want, stallErr.Status)
}

func TestOfferStallTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "ten_second_timeout", timeout: 10 * time.Second, want: 2 * time.Second},
		{name: "five_second_timeout", timeout: 5 * time.Second, want: time.Second},
		{name: "short_timeout", timeout: 2 * time.Second, want: 400 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, aeron.OfferStallTimeout(tc.timeout))
		})
	}
}

func TestOfferWithRetry_table(t *testing.T) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })
	twoCalls := 2

	tests := []struct {
		name      string
		pub       *fakePublicationOfferer
		timeout   time.Duration
		wantPos   int64
		wantCalls *int
		checkErr  func(t *testing.T, err error)
	}{
		{
			name: "successAfterBackpressure",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.results = []int64{aeron.BackPressured, 128}
			}),
			timeout:   50 * time.Millisecond,
			wantPos:   128,
			wantCalls: &twoCalls,
		},
		{
			name: "adminActionThenSuccess",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.results = []int64{aeron.AdminAction, 256}
			}),
			timeout:   50 * time.Millisecond,
			wantPos:   256,
			wantCalls: &twoCalls,
		},
		{
			name: "terminalClosed",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.results = []int64{aeron.Closed}
			}),
			timeout: time.Second,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				requireOfferTerminalStatus(t, err, aeron.Closed)
				require.EqualError(
					t,
					err,
					"aeron: publication 42 (stream 1001 session 2002) offer failed: status=Closed; position=0",
				)
			},
		},
		{
			name: "terminalMaxPositionExceeded",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.results = []int64{aeron.MaxPositionExceeded}
			}),
			timeout: time.Second,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				requireOfferTerminalStatus(t, err, aeron.MaxPositionExceeded)
				require.Contains(t, err.Error(), "status=MaxPositionExceeded")
			},
		},
		{
			name: "terminalUnsupportedMessageLength",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.results = []int64{aeron.UnsupportedMessageLength}
			}),
			timeout: time.Second,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				requireOfferTerminalStatus(t, err, aeron.UnsupportedMessageLength)
				require.Contains(t, err.Error(), "status=UnsupportedMessageLength")
			},
		},
		{
			name: "timeoutNotConnected",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.exhausted = aeron.NotConnected
			}),
			timeout: 5 * time.Millisecond,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				var timeoutErr *aeron.OfferTimeoutError
				require.ErrorAs(t, err, &timeoutErr)
				require.Equal(t, aeron.NotConnected, timeoutErr.Status)
				require.Contains(t, err.Error(), "offer timed out within 5ms")
			},
		},
		{
			name: "stallBackPressured",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.exhausted = aeron.BackPressured
			}),
			timeout: 5 * time.Second,
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var stallErr *aeron.OfferStallError
				require.ErrorAs(t, err, &stallErr)
				require.Equal(t, aeron.BackPressured, stallErr.Status)
				require.Equal(t, time.Second, stallErr.StallTimeout)
				require.Contains(t, err.Error(), "offer stalled within 1s")
			},
		},
		{
			name: "stallAdminAction",
			pub: offerRetryTestPub(func(pub *fakePublicationOfferer) {
				pub.exhausted = aeron.AdminAction
			}),
			timeout: 5 * time.Second,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				requireOfferStallStatus(t, err, aeron.AdminAction)
				require.Contains(t, err.Error(), "status=AdminAction")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aeron.SetClientPollTimeoutsForTest(client, tc.timeout, tc.timeout)
			pos, err := client.OfferWithRetry(t.Context(), tc.pub, []byte{0x01})
			if tc.checkErr != nil {
				tc.checkErr(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantPos, pos)
			}
			if tc.wantCalls != nil {
				require.Equal(t, *tc.wantCalls, tc.pub.call)
			}
		})
	}
}

func TestOfferWithRetryObserved_retriableObserver(t *testing.T) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })
	pub := offerRetryTestPub(func(pub *fakePublicationOfferer) {
		pub.results = []int64{aeron.BackPressured, aeron.AdminAction, 512}
	})

	var observed []int64
	pos, err := client.OfferWithRetryObserved(
		t.Context(), pub, []byte{0x01}, func(status int64) {
			observed = append(observed, status)
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(512), pos)
	require.Equal(t, []int64{aeron.BackPressured, aeron.AdminAction}, observed)
}

func TestOfferWithRetry_contextCanceled(t *testing.T) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })

	t.Run("preCanceled", func(t *testing.T) {
		pub := offerRetryTestPub(func(pub *fakePublicationOfferer) {
			pub.exhausted = aeron.BackPressured
		})

		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		pos, err := client.OfferWithRetry(cancelCtx, pub, []byte{0x01})
		require.Zero(t, pos)
		require.Zero(t, pub.call)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("midLoop", func(t *testing.T) {
		pub := offerRetryTestPub(func(pub *fakePublicationOfferer) {
			pub.exhausted = aeron.BackPressured
		})

		cancelCtx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		pos, err := client.OfferWithRetryObserved(
			cancelCtx, pub, []byte{0x01}, func(int64) {
				if pub.call >= 2 {
					cancel()
				}
			},
		)
		require.Zero(t, pos)
		require.Equal(t, 2, pub.call)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestOfferWithRetry_contextDeadline(t *testing.T) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })

	t.Run("preExpired", func(t *testing.T) {
		pub := offerRetryTestPub(func(pub *fakePublicationOfferer) {
			pub.exhausted = aeron.BackPressured
		})

		deadlineCtx, cancel := context.WithDeadline(
			t.Context(),
			time.Now().Add(-time.Second),
		)
		defer cancel()

		pos, err := client.OfferWithRetry(deadlineCtx, pub, []byte{0x01})
		require.Zero(t, pos)
		require.Zero(t, pub.call)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("ctxDeadlineSooner", func(t *testing.T) {
		pub := offerRetryTestPub(func(pub *fakePublicationOfferer) {
			pub.exhausted = aeron.BackPressured
		})

		deadlineCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
		defer cancel()

		start := time.Now()
		pos, err := client.OfferWithRetry(deadlineCtx, pub, []byte{0x01})
		elapsed := time.Since(start)

		require.Zero(t, pos)
		require.Equal(t, 1, pub.call)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, 200*time.Millisecond)
	})
}

type cancelOnOfferPublication struct {
	*fakePublicationOfferer
	cancel context.CancelFunc
}

func (pub *cancelOnOfferPublication) Offer(payload []byte) int64 {
	pub.cancel()
	return pub.fakePublicationOfferer.Offer(payload)
}

func TestOfferWithRetry_terminalAfterCancel(t *testing.T) {
	client := aeron.NewClientForTest(nil, func() int64 { return 0 })

	cancelCtx, cancel := context.WithCancel(t.Context())
	pub := &cancelOnOfferPublication{
		fakePublicationOfferer: offerRetryTestPub(func(pub *fakePublicationOfferer) {
			pub.exhausted = aeron.Closed
		}),
		cancel: cancel,
	}

	pos, err := client.OfferWithRetry(cancelCtx, pub, []byte{0x01})
	require.Zero(t, pos)
	require.Equal(t, 1, pub.call)
	require.ErrorIs(t, err, context.Canceled)
}

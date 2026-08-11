package cluster

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// retryScriptAdder scripts add outcomes for the retry policy tests. One value
// implements all three adder interfaces. Each add kind records returned handles
// in its own slice.
//
//   - failLeft: remaining transient failures before success. Negative never
//     succeeds.
//   - nilNil: every call returns (nil, nil)
type retryScriptAdder struct {
	calls    int
	failLeft int
	nilNil   bool
	failErr  error
	pubs     []*aeron.ExclusivePublication
	concPubs []*aeron.Publication
	subs     []*aeron.Subscription
}

func (f *retryScriptAdder) AddExclusivePublication(
	_ int32,
	_ string,
) (*aeron.ExclusivePublication, error) {
	return scriptNextAdd(f, &f.pubs)
}

func (f *retryScriptAdder) AddPublication(
	_ int32,
	_ string,
) (*aeron.Publication, error) {
	return scriptNextAdd(f, &f.concPubs)
}

func (f *retryScriptAdder) AddSubscription(
	_ int32,
	_ string,
) (*aeron.Subscription, error) {
	return scriptNextAdd(f, &f.subs)
}

// scriptNextAdd advances the script by one add call. On success the helper
// appends the fresh handle to added. Tests assert the checked helper returns
// that same handle.
func scriptNextAdd[R any](
	f *retryScriptAdder,
	added *[]*R,
) (*R, error) {
	f.calls++
	if f.nilNil {
		return nil, nil
	}
	if f.failLeft != 0 {
		if f.failLeft > 0 {
			f.failLeft--
		}
		return nil, f.failErr
	}
	resource := new(R)
	*added = append(*added, resource)
	return resource, nil
}

// withDriverAddRetryWindow sets driverAddRetryBudget and driverAddRetryPause for
// the test body. The helper mutates package-level pacing. Callers must not use
// t.Parallel.
func withDriverAddRetryWindow(
	t *testing.T,
	budget time.Duration,
	pause time.Duration,
) {
	t.Helper()
	origBudget := driverAddRetryBudget
	origPause := driverAddRetryPause
	driverAddRetryBudget = budget
	driverAddRetryPause = pause
	t.Cleanup(func() {
		driverAddRetryBudget = origBudget
		driverAddRetryPause = origPause
	})
}

// retriableRegistrationErr builds one code-10 driver error. endpoint names the
// driver resource still in CLOSING state. Use send_channel_endpoint for a
// publication add. Use receive_channel_endpoint for a subscription add.
func retriableRegistrationErr(
	correlationID int64,
	endpoint string,
) *aeron.RegistrationError {
	return &aeron.RegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     aeron.ErrorCodeResourceTemporarilyUnavailable,
		Message: "(-10) resource temporarily unavailable: " +
			endpoint + " found in CLOSING state, please retry",
	}
}

func TestAddExclusivePublicationChecked_retriesResourceTemporarilyUnavailable(
	t *testing.T,
) {
	// Shrink budget so two retries complete quickly.
	withDriverAddRetryWindow(t, time.Second, time.Millisecond)

	cause := retriableRegistrationErr(76, "send_channel_endpoint")
	adder := &retryScriptAdder{failLeft: 2, failErr: cause}

	pub, err := addExclusivePublicationChecked(
		adder,
		100,
		"aeron:udp?endpoint=127.0.0.1:9001",
		"cluster log publication",
	)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Equal(t, 3, adder.calls)
	require.Len(t, adder.pubs, 1)
	require.Same(t, pub, adder.pubs[0])
}

func TestAddExclusivePublicationChecked_nonRetriableFailsOnce(t *testing.T) {
	permanentReg := &aeron.RegistrationError{
		CorrelationID: 1,
		ErrorCode:     11,
		Message:       "invalid channel",
	}
	plain := errors.New("driver timeout")

	tests := []struct {
		name    string
		cause   error
		channel string
		what    string
		// wantMsg, when non-empty, is the exact EqualError string.
		wantMsg string
	}{
		{
			name:    "permanent registration",
			cause:   permanentReg,
			channel: "aeron:udp?endpoint=127.0.0.1:9001",
			what:    "cluster log publication",
			wantMsg: fmt.Sprintf(
				"cluster: add cluster log publication on aeron:udp?endpoint=127.0.0.1:9001: %s",
				permanentReg.Error(),
			),
		},
		{
			name:    "plain error",
			cause:   plain,
			channel: "aeron:ipc",
			what:    "consensus publication for member 2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adder := &retryScriptAdder{failLeft: -1, failErr: tc.cause}
			pub, err := addExclusivePublicationChecked(adder, 100, tc.channel, tc.what)
			require.Nil(t, pub)
			require.Equal(t, 1, adder.calls)
			require.ErrorIs(t, err, tc.cause)
			if tc.wantMsg != "" {
				require.EqualError(t, err, tc.wantMsg)
			}
		})
	}
}

func TestAddExclusivePublicationChecked_budgetExhaustedReturnsLastError(t *testing.T) {
	// Zero budget. First retriable failure returns without sleep.
	withDriverAddRetryWindow(t, 0, time.Millisecond)

	cause := retriableRegistrationErr(99, "send_channel_endpoint")
	adder := &retryScriptAdder{failLeft: -1, failErr: cause}

	pub, err := addExclusivePublicationChecked(
		adder,
		100,
		"aeron:udp?endpoint=127.0.0.1:9002",
		"cluster log publication reseed",
	)
	require.Nil(t, pub)
	require.Equal(t, 1, adder.calls)
	require.ErrorIs(t, err, cause)
	want := fmt.Sprintf(
		"cluster: add cluster log publication reseed on aeron:udp?endpoint=127.0.0.1:9002: %s",
		cause.Error(),
	)
	require.EqualError(t, err, want)
}

func TestAddExclusivePublicationChecked_nilNilNotRetried(t *testing.T) {
	adder := &retryScriptAdder{nilNil: true}

	pub, err := addExclusivePublicationChecked(
		adder,
		100,
		"aeron:ipc",
		"cluster log publication",
	)
	require.Nil(t, pub)
	require.Equal(t, 1, adder.calls)
	require.ErrorIs(t, err, errAdderReturnedNoPublication)
}

func TestAddPublicationChecked_retriesResourceTemporarilyUnavailable(t *testing.T) {
	// Shrink budget so two retries complete quickly.
	withDriverAddRetryWindow(t, time.Second, time.Millisecond)

	cause := retriableRegistrationErr(76, "send_channel_endpoint")
	adder := &retryScriptAdder{failLeft: 2, failErr: cause}

	pub, err := addPublicationChecked(
		adder,
		104,
		"aeron:ipc?term-length=128k",
		"service control publication",
	)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Equal(t, 3, adder.calls)
	require.Len(t, adder.concPubs, 1)
	require.Same(t, pub, adder.concPubs[0])
}

func TestAddPublicationChecked_nilNilNotRetried(t *testing.T) {
	adder := &retryScriptAdder{nilNil: true}

	pub, err := addPublicationChecked(
		adder,
		104,
		"aeron:ipc",
		"service control publication",
	)
	require.Nil(t, pub)
	require.Equal(t, 1, adder.calls)
	require.ErrorIs(t, err, errAdderReturnedNoPublication)
}

func TestAddSubscriptionChecked_retriesResourceTemporarilyUnavailable(
	t *testing.T,
) {
	// Shrink budget so two retries complete quickly.
	withDriverAddRetryWindow(t, time.Second, time.Millisecond)

	cause := retriableRegistrationErr(68, "receive_channel_endpoint")
	adder := &retryScriptAdder{failLeft: 2, failErr: cause}

	sub, err := addSubscriptionChecked(
		adder,
		100,
		"aeron:udp?term-length=64k|endpoint=127.0.0.1:26303",
		"consensus subscription",
	)
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Equal(t, 3, adder.calls)
	require.Len(t, adder.subs, 1)
	require.Same(t, sub, adder.subs[0])
}

func TestAddSubscriptionChecked_nilNilNotRetried(t *testing.T) {
	adder := &retryScriptAdder{nilNil: true}

	sub, err := addSubscriptionChecked(
		adder,
		100,
		"aeron:udp?endpoint=127.0.0.1:26303",
		"consensus subscription",
	)
	require.Nil(t, sub)
	require.Equal(t, 1, adder.calls)
	require.ErrorIs(t, err, errAdderReturnedNoSubscription)
}

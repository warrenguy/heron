package cluster

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
)

// retryScriptAdder scripts exclusive publication add outcomes for policy tests.
//
//   - permanent: every call returns failErr
//   - failLeft: remaining transient failures before success (ignored when permanent)
//   - nilNil: every call returns (nil, nil)
type retryScriptAdder struct {
	calls     int
	failLeft  int
	permanent bool
	nilNil    bool
	failErr   error
	pubs      []*aeron.ExclusivePublication
}

func (f *retryScriptAdder) AddExclusivePublication(
	streamID int32,
	channel string,
) (*aeron.ExclusivePublication, error) {
	_ = streamID
	_ = channel
	f.calls++
	if f.nilNil {
		return nil, nil
	}
	if f.permanent {
		return nil, f.failErr
	}
	if f.failLeft > 0 {
		f.failLeft--
		return nil, f.failErr
	}
	pub := new(aeron.ExclusivePublication)
	f.pubs = append(f.pubs, pub)
	return pub, nil
}

// withExclusivePubAddRetryWindow sets exclusivePubAddRetryBudget and
// exclusivePubAddRetryPause for the test body. Mutates package-level pacing.
// Callers must not use t.Parallel.
func withExclusivePubAddRetryWindow(
	t *testing.T,
	budget time.Duration,
	pause time.Duration,
) {
	t.Helper()
	origBudget := exclusivePubAddRetryBudget
	origPause := exclusivePubAddRetryPause
	exclusivePubAddRetryBudget = budget
	exclusivePubAddRetryPause = pause
	t.Cleanup(func() {
		exclusivePubAddRetryBudget = origBudget
		exclusivePubAddRetryPause = origPause
	})
}

func retriableRegistrationErr(correlationID int64) *aeron.RegistrationError {
	return &aeron.RegistrationError{
		CorrelationID: correlationID,
		ErrorCode:     aeron.ErrorCodeResourceTemporarilyUnavailable,
		Message: "(-10) resource temporarily unavailable: " +
			"send_channel_endpoint found in CLOSING state, please retry",
	}
}

func TestAddExclusivePublicationChecked_retriesResourceTemporarilyUnavailable(
	t *testing.T,
) {
	// Short pause. Two retries fit the default budget.
	withExclusivePubAddRetryWindow(t, time.Second, time.Millisecond)

	cause := retriableRegistrationErr(76)
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
			adder := &retryScriptAdder{permanent: true, failErr: tc.cause}
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
	withExclusivePubAddRetryWindow(t, 0, time.Millisecond)

	cause := retriableRegistrationErr(99)
	adder := &retryScriptAdder{permanent: true, failErr: cause}

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

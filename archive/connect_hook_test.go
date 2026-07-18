//go:build testhooks

package archive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectHookObservers_notifyAndCleanup(t *testing.T) {
	t.Parallel()

	harness, ac := newAsyncConnectFixture(t, func(cfg Config) Config { return cfg })
	registerAsyncConnectClose(t, ac)

	var asyncConnectCalls int
	SetConnectAsyncConnectObserverForTest(t, harness.Client, func(observed *AsyncConnect) {
		asyncConnectCalls++
		require.Same(t, ac, observed)
	})

	var stepCalls int
	SetAwaitConnectStepObserverForTest(t, ac, func() {
		stepCalls++
	})

	notifyConnectAsyncConnectObserver(ac)
	notifyAwaitConnectStepObserver(ac)
	notifyAwaitConnectStepObserver(ac)

	require.Equal(t, 1, asyncConnectCalls)
	require.Equal(t, 2, stepCalls)

	// Cleanup registered by Set*ForTest runs at test end; simulate it to assert deregistration.
	setConnectAsyncConnectObserver(harness.Client, nil)
	setAwaitConnectStepObserver(ac, nil)

	notifyConnectAsyncConnectObserver(ac)
	notifyAwaitConnectStepObserver(ac)
	require.Equal(t, 1, asyncConnectCalls)
	require.Equal(t, 2, stepCalls)
}

func TestConnectHookObservers_nilNotifyNoOp(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		notifyConnectAsyncConnectObserver(nil)
		notifyAwaitConnectStepObserver(nil)
	})
}

func TestConnectHookObservers_nilRegistrationPanics(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		SetConnectAsyncConnectObserverForTest(t, nil, func(*AsyncConnect) {})
	})
	require.Panics(t, func() {
		SetAwaitConnectStepObserverForTest(t, nil, func() {})
	})
}

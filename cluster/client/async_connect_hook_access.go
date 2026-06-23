//go:build testhooks

package client

import (
	"sync"
	"testing"

	"github.com/warrenguy/heron/aeron"
)

type asyncConnectEgressHooks struct {
	pollAddEgressSubscription       func(*aeron.Client, int64) (*aeron.Subscription, bool, error)
	egressSubscriptionRetryNotReady func() bool
}

var (
	asyncConnectEgressHooksMu  sync.Mutex
	asyncConnectEgressHooksMap = make(map[*AsyncConnect]*asyncConnectEgressHooks)
)

func setAsyncConnectEgressHooks(ac *AsyncConnect, hooks *asyncConnectEgressHooks) {
	asyncConnectEgressHooksMu.Lock()
	defer asyncConnectEgressHooksMu.Unlock()
	if hooks == nil {
		delete(asyncConnectEgressHooksMap, ac)
		return
	}
	asyncConnectEgressHooksMap[ac] = hooks
}

func asyncConnectEgressHooksFor(ac *AsyncConnect) *asyncConnectEgressHooks {
	if ac == nil {
		return nil
	}
	asyncConnectEgressHooksMu.Lock()
	hooks := asyncConnectEgressHooksMap[ac]
	asyncConnectEgressHooksMu.Unlock()
	return hooks
}

// SetEgressHooksForTest overrides egress PollAddSubscription and retry-not-ready for ac.
func (ac *AsyncConnect) SetEgressHooksForTest(
	tb testing.TB,
	pollAddEgressSubscription func(*aeron.Client, int64) (*aeron.Subscription, bool, error),
	egressSubscriptionRetryNotReady func() bool,
) {
	tb.Helper()
	if ac == nil {
		panic("cluster client: nil async connect")
	}
	setAsyncConnectEgressHooks(ac, &asyncConnectEgressHooks{
		pollAddEgressSubscription:       pollAddEgressSubscription,
		egressSubscriptionRetryNotReady: egressSubscriptionRetryNotReady,
	})
	tb.Cleanup(func() { setAsyncConnectEgressHooks(ac, nil) })
}

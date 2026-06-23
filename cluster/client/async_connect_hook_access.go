//go:build testhooks

package client

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

type asyncConnectEgressHooks struct {
	pollAddEgressSubscription       func(*aeron.Client, int64) (*aeron.Subscription, bool, error)
	egressSubscriptionRetryNotReady func() bool
}

var asyncConnectEgressHooksMap = testhook.NewRegistry[*AsyncConnect, *asyncConnectEgressHooks]()

func setAsyncConnectEgressHooks(ac *AsyncConnect, hooks *asyncConnectEgressHooks) {
	if hooks == nil {
		asyncConnectEgressHooksMap.Delete(ac)
		return
	}
	asyncConnectEgressHooksMap.Store(ac, hooks)
}

func asyncConnectEgressHooksFor(ac *AsyncConnect) *asyncConnectEgressHooks {
	if ac == nil {
		return nil
	}
	return asyncConnectEgressHooksMap.Load(ac)
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

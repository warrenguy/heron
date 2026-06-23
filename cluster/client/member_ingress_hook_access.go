//go:build testhooks

package client

import (
	"sync"
	"testing"

	"github.com/warrenguy/heron/aeron"
)

type memberIngressAsyncPollAddHook func(
	*MemberIngress,
	ClusterContext,
	*aeron.Client,
) (bool, error)

var (
	memberIngressAsyncPollAddHooksMu sync.Mutex
	memberIngressAsyncPollAddHooks   = make(map[*MemberIngress]memberIngressAsyncPollAddHook)
)

func setMemberIngressAsyncPollAddHook(
	ingress *MemberIngress,
	hook memberIngressAsyncPollAddHook,
) {
	memberIngressAsyncPollAddHooksMu.Lock()
	defer memberIngressAsyncPollAddHooksMu.Unlock()
	if hook == nil {
		delete(memberIngressAsyncPollAddHooks, ingress)
		return
	}
	memberIngressAsyncPollAddHooks[ingress] = hook
}

func memberIngressAsyncPollAddHookFor(
	ingress *MemberIngress,
) memberIngressAsyncPollAddHook {
	if ingress == nil {
		return nil
	}
	memberIngressAsyncPollAddHooksMu.Lock()
	hook := memberIngressAsyncPollAddHooks[ingress]
	memberIngressAsyncPollAddHooksMu.Unlock()
	return hook
}

// SetAsyncPollAddHookForTest overrides async ingress poll resolution for ingress.
func (ingress *MemberIngress) SetAsyncPollAddHookForTest(
	tb testing.TB,
	hook func(*MemberIngress, ClusterContext, *aeron.Client) (bool, error),
) {
	tb.Helper()
	if ingress == nil {
		panic("cluster client: nil member ingress")
	}
	setMemberIngressAsyncPollAddHook(ingress, hook)
	tb.Cleanup(func() { setMemberIngressAsyncPollAddHook(ingress, nil) })
}

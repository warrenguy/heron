//go:build testhooks

package client

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

type memberIngressAsyncPollAddHook func(
	*MemberIngress,
	ClusterContext,
	*aeron.Client,
) (bool, error)

var memberIngressAsyncPollAddHooks = testhook.NewRegistry[
	*MemberIngress,
	memberIngressAsyncPollAddHook,
]()

func setMemberIngressAsyncPollAddHook(
	ingress *MemberIngress,
	hook memberIngressAsyncPollAddHook,
) {
	if hook == nil {
		memberIngressAsyncPollAddHooks.Delete(ingress)
		return
	}
	memberIngressAsyncPollAddHooks.Store(ingress, hook)
}

func memberIngressAsyncPollAddHookFor(
	ingress *MemberIngress,
) memberIngressAsyncPollAddHook {
	if ingress == nil {
		return nil
	}
	return memberIngressAsyncPollAddHooks.Load(ingress)
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

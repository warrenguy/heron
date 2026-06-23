//go:build testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	ctx ClusterContext,
	client *aeron.Client,
) (bool, error) {
	if hook := memberIngressAsyncPollAddHookFor(ingress); hook != nil {
		return hook(ingress, ctx, client)
	}
	return ingress.asyncPollAddUnhooked(ctx, client)
}

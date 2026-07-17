//go:build testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	clusterCtx ClusterContext,
	client *aeron.Client,
) (bool, error) {
	if hook := memberIngressAsyncPollAddHookFor(ingress); hook != nil {
		return hook(ingress, clusterCtx, client)
	}
	return ingress.asyncPollAddUnhooked(clusterCtx, client)
}

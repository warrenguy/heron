//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	clusterCtx ClusterContext,
	client *aeron.Client,
) (bool, error) {
	return ingress.asyncPollAddUnhooked(clusterCtx, client)
}

//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	ctx ClusterContext,
	client *aeron.Client,
) (bool, error) {
	return ingress.asyncPollAddUnhooked(ctx, client)
}

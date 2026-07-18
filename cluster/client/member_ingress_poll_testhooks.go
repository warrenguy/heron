//go:build testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	cfg Config,
	client *aeron.Client,
) (bool, error) {
	if hook := memberIngressAsyncPollAddHookFor(ingress); hook != nil {
		return hook(ingress, cfg, client)
	}
	return ingress.asyncPollAddUnhooked(cfg, client)
}

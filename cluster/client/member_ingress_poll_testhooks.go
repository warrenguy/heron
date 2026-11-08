//go:build testhooks

package client

import "luas.fast/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	cfg Config,
	client *aeron.Client,
) (bool, error) {
	if hook := memberIngressAsyncPollAddHookFor(ingress); hook != nil {
		return hook(ingress, cfg, client)
	}
	return ingress.asyncPollAddUnhooked(cfg, client)
}

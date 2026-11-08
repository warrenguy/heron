//go:build !testhooks

package client

import "luas.fast/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	cfg Config,
	client *aeron.Client,
) (bool, error) {
	return ingress.asyncPollAddUnhooked(cfg, client)
}

//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func (ingress *MemberIngress) asyncPollAdd(
	cfg Config,
	client *aeron.Client,
) (bool, error) {
	return ingress.asyncPollAddUnhooked(cfg, client)
}

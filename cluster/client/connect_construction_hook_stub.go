//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	return nil, false
}

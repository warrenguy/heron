//go:build !testhooks

package client

import "luas.fast/aeron"

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	return nil, false
}

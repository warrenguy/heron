//go:build !testhooks

package archive

import "github.com/warrenguy/heron/aeron"

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	return nil, false
}

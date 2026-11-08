//go:build !testhooks

package archive

import "github.com/warrenguy/luas/aeron"

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	return nil, false
}

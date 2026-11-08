//go:build !testhooks

package archive

import "luas.fast/aeron"

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	return nil, false
}

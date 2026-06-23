//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func ownedConnectClientFactoryForContext(
	ctx ClusterContext,
) (func(ClusterContext) (*aeron.Client, error), bool) {
	return nil, false
}

//go:build !testhooks

package client

import "github.com/warrenguy/heron/aeron"

func ownedConnectClientFactoryForContext(
	ctx ArchiveContext,
) (func(ArchiveContext) (*aeron.Client, error), bool) {
	return nil, false
}

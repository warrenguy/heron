//go:build !testhooks

package archive

import "github.com/warrenguy/heron/aeron"

func ownedConnectClientFactoryForContext(
	archiveCtx Context,
) (func(Context) (*aeron.Client, error), bool) {
	return nil, false
}

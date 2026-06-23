//go:build testhooks

package client

import (
	"sync"
	"testing"

	"github.com/warrenguy/heron/aeron"
)

type connectConstructionHooks struct {
	ownedClientFactory func(ClusterContext) (*aeron.Client, error)
}

var (
	connectConstructionHooksMu  sync.Mutex
	connectConstructionHooksMap = make(map[string]*connectConstructionHooks)
)

func setConnectConstructionHooks(aeronDir string, hooks *connectConstructionHooks) {
	connectConstructionHooksMu.Lock()
	defer connectConstructionHooksMu.Unlock()
	if hooks == nil {
		delete(connectConstructionHooksMap, aeronDir)
		return
	}
	connectConstructionHooksMap[aeronDir] = hooks
}

func connectConstructionHooksFor(aeronDir string) *connectConstructionHooks {
	if aeronDir == "" {
		return nil
	}
	connectConstructionHooksMu.Lock()
	hooks := connectConstructionHooksMap[aeronDir]
	connectConstructionHooksMu.Unlock()
	return hooks
}

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// ctx.AeronDir. Each parallel test must use a distinct AeronDir such as t.TempDir().
func SetConnectConstructionHooksForTest(
	tb testing.TB,
	aeronDir string,
	ownedClientFactory func(ClusterContext) (*aeron.Client, error),
) {
	tb.Helper()
	if aeronDir == "" {
		panic("cluster client: AeronDir is required for connect construction hooks")
	}
	setConnectConstructionHooks(aeronDir, &connectConstructionHooks{
		ownedClientFactory: ownedClientFactory,
	})
	tb.Cleanup(func() { setConnectConstructionHooks(aeronDir, nil) })
}

func ownedConnectClientFactoryForContext(
	ctx ClusterContext,
) (func(ClusterContext) (*aeron.Client, error), bool) {
	hooks := connectConstructionHooksFor(ctx.AeronDir)
	if hooks == nil || hooks.ownedClientFactory == nil {
		return nil, false
	}
	return hooks.ownedClientFactory, true
}

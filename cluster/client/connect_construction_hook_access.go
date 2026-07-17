//go:build testhooks

package client

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

type connectConstructionHooks struct {
	ownedClientFactory func(ClusterContext) (*aeron.Client, error)
}

var connectConstructionHooksMap = testhook.NewRegistry[string, *connectConstructionHooks]()

func setConnectConstructionHooks(aeronDir string, hooks *connectConstructionHooks) {
	if hooks == nil {
		connectConstructionHooksMap.Delete(aeronDir)
		return
	}
	connectConstructionHooksMap.Store(aeronDir, hooks)
}

func connectConstructionHooksFor(aeronDir string) *connectConstructionHooks {
	if aeronDir == "" {
		return nil
	}
	return connectConstructionHooksMap.Load(aeronDir)
}

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// clusterCtx.AeronDir. Each parallel test must use a distinct AeronDir such as t.TempDir().
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
	clusterCtx ClusterContext,
) (func(ClusterContext) (*aeron.Client, error), bool) {
	hooks := connectConstructionHooksFor(clusterCtx.AeronDir)
	if hooks == nil || hooks.ownedClientFactory == nil {
		return nil, false
	}
	return hooks.ownedClientFactory, true
}

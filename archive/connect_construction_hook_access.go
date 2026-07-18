//go:build testhooks

package archive

import (
	"context"
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

var connectConstructionHooksMap = testhook.NewRegistry[
	string,
	func(Config) (*aeron.Client, error),
]()

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// cfg.AeronDir or cfg.AeronConfig.AeronDir. Each parallel test must use a
// distinct AeronDir such as t.TempDir().
func SetConnectConstructionHooksForTest(
	tb testing.TB,
	aeronDir string,
	ownedClientFactory func(Config) (*aeron.Client, error),
) {
	tb.Helper()
	if aeronDir == "" {
		panic("archive: AeronDir is required for connect construction hooks")
	}
	connectConstructionHooksMap.Store(aeronDir, ownedClientFactory)
	tb.Cleanup(func() { connectConstructionHooksMap.Delete(aeronDir) })
}

func ownedConnectClientFactoryForConfig(
	cfg Config,
) (func(Config) (*aeron.Client, error), bool) {
	factory := connectConstructionHooksMap.Load(archiveAeronDir(cfg))
	return factory, factory != nil
}

// ConcludeConfigForTest exercises concludeConfig during connect tests.
//
// ctx bounds owned transport attach; tests that inject Aeron or factories
// may pass context.Background().
func ConcludeConfigForTest(
	ctx context.Context,
	cfg Config,
) (Config, *aeron.Client, bool, error) {
	return concludeConfig(ctx, cfg)
}

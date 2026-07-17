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
	func(Context) (*aeron.Client, error),
]()

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// archiveCtx.AeronDir or archiveCtx.AeronContext.AeronDir. Each parallel test must use a
// distinct AeronDir such as t.TempDir().
func SetConnectConstructionHooksForTest(
	tb testing.TB,
	aeronDir string,
	ownedClientFactory func(Context) (*aeron.Client, error),
) {
	tb.Helper()
	if aeronDir == "" {
		panic("archive: AeronDir is required for connect construction hooks")
	}
	connectConstructionHooksMap.Store(aeronDir, ownedClientFactory)
	tb.Cleanup(func() { connectConstructionHooksMap.Delete(aeronDir) })
}

func ownedConnectClientFactoryForContext(
	archiveCtx Context,
) (func(Context) (*aeron.Client, error), bool) {
	factory := connectConstructionHooksMap.Load(archiveAeronDir(archiveCtx))
	return factory, factory != nil
}

// ConcludeContextForTest exercises concludeContext during connect tests.
//
// ctx bounds owned transport attach; tests that inject Aeron or factories
// may pass context.Background().
func ConcludeContextForTest(
	ctx context.Context,
	archiveCtx Context,
) (Context, *aeron.Client, bool, error) {
	return concludeContext(ctx, archiveCtx)
}

//go:build testhooks

package client

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

var connectConstructionHooksMap = testhook.NewRegistry[
	string,
	func(ArchiveContext) (*aeron.Client, error),
]()

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// ctx.AeronDir or ctx.AeronContext.AeronDir. Each parallel test must use a distinct
// AeronDir such as t.TempDir().
func SetConnectConstructionHooksForTest(
	tb testing.TB,
	aeronDir string,
	ownedClientFactory func(ArchiveContext) (*aeron.Client, error),
) {
	tb.Helper()
	if aeronDir == "" {
		panic("archive client: AeronDir is required for connect construction hooks")
	}
	connectConstructionHooksMap.Store(aeronDir, ownedClientFactory)
	tb.Cleanup(func() { connectConstructionHooksMap.Delete(aeronDir) })
}

func ownedConnectClientFactoryForContext(
	ctx ArchiveContext,
) (func(ArchiveContext) (*aeron.Client, error), bool) {
	factory := connectConstructionHooksMap.Load(archiveAeronDir(ctx))
	return factory, factory != nil
}

// ConcludeArchiveContextForTest exercises concludeArchiveContext during connect tests.
func ConcludeArchiveContextForTest(
	ctx ArchiveContext,
) (ArchiveContext, *aeron.Client, bool, error) {
	return concludeArchiveContext(ctx)
}

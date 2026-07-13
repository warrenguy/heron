//go:build testhooks

package archive

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

var connectConstructionHooksMap = testhook.NewRegistry[
	string,
	func(Context) (*aeron.Client, error),
]()

// SetConnectConstructionHooksForTest registers owned-client connect overrides keyed by
// ctx.AeronDir or ctx.AeronContext.AeronDir. Each parallel test must use a distinct
// AeronDir such as t.TempDir().
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
	ctx Context,
) (func(Context) (*aeron.Client, error), bool) {
	factory := connectConstructionHooksMap.Load(archiveAeronDir(ctx))
	return factory, factory != nil
}

// ConcludeContextForTest exercises concludeContext during connect tests.
func ConcludeContextForTest(
	ctx Context,
) (Context, *aeron.Client, bool, error) {
	return concludeContext(ctx)
}

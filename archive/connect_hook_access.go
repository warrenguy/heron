//go:build testhooks

package archive

import (
	"testing"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/internal/testhook"
)

type connectAsyncConnectObserver func(*AsyncConnect)

var connectAsyncConnectObservers = testhook.NewRegistry[
	*aeron.Client,
	connectAsyncConnectObserver,
]()

func setConnectAsyncConnectObserver(client *aeron.Client, observer connectAsyncConnectObserver) {
	if observer == nil {
		connectAsyncConnectObservers.Delete(client)
		return
	}
	connectAsyncConnectObservers.Store(client, observer)
}

func connectAsyncConnectObserverFor(client *aeron.Client) connectAsyncConnectObserver {
	if client == nil {
		return nil
	}
	return connectAsyncConnectObservers.Load(client)
}

// SetConnectAsyncConnectObserverForTest registers a callback keyed by the transport
// *aeron.Client pointer. Connect looks up the observer with ac.client after
// NewAsyncConnect.
//
// Injected-client tests: pass archiveCtx.Aeron (e.g. harness.Client).
// Owned-client tests: archiveCtx.Aeron is nil before Connect; register on the same
// *aeron.Client returned by SetConnectConstructionHooksForTest's factory (see
// ownedConnectContextForTest). A mismatched pointer silently skips the observer.
func SetConnectAsyncConnectObserverForTest(
	tb testing.TB,
	client *aeron.Client,
	observer connectAsyncConnectObserver,
) {
	tb.Helper()
	if client == nil {
		panic("archive: nil aeron client")
	}
	setConnectAsyncConnectObserver(client, observer)
	tb.Cleanup(func() { setConnectAsyncConnectObserver(client, nil) })
}

func notifyConnectAsyncConnectObserver(ac *AsyncConnect) {
	if ac == nil {
		return
	}
	if observer := connectAsyncConnectObserverFor(ac.client); observer != nil {
		observer(ac)
	}
}

type awaitConnectStepObserver func()

var awaitConnectStepObservers = testhook.NewRegistry[
	*AsyncConnect,
	awaitConnectStepObserver,
]()

func setAwaitConnectStepObserver(
	ac *AsyncConnect,
	observer awaitConnectStepObserver,
) {
	if observer == nil {
		awaitConnectStepObservers.Delete(ac)
		return
	}
	awaitConnectStepObservers.Store(ac, observer)
}

func awaitConnectStepObserverFor(ac *AsyncConnect) awaitConnectStepObserver {
	if ac == nil {
		return nil
	}
	return awaitConnectStepObservers.Load(ac)
}

// SetAwaitConnectStepObserverForTest registers a callback invoked at the start
// of each awaitConnect loop iteration before DoWork and Poll.
func SetAwaitConnectStepObserverForTest(
	tb testing.TB,
	ac *AsyncConnect,
	observer awaitConnectStepObserver,
) {
	tb.Helper()
	if ac == nil {
		panic("archive: nil async connect")
	}
	setAwaitConnectStepObserver(ac, observer)
	tb.Cleanup(func() { setAwaitConnectStepObserver(ac, nil) })
}

func notifyAwaitConnectStepObserver(ac *AsyncConnect) {
	if ac == nil {
		return
	}
	if observer := awaitConnectStepObserverFor(ac); observer != nil {
		observer()
	}
}

//go:build testhooks

package client

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
// Injected-client tests: pass ctx.Aeron (e.g. harness.Client).
// Owned-client tests: ctx.Aeron is nil before Connect; register on the same
// *aeron.Client returned by SetConnectConstructionHooksForTest's factory (see
// ownedConnectContextForTest). A mismatched pointer silently skips the observer.
func SetConnectAsyncConnectObserverForTest(
	tb testing.TB,
	client *aeron.Client,
	observer connectAsyncConnectObserver,
) {
	tb.Helper()
	if client == nil {
		panic("archive client: nil aeron client")
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

type awaitArchiveConnectStepObserver func()

var awaitArchiveConnectStepObservers = testhook.NewRegistry[
	*AsyncConnect,
	awaitArchiveConnectStepObserver,
]()

func setAwaitArchiveConnectStepObserver(
	ac *AsyncConnect,
	observer awaitArchiveConnectStepObserver,
) {
	if observer == nil {
		awaitArchiveConnectStepObservers.Delete(ac)
		return
	}
	awaitArchiveConnectStepObservers.Store(ac, observer)
}

func awaitArchiveConnectStepObserverFor(ac *AsyncConnect) awaitArchiveConnectStepObserver {
	if ac == nil {
		return nil
	}
	return awaitArchiveConnectStepObservers.Load(ac)
}

// SetAwaitArchiveConnectStepObserverForTest registers a callback invoked at the start
// of each awaitArchiveConnect loop iteration before DoWork and Poll.
func SetAwaitArchiveConnectStepObserverForTest(
	tb testing.TB,
	ac *AsyncConnect,
	observer awaitArchiveConnectStepObserver,
) {
	tb.Helper()
	if ac == nil {
		panic("archive client: nil async connect")
	}
	setAwaitArchiveConnectStepObserver(ac, observer)
	tb.Cleanup(func() { setAwaitArchiveConnectStepObserver(ac, nil) })
}

func notifyAwaitArchiveConnectStepObserver(ac *AsyncConnect) {
	if ac == nil {
		return
	}
	if observer := awaitArchiveConnectStepObserverFor(ac); observer != nil {
		observer()
	}
}

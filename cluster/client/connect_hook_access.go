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
// Injected-client tests: pass cfg.Aeron (e.g. harness.Client).
// Owned-client tests: cfg.Aeron is nil before Connect; register on the same
// *aeron.Client returned by SetConnectConstructionHooksForTest's factory (see
// ownedConnectConfigForTest). A mismatched pointer silently skips the observer.
func SetConnectAsyncConnectObserverForTest(
	tb testing.TB,
	client *aeron.Client,
	observer connectAsyncConnectObserver,
) {
	tb.Helper()
	if client == nil {
		panic("cluster client: nil aeron client")
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

type awaitClusterConnectStepObserver func()

var awaitClusterConnectStepObservers = testhook.NewRegistry[
	*AsyncConnect,
	awaitClusterConnectStepObserver,
]()

func setAwaitClusterConnectStepObserver(
	ac *AsyncConnect,
	observer awaitClusterConnectStepObserver,
) {
	if observer == nil {
		awaitClusterConnectStepObservers.Delete(ac)
		return
	}
	awaitClusterConnectStepObservers.Store(ac, observer)
}

func awaitClusterConnectStepObserverFor(ac *AsyncConnect) awaitClusterConnectStepObserver {
	if ac == nil {
		return nil
	}
	return awaitClusterConnectStepObservers.Load(ac)
}

// SetAwaitClusterConnectStepObserverForTest registers a callback invoked at the start
// of each awaitClusterConnect loop iteration before DoWork and Poll.
func SetAwaitClusterConnectStepObserverForTest(
	tb testing.TB,
	ac *AsyncConnect,
	observer awaitClusterConnectStepObserver,
) {
	tb.Helper()
	if ac == nil {
		panic("cluster client: nil async connect")
	}
	setAwaitClusterConnectStepObserver(ac, observer)
	tb.Cleanup(func() { setAwaitClusterConnectStepObserver(ac, nil) })
}

func notifyAwaitClusterConnectStepObserver(ac *AsyncConnect) {
	if ac == nil {
		return
	}
	if observer := awaitClusterConnectStepObserverFor(ac); observer != nil {
		observer()
	}
}

type connectSessionHook func(*ConnectResult) (*AeronCluster, error)

var connectSessionHooks = testhook.NewRegistry[*aeron.Client, connectSessionHook]()

func setConnectSessionHook(client *aeron.Client, hook connectSessionHook) {
	if hook == nil {
		connectSessionHooks.Delete(client)
		return
	}
	connectSessionHooks.Store(client, hook)
}

func connectSessionHookFor(client *aeron.Client) connectSessionHook {
	if client == nil {
		return nil
	}
	return connectSessionHooks.Load(client)
}

// SetNewAeronClusterHookForTest overrides session construction from Connect for tests.
// Keyed by result.Aeron (the transport client at handshake completion).
//
// Injected-client tests: pass harness.Client before Connect.
// Owned-client tests: pass the factory-returned *aeron.Client, not pre-connect cfg.Aeron.
func SetNewAeronClusterHookForTest(
	tb testing.TB,
	client *aeron.Client,
	hook connectSessionHook,
) {
	tb.Helper()
	if client == nil {
		panic("cluster client: nil aeron client")
	}
	setConnectSessionHook(client, hook)
	tb.Cleanup(func() { setConnectSessionHook(client, nil) })
}

func newAeronClusterForConnect(result *ConnectResult) (*AeronCluster, error) {
	if result != nil {
		if hook := connectSessionHookFor(result.Aeron); hook != nil {
			return hook(result)
		}
	}
	return NewAeronCluster(result)
}

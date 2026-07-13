//go:build !testhooks

package archive

func notifyConnectAsyncConnectObserver(*AsyncConnect) {}

func notifyAwaitConnectStepObserver(*AsyncConnect) {}

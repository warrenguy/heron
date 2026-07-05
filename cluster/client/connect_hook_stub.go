//go:build !testhooks

package client

func notifyConnectAsyncConnectObserver(*AsyncConnect) {}

func notifyAwaitClusterConnectStepObserver(*AsyncConnect) {}

func newAeronClusterForConnect(result *ConnectResult) (*AeronCluster, error) {
	return NewAeronCluster(result)
}

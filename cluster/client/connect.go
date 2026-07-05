package client

// Connect performs the blocking cluster connect handshake and returns a connected
// AeronCluster session.
//
// It drives DoWork on the transport client until the handshake completes or
// MessageTimeout elapses.
//
// Requires a non-nil EgressHandler on ctx.
//
// Pass ctx.Aeron to share the transport client with the session; when Aeron is nil,
// Connect creates an owned client that AeronCluster.Close releases.
//
// On handshake failure, partial resources are released via AsyncConnect.Close.
//
// Not safe for concurrent use during connect.
func Connect(ctx ClusterContext) (*AeronCluster, error) {
	if err := ctx.Normalize().Validate(); err != nil {
		return nil, err
	}

	connect, err := NewAsyncConnect(ctx)
	if err != nil {
		return nil, err
	}

	notifyConnectAsyncConnectObserver(connect)

	result, err := awaitClusterConnect(connect)
	if err != nil {
		_ = connect.Close()
		return nil, err
	}

	session, err := newAeronClusterForConnect(result)
	if err != nil {
		releaseConnectResult(result)
		return nil, err
	}
	return session, nil
}

func awaitClusterConnect(connect *AsyncConnect) (*ConnectResult, error) {
	idle := connect.ctx.IdleStrategy
	if idle == nil {
		idle = NewDefaultIdleStrategy()
	}
	defer idle.Reset()

	for {
		notifyAwaitClusterConnectStepObserver(connect)
		if connect.client != nil {
			connect.client.DoWork()
		}
		result, err := connect.Poll()
		if err != nil {
			return nil, err
		}
		if result != nil {
			return result, nil
		}
		idle.Idle(0)
	}
}

func releaseConnectResult(result *ConnectResult) {
	if result == nil {
		return
	}

	// concludeConnect already drained pending adds and zeroed wire buffers.
	client := result.Aeron
	result.MemberIngressMap.closeAll(client)

	// Poll success always sets MemberIngressMap; nil map is test-only partial results.
	if result.MemberIngressMap == nil && result.IngressPublication != nil {
		_ = result.IngressPublication.Close()
	}
	if result.EgressSubscription != nil {
		_ = result.EgressSubscription.Close()
	}
	if result.OwnsAeronClient && client != nil {
		_ = client.Close()
	}
}

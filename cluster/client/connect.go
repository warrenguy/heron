// Package client implements the Aeron Cluster Java client contract.
package client

// Connect performs the blocking cluster connect handshake and returns a connected
// AeronCluster session.
//
// Prefer Connect when a blocking handshake is acceptable. Pass a raw
// ClusterContext; Connect normalizes internally and must see zero fields for
// default detection (do not pre-normalize before Connect or NewAsyncConnect).
//
// It drives DoWork on the transport client until the handshake completes or
// MessageTimeout elapses. The await loop paces via ctx.IdleStrategy.Idle(0) and
// calls IdleStrategy.Reset on exit so a shared backoff instance is not left in
// PARKING before the post-connect duty cycle.
//
// Requires a non-nil EgressHandler on ctx.
//
// Pass ctx.Aeron to share the transport client with the session; when Aeron is nil,
// Connect creates an owned client that AeronCluster.Close releases.
//
// Connect blocks without context cancellation; it returns only on success or
// MessageTimeout. For signal or shutdown during connect, use NewAsyncConnect,
// wrap the poll loop with context.Context, and check cancelCtx.Err() between
// Poll calls.
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

// Package client implements the Aeron Cluster Java client contract.
package client

import (
	"context"

	"github.com/warrenguy/heron/aeron"
)

// Connect performs the blocking cluster connect handshake and returns a connected
// AeronCluster session.
//
// It drives DoWork on the transport client until the handshake completes,
// ctx is canceled or its deadline is exceeded, or MessageTimeout elapses. The await loop paces via
// clusterCtx.IdleStrategy.Idle(0) and calls IdleStrategy.Reset on exit so a shared
// backoff instance is not left in PARKING before the post-connect duty cycle.
//
// When clusterCtx.Aeron is nil, ctx also bounds owned transport attach (CNC ready
// and driver heartbeat) via NewAsyncConnect. Shared Aeron skips that wait.
//
// Pass a raw ClusterContext; Connect normalizes internally and must see
// zero fields for default detection (do not pre-normalize before Connect or
// NewAsyncConnect).
//
// Requires a non-nil EgressHandler on clusterCtx.
//
// Pass clusterCtx.Aeron to share the transport client with the session; when Aeron
// is nil, Connect creates an owned client that AeronCluster.Close releases.
//
// On handshake failure or ctx cancellation, partial resources are released via
// AsyncConnect.Close.
//
// Returns ctx.Err() when the context is done before handshake success
// (context.Canceled or context.DeadlineExceeded). Connect timeout errors apply
// only while the context remains active. A completed handshake returns a
// session even if ctx is canceled concurrently on that iteration.
//
// Not safe for concurrent use during connect.
func Connect(
	ctx context.Context,
	clusterCtx ClusterContext,
) (*AeronCluster, error) {
	if err := clusterCtx.Normalize().Validate(); err != nil {
		return nil, err
	}

	connect, err := NewAsyncConnect(ctx, clusterCtx)
	if err != nil {
		return nil, err
	}

	notifyConnectAsyncConnectObserver(connect)

	result, err := awaitClusterConnect(ctx, connect)
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

func awaitClusterConnect(
	ctx context.Context,
	connect *AsyncConnect,
) (*ConnectResult, error) {
	idle := connect.clusterCtx.IdleStrategy
	defer idle.Reset()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		notifyAwaitClusterConnectStepObserver(connect)
		if connect.client != nil {
			connect.client.DoWork()
		}
		result, err := connect.Poll()
		if err != nil {
			return nil, aeron.PreferContextError(ctx, err)
		}
		if result != nil {
			return result, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
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

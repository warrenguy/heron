//go:build bench

package bench_test

import (
	"testing"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/atomicbuffer"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/cluster/codecs"
	"github.com/warrenguy/luas/cluster/codecs/clusterwire"
	"github.com/warrenguy/luas/test/bench/harness"
)

var (
	clusterClientBenchSink     int64
	clusterCodecBenchSink      int
	asyncConnectBenchSink      int
	clusterEgressPollSink      int
	clusterEgressMessageSink   int
	clusterCodecControlScratch []byte
)

func init() {
	wireLen, ok := codecs.ClusterWireMessageLen(clusterwire.SessionKeepAliveSbeBlockLength)
	if !ok {
		panic("bench_test: SessionKeepAlive wire length unavailable")
	}
	clusterCodecControlScratch = make([]byte, wireLen)
}

type aeronClusterBenchIter func(
	b *testing.B,
	ac *client.AeronCluster,
	payload []byte,
) (int64, bool)

func runAeronClusterBench(
	b *testing.B,
	payload []byte,
	iter aeronClusterBenchIter,
) {
	b.Helper()

	ac := harness.NewAeronClusterForBench()
	b.ReportAllocs()
	if len(payload) > 0 {
		b.SetBytes(int64(len(payload)))
	}
	b.ResetTimer()
	for b.Loop() {
		pos, ok := iter(b, ac, payload)
		if !ok {
			continue
		}
		clusterClientBenchSink = pos
	}
}

type aeronClusterBenchStep func(
	b *testing.B,
	ac *client.AeronCluster,
	payload []byte,
) int64

// Mirrors runExclusivePublicationOfferBench in exclusive_publication_bench_test.go;
// keep both in sync when the offer runner contract changes.
func runAeronClusterIngressBench(
	b *testing.B,
	payload []byte,
	step aeronClusterBenchStep,
) {
	runAeronClusterBench(b, payload, func(
		b *testing.B,
		ac *client.AeronCluster,
		payload []byte,
	) (int64, bool) {
		pos := step(b, ac, payload)
		return pos, pos > 0
	})
}

func runAeronClusterTryClaimCommitBench(b *testing.B, payload []byte) {
	runAeronClusterBench(b, payload, func(
		b *testing.B,
		ac *client.AeronCluster,
		payload []byte,
	) (int64, bool) {
		pos, err := harness.TryClaimCommitAeronClusterWithTermOverflowReset(b, ac, payload)
		return pos, pos > 0 && err == nil
	})
}

// BenchmarkAeronCluster_Offer registers payload=32 only: header plus 32 application
// bytes must fit exclusive ingress single-frame maxPayloadLength; larger harness
// payloads exceed that limit and are not a steady cluster ingress path.
func BenchmarkAeronCluster_Offer(b *testing.B) {
	payload := harness.Payload32[:]
	b.Run("payload=32", func(b *testing.B) {
		runAeronClusterIngressBench(b, payload, harness.OfferAeronClusterWithTermOverflowReset)
	})
}

// BenchmarkAeronCluster_TryClaimCommit registers payload=32 only for the same
// exclusive ingress single-frame limit as BenchmarkAeronCluster_Offer.
func BenchmarkAeronCluster_TryClaimCommit(b *testing.B) {
	payload := harness.Payload32[:]
	b.Run("payload=32", func(b *testing.B) {
		runAeronClusterTryClaimCommitBench(b, payload)
	})
}

func BenchmarkAeronCluster_SendKeepAlive(b *testing.B) {
	runAeronClusterIngressBench(b, nil, func(
		b *testing.B,
		ac *client.AeronCluster,
		_ []byte,
	) int64 {
		return harness.SendKeepAliveAeronClusterWithTermOverflowReset(b, ac)
	})
}

const benchAdminCorrelationID int64 = 303

func BenchmarkAeronCluster_SendSnapshotRequest(b *testing.B) {
	runAeronClusterIngressBench(b, nil, func(
		b *testing.B,
		ac *client.AeronCluster,
		_ []byte,
	) int64 {
		return harness.SendSnapshotRequestAeronClusterWithTermOverflowReset(
			b, ac, benchAdminCorrelationID,
		)
	})
}

// BenchmarkAeronCluster_PollEgress polls the same unfragmented session message
// each iteration.
func BenchmarkAeronCluster_PollEgress(b *testing.B) {
	payload := harness.Payload32[:]
	handler := &client.EgressHandlers{
		MessageFunc: func(
			_ int64,
			_ int64,
			_ atomicbuffer.AtomicBuffer,
			_ int,
			length int,
			_ *aeron.Header,
		) {
			clusterEgressMessageSink = length
		},
	}
	b.Run("payload=32", func(b *testing.B) {
		ac, image := harness.NewAeronClusterWithEgressForBench(payload, handler)
		if work := ac.PollEgress(); work != 1 {
			b.Fatalf("PollEgress work = %d, want 1", work)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for b.Loop() {
			aeron.SetImageSubscriberPositionForBench(image, 0)
			clusterEgressPollSink = ac.PollEgress()
		}
	})
}

func BenchmarkClusterCodec_EncodeSessionKeepAlive(b *testing.B) {
	fields := codecs.SessionKeepAliveFields{
		LeadershipTermID: harness.BenchLeadershipTermID,
		ClusterSessionID: harness.BenchClusterSessionID,
	}
	actingVersion := uint16(clusterwire.SessionKeepAliveSbeSchemaVersion)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		end, ok := codecs.EncodeSessionKeepAlive(
			clusterCodecControlScratch,
			0,
			actingVersion,
			fields,
		)
		if !ok {
			b.Fatal("EncodeSessionKeepAlive failed")
		}
		clusterCodecBenchSink = end
	}
}

func BenchmarkAsyncConnect_prepareConnectRequest(b *testing.B) {
	ac, err := client.NewAsyncConnectForBenchEncode()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := ac.PrepareConnectRequestForBench(); err != nil {
			b.Fatal(err)
		}
		asyncConnectBenchSink = ac.ConnectWireLengthForBench()
	}
}

//go:build bench

// Bench-only cluster client fixtures for contract benchmarks. Not for production use.
package client

import (
	"github.com/warrenguy/heron/aeron"
)

const benchNanoClockNs int64 = 1700000000123

// DefaultBenchConfigForBench returns a normalized cluster config for contract
// benchmarks: IPC ingress and egress, noop egress handler, null credentials, and a
// fixed NanoClock.
func DefaultBenchConfigForBench() Config {
	return Config{
		IngressChannel:      "aeron:ipc",
		EgressChannel:       "aeron:ipc",
		EgressHandler:       &EgressHandlers{},
		CredentialsSupplier: NullCredentialsSupplier{},
		IdleStrategy:        NewDefaultIdleStrategy(),
		AeronConfig: aeron.Config{
			NanoClock: func() int64 { return benchNanoClockNs },
		},
	}.Normalize()
}

// NewAsyncConnectForBenchEncode returns AsyncConnect ready for steady
// prepareConnectRequest encoding. One prepareConnectRequest call warms
// connectWireBuffer before the benchmark timer starts. Correlation ID stays 0
// (not NullCorrelationID) so prepareConnectRequest needs no live Aeron client.
func NewAsyncConnectForBenchEncode() (*AsyncConnect, error) {
	cfg := DefaultBenchConfigForBench()
	ac := &AsyncConnect{
		cfg:                     cfg,
		cachedResponseChannel:   cfg.EgressChannel,
		cachedResponseChannelOK: true,
	}
	if err := ac.prepareConnectRequest(); err != nil {
		return nil, err
	}
	return ac, nil
}

// PrepareConnectRequestForBench encodes the session connect request into
// connectWireBuffer for contract benchmarks.
func (ac *AsyncConnect) PrepareConnectRequestForBench() error {
	return ac.prepareConnectRequest()
}

// ConnectWireLengthForBench returns the encoded connect wire length after
// PrepareConnectRequestForBench.
func (ac *AsyncConnect) ConnectWireLengthForBench() int {
	if ac == nil {
		return 0
	}
	return ac.connectWireLength
}

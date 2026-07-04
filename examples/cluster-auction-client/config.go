//go:build example

package main

import (
	"cmp"
	"errors"
	"flag"
	"os"
	"strings"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/client"
	"github.com/warrenguy/heron/examples/cluster-auction-client/auctionwire"
)

// Config holds parsed CLI settings for the cluster auction client example.
type Config struct {
	CustomerID       int64
	NumBids          int
	BidIntervalMs    int
	AeronDir         string
	IngressChannel   string
	EgressChannel    string
	IngressEndpoints string
}

func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("cluster-auction-client", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	aeronDirDefault := cmp.Or(os.Getenv("AERON_DIR"), "/tmp/aeron")

	customerID := fs.Int64("customer-id", 1, "customer id for bids")
	numBids := fs.Int("num-bids", 10, "number of bids to send")
	bidIntervalMs := fs.Int("bid-interval-ms", 1000, "max random delay between bids in ms")
	hostnames := fs.String(
		"hostnames",
		"localhost,localhost,localhost",
		"comma-separated cluster member hostnames",
	)
	aeronDir := fs.String(
		"aeron-dir",
		aeronDirDefault,
		"Aeron media driver directory for the Go client",
	)
	ingressChannel := fs.String("ingress-channel", "aeron:udp", "cluster ingress channel")
	egressChannel := fs.String(
		"egress-channel",
		"aeron:udp?endpoint=localhost:0",
		"cluster egress channel",
	)
	ingressEndpoints := fs.String(
		"ingress-endpoints",
		"",
		"member ingress endpoints; empty auto-builds from hostnames for UDP",
	)
	portBase := fs.Int(
		"port-base",
		auctionwire.DefaultPortBase,
		"port base for auto ingress endpoints",
	)
	portsPerNode := fs.Int(
		"ports-per-node",
		auctionwire.DefaultPortsPerNode,
		"ports per node for auto ingress endpoints",
	)
	clientFacingPortOffset := fs.Int(
		"client-facing-port-offset",
		auctionwire.DefaultClientFacingPortOffset,
		"client-facing port offset for auto ingress endpoints",
	)

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	config := Config{
		CustomerID:       *customerID,
		NumBids:          *numBids,
		BidIntervalMs:    *bidIntervalMs,
		AeronDir:         *aeronDir,
		IngressChannel:   *ingressChannel,
		EgressChannel:    *egressChannel,
		IngressEndpoints: *ingressEndpoints,
	}

	if config.BidIntervalMs <= 0 {
		return Config{}, errors.New("cluster-auction-client: -bid-interval-ms must be positive")
	}

	if err := resolveIngressEndpoints(
		&config,
		splitHostnames(*hostnames),
		*portBase,
		*portsPerNode,
		*clientFacingPortOffset,
	); err != nil {
		return Config{}, err
	}

	return config, nil
}

func splitHostnames(raw string) []string {
	var out []string
	for part := range strings.SplitSeq(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func resolveIngressEndpoints(
	config *Config,
	hostnames []string,
	portBase, portsPerNode, clientFacingPortOffset int,
) error {
	if client.IngressChannelPassthrough(config.IngressChannel) {
		config.IngressEndpoints = ""
	} else if config.IngressEndpoints == "" && client.IngressChannelIsUDP(config.IngressChannel) {
		if len(hostnames) == 0 {
			return errors.New("cluster-auction-client: -hostnames required for UDP ingress auto-build")
		}
		config.IngressEndpoints = auctionwire.IngressEndpoints(
			hostnames,
			portBase,
			portsPerNode,
			clientFacingPortOffset,
		)
	}
	return nil
}

func (config Config) clusterContext(
	transport *aeron.Client,
	handlers *client.EgressHandlers,
) client.ClusterContext {
	return client.ClusterContext{
		Aeron:            transport,
		IngressChannel:   config.IngressChannel,
		EgressChannel:    config.EgressChannel,
		IngressEndpoints: config.IngressEndpoints,
		EgressHandler:    handlers,
		ClientName:       "cluster-auction-client",
	}
}

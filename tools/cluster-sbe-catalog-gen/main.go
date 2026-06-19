package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

const (
	toolName           = "cluster-sbe-catalog-gen"
	defaultCatalogPath = "test/integration/cluster_sbe/clustersbe/fixtures/catalog.json"
)

func main() { os.Exit(run()) }

func run() int {
	catalog := flag.String("catalog", defaultCatalogPath, "")
	out := flag.String("out", "", "")
	flag.Parse()
	if *out == "" {
		*out = *catalog
	}

	regen, err := regenCatalogFromFile(*catalog)
	if err != nil {
		return fail(err)
	}
	data, err := catalogschema.MarshalCanonical(regen)
	if err != nil {
		return fail(err)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		return fail(fmt.Errorf("write %s: %w", *out, err))
	}
	return 0
}

func fail(err error) int {
	_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", toolName, err)
	return 1
}

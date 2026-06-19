package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

func regenCatalogFromFile(path string) (catalogschema.CatalogFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return catalogschema.CatalogFile{}, fmt.Errorf("read catalog: %w", err)
	}
	catalog, err := catalogschema.ParseJSON(data)
	if err != nil {
		return catalogschema.CatalogFile{}, fmt.Errorf("parse catalog: %w", err)
	}
	if err := catalogschema.Validate(catalog); err != nil {
		return catalogschema.CatalogFile{}, err
	}

	blobBytes := catalogschema.ResolveBlobs(catalog.Blobs)
	for i, entry := range catalog.Fixtures {
		wire, err := clustersbe.EncodeCatalogEntry(entry, blobBytes)
		if err != nil {
			return catalogschema.CatalogFile{}, fmt.Errorf("%s: %w", entry.Name, err)
		}
		catalog.Fixtures[i].Hex = hex.EncodeToString(wire)
	}
	return catalog, nil
}

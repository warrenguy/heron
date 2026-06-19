package clustersbe

import (
	_ "embed"
	"fmt"

	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe/catalogschema"
)

//go:embed fixtures/catalog.json
var catalogJSONForTest []byte

var wireFixtures []WireFixture

func init() {
	loaded, err := loadCatalog()
	if err != nil {
		panic(fmt.Errorf("clustersbe: load catalog: %w", err))
	}

	wireFixtures = loaded
}

func CatalogJSONForTest() []byte {
	return catalogJSONForTest
}

type FixtureEntry = catalogschema.FixtureEntry

func loadCatalog() ([]WireFixture, error) {
	catalog, err := catalogschema.ParseJSON(catalogJSONForTest)
	if err != nil {
		return nil, fmt.Errorf("parse catalog.json: %w", err)
	}
	if err := catalogschema.Validate(catalog); err != nil {
		return nil, err
	}

	blobs := catalogschema.ResolveBlobs(catalog.Blobs)

	fixtures := make([]WireFixture, len(catalog.Fixtures))
	for i, entry := range catalog.Fixtures {
		fixture, err := catalogEntryToWireFixture(entry, blobs)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", entry.Name, err)
		}
		fixtures[i] = fixture
	}

	return fixtures, nil
}

func projectCatalogEntry(
	entry FixtureEntry,
	blobs map[string][]byte,
) (templateID uint16, fields any, err error) {
	templateID, err = catalogschema.TemplateIDFromName(entry.Template)
	if err != nil {
		return 0, nil, err
	}

	fields, err = catalogschema.ProjectCatalogToFields(
		entry.Template,
		entry.ActingVersion,
		entry.Fields,
		blobs,
	)
	if err != nil {
		return 0, nil, err
	}

	return templateID, fields, nil
}

func catalogEntryToWireFixture(entry FixtureEntry, blobs map[string][]byte) (WireFixture, error) {
	templateID, fields, err := projectCatalogEntry(entry, blobs)
	if err != nil {
		return WireFixture{}, err
	}

	return WireFixture{
		Name:          entry.Name,
		TemplateID:    templateID,
		ActingVersion: entry.ActingVersion,
		Hex:           entry.Hex,
		Fields:        fields,
	}, nil
}

func CatalogEntryToWireFixtureForTest(
	entry FixtureEntry,
	blobs map[string][]byte,
) (WireFixture, error) {
	return catalogEntryToWireFixture(entry, blobs)
}

// Regenerates committed fixture hex from catalog fields.
func EncodeCatalogEntry(entry FixtureEntry, blobs map[string][]byte) ([]byte, error) {
	templateID, fields, err := projectCatalogEntry(entry, blobs)
	if err != nil {
		return nil, err
	}
	wire, ok := ReencodeWire(WireFixture{
		TemplateID:    templateID,
		ActingVersion: entry.ActingVersion,
		Fields:        fields,
	})
	if !ok {
		return nil, fmt.Errorf(
			"encode wire (template=%s actingVersion=%d)",
			entry.Template,
			entry.ActingVersion,
		)
	}
	return wire, nil
}

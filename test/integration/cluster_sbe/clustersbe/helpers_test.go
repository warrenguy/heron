//go:build integration && cluster_sbe

package clustersbe_test

import (
	"github.com/warrenguy/heron/test/integration/cluster_sbe/clustersbe"
)

var fixturesByNameMap = func() map[string]clustersbe.WireFixture {
	fixtures := clustersbe.Fixtures()
	byName := make(map[string]clustersbe.WireFixture, len(fixtures))
	for _, fixture := range fixtures {
		byName[fixture.Name] = fixture
	}
	return byName
}()

func fixturesByName() map[string]clustersbe.WireFixture {
	return fixturesByNameMap
}

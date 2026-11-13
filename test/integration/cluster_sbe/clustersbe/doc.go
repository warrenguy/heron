//go:build integration || testhooks

// Package clustersbe loads the cluster SBE wire fixture catalog from
// fixtures/catalog.json. Each WireFixture holds the hex encoding of one
// message. The Java encoder must produce the same bytes.
package clustersbe

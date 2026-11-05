// Package harness provides integration-test fixtures for Aeron clients, CnC
// wiring, and media drivers.
//
// HERON_SHARED_MEDIA_DRIVER selects shared driver mode:
//   - "0": shared mode off
//   - unset or any other value, including "", "false", and "1": shared mode on
//
// With shared mode off, RunTestMainWithSharedDriver does not start or stop a
// shared driver. Shared(t) falls back to Start(t). Shared(t) also falls back
// when TestMain did not start a shared driver.
//
// HERON_SHARED_MEDIA_DRIVER_KEEP=1 keeps the shared driver after the run. The
// aeronmd process and the CnC directory remain. Use it to debug stale CnC
// state.
//
// RunTestMainWithSharedDriver in TestMain starts one long-lived aeronmd per Go
// test package binary. make test-integration starts two shared drivers:
//   - test/integration/harness: one binary for harness and harness_test, wired
//     in shared_testmain_external_test.go
//   - test/integration: wired in shared_testmain_test.go
//
// A package that calls Shared(t) should add TestMain and the
// AssertSharedDriverWiring acceptance tests.
package harness

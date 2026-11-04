// Package aeron implements the Aeron transport client.
//
// Connect maps the media driver CnC file and returns a Client. The Client adds
// publications and subscriptions through an external C or Java media driver.
//
// The caller drives conductor work with Client.DoWork. The package does not
// start a background conductor goroutine.
package aeron

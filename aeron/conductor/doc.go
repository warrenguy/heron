// Package conductor implements the Aeron client conductor.
//
// The conductor sends commands to the media driver over the to-driver ring
// buffer. The conductor reads driver events from the to-clients broadcast
// buffer. The conductor tracks pending registrations and the publications,
// subscriptions, and images they create.
package conductor

// Package linearizability checks client histories of a key-value service
// against a sequential model.
//
// Each service replica applies operations to a [Store]. [EncodeRequest],
// [DecodeRequest], [EncodeReply], and [DecodeReply] carry operations between
// a test client and the service. [History] collects the timed operations of
// one run. [Check] and [RequireLinearizable] run [Model] over those operations.
//
// Non-test code must not import this package.
package linearizability

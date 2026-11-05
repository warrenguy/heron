// Package cluster assembles the consensus module member process. The package
// holds:
//   - configuration and validation
//   - the cluster channel topology, derived from a cluster members string
//   - the consensus publication lifecycle for each peer
//   - the inbound consensus subscription
//   - the client ingress subscription
//   - the cluster log publication and subscription, bound into the consensus
//     agent before the first duty cycle
//
// Package cluster is not the consensus agent library. Package consensus holds
// the agent, the log publisher, and the log adapter. Package election holds the
// election state machine and member parsing.
//
// Package cluster also opens the member recovery state, the recording log, and
// the node state file. It seeds the election configuration from the recovery
// plan.
package cluster

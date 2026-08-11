package cluster

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster/election"
)

// ExclusivePublicationAdder registers exclusive publications on a media driver.
//
// [*aeron.Client] satisfies this interface as-built. The argument order is stream
// id first, matching [aeron.Client.AddExclusivePublication]; the compile-time
// assertion below fails the build if either side moves.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// [ConsensusPublications] calls it only from the goroutine that opens the module.
type ExclusivePublicationAdder interface {
	AddExclusivePublication(
		streamID int32,
		channel string,
	) (*aeron.ExclusivePublication, error)
}

var _ ExclusivePublicationAdder = (*aeron.Client)(nil)

// PublicationAdder registers concurrent publications on a media driver.
//
// [*aeron.Client] satisfies this interface as-built. The argument order is
// stream id first, matching [aeron.Client.AddPublication]. The compile-time
// assertion below fails the build if either side moves.
//
// Concurrency: implementations are not required to be safe for concurrent use.
// Callers use it only from the goroutine that opens the module.
type PublicationAdder interface {
	AddPublication(streamID int32, channel string) (*aeron.Publication, error)
}

var _ PublicationAdder = (*aeron.Client)(nil)

// ConsensusPublicationsDeps are the collaborators [OpenConsensusPublications]
// needs. All three are required.
//
// Named for [election.ElectionDeps], which carries injected collaborators the
// same way.
type ConsensusPublicationsDeps struct {
	// Adder registers one exclusive publication per peer. Production passes the
	// connected [*aeron.Client].
	Adder ExclusivePublicationAdder

	// Topology supplies the peer consensus channel map and the consensus stream
	// id. Self is already excluded by [Topology]; do not filter again.
	//
	// [OpenConsensusPublications] does not retain Topology.
	Topology *Topology

	// Publisher receives every peer publication. Pass the one
	// [*election.AeronPublisher] that also reaches
	// [election.ElectionDeps.Publisher] and
	// [election.HostAdapterConfig.Publisher].
	//
	// Must come from [election.NewAeronPublisher]. A hand-built
	// &election.AeronPublisher{} has a nil publication map, and binding into it
	// panics with assignment to entry in nil map. That is not detectable from
	// here: the map is unexported and a nil map reads exactly like an empty one.
	Publisher *election.AeronPublisher
}

// ConsensusPublications owns the per-peer consensus exclusive publications.
//
// Open with [OpenConsensusPublications], which registers every peer publication
// and binds it into the publisher before returning. There is no unbound state and
// no lazy add: most election send paths return no value and have nowhere to
// report a late registration failure.
//
// Concurrency: invoker-only. Every method must run on the same goroutine as
// [election.Election.DoWork], consensus.Agent.DoWork, and the transport client
// duty cycle, matching [election.AeronPublisher]. Not safe for concurrent use.
//
// Lifetime: this value owns the publications and closes them. The publisher does
// not; [election.AeronPublisher.SetPublication] never closes a handle it
// replaces. Close before closing the transport client when the process owns the
// client. Closing the client first is safe but leaves the publisher briefly bound
// to closed handles.
//
// Do not copy. Both fields are reference types, so a value copy is a silent
// alias of the same publications and the same publisher: two values that each
// look like an owner and each look responsible for Close. Every method also takes
// a pointer receiver, so a copy taken to "avoid the pointer" bypasses the
// nil-receiver checks a [*ConsensusPublications] caller relies on. Pass the
// [*ConsensusPublications].
type ConsensusPublications struct {
	publisher *election.AeronPublisher
	pubs      map[int32]*aeron.ExclusivePublication
}

var errAdderReturnedNoPublication = errors.New("adder returned no publication")

// Retry pacing for [addWithRetry] when the driver returns
// [aeron.ErrorCodeResourceTemporarilyUnavailable].
//
// driverAddRetryBudget caps total retry time. driverAddRetryPause is the sleep
// between attempts. Tests may shrink both.
var (
	driverAddRetryBudget = 5 * time.Second
	driverAddRetryPause  = 10 * time.Millisecond
)

// addWithRetry runs one driver registration under the shared code-10 retry
// budget. Each attempt is one fresh call to add.
//
// Outcome per attempt:
//   - non-nil resource: return success
//   - nil-nil: return nilResult (not retried)
//   - [aeron.IsRetriableRegistration] (code 10, including wrapped): sleep
//     [driverAddRetryPause] and retry until [driverAddRetryBudget] expires
//   - any other error: return immediately
//
// Typical code-10 cases are send_channel_endpoint or receive_channel_endpoint
// still in CLOSING after a peer or local close.
//
// On failure returns the last error wrapped as `cluster: add %s on %s: %w`
// with what and channel.
func addWithRetry[R any](
	add func(streamID int32, channel string) (*R, error),
	streamID int32,
	channel string,
	what string,
	nilResult error,
) (*R, error) {
	deadline := time.Now().Add(driverAddRetryBudget)
	for {
		resource, err := add(streamID, channel)
		if err == nil && resource == nil {
			err = nilResult
		}
		if err == nil {
			return resource, nil
		}
		if !aeron.IsRetriableRegistration(err) || !time.Now().Before(deadline) {
			return nil, fmt.Errorf("cluster: add %s on %s: %w", what, channel, err)
		}
		time.Sleep(driverAddRetryPause)
	}
}

// addExclusivePublicationChecked adds one exclusive publication.
//
// what names the stream in the wrapped error. Retry budget and nil-nil
// handling live in [addWithRetry].
func addExclusivePublicationChecked(
	adder ExclusivePublicationAdder,
	streamID int32,
	channel string,
	what string,
) (*aeron.ExclusivePublication, error) {
	return addWithRetry(
		adder.AddExclusivePublication,
		streamID,
		channel,
		what,
		errAdderReturnedNoPublication,
	)
}

// addPublicationChecked adds one concurrent publication.
//
// what names the stream in the wrapped error. Retry budget and nil-nil
// handling live in [addWithRetry].
func addPublicationChecked(
	adder PublicationAdder,
	streamID int32,
	channel string,
	what string,
) (*aeron.Publication, error) {
	return addWithRetry(
		adder.AddPublication,
		streamID,
		channel,
		what,
		errAdderReturnedNoPublication,
	)
}

// OpenConsensusPublications registers one exclusive consensus publication per
// peer and binds all of them into deps.Publisher.
//
// Peers come from [Topology.PeerConsensusChannels], which already excludes this
// member. Publications are added in ascending member id on
// [Topology.ConsensusStreamID]. A cluster with a single member yields an empty
// set and no error.
//
// Open does not wait for any publication to connect. A cold cluster has no peer
// subscriptions yet; an unconnected publication offers NotConnected, which the
// election protocol retries on its own duty cycle.
//
// Errors when any dependency is nil, and when adding any peer publication fails.
// On error every publication added so far is closed, the publisher is left
// untouched, and the returned set is nil.
func OpenConsensusPublications(
	deps ConsensusPublicationsDeps,
) (*ConsensusPublications, error) {
	if deps.Adder == nil {
		return nil, errors.New("cluster: exclusive publication adder is required")
	}
	if deps.Topology == nil {
		return nil, errors.New("cluster: topology is required")
	}
	if deps.Publisher == nil {
		return nil, errors.New("cluster: consensus publisher is required")
	}

	channels := deps.Topology.PeerConsensusChannels()
	streamID := deps.Topology.ConsensusStreamID()
	peerIDs := slices.Sorted(maps.Keys(channels))

	pubs := make(map[int32]*aeron.ExclusivePublication, len(peerIDs))
	added := make([]*aeron.ExclusivePublication, 0, len(peerIDs))

	for _, memberID := range peerIDs {
		channel := channels[memberID]
		pub, err := addExclusivePublicationChecked(
			deps.Adder,
			streamID,
			channel,
			fmt.Sprintf("consensus publication for member %d", memberID),
		)
		if err != nil {
			closeAdded(added)
			return nil, err
		}
		added = append(added, pub)
		pubs[memberID] = pub
	}

	for _, memberID := range peerIDs {
		deps.Publisher.SetPublication(memberID, pubs[memberID])
	}

	return &ConsensusPublications{publisher: deps.Publisher, pubs: pubs}, nil
}

func closeAdded(pubs []*aeron.ExclusivePublication) {
	for _, pub := range pubs {
		_ = pub.Close()
	}
}

// Publication returns the bound exclusive publication for memberID.
//
// Nil for this member's own id, for an unknown member id, and for every id after
// Close. Mirrors [election.AeronPublisher.Publication].
//
// Concurrency: invoker-only. See [ConsensusPublications].
func (c *ConsensusPublications) Publication(
	memberID int32,
) *aeron.ExclusivePublication {
	if c == nil {
		return nil
	}
	return c.pubs[memberID]
}

// PeerCount returns the number of bound peer publications. Zero after Close.
func (c *ConsensusPublications) PeerCount() int {
	if c == nil {
		return 0
	}
	return len(c.pubs)
}

// Close unbinds every peer publication from the publisher, then closes every
// publication.
//
// Idempotent. After Close the value is inert: [ConsensusPublications.PeerCount]
// is zero and [ConsensusPublications.Publication] returns nil for every id. Safe
// on a nil receiver and after the transport client has already closed, which
// closes publications itself.
//
// Returns the joined close errors, or nil.
func (c *ConsensusPublications) Close() error {
	if c == nil {
		return nil
	}
	peerIDs := slices.Sorted(maps.Keys(c.pubs))
	for _, memberID := range peerIDs {
		c.publisher.SetPublication(memberID, nil)
	}

	var err error
	for _, memberID := range peerIDs {
		err = errors.Join(err, c.pubs[memberID].Close())
		delete(c.pubs, memberID)
	}
	return err
}

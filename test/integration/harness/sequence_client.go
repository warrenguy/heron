//go:build integration

package harness

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warrenguy/heron/aeron"
	"github.com/warrenguy/heron/cluster"
	"github.com/warrenguy/heron/cluster/client"
)

const (
	sequenceClientPayloadLen  = 64
	sequenceClientWindow      = 16
	sequenceClientConnectWait = 60 * time.Second
	sequenceClientKeepalive   = time.Second
)

// SequenceClient offers sequenced payloads to a process cluster. It stores
// each echoed sequence.
//
// Call every method from the test goroutine. Pump is the only method that
// drives the transport. A wait that omits Pump does not send a keepalive.
//
// Pump clears the pending sequences on a leadership-term change. Pump does not
// offer those sequences again.
type SequenceClient struct {
	transport *aeron.Client
	session   *client.AeronCluster

	sending bool
	nextSeq int64
	echoed  []int64
	pending []int64
	term    int64

	lastKeepalive time.Time
}

// StartSequenceClient connects one cluster session through its own media driver.
//
// Sequences start at 1. The first 8 payload bytes hold the sequence,
// big-endian.
func StartSequenceClient(t testing.TB, pc *ProcessCluster) *SequenceClient {
	t.Helper()

	driver := Start(t)
	transport := ConnectClient(t, driver, aeron.Config{})
	sc := &SequenceClient{
		transport: transport,
		sending:   true,
		nextSeq:   1,
	}

	egress := "aeron:udp?endpoint=" + LoopbackUDPEndpoint(ClientEgressPort(pc.portBase))
	start := time.Now()
	session, err := client.Connect(t.Context(), client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    pc.IngressEndpoints(),
		EgressChannel:       egress,
		Aeron:               transport,
		MessageTimeout:      sequenceClientConnectWait,
		IngressRetryTimeout: sequenceClientConnectWait,
		EgressHandler:       &client.EgressHandlers{MessageBytesFunc: sc.handleEcho},
	})
	t.Logf("sequence client connect elapsed=%s", time.Since(start))
	require.NoError(t, err, "sequence client connect\n%s", pc.State())

	CleanupClose(t, "sequence client", session.Close)
	sc.session = session
	sc.term = session.LeadershipTermID()
	return sc
}

func (sc *SequenceClient) handleEcho(_, _ int64, payload []byte, _ *aeron.Header) {
	if len(payload) < 8 {
		return
	}
	seq := int64(binary.BigEndian.Uint64(payload[:8]))
	sc.echoed = append(sc.echoed, seq)

	if i := slices.Index(sc.pending, seq); i >= 0 {
		sc.pending = slices.Delete(sc.pending, i, i+1)
	}
}

// Pump drives the transport and polls egress. On an open session, Pump also:
//   - sends a due keepalive
//   - offers the next sequence when sending is on and fewer than
//     sequenceClientWindow sequences wait for an echo
//
// A negative Offer result keeps the sequence for the next call.
func (sc *SequenceClient) Pump() {
	sc.transport.DoWork()
	sc.session.PollEgress()

	if sc.session.IsClosed() {
		return
	}

	now := time.Now()
	if now.Sub(sc.lastKeepalive) >= sequenceClientKeepalive {
		_ = sc.session.SendKeepAlive()
		sc.lastKeepalive = now
	}

	term := sc.session.LeadershipTermID()
	if term != sc.term {
		sc.pending = nil
	}
	sc.term = term

	if !sc.sending || len(sc.pending) >= sequenceClientWindow {
		return
	}
	var payload [sequenceClientPayloadLen]byte
	binary.BigEndian.PutUint64(payload[:8], uint64(sc.nextSeq))

	if sc.session.Offer(payload[:]) < 0 {
		return
	}
	sc.pending = append(sc.pending, sc.nextSeq)
	sc.nextSeq++
}

// Offered returns how many sequences the client has offered.
func (sc *SequenceClient) Offered() int {
	return int(sc.nextSeq - 1)
}

// Acked returns how many echoed payloads the client has accepted.
func (sc *SequenceClient) Acked() int {
	return len(sc.echoed)
}

// Echoed returns a copy of every echoed sequence, oldest first.
func (sc *SequenceClient) Echoed() []int64 {
	return slices.Clone(sc.echoed)
}

// InFlight returns how many offered sequences are still waiting for an echo.
func (sc *SequenceClient) InFlight() int {
	return len(sc.pending)
}

// Pending returns a copy of every offered sequence still waiting for an echo,
// oldest first.
func (sc *SequenceClient) Pending() []int64 {
	return slices.Clone(sc.pending)
}

// SetSending turns offers on or off. Keepalives continue either way.
func (sc *SequenceClient) SetSending(on bool) {
	sc.sending = on
}

// LeaderMemberID returns the session's current leader.
func (sc *SequenceClient) LeaderMemberID() int32 {
	return sc.session.LeaderMemberID()
}

// LeadershipTermID returns the session's current term.
func (sc *SequenceClient) LeadershipTermID() int64 {
	return sc.session.LeadershipTermID()
}

// IsClosed reports whether the session is closed.
func (sc *SequenceClient) IsClosed() bool {
	return sc.session.IsClosed()
}

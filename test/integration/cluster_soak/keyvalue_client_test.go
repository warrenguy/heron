//go:build integration && cluster_soak

package cluster_soak_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/warrenguy/luas/aeron"
	"github.com/warrenguy/luas/cluster"
	"github.com/warrenguy/luas/cluster/client"
	"github.com/warrenguy/luas/internal/linearizability"
	"github.com/warrenguy/luas/test/integration/harness"
)

const (
	kvMessageTimeout = 60 * time.Second
	kvOfferDeadline  = 30 * time.Second
	kvReplyDeadline  = 10 * time.Second
	kvKeepalive      = time.Second
	kvConnectGap     = time.Second
	kvPassSleep      = time.Millisecond
	kvGenOps         = 3200
	kvKeys           = 8
	kvGapMinMs       = 20
	kvGapSpanMs      = 61
	kvGetPercent     = 40
	kvAppendPercent  = 85
)

// keyValueClient offers key-value operations on three cluster sessions.
//
// Connect each session from the test goroutine before Run. After Run, only
// the client goroutine touches the history. Stop is idempotent.
type keyValueClient struct {
	transport  *aeron.Client
	ingress    string
	clientHost string
	portBase   int

	history    *linearizability.History
	generation atomic.Int64
	returned   [kvSessions]atomic.Int64
	stopped    atomic.Bool
	done       chan struct{}

	rng      *rand.Rand
	clock    int64
	nextID   int
	seenGen  int64
	opsInGen int
	reopens  int

	// owners holds the session index of each operation, by history id.
	owners []int

	sessions  [kvSessions]*client.AeronCluster
	connects  [kvSessions]*client.AsyncConnect
	ops       [kvSessions]*kvOp
	nextOffer [kvSessions]time.Time
	keepalive [kvSessions]time.Time
	lastConn  [kvSessions]time.Time

	replyErr      error
	events        []kvEvent
	connectErrors []string
}

type kvOp struct {
	id         int
	payload    []byte
	offered    bool
	offerStart time.Time
	replyStart time.Time
}

// kvEvent is one stored session event other than close.
type kvEvent struct {
	Code   client.EventCode
	Detail string
}

// newKeyValueClient builds a client on transport. seed draws the mix and the gaps.
//
// The history clock runs only on the client goroutine after Run.
func newKeyValueClient(
	transport *aeron.Client,
	ingress string,
	clientHost string,
	portBase int,
	seed uint64,
) *keyValueClient {
	c := &keyValueClient{
		transport:  transport,
		ingress:    ingress,
		clientHost: clientHost,
		portBase:   portBase,
		done:       make(chan struct{}),
		rng:        rand.New(rand.NewPCG(seed, 1)),
	}
	c.history = linearizability.NewHistory(c.nowNs)

	return c
}

// Returned reads how many matching replies each session has accepted.
func (c *keyValueClient) Returned() [kvSessions]int64 {
	var out [kvSessions]int64

	for i := range kvSessions {
		out[i] = c.returned[i].Load()
	}

	return out
}

// AdvanceGeneration starts a new key generation.
func (c *keyValueClient) AdvanceGeneration() {
	c.generation.Add(1)
}

// Stop asks the client goroutine to finish open work and exit.
func (c *keyValueClient) Stop() {
	c.stopped.Store(true)
}

// Done closes when the client goroutine exits.
func (c *keyValueClient) Done() <-chan struct{} {
	return c.done
}

// Connect opens session through the shared transport.
//
// Call it from the test goroutine before Run.
func (c *keyValueClient) Connect(ctx context.Context, session int) error {
	sess, err := client.Connect(ctx, c.config(session))
	if err != nil {
		return err
	}

	c.sessions[session] = sess

	return nil
}

// Run starts the client goroutine.
func (c *keyValueClient) Run() {
	go c.loop()
}

func (c *keyValueClient) loop() {
	defer close(c.done)

	for {
		c.transport.DoWork()
		now := time.Now()
		idle := true

		for i := range kvSessions {
			c.visit(i, now)

			if c.sessions[i] != nil || c.connects[i] != nil {
				idle = false
			}
		}

		if c.stopped.Load() && idle {
			return
		}

		time.Sleep(kvPassSleep)
	}
}

func (c *keyValueClient) visit(i int, now time.Time) {
	if c.stopped.Load() && c.connects[i] != nil {
		_ = c.connects[i].Close()
		c.connects[i] = nil
	}

	switch {
	case c.sessions[i] != nil:
		c.serviceSession(i, now)
	case c.connects[i] != nil:
		c.pollConnect(i)
	case !c.stopped.Load():
		c.maybeStartConnect(i)
	}
}

func (c *keyValueClient) serviceSession(i int, now time.Time) {
	c.sessions[i].PollEgress()

	if c.sessions[i].IsClosed() {
		c.handleClosed(i)

		return
	}

	c.sendKeepalive(i, now)
	c.offer(i, now)

	if c.stopped.Load() && c.ops[i] == nil {
		_ = c.sessions[i].Close()
		c.sessions[i] = nil
	}
}

func (c *keyValueClient) handleClosed(i int) {
	c.dropOp(i)
	c.sessions[i] = nil
	c.reopens++
}

func (c *keyValueClient) dropOp(i int) {
	op := c.ops[i]

	if op == nil {
		return
	}

	if op.offered {
		c.history.Abandon(op.id)
	} else {
		c.history.Discard(op.id)
	}

	c.ops[i] = nil
	c.armGap(i)
}

func (c *keyValueClient) sendKeepalive(i int, now time.Time) {
	if now.Sub(c.keepalive[i]) < kvKeepalive {
		return
	}

	_ = c.sessions[i].SendKeepAlive()
	c.keepalive[i] = now
}

func (c *keyValueClient) offer(i int, now time.Time) {
	if c.ops[i] == nil {
		if c.stopped.Load() || now.Before(c.nextOffer[i]) {
			return
		}

		c.beginOp(i, now)
	}

	op := c.ops[i]

	if op.offered {
		if now.Sub(op.replyStart) >= kvReplyDeadline {
			c.dropOp(i)
		}

		return
	}

	result := c.sessions[i].Offer(op.payload)

	if result > 0 {
		op.offered = true
		op.replyStart = now

		return
	}

	if now.Sub(op.offerStart) >= kvOfferDeadline {
		c.dropOp(i)
	}
}

func (c *keyValueClient) beginOp(i int, now time.Time) {
	gen := c.generation.Load()

	if gen == c.seenGen && c.opsInGen >= kvGenOps {
		gen = c.generation.Add(1)
	}

	if gen != c.seenGen {
		c.seenGen = gen
		c.opsInGen = 0
	}

	c.opsInGen++
	kind := c.drawKind()
	in := linearizability.Input{
		Kind: kind,
		Key:  fmt.Sprintf("g%dk%d", gen, c.rng.IntN(kvKeys)),
	}

	if kind != linearizability.OpGet {
		in.Value = strconv.Itoa(c.nextID) + ","
	}

	id := c.history.Call(i, in)
	c.owners = append(c.owners, i)
	c.nextID++
	c.ops[i] = &kvOp{
		id:         id,
		payload:    linearizability.EncodeRequest(nil, int64(id), in),
		offerStart: now,
	}
}

func (c *keyValueClient) drawKind() linearizability.OpKind {
	n := c.rng.IntN(100)

	switch {
	case n < kvGetPercent:
		return linearizability.OpGet
	case n < kvAppendPercent:
		return linearizability.OpAppend
	default:
		return linearizability.OpPut
	}
}

func (c *keyValueClient) armGap(i int) {
	gap := time.Duration(kvGapMinMs+c.rng.IntN(kvGapSpanMs)) * time.Millisecond
	c.nextOffer[i] = time.Now().Add(gap)
}

func (c *keyValueClient) handleReply(i int, payload []byte) {
	id, out, err := linearizability.DecodeReply(payload)
	if err == nil {
		err = c.checkReplyOwner(i, id)
	}

	if err == nil {
		err = c.history.Return(int(id), out, c.sessions[i].LeadershipTermID())
	}

	if err != nil {
		c.replyErr = cmp.Or(c.replyErr, err)

		return
	}

	op := c.ops[i]

	if op == nil || op.id != int(id) {
		return
	}

	c.returned[i].Add(1)
	c.ops[i] = nil
	c.armGap(i)
}

// checkReplyOwner rejects a reply that arrives on a session other than the
// one that issued the operation. The history does not record such a reply. An
// unknown id passes, and History.Return rejects it.
func (c *keyValueClient) checkReplyOwner(i int, id int64) error {
	if id < 0 || id >= int64(len(c.owners)) {
		return nil
	}

	if owner := c.owners[id]; owner != i {
		return fmt.Errorf("reply for operation %d on session %d: session %d issued it", id, i, owner)
	}
	return nil
}

func (c *keyValueClient) pollConnect(i int) {
	result, err := c.connects[i].Poll()
	if err != nil {
		_ = c.connects[i].Close()
		c.connects[i] = nil
		c.noteConnect(err)

		return
	}

	if result == nil {
		return
	}

	c.connects[i] = nil

	sess, err := client.NewAeronCluster(result)
	if err != nil {
		c.releaseResult(result)
		c.noteConnect(err)

		return
	}

	c.sessions[i] = sess
}

func (c *keyValueClient) releaseResult(result *client.ConnectResult) {
	if result.IngressPublication != nil {
		_ = result.IngressPublication.Close()
	}

	if result.EgressSubscription != nil {
		_ = result.EgressSubscription.Close()
	}
}

func (c *keyValueClient) maybeStartConnect(i int) {
	if time.Since(c.lastConn[i]) < kvConnectGap {
		return
	}

	c.lastConn[i] = time.Now()
	connect, err := client.NewAsyncConnect(context.Background(), c.config(i))
	if err != nil {
		c.noteConnect(err)

		return
	}

	c.connects[i] = connect
}

func (c *keyValueClient) noteConnect(err error) {
	c.connectErrors = append(c.connectErrors, err.Error())
}

func (c *keyValueClient) config(session int) client.Config {
	port := harness.ClientEgressPort(c.portBase) + session
	egress := "aeron:udp?endpoint=" + net.JoinHostPort(c.clientHost, strconv.Itoa(port))

	return client.Config{
		IngressChannel:      cluster.DefaultIngressChannel,
		IngressEndpoints:    c.ingress,
		EgressChannel:       egress,
		Aeron:               c.transport,
		MessageTimeout:      kvMessageTimeout,
		IngressRetryTimeout: kvMessageTimeout,
		EgressHandler:       c.handler(session),
	}
}

func (c *keyValueClient) handler(i int) *client.EgressHandlers {
	return &client.EgressHandlers{
		MessageBytesFunc: func(_, _ int64, payload []byte, _ *aeron.Header) {
			c.handleReply(i, payload)
		},
		SessionEventFunc: func(_, _, _ int64, _ int32, code client.EventCode, detail []byte) {
			if code == client.EventCodeClosed {
				return
			}

			c.events = append(c.events, kvEvent{Code: code, Detail: string(detail)})
		},
		NewLeaderFunc: func(_, _ int64, _ int32, _ []byte) {
			c.dropOp(i)
		},
	}
}

func (c *keyValueClient) nowNs() int64 {
	c.clock = max(time.Now().UnixNano(), c.clock+1)

	return c.clock
}

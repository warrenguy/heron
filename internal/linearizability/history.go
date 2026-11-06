package linearizability

import (
	"fmt"
	"math"

	"github.com/anishathalye/porcupine"
)

type operationState uint8

const (
	operationOpen operationState = iota
	operationReturned
	operationAbandoned
	operationDiscarded
)

type historyEntry struct {
	client      int
	kind        OpKind
	state       operationState
	lateReplied bool
	operation   porcupine.Operation
}

// KindCounts holds one count per operation kind.
type KindCounts struct {
	Get    int
	Put    int
	Append int
}

// Total returns the sum across kinds.
func (c KindCounts) Total() int {
	return c.Get + c.Put + c.Append
}

func (c *KindCounts) increment(kind OpKind) {
	switch kind {
	case OpGet:
		c.Get++
	case OpPut:
		c.Put++
	case OpAppend:
		c.Append++
	}
}

// Counts holds the outcome counts of one run.
type Counts struct {
	// Returned counts operations that ended with a reply.
	Returned KindCounts

	// Abandoned counts operations whose client stopped waiting for a reply.
	Abandoned KindCounts

	// Discarded counts operations that the cluster never received.
	Discarded KindCounts

	// Late counts replies to abandoned operations.
	Late KindCounts
}

// History collects the operations of one run.
//
// An operation starts at [History.Call]. It then ends in one of three ways:
//   - [History.Return]: a reply arrived.
//   - [History.Discard]: the cluster never received the request.
//   - [History.Abandon]: the client stopped waiting for a reply.
//
// A client must end its open operation before its next call.
//
// Concurrency: call every method from one goroutine.
type History struct {
	nowNs        func() int64
	entries      []historyEntry
	clientIDs    map[int]int
	nextClientID int
	counts       Counts
}

// NewHistory returns an empty history. The nowNs clock stamps each call and
// each return. The clock must never go backward.
func NewHistory(nowNs func() int64) *History {
	return &History{nowNs: nowNs, clientIDs: make(map[int]int)}
}

// Call opens one operation for client. Returns the operation id, which the
// client puts on the wire.
func (h *History) Call(client int, in Input) int {
	clientID, assigned := h.clientIDs[client]
	if !assigned {
		clientID = h.nextClientID
		h.nextClientID++
		h.clientIDs[client] = clientID
	}

	id := len(h.entries)
	h.entries = append(h.entries, historyEntry{
		client: client,
		kind:   in.Kind,
		state:  operationOpen,
		operation: porcupine.Operation{
			ClientId: clientID,
			Input:    in,
			Call:     h.nowNs(),
		},
	})
	return id
}

// Return ends an open operation with its reply. The operation keeps
// leadershipTermID as its metadata.
//
// The first reply to an abandoned operation counts as late. The operation stays
// abandoned.
//
// Returns an error when:
//   - the id is unknown
//   - the operation already has a reply
//   - the client discarded the operation
//
// Each error means a broken reply path, not a linearizability result.
func (h *History) Return(id int, out Output, leadershipTermID int64) error {
	if id < 0 || id >= len(h.entries) {
		return fmt.Errorf("return operation %d: unknown id", id)
	}

	entry := &h.entries[id]
	switch entry.state {
	case operationOpen:
		entry.state = operationReturned
		entry.operation.Output = out
		entry.operation.Return = h.nowNs()
		entry.operation.Metadata = leadershipTermID
		h.counts.Returned.increment(entry.kind)
	case operationReturned:
		return fmt.Errorf("return operation %d: second reply", id)
	case operationDiscarded:
		return fmt.Errorf("return operation %d: reply to a discarded operation", id)
	case operationAbandoned:
		if entry.lateReplied {
			return fmt.Errorf("return operation %d: second reply", id)
		}
		entry.lateReplied = true
		h.counts.Late.increment(entry.kind)
	}
	return nil
}

// Discard drops an open operation that the cluster never received. Discard
// ignores an unknown id or an operation that already ended.
func (h *History) Discard(id int) {
	entry := h.openEntry(id)
	if entry == nil {
		return
	}
	entry.state = operationDiscarded
	h.counts.Discarded.increment(entry.kind)
}

// Abandon ends an open operation whose client stopped waiting for a reply.
// Abandon ignores an unknown id or an operation that already ended.
//
// An abandoned write stays in the history as a pending operation. The write
// returns at [math.MaxInt64] with an unknown output. The checker may place the
// write anywhere after its call, even after every read. An abandoned get leaves
// the history, since a get does not change the state.
//
// The client's next call takes a fresh Porcupine client id, so each
// visualization row stays sequential.
func (h *History) Abandon(id int) {
	entry := h.openEntry(id)
	if entry == nil {
		return
	}
	entry.state = operationAbandoned
	entry.operation.Output = Output{Unknown: true}
	entry.operation.Return = math.MaxInt64
	h.counts.Abandoned.increment(entry.kind)
	delete(h.clientIDs, entry.client)
}

// Operations abandons every open operation. Returns the returned operations and
// the pending writes in call order.
func (h *History) Operations() []porcupine.Operation {
	operations := make([]porcupine.Operation, 0, len(h.entries))
	for id := range h.entries {
		h.Abandon(id)

		entry := &h.entries[id]
		pendingWrite := entry.state == operationAbandoned && entry.kind != OpGet
		if entry.state == operationReturned || pendingWrite {
			operations = append(operations, entry.operation)
		}
	}
	return operations
}

// Counts returns the outcome counts so far.
func (h *History) Counts() Counts {
	return h.counts
}

func (h *History) openEntry(id int) *historyEntry {
	if id < 0 || id >= len(h.entries) {
		return nil
	}
	entry := &h.entries[id]
	if entry.state != operationOpen {
		return nil
	}
	return entry
}

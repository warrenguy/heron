//go:build integration

package harness

import (
	"fmt"
	"hash"
	"hash/fnv"

	"luas.fast/aeron"
	"luas.fast/cluster/codecs/clusterwire"
	"luas.fast/cluster/service"
	"luas.fast/internal/linearizability"
)

func newChildService(workload string, sink *statusSink) (childService, error) {
	switch workload {
	case "", "sequence":
		return &sequenceService{sink: sink}, nil

	case "keyvalue":
		return newKeyValueProcessService(sink), nil

	default:
		return nil, fmt.Errorf("process member: unknown workload %q", workload)
	}
}

// keyValueProcessService starts each new instance from an empty store.
type keyValueProcessService struct {
	sink      *statusSink
	store     *linearizability.Store
	hash      hash.Hash64
	applied   int64
	decodeErr error
}

func newKeyValueProcessService(sink *statusSink) *keyValueProcessService {
	return &keyValueProcessService{
		sink:  sink,
		store: linearizability.NewStore(),
		hash:  fnv.New64a(),
	}
}

func (s *keyValueProcessService) Start(service.Cluster) error { return nil }

func (s *keyValueProcessService) SessionOpen(service.ClientSession, int64) {}

func (s *keyValueProcessService) SessionClose(
	service.ClientSession,
	int64,
	clusterwire.CloseReason,
) {
}

func (s *keyValueProcessService) TimerEvent(int64, int64) {}

func (s *keyValueProcessService) DecodeError() error { return s.decodeErr }

func (s *keyValueProcessService) appliedCount() int64 { return s.applied }

func (s *keyValueProcessService) applyDigest() uint64 { return s.hash.Sum64() }

func (s *keyValueProcessService) echoFailures() int64 { return 0 }

func (s *keyValueProcessService) SessionMessage(
	session service.ClientSession,
	_ int64,
	payload []byte,
) {
	opID, in, err := linearizability.DecodeRequest(payload)
	if err != nil {
		if s.decodeErr == nil {
			s.decodeErr = err
		}

		return
	}

	out := s.store.Apply(in)
	s.applied++
	s.fold(payload, out.Value)
	s.sink.write(memberRecord{Applied: &MemberApplied{Seq: opID}})

	if session == nil {
		return
	}

	reply := linearizability.EncodeReply(nil, opID, out)

	if session.Offer(reply) == aeron.AdminAction {
		_ = session.Offer(reply)
	}
}

func (s *keyValueProcessService) fold(payload []byte, value string) {
	_, _ = s.hash.Write(payload)
	_, _ = s.hash.Write([]byte(value))
}

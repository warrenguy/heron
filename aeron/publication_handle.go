package aeron

import (
	"errors"
	"fmt"

	"github.com/warrenguy/heron/aeron/conductor"
	"github.com/warrenguy/heron/aeron/logbuffer"
)

// publicationHandleOps holds the steps that differ between *Publication and
// *ExclusivePublication. The retire, bind, and mark-closed helpers resolve a
// handle through it, so both types close the same way.
type publicationHandleOps struct {
	registryKey string
	markClosed  func() error
	bind        func(*conductor.Conductor, *Client)
}

func retirePublicationHandle(pub conductor.PublicationHandle) (string, error) {
	ops, err := publicationHandleOpsFor(pub)
	if err != nil {
		return "", err
	}
	return ops.registryKey, ops.markClosed()
}

func bindPublicationHandle(
	pub conductor.PublicationHandle,
	clientConductor *conductor.Conductor,
	client *Client,
) error {
	ops, err := publicationHandleOpsFor(pub)
	if err != nil {
		return err
	}
	ops.bind(clientConductor, client)
	return nil
}

func markPublicationHandleClosed(pub conductor.PublicationHandle) {
	ops, err := publicationHandleOpsFor(pub)
	if err != nil {
		return
	}
	_ = ops.markClosed()
}

func publicationHandleOpsFor(pub conductor.PublicationHandle) (publicationHandleOps, error) {
	if pub == nil {
		return publicationHandleOps{}, errors.New("aeron: publication handle is nil")
	}
	switch p := pub.(type) {
	case *Publication:
		if p == nil {
			return publicationHandleOps{}, errors.New("aeron: publication handle is nil")
		}
		return publicationHandleOps{
			registryKey: publicationRegistryKey(p.log),
			markClosed:  p.markClosed,
			bind: func(c *conductor.Conductor, client *Client) {
				p.conductor = c
				p.client = client
			},
		}, nil
	case *ExclusivePublication:
		if p == nil {
			return publicationHandleOps{}, errors.New("aeron: publication handle is nil")
		}
		return publicationHandleOps{
			registryKey: publicationRegistryKey(p.log),
			markClosed:  p.markClosed,
			bind: func(c *conductor.Conductor, client *Client) {
				p.conductor = c
				p.client = client
			},
		}, nil
	default:
		return publicationHandleOps{}, publicationHandleTypeError(pub)
	}
}

func publicationRegistryKey(log *logbuffer.MappedLogBuffer) string {
	if log == nil {
		return ""
	}
	return log.Path()
}

func publicationHandleTypeError(pub conductor.PublicationHandle) error {
	return fmt.Errorf(
		"aeron: publication handle %T is not *Publication or *ExclusivePublication",
		pub,
	)
}

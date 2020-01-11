package streamrevision

import (
	"fmt"

	api "github.com/eventstore/EventStore-Client-Go/protos"
)

// StreamRevision ...
type StreamRevision uint64

const (
	// StreamRevisionNoStream ...
	StreamRevisionNoStream StreamRevision = iota
	// StreamRevisionAny ...
	StreamRevisionAny
	// StreamRevisionStreamExists ...
	StreamRevisionStreamExists
)

// StreamRevisionStart ...
const StreamRevisionStart = 0

// StreamRevisionEnd ...
const StreamRevisionEnd = -1

// SetStreamRevision ...
func SetStreamRevision(header *api.AppendReq, streamRevision StreamRevision) error {
	switch streamRevision {
	case StreamRevisionAny:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &api.AppendReq_Empty{},
		}
		return nil
	case StreamRevisionNoStream:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &api.AppendReq_Empty{},
		}
		return nil
	case StreamRevisionStreamExists:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &api.AppendReq_Empty{},
		}
		return nil
	}
	return fmt.Errorf("Failed to set ExpectedStreamRevision with %+v", streamRevision)
}

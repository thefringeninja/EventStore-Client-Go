package protoutils

import (
	direction "github.com/eventstore/EventStore-Client-Go/pkg/direction"
	messages "github.com/eventstore/EventStore-Client-Go/pkg/messages"
	position "github.com/eventstore/EventStore-Client-Go/pkg/position"
	stream_revision "github.com/eventstore/EventStore-Client-Go/pkg/streamrevision"
	system_metadata "github.com/eventstore/EventStore-Client-Go/pkg/systemmetadata"
	api "github.com/eventstore/EventStore-Client-Go/protos"
)

//AppendHeaderFromStreamIDAndStreamRevision ...
func AppendHeaderFromStreamIDAndStreamRevision(streamID string, streamRevision stream_revision.StreamRevision) *api.AppendReq {
	appendReq := &api.AppendReq{
		Content: &api.AppendReq_Options_{
			Options: &api.AppendReq_Options{},
		},
	}
	appendReq.GetOptions().StreamName = streamID
	switch streamRevision {
	case stream_revision.StreamRevisionAny:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &api.AppendReq_Empty{},
		}
	case stream_revision.StreamRevisionNoStream:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &api.AppendReq_Empty{},
		}
	case stream_revision.StreamRevisionStreamExists:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &api.AppendReq_Empty{},
		}
	default:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Revision{
			Revision: uint64(streamRevision),
		}
	}
	return appendReq
}

//ToProposedMessage ...
func ToProposedMessage(event messages.ProposedEvent) *api.AppendReq_ProposedMessage {
	metadata := map[string]string{}
	if event.IsJSON {
		metadata[system_metadata.SystemMetadataKeysIsJSON] = "true"
	} else {
		metadata[system_metadata.SystemMetadataKeysIsJSON] = "false"
	}
	metadata[system_metadata.SystemMetadataKeysType] = event.EventType
	return &api.AppendReq_ProposedMessage{
		Id: &api.UUID{
			Value: &api.UUID_String_{
				String_: event.EventID.String(),
			},
		},
		Data:           event.Data,
		CustomMetadata: event.UserMetadata,
		Metadata:       metadata,
	}
}

//ReadDirectionFromDirection ...
func ReadDirectionFromDirection(dir direction.Direction) api.ReadReq_Options_ReadDirection {
	var readDirection api.ReadReq_Options_ReadDirection
	switch dir {
	case direction.Forwards:
		readDirection = api.ReadReq_Options_Forwards
	case direction.Backwards:
		readDirection = api.ReadReq_Options_Backwards
	}
	return readDirection
}

//AllReadOptionsFromPosition ...
func AllReadOptionsFromPosition(position position.Position) *api.ReadReq_Options_All {
	return &api.ReadReq_Options_All{
		All: &api.ReadReq_Options_AllOptions{
			AllOption: &api.ReadReq_Options_AllOptions_Position{
				Position: &api.ReadReq_Options_Position{
					PreparePosition: uint64(position.Prepare),
					CommitPosition:  uint64(position.Commit),
				},
			},
		},
	}
}

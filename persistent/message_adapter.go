package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

func FromPersistentProtoResponse(resp *persistent.ReadResp) *messages.ResolvedEvent {
	readEvent := resp.GetEvent()
	positionWire := readEvent.GetPosition()
	eventWire := readEvent.GetEvent()
	linkWire := readEvent.GetLink()

	var event *messages.RecordedEvent = nil
	var link *messages.RecordedEvent = nil
	var commit *uint64

	if positionWire != nil {
		switch value := positionWire.(type) {
		case *persistent.ReadResp_ReadEvent_CommitPosition:
			{
				commit = &value.CommitPosition
			}
		case *persistent.ReadResp_ReadEvent_NoPosition:
			{
				commit = nil
			}
		}
	}

	if eventWire != nil {
		recordedEvent := NewMessageFromPersistentProto(eventWire)
		event = &recordedEvent
	}

	if linkWire != nil {
		recordedEvent := NewMessageFromPersistentProto(linkWire)
		link = &recordedEvent
	}

	return &messages.ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}
}

func NewMessageFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) messages.RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return messages.RecordedEvent{
		EventID:        EventIDFromPersistentProto(recordedEvent),
		EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
		ContentType:    GetContentTypeFromPersistentProto(recordedEvent),
		StreamID:       string(streamIdentifier.StreamName),
		EventNumber:    recordedEvent.GetStreamRevision(),
		CreatedDate:    CreatedFromPersistentProto(recordedEvent),
		Position:       PositionFromPersistentProto(recordedEvent),
		Data:           recordedEvent.GetData(),
		SystemMetadata: recordedEvent.GetMetadata(),
		UserMetadata:   recordedEvent.GetCustomMetadata(),
	}
}

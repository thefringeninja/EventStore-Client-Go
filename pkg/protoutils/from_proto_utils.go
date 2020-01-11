package protoutils

import (
	"log"
	"strconv"
	"time"

	position "github.com/eventstore/EventStore-Client-Go/pkg/position"
	system_metadata "github.com/eventstore/EventStore-Client-Go/pkg/systemmetadata"

	api "github.com/eventstore/EventStore-Client-Go/protos"
	uuid "github.com/gofrs/uuid"
)

//EventIDFromProto ...
func EventIDFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

//CreatedFromProto ...
func CreatedFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v", recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

//PositionFromProto ...
func PositionFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) position.Position {
	return position.NewPosition(int64(recordedEvent.GetCommitPosition()), int64(recordedEvent.GetPreparePosition()))
}

//IsJSONFromProto ...
func IsJSONFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) bool {
	isJSON, err := strconv.ParseBool(recordedEvent.Metadata[system_metadata.SystemMetadataKeysIsJSON])
	if err != nil {
		log.Fatalf("Failed to parse isJSON as bool from %+v", recordedEvent.Metadata[system_metadata.SystemMetadataKeysIsJSON])
	}
	return isJSON
}

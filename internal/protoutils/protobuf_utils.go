package protoutils

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/types"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	filtering "github.com/EventStore/EventStore-Client-Go/types"
	"github.com/gofrs/uuid"
)

type SubscriptionFilterOptions struct {
	MaxSearchWindow    int
	CheckpointInterval int
	SubscriptionFilter *types.SubscriptionFilter
}

// SystemMetadataKeysType ...
const SystemMetadataKeysType = "type"

// SystemMetadataKeysIsJSON ...
const SystemMetadataKeysContentType = "content-type"

// SystemMetadataKeysCreated ...
const SystemMetadataKeysCreated = "created"

// ToAppendHeader ...
func ToAppendHeader(streamID string, streamRevision filtering.ExpectedRevision) *api.AppendReq {
	appendReq := &api.AppendReq{
		Content: &api.AppendReq_Options_{
			Options: &api.AppendReq_Options{},
		},
	}

	appendReq.GetOptions().StreamIdentifier = &shared.StreamIdentifier{
		StreamName: []byte(streamID),
	}

	switch value := streamRevision.(type) {
	case types.Any:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &shared.Empty{},
		}
	case types.NoStream:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case types.StreamExists:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case types.StreamRevision:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return appendReq
}

// ToProposedMessage ...
func ToProposedMessage(event filtering.ProposedEvent) *api.AppendReq_ProposedMessage {
	contentType := "application/octet-stream"
	if event.ContentType == types.JsonContentType {
		contentType = "application/json"
	}

	metadata := make(map[string]string)
	metadata[SystemMetadataKeysContentType] = contentType
	metadata[SystemMetadataKeysType] = event.EventType
	eventId := event.EventID

	if event.Data == nil {
		event.Data = []byte{}
	}

	if event.Metadata == nil {
		event.Metadata = []byte{}
	}

	if eventId == uuid.Nil {
		eventId = uuid.Must(uuid.NewV4())
	}

	return &api.AppendReq_ProposedMessage{
		Id: &shared.UUID{
			Value: &shared.UUID_String_{
				String_: eventId.String(),
			},
		},
		Data:           event.Data,
		CustomMetadata: event.Metadata,
		Metadata:       metadata,
	}
}

// toReadDirectionFromDirection ...
func toReadDirectionFromDirection(dir types.Direction) api.ReadReq_Options_ReadDirection {
	var readDirection api.ReadReq_Options_ReadDirection
	switch dir {
	case types.Forwards:
		readDirection = api.ReadReq_Options_Forwards
	case types.Backwards:
		readDirection = api.ReadReq_Options_Backwards
	}
	return readDirection
}

// toAllReadOptionsFromPosition ...
func toAllReadOptionsFromPosition(position filtering.AllPosition) *api.ReadReq_Options_All {
	options := &api.ReadReq_Options_AllOptions{}

	switch value := position.(type) {
	case types.Start:
		options.AllOption = &api.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		options.AllOption = &api.ReadReq_Options_AllOptions_End{
			End: &shared.Empty{},
		}
	case types.Position:
		options.AllOption = &api.ReadReq_Options_AllOptions_Position{
			Position: &api.ReadReq_Options_Position{
				PreparePosition: value.Prepare,
				CommitPosition:  value.Commit,
			},
		}
	}

	return &api.ReadReq_Options_All{
		All: options,
	}
}

func toReadStreamOptionsFromStreamAndStreamRevision(streamID string, streamPosition filtering.StreamPosition) *api.ReadReq_Options_Stream {
	options := &api.ReadReq_Options_StreamOptions{
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamID),
		},
	}

	switch value := streamPosition.(type) {
	case types.Start:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_End{
			End: &shared.Empty{},
		}
	case types.StreamRevision:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return &api.ReadReq_Options_Stream{
		Stream: options,
	}
}

// toFilterOptions ...
func toFilterOptions(options *SubscriptionFilterOptions) (*api.ReadReq_Options_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
		return nil, fmt.Errorf("the subscription filter requires a set of prefixes or a regex")
	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
		return nil, fmt.Errorf("the subscription filter may only contain a regex or a set of prefixes, but not both")
	}
	filterOptions := api.ReadReq_Options_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.Type == filtering.EventFilterType {
		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_EventType{
			EventType: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	} else {
		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	}
	if options.MaxSearchWindow == filtering.NoMaxSearchWindow {
		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	} else {
		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Max{
			Max: uint32(options.MaxSearchWindow),
		}
	}
	return &filterOptions, nil
}

func ToDeleteRequest(streamID string, streamRevision types.ExpectedRevision) *api.DeleteReq {
	deleteReq := &api.DeleteReq{
		Options: &api.DeleteReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamID),
			},
		},
	}

	switch value := streamRevision.(type) {
	case types.Any:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Any{
			Any: &shared.Empty{},
		}
	case types.StreamExists:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case types.NoStream:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case types.StreamRevision:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return deleteReq
}

func ToTombstoneRequest(streamID string, streamRevision types.ExpectedRevision) *api.TombstoneReq {
	tombstoneReq := &api.TombstoneReq{
		Options: &api.TombstoneReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamID),
			},
		},
	}

	switch value := streamRevision.(type) {
	case types.Any:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Any{
			Any: &shared.Empty{},
		}
	case types.StreamExists:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case types.NoStream:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case types.StreamRevision:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return tombstoneReq
}

func ToReadStreamRequest(streamID string, direction types.Direction, from filtering.StreamPosition, count uint64, resolveLinks bool) *api.ReadReq {
	return &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: toReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
}

func ToReadAllRequest(direction types.Direction, from filtering.AllPosition, count uint64, resolveLinks bool) *api.ReadReq {
	return &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: toReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  toAllReadOptionsFromPosition(from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
}

func ToStreamSubscriptionRequest(streamID string, from filtering.StreamPosition, resolveLinks bool, filterOptions *SubscriptionFilterOptions) (*api.ReadReq, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Subscription{
				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &shared.Empty{},
			},
			ReadDirection: toReadDirectionFromDirection(types.Forwards),
			ResolveLinks:  resolveLinks,
			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	if filterOptions != nil {
		options, err := toFilterOptions(filterOptions)
		if err != nil {
			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
		}
		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
			Filter: options,
		}
	}
	return readReq, nil
}

func ToAllSubscriptionRequest(from filtering.AllPosition, resolveLinks bool, filterOptions *SubscriptionFilterOptions) (*api.ReadReq, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Subscription{
				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &shared.Empty{},
			},
			ReadDirection: toReadDirectionFromDirection(types.Forwards),
			ResolveLinks:  resolveLinks,
			StreamOption:  toAllReadOptionsFromPosition(from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	if filterOptions != nil {
		options, err := toFilterOptions(filterOptions)
		if err != nil {
			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
		}
		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
			Filter: options,
		}
	}
	return readReq, nil
}

// EventIDFromProto ...
func EventIDFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

// CreatedFromProto ...
func CreatedFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(recordedEvent.Metadata[SystemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v", recordedEvent.Metadata[SystemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

func PositionFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) types.Position {
	return types.Position{Commit: recordedEvent.GetCommitPosition(), Prepare: recordedEvent.GetPreparePosition()}
}

func DeletePositionFromProto(deleteResponse *api.DeleteResp) types.Position {
	return types.Position{
		Commit:  deleteResponse.GetPosition().CommitPosition,
		Prepare: deleteResponse.GetPosition().PreparePosition,
	}
}

func TombstonePositionFromProto(tombstoneResponse *api.TombstoneResp) types.Position {
	return types.Position{
		Commit:  tombstoneResponse.GetPosition().CommitPosition,
		Prepare: tombstoneResponse.GetPosition().PreparePosition,
	}
}

// GetContentTypeFromProto ...
func GetContentTypeFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) string {
	return recordedEvent.Metadata[SystemMetadataKeysContentType]
}

// RecordedEventFromProto
func RecordedEventFromProto(result *api.ReadResp_ReadEvent) filtering.RecordedEvent {
	recordedEvent := result.GetEvent()
	return GetRecordedEventFromProto(recordedEvent)
}
func GetRecordedEventFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) filtering.RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()
	return filtering.RecordedEvent{
		EventID:        EventIDFromProto(recordedEvent),
		EventType:      recordedEvent.Metadata[SystemMetadataKeysType],
		ContentType:    GetContentTypeFromProto(recordedEvent),
		StreamID:       string(streamIdentifier.StreamName),
		EventNumber:    recordedEvent.GetStreamRevision(),
		CreatedDate:    CreatedFromProto(recordedEvent),
		Position:       PositionFromProto(recordedEvent),
		Data:           recordedEvent.GetData(),
		SystemMetadata: recordedEvent.GetMetadata(),
		UserMetadata:   recordedEvent.GetCustomMetadata(),
	}
}

func GetResolvedEventFromProto(result *api.ReadResp_ReadEvent) filtering.ResolvedEvent {
	positionWire := result.GetPosition()
	linkWire := result.GetLink()
	eventWire := result.GetEvent()

	var event *filtering.RecordedEvent = nil
	var link *filtering.RecordedEvent = nil
	var commit *uint64

	if positionWire != nil {
		switch value := positionWire.(type) {
		case *api.ReadResp_ReadEvent_CommitPosition:
			{
				commit = &value.CommitPosition
			}
		case *api.ReadResp_ReadEvent_NoPosition:
			{
				commit = nil
			}
		}
	}

	if eventWire != nil {
		recordedEvent := GetRecordedEventFromProto(eventWire)
		event = &recordedEvent
	}

	if linkWire != nil {
		recordedEvent := GetRecordedEventFromProto(linkWire)
		link = &recordedEvent
	}

	return filtering.ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}
}

func EventIDFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

func ToProtoUUID(id uuid.UUID) *shared.UUID {
	return &shared.UUID{
		Value: &shared.UUID_String_{
			String_: id.String(),
		},
	}
}

func GetContentTypeFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) string {
	return recordedEvent.Metadata[SystemMetadataKeysContentType]
}

func CreatedFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(
		recordedEvent.Metadata[SystemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v",
			recordedEvent.Metadata[SystemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

func PositionFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) types.Position {
	return types.Position{
		Commit:  recordedEvent.GetCommitPosition(),
		Prepare: recordedEvent.GetPreparePosition(),
	}
}

func FromPersistentProtoResponse(resp *persistent.ReadResp) *filtering.ResolvedEvent {
	readEvent := resp.GetEvent()
	positionWire := readEvent.GetPosition()
	eventWire := readEvent.GetEvent()
	linkWire := readEvent.GetLink()

	var event *filtering.RecordedEvent = nil
	var link *filtering.RecordedEvent = nil
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

	return &filtering.ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}
}

func NewMessageFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) filtering.RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return filtering.RecordedEvent{
		EventID:        EventIDFromPersistentProto(recordedEvent),
		EventType:      recordedEvent.Metadata[SystemMetadataKeysType],
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

func UpdatePersistentRequestStreamProto(
	streamName string,
	groupName string,
	position filtering.StreamPosition,
	settings types.SubscriptionSettings,
) *persistent.UpdateReq {
	return &persistent.UpdateReq{
		Options: UpdatePersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func UpdatePersistentRequestAllOptionsProto(
	groupName string,
	position filtering.AllPosition,
	settings types.SubscriptionSettings,
) *persistent.UpdateReq {
	options := UpdatePersistentRequestAllOptionsSettingsProto(position)

	return &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: options,
			GroupName:    groupName,
			Settings:     UpdatePersistentSubscriptionSettingsProto(settings),
		},
	}
}

func UpdatePersistentRequestAllOptionsSettingsProto(
	position filtering.AllPosition,
) *persistent.UpdateReq_Options_All {
	options := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{
			AllOption: nil,
		},
	}

	switch value := position.(type) {
	case types.Start:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case types.Position:
		options.All.AllOption = ToUpdatePersistentRequestAllOptionsFromPosition(value)
	}

	return options
}

func UpdatePersistentSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position filtering.StreamPosition,
	settings types.SubscriptionSettings,
) *persistent.UpdateReq_Options {
	return &persistent.UpdateReq_Options{
		StreamOption: UpdatePersistentSubscriptionStreamSettingsProto(streamName, groupName, position),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamName),
		},
		GroupName: groupName,
		Settings:  UpdatePersistentSubscriptionSettingsProto(settings),
	}
}

func UpdatePersistentSubscriptionStreamSettingsProto(
	streamName string,
	groupName string,
	position filtering.StreamPosition,
) *persistent.UpdateReq_Options_Stream {
	streamOption := &persistent.UpdateReq_Options_Stream{
		Stream: &persistent.UpdateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamName),
			},
			RevisionOption: nil,
		},
	}

	switch value := position.(type) {
	case types.Start:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case types.StreamRevision:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func UpdatePersistentSubscriptionSettingsProto(
	settings types.SubscriptionSettings,
) *persistent.UpdateReq_Settings {
	return &persistent.UpdateReq_Settings{
		ResolveLinks:          settings.ResolveLinkTos,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.MinCheckpointCount,
		MaxCheckpointCount:    settings.MaxCheckpointCount,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: UpdatePersistentRequestConsumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        UpdatePersistentRequestMessageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       UpdatePersistentRequestCheckpointAfterMsProto(settings.CheckpointAfterInMs),
	}
}

func UpdatePersistentRequestConsumerStrategyProto(
	strategy types.ConsumerStrategy,
) persistent.UpdateReq_ConsumerStrategy {
	switch strategy {
	case types.ConsumerStrategy_DispatchToSingle:
		return persistent.UpdateReq_DispatchToSingle
	case types.ConsumerStrategy_Pinned:
		return persistent.UpdateReq_Pinned
	// FIXME: support Pinned by correlation case ConsumerStrategy_PinnedByCorrelation:
	case types.ConsumerStrategy_RoundRobin:
		return persistent.UpdateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func UpdatePersistentRequestMessageTimeOutInMsProto(
	timeout int32,
) *persistent.UpdateReq_Settings_MessageTimeoutMs {
	return &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func UpdatePersistentRequestCheckpointAfterMsProto(
	checkpointAfterMs int32,
) *persistent.UpdateReq_Settings_CheckpointAfterMs {
	return &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// ToUpdatePersistentRequestAllOptionsFromPosition ...
func ToUpdatePersistentRequestAllOptionsFromPosition(position types.Position) *persistent.UpdateReq_AllOptions_Position {
	return &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

func CreatePersistentRequestProto(
	streamName string,
	groupName string,
	position filtering.StreamPosition,
	settings types.SubscriptionSettings,
) *persistent.CreateReq {
	return &persistent.CreateReq{
		Options: CreatePersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func CreatePersistentRequestAllOptionsProto(
	groupName string,
	position filtering.AllPosition,
	settings types.SubscriptionSettings,
	filter *SubscriptionFilterOptions,
) (*persistent.CreateReq, error) {
	options, err := CreatePersistentRequestAllOptionsSettingsProto(position, filter)
	if err != nil {
		return nil, err
	}

	return &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: options,
			GroupName:    groupName,
			Settings:     CreatePersistentSubscriptionSettingsProto(settings),
		},
	}, nil
}

func CreatePersistentRequestAllOptionsSettingsProto(
	pos filtering.AllPosition,
	filter *SubscriptionFilterOptions,
) (*persistent.CreateReq_Options_All, error) {
	options := &persistent.CreateReq_Options_All{
		All: &persistent.CreateReq_AllOptions{
			AllOption: nil,
			FilterOption: &persistent.CreateReq_AllOptions_NoFilter{
				NoFilter: &shared.Empty{},
			},
		},
	}

	switch value := pos.(type) {
	case types.Start:
		options.All.AllOption = &persistent.CreateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		options.All.AllOption = &persistent.CreateReq_AllOptions_End{
			End: &shared.Empty{},
		}
	case types.Position:
		options.All.AllOption = ToCreatePersistentRequestAllOptionsFromPosition(value)
	}

	if filter != nil {
		filter, err := CreateRequestFilterOptionsProto(filter)
		if err != nil {
			return nil, err
		}
		options.All.FilterOption = &persistent.CreateReq_AllOptions_Filter{
			Filter: filter,
		}
	}

	return options, nil
}

func CreatePersistentSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position filtering.StreamPosition,
	settings types.SubscriptionSettings,
) *persistent.CreateReq_Options {
	return &persistent.CreateReq_Options{
		StreamOption: CreatePersistentSubscriptionStreamSettingsProto(streamName, position),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamName),
		},
		GroupName: groupName,
		Settings:  CreatePersistentSubscriptionSettingsProto(settings),
	}
}

func CreatePersistentSubscriptionStreamSettingsProto(
	streamName string,
	position filtering.StreamPosition,
) *persistent.CreateReq_Options_Stream {
	streamOption := &persistent.CreateReq_Options_Stream{
		Stream: &persistent.CreateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamName),
			},
		},
	}

	switch value := position.(type) {
	case types.Start:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case types.End:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_End{
			End: &shared.Empty{},
		}
	case types.StreamRevision:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func CreatePersistentSubscriptionSettingsProto(
	settings types.SubscriptionSettings,
) *persistent.CreateReq_Settings {
	return &persistent.CreateReq_Settings{
		ResolveLinks:          settings.ResolveLinkTos,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.MinCheckpointCount,
		MaxCheckpointCount:    settings.MaxCheckpointCount,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: ConsumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        MessageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       CheckpointAfterMsProto(settings.CheckpointAfterInMs),
	}
}

func ConsumerStrategyProto(strategy types.ConsumerStrategy) persistent.CreateReq_ConsumerStrategy {
	switch strategy {
	case types.ConsumerStrategy_DispatchToSingle:
		return persistent.CreateReq_DispatchToSingle
	case types.ConsumerStrategy_Pinned:
		return persistent.CreateReq_Pinned
	case types.ConsumerStrategy_RoundRobin:
		return persistent.CreateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func MessageTimeOutInMsProto(timeout int32) *persistent.CreateReq_Settings_MessageTimeoutMs {
	return &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func CheckpointAfterMsProto(checkpointAfterMs int32) *persistent.CreateReq_Settings_CheckpointAfterMs {
	return &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// CreateRequestFilterOptionsProto ...
func CreateRequestFilterOptionsProto(
	options *SubscriptionFilterOptions,
) (*persistent.CreateReq_AllOptions_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
		return nil, &types.PersistentSubscriptionToAllMustProvideRegexOrPrefixError

	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
		return nil, &types.PersistentSubscriptionToAllCanSetOnlyRegexOrPrefixError
	}
	filterOptions := persistent.CreateReq_AllOptions_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.Type == filtering.EventFilterType {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_EventType{
			EventType: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	} else {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
			StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	}
	if options.MaxSearchWindow == filtering.NoMaxSearchWindow {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	} else {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Max{
			Max: uint32(options.MaxSearchWindow),
		}
	}
	return &filterOptions, nil
}

// ToUpdatePersistentRequestAllOptionsFromPosition ...
func ToCreatePersistentRequestAllOptionsFromPosition(
	position types.Position,
) *persistent.CreateReq_AllOptions_Position {
	return &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

func DeletePersistentRequestStreamProto(streamName string, groupName string) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_StreamIdentifier{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(streamName),
				},
			},
		},
	}
}

func DeletePersistentRequestAllOptionsProto(groupName string) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_All{
				All: &shared.Empty{},
			},
		},
	}
}

func ToPersistentReadRequest(
	bufferSize int32,
	groupName string,
	streamName []byte,
) *persistent.ReadReq {
	return &persistent.ReadReq{
		Content: &persistent.ReadReq_Options_{
			Options: &persistent.ReadReq_Options{
				BufferSize: bufferSize,
				GroupName:  groupName,
				StreamOption: &persistent.ReadReq_Options_StreamIdentifier{
					StreamIdentifier: &shared.StreamIdentifier{
						StreamName: streamName,
					},
				},
				UuidOption: &persistent.ReadReq_Options_UUIDOption{
					Content: &persistent.ReadReq_Options_UUIDOption_String_{
						String_: nil,
					},
				},
			},
		},
	}
}

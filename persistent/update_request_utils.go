package persistent

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/stream"

	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

func UpdatePersistentRequestStreamProto(
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings SubscriptionSettings,
) *persistent.UpdateReq {
	return &persistent.UpdateReq{
		Options: UpdatePersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func UpdatePersistentRequestAllOptionsProto(
	groupName string,
	position stream.AllStreamPosition,
	settings SubscriptionSettings,
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
	position stream.AllStreamPosition,
) *persistent.UpdateReq_Options_All {
	options := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{
			AllOption: nil,
		},
	}

	switch value := position.(type) {
	case stream.RevisionStart:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionEnd:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionPosition:
		options.All.AllOption = ToUpdatePersistentRequestAllOptionsFromPosition(value.Value)
	}

	return options
}

func UpdatePersistentSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings SubscriptionSettings,
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
	position stream.StreamPosition,
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
	case stream.RevisionStart:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionEnd:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionExact:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func UpdatePersistentSubscriptionSettingsProto(
	settings SubscriptionSettings,
) *persistent.UpdateReq_Settings {
	return &persistent.UpdateReq_Settings{
		ResolveLinks:          settings.ResolveLinks,
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
	strategy ConsumerStrategy,
) persistent.UpdateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategy_DispatchToSingle:
		return persistent.UpdateReq_DispatchToSingle
	case ConsumerStrategy_Pinned:
		return persistent.UpdateReq_Pinned
	// FIXME: support Pinned by correlation case ConsumerStrategy_PinnedByCorrelation:
	case ConsumerStrategy_RoundRobin:
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
func ToUpdatePersistentRequestAllOptionsFromPosition(position position.Position) *persistent.UpdateReq_AllOptions_Position {
	return &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

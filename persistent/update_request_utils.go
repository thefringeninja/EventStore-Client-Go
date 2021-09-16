package persistent

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

func updateRequestStreamProto(
	streamName string,
	groupName string,
	position stream_position.StreamPosition,
	settings SubscriptionSettings,
) *persistent.UpdateReq {
	return &persistent.UpdateReq{
		Options: updateSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func UpdateRequestAllOptionsProto(
	groupName string,
	position stream_position.AllStreamPosition,
	settings SubscriptionSettings,
) *persistent.UpdateReq {
	options := updateRequestAllOptionsSettingsProto(position)

	return &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: options,
			GroupName:    groupName,
			Settings:     updateSubscriptionSettingsProto(settings),
		},
	}
}

func updateRequestAllOptionsSettingsProto(
	position stream_position.AllStreamPosition,
) *persistent.UpdateReq_Options_All {
	options := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{
			AllOption: nil,
		},
	}

	switch value := position.(type) {
	case stream_position.RevisionStart:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case stream_position.RevisionEnd:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case stream_position.RevisionPosition:
		options.All.AllOption = toUpdateRequestAllOptionsFromPosition(value.Value)
	}

	return options
}

func updateSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position stream_position.StreamPosition,
	settings SubscriptionSettings,
) *persistent.UpdateReq_Options {
	return &persistent.UpdateReq_Options{
		StreamOption: updateSubscriptionStreamSettingsProto(streamName, groupName, position),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamName),
		},
		GroupName: groupName,
		Settings:  updateSubscriptionSettingsProto(settings),
	}
}

func updateSubscriptionStreamSettingsProto(
	streamName string,
	groupName string,
	position stream_position.StreamPosition,
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
	case stream_position.RevisionStart:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case stream_position.RevisionEnd:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case stream_position.RevisionExact:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func updateSubscriptionSettingsProto(
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
		NamedConsumerStrategy: updateRequestConsumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        updateRequestMessageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       updateRequestCheckpointAfterMsProto(settings.CheckpointAfterInMs),
	}
}

func updateRequestConsumerStrategyProto(
	strategy ConsumerStrategy,
) persistent.UpdateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategy_DispatchToSingle:
		return persistent.UpdateReq_DispatchToSingle
	case ConsumerStrategy_Pinned:
		return persistent.UpdateReq_Pinned
	case ConsumerStrategy_RoundRobin:
		return persistent.UpdateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func updateRequestMessageTimeOutInMsProto(
	timeout int32,
) *persistent.UpdateReq_Settings_MessageTimeoutMs {
	return &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func updateRequestCheckpointAfterMsProto(
	checkpointAfterMs int32,
) *persistent.UpdateReq_Settings_CheckpointAfterMs {
	return &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// toUpdateRequestAllOptionsFromPosition ...
func toUpdateRequestAllOptionsFromPosition(position position.Position) *persistent.UpdateReq_AllOptions_Position {
	return &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

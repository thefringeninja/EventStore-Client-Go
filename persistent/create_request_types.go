package persistent

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/stream"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

func CreatePersistentRequestProto(
	streamName string,
	groupName string,
	position stream.StreamPosition,
	settings SubscriptionSettings,
) *persistent.CreateReq {
	return &persistent.CreateReq{
		Options: CreatePersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func CreatePersistentRequestAllOptionsProto(
	groupName string,
	position stream.AllStreamPosition,
	settings SubscriptionSettings,
	filter *filtering.SubscriptionFilterOptions,
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
	pos stream.AllStreamPosition,
	filter *filtering.SubscriptionFilterOptions,
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
	case stream.RevisionStart:
		options.All.AllOption = &persistent.CreateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionEnd:
		options.All.AllOption = &persistent.CreateReq_AllOptions_End{
			End: &shared.Empty{},
		}
	case stream.RevisionPosition:
		options.All.AllOption = ToCreatePersistentRequestAllOptionsFromPosition(value.Value)
	}

	if filter != nil {
		filter, err := CreateRequestFilterOptionsProto(*filter)
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
	position stream.StreamPosition,
	settings SubscriptionSettings,
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
	position stream.StreamPosition,
) *persistent.CreateReq_Options_Stream {
	streamOption := &persistent.CreateReq_Options_Stream{
		Stream: &persistent.CreateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamName),
			},
		},
	}

	switch value := position.(type) {
	case stream.RevisionStart:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case stream.RevisionEnd:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_End{
			End: &shared.Empty{},
		}
	case stream.RevisionExact:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func CreatePersistentSubscriptionSettingsProto(
	settings SubscriptionSettings,
) *persistent.CreateReq_Settings {
	return &persistent.CreateReq_Settings{
		ResolveLinks:          settings.ResolveLinks,
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

func ConsumerStrategyProto(strategy ConsumerStrategy) persistent.CreateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategy_DispatchToSingle:
		return persistent.CreateReq_DispatchToSingle
	case ConsumerStrategy_Pinned:
		return persistent.CreateReq_Pinned
	case ConsumerStrategy_RoundRobin:
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

const (
	CreateRequestFilterOptionsProto_MustProvideRegexOrPrefixErr ErrorCode = "CreateRequestFilterOptionsProto_MustProvideRegexOrPrefixErr"
	CreateRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr  ErrorCode = "CreateRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr"
)

// CreateRequestFilterOptionsProto ...
func CreateRequestFilterOptionsProto(
	options filtering.SubscriptionFilterOptions,
) (*persistent.CreateReq_AllOptions_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.RegexValue) == 0 {
		return nil, NewErrorCodeMsg(CreateRequestFilterOptionsProto_MustProvideRegexOrPrefixErr,
			"the subscription filter requires a set of prefixes or a regex")
	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.RegexValue) > 0 {
		return nil, NewErrorCodeMsg(CreateRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr,
			"the subscription filter may only contain a regex or a set of prefixes, but not both")
	}
	filterOptions := persistent.CreateReq_AllOptions_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.FilterType == filtering.EventFilter {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_EventType{
			EventType: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.RegexValue,
			},
		}
	} else {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
			StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.RegexValue,
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

// toUpdateRequestAllOptionsFromPosition ...
func ToCreatePersistentRequestAllOptionsFromPosition(
	position position.Position,
) *persistent.CreateReq_AllOptions_Position {
	return &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

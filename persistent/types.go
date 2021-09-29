package persistent

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/messages"
)

type (
	EventAppearedHandler       func(context.Context, messages.RecordedEvent) error
	SubscriptionDroppedHandler func(reason string)
)

const SUBSCRIBER_COUNT_UNLIMITED = 0

type ConsumerStrategy int32

const (
	ConsumerStrategy_RoundRobin          ConsumerStrategy = 0
	ConsumerStrategy_DispatchToSingle    ConsumerStrategy = 1
	ConsumerStrategy_Pinned              ConsumerStrategy = 2
	ConsumerStrategy_PinnedByCorrelation ConsumerStrategy = 3
)

type SubscriptionSettings struct {
	ResolveLinks          bool
	ExtraStatistics       bool
	MaxRetryCount         int32
	MinCheckpointCount    int32
	MaxCheckpointCount    int32
	MaxSubscriberCount    int32
	LiveBufferSize        int32
	ReadBatchSize         int32
	HistoryBufferSize     int32
	NamedConsumerStrategy ConsumerStrategy
	MessageTimeoutInMs    int32
	CheckpointAfterInMs   int32
}

func SubscriptionSettingsDefault() SubscriptionSettings {
	return SubscriptionSettings{
		ResolveLinks:          false,
		ExtraStatistics:       false,
		MaxRetryCount:         10,
		MinCheckpointCount:    10,
		MaxCheckpointCount:    10 * 1000,
		MaxSubscriberCount:    SUBSCRIBER_COUNT_UNLIMITED,
		LiveBufferSize:        500,
		ReadBatchSize:         20,
		HistoryBufferSize:     500,
		NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
		MessageTimeoutInMs:    30 * 1000,
		CheckpointAfterInMs:   2 * 1000,
	}
}

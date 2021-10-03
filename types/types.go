package types

import "fmt"

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

type PersistentSubscriptionError struct {
	Code int
	Err  error
}

var PersistentSubscriptionToAllMustProvideRegexOrPrefixError = PersistentSubscriptionError{
	Code: 0,
}

var PersistentSubscriptionToAllCanSetOnlyRegexOrPrefixError = PersistentSubscriptionError{
	Code: 1,
}

func PersistentSubscriptionFailedToInitClientError(err error) error {
	return &PersistentSubscriptionError{
		Code: 2,
		Err:  err,
	}
}

func PersistentSubscriptionFailedSendStreamInitError(err error) error {
	return &PersistentSubscriptionError{
		Code: 3,
		Err:  err,
	}
}

func PersistentSubscriptionFailedReceiveStreamInitError(err error) error {
	return &PersistentSubscriptionError{
		Code: 4,
		Err:  err,
	}
}

func PersistentSubscriptionNoConfirmationError(err error) error {
	return &PersistentSubscriptionError{
		Code: 5,
		Err:  err,
	}
}

func PersistentSubscriptionFailedCreationError(err error) error {
	return &PersistentSubscriptionError{
		Code: 6,
		Err:  err,
	}
}

func PersistentSubscriptionUpdateFailedError(err error) error {
	return &PersistentSubscriptionError{
		Code: 7,
		Err:  err,
	}
}

func PersistentSubscriptionDeletionFailedError(err error) error {
	return &PersistentSubscriptionError{
		Code: 8,
		Err:  err,
	}
}

var PersistentSubscriptionExceedsMaxMessageCountError = PersistentSubscriptionError{
	Code: 9,
}

func (e *PersistentSubscriptionError) Error() string {
	switch e.Code {
	case 0:
		return "the persistent subscription filter requires a set of prefixes or a regex"
	case 1:
		return "the persistent subscription filter may only contain a regex or a set of prefixes, but not both"
	case 2:
		return fmt.Sprintf("failed to init the persistent subscription client: %s", e.Err)
	case 3:
		return fmt.Sprintf("failed to init persistent subscription send stream: %s", e.Err)
	case 4:
		return fmt.Sprintf("failed to init persistent subscription receive stream: %s", e.Err)
	case 5:
		return fmt.Sprintf("persistent subscription received not confirmation: %s", e.Err)
	case 6:
		return fmt.Sprintf("failed to create persistent subscription: %s", e.Err)
	case 7:
		return fmt.Sprintf("failed to update persistent subscription: %s", e.Err)
	case 8:
		return fmt.Sprintf("failed to delete persistent subscription: %s", e.Err)
	case 9:
		return "persistent subscription max message count exceeds maximum value"
	default:
		return "unknown persistent subscription to all error"
	}
}

func (e *PersistentSubscriptionError) Is(target error) bool {
	t, ok := target.(*PersistentSubscriptionError)

	if !ok {
		return false
	}

	return e.Code == t.Code
}

func (e *PersistentSubscriptionError) Unwrap() error {
	return e.Err
}

// Position ...
type Position struct {
	Commit  uint64
	Prepare uint64
}

// EmptyPosition ...
var EmptyPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}

// StartPosition ...
var StartPosition Position = Position{Commit: 0, Prepare: 0}

// EndPosition ...
var EndPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}

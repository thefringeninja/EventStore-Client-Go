package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type PersistentStreamSubscriptionOptions struct {
	Settings *types.SubscriptionSettings
	From     types.StreamPosition
}

func (o *PersistentStreamSubscriptionOptions) setDefaults() {
	if o.From == nil {
		o.From = types.End{}
	}
}

type PersistentAllSubscriptionOptions struct {
	Settings           *types.SubscriptionSettings
	From               types.AllPosition
	MaxSearchWindow    int
	CheckpointInterval int
	Filter             *types.SubscriptionFilter
}

func (o *PersistentAllSubscriptionOptions) setDefaults() {
	if o.From == nil {
		o.From = types.End{}
	}

	if o.Filter != nil {
		if o.MaxSearchWindow == 0 {
			o.MaxSearchWindow = 32
		}

		if o.CheckpointInterval == 0 {
			o.CheckpointInterval = 1
		}
	}
}

type ConnectToPersistentSubscriptionOptions struct {
	BatchSize uint32
}

func (o *ConnectToPersistentSubscriptionOptions) setDefaults() {
	if o.BatchSize == 0 {
		o.BatchSize = 10
	}
}

func (o *ConnectToPersistentSubscriptionOptions) SetBatchSize(value uint32) {
	o.BatchSize = value
}

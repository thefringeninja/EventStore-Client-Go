package options

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type PersistentStreamSubscriptionOptions struct {
	settings persistent.SubscriptionSettings
	position stream_position.StreamPosition
}

func PersistentStreamSubscriptionOptionsDefault() PersistentStreamSubscriptionOptions {
	return PersistentStreamSubscriptionOptions{
		settings: persistent.DefaultSubscriptionSettings,
		position: stream_position.End(),
	}
}

func (opts PersistentStreamSubscriptionOptions) Settings(settings persistent.SubscriptionSettings) PersistentStreamSubscriptionOptions {
	opts.settings = settings
	return opts
}

func (opts PersistentStreamSubscriptionOptions) Position(position stream_position.StreamPosition) PersistentStreamSubscriptionOptions {
	opts.position = position
	return opts
}

func (opts PersistentStreamSubscriptionOptions) GetSettings() persistent.SubscriptionSettings {
	return opts.settings
}

func (opts PersistentStreamSubscriptionOptions) GetPosition() stream_position.StreamPosition {
	return opts.position
}

type PersistentAllSubscriptionOptions struct {
	settings persistent.SubscriptionSettings
	position stream_position.AllStreamPosition
	filter   filtering.SubscriptionFilterOptions
}

func PersistentAllSubscriptionOptionsDefault() PersistentAllSubscriptionOptions {
	return PersistentAllSubscriptionOptions{
		settings: persistent.DefaultSubscriptionSettings,
		position: stream_position.End(),
	}
}

func (opts PersistentAllSubscriptionOptions) Settings(settings persistent.SubscriptionSettings) PersistentAllSubscriptionOptions {
	opts.settings = settings
	return opts
}

func (opts PersistentAllSubscriptionOptions) Position(position stream_position.AllStreamPosition) PersistentAllSubscriptionOptions {
	opts.position = position
	return opts
}

func (opts PersistentAllSubscriptionOptions) Filter(filter filtering.SubscriptionFilterOptions) PersistentAllSubscriptionOptions {
	opts.filter = filter
	return opts
}

func (opts PersistentAllSubscriptionOptions) GetSettings() persistent.SubscriptionSettings {
	return opts.settings
}

func (opts PersistentAllSubscriptionOptions) GetPosition() stream_position.AllStreamPosition {
	return opts.position
}

type ConnectToPersistentSubscriptionOptions struct {
	batchSize uint32
}

func ConnectToPersistentSubscriptionOptionsDefault() ConnectToPersistentSubscriptionOptions {
	return ConnectToPersistentSubscriptionOptions{
		batchSize: 10,
	}
}

func (opts ConnectToPersistentSubscriptionOptions) GetBatchSize() uint32 {
	return opts.batchSize
}

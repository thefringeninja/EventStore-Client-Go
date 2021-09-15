package options

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type SubscribeToStreamOptions struct {
	position stream_position.StreamPosition
	resolveLinks    bool
}

func SubscribeToStreamOptionsDefault() SubscribeToStreamOptions {
	return SubscribeToStreamOptions{
		position: stream_position.End(),
		resolveLinks:    false,
	}
}

func (opts SubscribeToStreamOptions) Position(value stream_position.StreamPosition) SubscribeToStreamOptions {
	opts.position = value
	return opts
}

func (opts SubscribeToStreamOptions) ResolveLinks() SubscribeToStreamOptions {
	opts.resolveLinks = true
	return opts
}

func (opts SubscribeToStreamOptions) GetPosition() stream_position.StreamPosition {
	return opts.position
}

func (opts SubscribeToStreamOptions) GetResolveLinks() bool {
	return opts.resolveLinks
}

type SubscribeToAllOptions struct {
	position stream_position.AllStreamPosition
	resolveLinks    bool
	filter   []filtering.SubscriptionFilterOptions
}

func SubscribeToAllOptionsDefault() SubscribeToAllOptions {
	return SubscribeToAllOptions{
		position: stream_position.End(),
		resolveLinks:    false,
		filter:   []filtering.SubscriptionFilterOptions{},
	}
}

func (opts SubscribeToAllOptions) Position(value stream_position.AllStreamPosition) SubscribeToAllOptions {
	opts.position = value
	return opts
}

func (opts SubscribeToAllOptions) ResolveLinks() SubscribeToAllOptions {
	opts.resolveLinks = true
	return opts
}

func (opts SubscribeToAllOptions) Filter(value filtering.SubscriptionFilterOptions) SubscribeToAllOptions {
	opts.filter = []filtering.SubscriptionFilterOptions{ value }
	return opts
}

func (opts SubscribeToAllOptions) GetPosition() stream_position.AllStreamPosition {
	return opts.position
}

func (opts SubscribeToAllOptions) GetResolveLinks() bool {
	return opts.resolveLinks
}

func (opts SubscribeToAllOptions) GetFilter() *filtering.SubscriptionFilterOptions {
	if len(opts.filter) == 0 {
		return nil
	}

	return &opts.filter[0]
}

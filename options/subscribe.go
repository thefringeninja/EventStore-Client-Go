package options

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type SubscribeToStreamOptions struct {
	PositionValue stream_position.StreamPosition
	ResolveToS    bool
}

func SubscribeToStreamOptionsDefault() *SubscribeToStreamOptions {
	return &SubscribeToStreamOptions{
		PositionValue: stream_position.End(),
		ResolveToS:    false,
	}
}

func (opts *SubscribeToStreamOptions) Position(value stream_position.StreamPosition) *SubscribeToStreamOptions {
	opts.PositionValue = value
	return opts
}

func (opts *SubscribeToStreamOptions) ResolveLinks() *SubscribeToStreamOptions {
	opts.ResolveToS = true
	return opts
}

type SubscribeToAllOptions struct {
	PositionValue stream_position.AllStreamPosition
	ResolveToS    bool
	FilterValue   *filtering.SubscriptionFilterOptions
}

func SubscribeToAllOptionsDefault() *SubscribeToAllOptions {
	return &SubscribeToAllOptions{
		PositionValue: stream_position.End(),
		ResolveToS:    false,
		FilterValue:   nil,
	}
}

func (opts *SubscribeToAllOptions) Position(value stream_position.AllStreamPosition) *SubscribeToAllOptions {
	opts.PositionValue = value
	return opts
}

func (opts *SubscribeToAllOptions) ResolveLinks() *SubscribeToAllOptions {
	opts.ResolveToS = true
	return opts
}

func (opts *SubscribeToAllOptions) Filter(value filtering.SubscriptionFilterOptions) *SubscribeToAllOptions {
	opts.FilterValue = &value
	return opts
}

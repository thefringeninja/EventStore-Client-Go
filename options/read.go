package options

import (
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type ReadStreamEventsOptions struct {
	DirectionValue direction.Direction
	From           stream_position.StreamPosition
	ResolveToS     bool
}

func NewReadStreamEventsOptions() *ReadStreamEventsOptions {
	return &ReadStreamEventsOptions{
		DirectionValue: direction.Forwards,
		From:           stream_position.Start{},
		ResolveToS:     false,
	}
}

func (opts *ReadStreamEventsOptions) Forwards() *ReadStreamEventsOptions {
	opts.DirectionValue = direction.Forwards
	return opts
}

func (opts *ReadStreamEventsOptions) Backwards() *ReadStreamEventsOptions {
	opts.DirectionValue = direction.Backwards
	return opts
}

func (opts *ReadStreamEventsOptions) Direction(dir direction.Direction) *ReadStreamEventsOptions {
	opts.DirectionValue = dir
	return opts
}

func (opts *ReadStreamEventsOptions) Position(position stream_position.StreamPosition) *ReadStreamEventsOptions {
	opts.From = position
	return opts
}

func (opts *ReadStreamEventsOptions) ResolveLinks() *ReadStreamEventsOptions {
	opts.ResolveToS = true
	return opts
}

type ReadAllEventsOptions struct {
	DirectionValue direction.Direction
	From           stream_position.AllStreamPosition
	ResolveToS     bool
}

func NewReadAllEventsOptions() *ReadAllEventsOptions {
	return &ReadAllEventsOptions{
		DirectionValue: direction.Forwards,
		From:           stream_position.Start{},
		ResolveToS:     false,
	}
}

func (opts *ReadAllEventsOptions) Forwards() *ReadAllEventsOptions {
	opts.DirectionValue = direction.Forwards
	return opts
}

func (opts *ReadAllEventsOptions) Backwards() *ReadAllEventsOptions {
	opts.DirectionValue = direction.Backwards
	return opts
}

func (opts *ReadAllEventsOptions) Direction(dir direction.Direction) *ReadAllEventsOptions {
	opts.DirectionValue = dir
	return opts
}

func (opts *ReadAllEventsOptions) Position(position stream_position.AllStreamPosition) *ReadAllEventsOptions {
	opts.From = position
	return opts
}

func (opts *ReadAllEventsOptions) ResolveLinks() *ReadAllEventsOptions {
	opts.ResolveToS = true
	return opts
}

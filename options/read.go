package options

import (
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

type ReadStreamEventsOptions struct {
	direction    direction.Direction
	position     stream_position.StreamPosition
	resolveLinks bool
}

func ReadStreamEventsOptionsDefault() ReadStreamEventsOptions {
	return ReadStreamEventsOptions{
		direction:    direction.Forwards,
		position:     stream_position.RevisionStart{},
		resolveLinks: false,
	}
}

func (opts ReadStreamEventsOptions) Forwards() ReadStreamEventsOptions {
	opts.direction = direction.Forwards
	return opts
}

func (opts ReadStreamEventsOptions) Backwards() ReadStreamEventsOptions {
	opts.direction = direction.Backwards
	return opts
}

func (opts ReadStreamEventsOptions) Direction(dir direction.Direction) ReadStreamEventsOptions {
	opts.direction = dir
	return opts
}

func (opts ReadStreamEventsOptions) Position(position stream_position.StreamPosition) ReadStreamEventsOptions {
	opts.position = position
	return opts
}

func (opts ReadStreamEventsOptions) ResolveLinks() ReadStreamEventsOptions {
	opts.resolveLinks = true
	return opts
}

func (opts ReadStreamEventsOptions) GetDirection() direction.Direction {
	return opts.direction
}

func (opts ReadStreamEventsOptions) GetPosition() stream_position.StreamPosition {
	return opts.position
}

func (opts ReadStreamEventsOptions) GetResolveLinks() bool {
	return opts.resolveLinks
}

type ReadAllEventsOptions struct {
	direction    direction.Direction
	position     stream_position.AllStreamPosition
	resolveLinks bool
}

func ReadAllEventsOptionsDefault() ReadAllEventsOptions {
	return ReadAllEventsOptions{
		direction:    direction.Forwards,
		position:     stream_position.RevisionStart{},
		resolveLinks: false,
	}
}

func (opts ReadAllEventsOptions) Forwards() ReadAllEventsOptions {
	opts.direction = direction.Forwards
	return opts
}

func (opts ReadAllEventsOptions) Backwards() ReadAllEventsOptions {
	opts.direction = direction.Backwards
	return opts
}

func (opts ReadAllEventsOptions) Direction(dir direction.Direction) ReadAllEventsOptions {
	opts.direction = dir
	return opts
}

func (opts ReadAllEventsOptions) Position(position stream_position.AllStreamPosition) ReadAllEventsOptions {
	opts.position = position
	return opts
}

func (opts ReadAllEventsOptions) ResolveLinks() ReadAllEventsOptions {
	opts.resolveLinks = true
	return opts
}

func (opts ReadAllEventsOptions) GetDirection() direction.Direction {
	return opts.direction
}

func (opts ReadAllEventsOptions) GetPosition() stream_position.AllStreamPosition {
	return opts.position
}

func (opts ReadAllEventsOptions) GetResolveLinks() bool {
	return opts.resolveLinks
}

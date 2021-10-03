package client

import (
	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/stream"
)

type PersistentStreamSubscriptionOptions struct {
	settings []persistent.SubscriptionSettings
	position stream.StreamPosition
}

func (o *PersistentStreamSubscriptionOptions) setDefaults() {
	if o.settings == nil {
		o.settings = []persistent.SubscriptionSettings{persistent.SubscriptionSettingsDefault()}
	}

	if o.position == nil {
		o.position = stream.End()
	}
}

func (o *PersistentStreamSubscriptionOptions) SetSettings(settings persistent.SubscriptionSettings) {
	o.settings = []persistent.SubscriptionSettings{persistent.SubscriptionSettingsDefault()}
}

func (o *PersistentStreamSubscriptionOptions) SetFrom(position stream.StreamPosition) {
	o.position = position
}

func (o *PersistentStreamSubscriptionOptions) SetFromStart() {
	o.position = stream.Start()
}

func (o *PersistentStreamSubscriptionOptions) SetFromEnd() {
	o.position = stream.End()
}

func (o *PersistentStreamSubscriptionOptions) SetFromRevision(value uint64) {
	o.position = stream.Revision(value)
}

func (o *PersistentStreamSubscriptionOptions) Settings() *persistent.SubscriptionSettings {
	if len(o.settings) == 0 {
		return nil
	}

	return &o.settings[0]
}

func (o *PersistentStreamSubscriptionOptions) From() stream.StreamPosition {
	return o.position
}

type PersistentAllSubscriptionOptions struct {
	settings           []persistent.SubscriptionSettings
	position           stream.AllStreamPosition
	maxSearchWindow    int
	checkpointInterval int
	filter             []filtering.SubscriptionFilter
}

func (o *PersistentAllSubscriptionOptions) setDefaults() {
	if o.settings == nil {
		o.settings = []persistent.SubscriptionSettings{persistent.SubscriptionSettingsDefault()}
	}

	if o.position == nil {
		o.position = stream.End()
	}

	if len(o.filter) != 0 {
		if o.maxSearchWindow == 0 {
			o.maxSearchWindow = 32
		}

		if o.checkpointInterval == 0 {
			o.checkpointInterval = 1
		}
	}
}

func (o *PersistentAllSubscriptionOptions) SetSettings(settings persistent.SubscriptionSettings) {
	o.settings = []persistent.SubscriptionSettings{persistent.SubscriptionSettingsDefault()}
}

func (o *PersistentAllSubscriptionOptions) SetFrom(position stream.AllStreamPosition) {
	o.position = position
}

func (o *PersistentAllSubscriptionOptions) SetFromStart() {
	o.position = stream.Start()
}

func (o *PersistentAllSubscriptionOptions) SetFromEnd() {
	o.position = stream.End()
}

func (o *PersistentAllSubscriptionOptions) SetMaxSearchWindow(value int) {
	o.maxSearchWindow = value
}

func (o *PersistentAllSubscriptionOptions) SetNoMaxSearchWindow() {
	o.maxSearchWindow = -1
}

func (o *PersistentAllSubscriptionOptions) SetCheckpointInterval(value int) {
	o.checkpointInterval = value
}

func (o *PersistentAllSubscriptionOptions) SetFromPosition(value position.Position) {
	o.position = stream.Position(value)
}

func (o *PersistentAllSubscriptionOptions) SetFilter(filter filtering.SubscriptionFilter) {
	o.filter = []filtering.SubscriptionFilter{filter}
}

func (o *PersistentAllSubscriptionOptions) Settings() *persistent.SubscriptionSettings {
	if len(o.settings) == 0 {
		return nil
	}

	return &o.settings[0]
}

func (o *PersistentAllSubscriptionOptions) From() stream.AllStreamPosition {
	return o.position
}

func (o *PersistentAllSubscriptionOptions) MaxSearchWindow() int {
	return o.maxSearchWindow
}

func (o *PersistentAllSubscriptionOptions) CheckpointInterval() int {
	return o.checkpointInterval
}

func (o *PersistentAllSubscriptionOptions) Filter() *filtering.SubscriptionFilter {
	if len(o.filter) == 0 {
		return nil
	}

	return &o.filter[0]
}

type ConnectToPersistentSubscriptionOptions struct {
	batchSize uint32
}

func (o *ConnectToPersistentSubscriptionOptions) setDefaults() {
	if o.batchSize == 0 {
		o.batchSize = 10
	}
}

func (o *ConnectToPersistentSubscriptionOptions) SetBatchSize(value uint32) {
	o.batchSize = value
}

func (o *ConnectToPersistentSubscriptionOptions) BatchSize() uint32 {
	return o.batchSize
}

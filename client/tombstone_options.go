package client

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type TombstoneStreamOptions struct {
	expectedRevision stream_revision.ExpectedRevision
}

func (o *TombstoneStreamOptions) setDefaults() {
	if o.expectedRevision == nil {
		o.expectedRevision = stream_revision.Any()
	}
}

func (o *TombstoneStreamOptions) SetExpectedRevision(revision stream_revision.ExpectedRevision) {
	o.expectedRevision = revision
}

func (o *TombstoneStreamOptions) SetExpectNoStream() {
	o.expectedRevision = stream_revision.NoStream()
}

func (o *TombstoneStreamOptions) SetExpectStreamExists() {
	o.expectedRevision = stream_revision.StreamExists()
}

func (o *TombstoneStreamOptions) SetExpectRevision(value uint64) {
	o.expectedRevision = stream_revision.Exact(value)
}

func (o *TombstoneStreamOptions) ExpectedRevision() stream_revision.ExpectedRevision {
	return o.expectedRevision
}

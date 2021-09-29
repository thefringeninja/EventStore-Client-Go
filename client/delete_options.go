package client

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type DeleteStreamOptions struct {
	expectedRevision stream_revision.ExpectedRevision
}

func (o *DeleteStreamOptions) SetDefaults() {
	o.expectedRevision = stream_revision.Any()
}

func (o *DeleteStreamOptions) SetExpectedRevision(revision stream_revision.ExpectedRevision) {
	o.expectedRevision = revision
}

func (o *DeleteStreamOptions) SetExpectNoStream() {
	o.expectedRevision = stream_revision.NoStream()
}

func (o *DeleteStreamOptions) SetExpectStreamExists() {
	o.expectedRevision = stream_revision.StreamExists()
}

func (o *DeleteStreamOptions) SetExpectRevision(value uint64) {
	o.expectedRevision = stream_revision.Exact(value)
}

func (o *DeleteStreamOptions) ExpectedRevision() stream_revision.ExpectedRevision {
	return o.expectedRevision
}

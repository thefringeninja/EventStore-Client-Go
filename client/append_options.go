package client

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type AppendToStreamOptions struct {
	revision stream_revision.ExpectedRevision
}

func (o *AppendToStreamOptions) setDefaults() {
	if o.revision == nil {
		o.revision = stream_revision.Any()
	}
}

func (o *AppendToStreamOptions) SetExpectedRevision(revision stream_revision.ExpectedRevision) {
	o.revision = revision
}

func (o *AppendToStreamOptions) ExpectedRevision() stream_revision.ExpectedRevision {
	return o.revision
}

func (o *AppendToStreamOptions) SetExpectNoStream() {
	o.revision = stream_revision.NoStream()
}

func (o *AppendToStreamOptions) SetExpectAny() {
	o.revision = stream_revision.Any()
}

func (o *AppendToStreamOptions) SetExpectStreamExists() {
	o.revision = stream_revision.StreamExists()
}

func (o *AppendToStreamOptions) SetExpectRevision(value uint64) {
	o.revision = stream_revision.Exact(value)
}

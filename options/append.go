package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type AppendToStreamOptions struct {
	Revision stream_revision.ExpectedRevision
}

func NewAppendToStreamOptions() *AppendToStreamOptions {
	return &AppendToStreamOptions {
		Revision: stream_revision.Any(),
	}
}

func (options *AppendToStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) *AppendToStreamOptions {
	options.Revision = revision

	return options
}

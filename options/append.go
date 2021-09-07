package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type AppendToStreamOptions struct {
	revision stream_revision.ExpectedRevision
}

func AppendToStreamOptionsDefault() AppendToStreamOptions {
	return AppendToStreamOptions {
		revision: stream_revision.Any(),
	}
}

func (options AppendToStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) AppendToStreamOptions {
	options.revision = revision

	return options
}

func (options AppendToStreamOptions) GetExpectedRevision() stream_revision.ExpectedRevision {
	return options.revision
}

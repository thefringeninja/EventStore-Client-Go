package options

import (
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type TombstoneStreamOptions struct {
	Revision stream_revision.ExpectedRevision
}

func TombstoneStreamOptionsDefault() *TombstoneStreamOptions {
	return &TombstoneStreamOptions {
		Revision: stream_revision.Any(),
	}
}

func (options *TombstoneStreamOptions) ExpectedRevision(revision stream_revision.ExpectedRevision) *TombstoneStreamOptions {
	options.Revision = revision

	return options
}

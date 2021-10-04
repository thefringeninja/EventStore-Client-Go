package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type AppendToStreamOptions struct {
	ExpectedRevision types.ExpectedRevision
	Authenticated    *types.Credentials
}

func (o *AppendToStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = types.Any{}
	}
}

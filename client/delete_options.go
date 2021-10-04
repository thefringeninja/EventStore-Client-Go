package client

import (
	"github.com/EventStore/EventStore-Client-Go/types"
)

type DeleteStreamOptions struct {
	ExpectedRevision types.ExpectedRevision
	Authenticated    *types.Credentials
}

func (o *DeleteStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = types.Any{}
	}
}

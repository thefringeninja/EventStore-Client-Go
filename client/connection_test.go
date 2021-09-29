package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_CloseConnection(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)

	testEvent := createTestEvent()
	testEvent.SetEventID(uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"))

	streamID := uuid.Must(uuid.NewV4())
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	opts := options.AppendToStreamOptions{}
	opts.SetDefaults()
	opts.SetExpectNoStream()
	_, err := client.AppendToStream(context, streamID.String(), opts, testEvent)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	client.Close()
	opts.SetDefaults()
	_, err = client.AppendToStream(context, streamID.String(), opts, testEvent)

	assert.NotNil(t, err)
	assert.Equal(t, "esdb connection is closed", err.Error())
}

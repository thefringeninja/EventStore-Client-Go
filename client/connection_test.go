package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/options"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_CloseConnection(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)

	testEvent := messages.NewBinaryEvent("TestEvent", []byte{0xb, 0xe, 0xe, 0xf}).
		SetEventID(uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872")).
		SetMetadata([]byte{0xd, 0xe, 0xa, 0xd}).
		Build()

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	opts := options.AppendToStreamOptionsDefault().ExpectedRevision(stream_revision.NoStream())
	_, err := client.AppendToStream(context, streamID.String(), opts, testEvent)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	client.Close()
	opts = options.AppendToStreamOptionsDefault()
	_, err = client.AppendToStream(context, streamID.String(), opts, testEvent)

	assert.NotNil(t, err)
	assert.Equal(t, "esdb connection is closed", err.Error())
}

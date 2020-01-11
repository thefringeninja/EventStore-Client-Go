package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eventstore/EventStore-Client-Go/pkg/client"
	direction "github.com/eventstore/EventStore-Client-Go/pkg/direction"
	client_errors "github.com/eventstore/EventStore-Client-Go/pkg/errors"
	messages "github.com/eventstore/EventStore-Client-Go/pkg/messages"
	stream_revision "github.com/eventstore/EventStore-Client-Go/pkg/streamrevision"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() messages.ProposedEvent {
	return messages.ProposedEvent{
		EventID:      uuid.Must(uuid.NewV4()),
		EventType:    "TestEvent",
		IsJSON:       false,
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
}

func TestAppendToStreamSingleEventNoStream(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()
	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		IsJSON:       false,
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	events := []messages.ProposedEvent{
		testEvent,
	}

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err := client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionNoStream, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	readResult, err := client.ReadStreamEvents(context, direction.Forwards, streamID.String(), stream_revision.StreamRevisionStart, 1, false)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, int32(1), int32(len(readResult.Events)), "Expected the correct number of messages to be returned")
	assert.Equal(t, testEvent.EventID, readResult.Events[0].EventID)
	assert.Equal(t, testEvent.EventType, readResult.Events[0].EventType)
	assert.Equal(t, streamID.String(), readResult.Events[0].StreamID)
	assert.Equal(t, testEvent.Data, readResult.Events[0].Data)
	assert.Equal(t, testEvent.UserMetadata, readResult.Events[0].UserMetadata)
}

func TestAppendWithInvalidStreamRevision(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()
	events := []messages.ProposedEvent{
		createTestEvent(),
		createTestEvent(),
	}

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err := client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionStreamExists, events)

	if !errors.Is(err, client_errors.ErrWrongExpectedStreamRevision) {
		t.Fatalf("Expected WrongExpectedVersion, got %+v", err)
	}
}

func TestAppendToSystemStreamWithIncorrectCredentials(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config := client.NewConfiguration()
	config.Address = container.Endpoint
	config.Username = "bad_user"
	config.Password = "bad_password"
	config.SkipCertificateVerification = true

	client, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}
	err = client.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}

	defer client.Close()
	events := []messages.ProposedEvent{
		createTestEvent(),
	}

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err = client.AppendToStream(context, streamID.String(), stream_revision.StreamRevisionAny, events)

	if !errors.Is(err, client_errors.ErrPermissionDenied) {
		t.Fatalf("Expected PermissionDenied, got %+v", err)
	}
}

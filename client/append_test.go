package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"
	client_errors "github.com/EventStore/EventStore-Client-Go/errors"
	messages "github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/metadata"
	"github.com/EventStore/EventStore-Client-Go/options"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() messages.ProposedEvent {
	return messages.NewBinaryProposedEvent("TestEvent", []byte{0xb, 0xe, 0xe, 0xf}).
		EventID(uuid.Must(uuid.NewV4())).
		Metadata([]byte{0xd, 0xe, 0xa, 0xd})
}

func collectStreamEvents(stream *client.ReadStream) ([]*messages.ResolvedEvent, error) {
	events := []*messages.ResolvedEvent{}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}

func TestAppendToStreamSingleEventNoStream(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()
	testEvent := messages.NewBinaryProposedEvent("TestEvent", []byte{0xb, 0xe, 0xe, 0xf}).
		EventID(uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872")).
		Metadata([]byte{0xd, 0xe, 0xa, 0xd})

	streamID := uuid.Must(uuid.NewV4())
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := options.AppendToStreamOptions{}
	opts.SetExpectNoStream()

	_, err := client.AppendToStream(context, streamID.String(), opts, testEvent)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	ropts := options.ReadStreamEventsOptionsDefault()
	stream, err := client.ReadStreamEvents(context, streamID.String(), &ropts, 1)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	defer stream.Close()

	events, err := collectStreamEvents(stream)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, int32(1), int32(len(events)), "Expected the correct number of messages to be returned")
	assert.Equal(t, testEvent.GetEventID(), events[0].GetOriginalEvent().EventID)
	assert.Equal(t, testEvent.GetEventType(), events[0].GetOriginalEvent().EventType)
	assert.Equal(t, streamID.String(), events[0].GetOriginalEvent().StreamID)
	assert.Equal(t, testEvent.GetData(), events[0].GetOriginalEvent().Data)
	assert.Equal(t, testEvent.GetMetadata(), events[0].GetOriginalEvent().UserMetadata)
}

func TestAppendWithInvalidStreamRevision(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := options.AppendToStreamOptions{}
	opts.SetExpectStreamExists()

	_, err := client.AppendToStream(context, streamID.String(), opts, createTestEvent())

	if !errors.Is(err, client_errors.ErrWrongExpectedStreamRevision) {
		t.Fatalf("Expected WrongExpectedVersion, got %+v", err)
	}
}

func TestAppendToSystemStreamWithIncorrectCredentials(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	conn := fmt.Sprintf("esdb://bad_user:bad_password@%s?tlsverifycert=false", container.Endpoint)
	config, err := client.ParseConnectionString(conn)
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	client, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	defer client.Close()

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := options.AppendToStreamOptions{}
	opts.SetExpectAny()

	_, err = client.AppendToStream(context, streamID.String(), opts, createTestEvent())

	if !errors.Is(err, client_errors.ErrUnauthenticated) {
		t.Fatalf("Expected Unauthenticated, got %+v", err)
	}
}

func TestMetadataOperation(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	str := fmt.Sprintf("esdb://admin:changeit@%s?tlsverifycert=false", container.Endpoint)
	config, err := client.ParseConnectionString(str)

	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	client, err := client.NewClient(config)

	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	defer client.Close()

	streamID := uuid.Must(uuid.NewV4())
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := options.AppendToStreamOptions{}
	opts.SetExpectAny()

	_, err = client.AppendToStream(context, streamID.String(), opts, createTestEvent())

	assert.Nil(t, err, "error when writing an event")

	acl := metadata.AclDefault().AddReadRoles("admin")
	meta := metadata.StreamMetadataDefault().
		MaxAge(2 * time.Second).
		Acl(acl)

	result, err := client.SetStreamMetadata(context, streamID.String(), opts, meta)

	assert.Nil(t, err, "no error from writing stream metadata")
	assert.NotNil(t, result, "defined write result after writing metadata")

	ropts := options.ReadStreamEventsOptionsDefault()
	metaActual, err := client.GetStreamMetadata(context, streamID.String(), &ropts)

	assert.Nil(t, err, "no error when reading stream metadata")

	assert.Equal(t, meta, *metaActual, "matching metadata")
}

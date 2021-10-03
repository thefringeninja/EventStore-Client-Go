package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/types"

	"github.com/EventStore/EventStore-Client-Go/client"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() types.ProposedEvent {
	event := types.ProposedEvent{}
	event.SetEventType("TestEvent")
	event.SetBinaryData([]byte{0xb, 0xe, 0xe, 0xf})
	event.SetEventID(uuid.Must(uuid.NewV4()))
	event.SetMetadata([]byte{0xd, 0xe, 0xa, 0xd})

	return event
}

func collectStreamEvents(stream *client.ReadStream) ([]*types.ResolvedEvent, error) {
	events := []*types.ResolvedEvent{}

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

	db := CreateTestClient(container, t)
	defer db.Close()

	testEvent := createTestEvent()
	testEvent.SetEventID(uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"))

	streamID := uuid.Must(uuid.NewV4())
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := client.AppendToStreamOptions{
		ExpectedRevision: types.NoStream{},
	}

	_, err := db.AppendToStream(context, streamID.String(), opts, testEvent)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	stream, err := db.ReadStreamEvents(context, streamID.String(), client.ReadStreamEventsOptions{}, 1)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	defer stream.Close()

	events, err := collectStreamEvents(stream)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, int32(1), int32(len(events)), "Expected the correct number of messages to be returned")
	assert.Equal(t, testEvent.EventID(), events[0].OriginalEvent().EventID)
	assert.Equal(t, testEvent.EventType(), events[0].OriginalEvent().EventType)
	assert.Equal(t, streamID.String(), events[0].OriginalEvent().StreamID)
	assert.Equal(t, testEvent.Data(), events[0].OriginalEvent().Data)
	assert.Equal(t, testEvent.Metadata(), events[0].OriginalEvent().UserMetadata)
}

func TestAppendWithInvalidStreamRevision(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := client.AppendToStreamOptions{
		ExpectedRevision: types.StreamExists{},
	}

	_, err := db.AppendToStream(context, streamID.String(), opts, createTestEvent())

	if !errors.Is(err, client.ErrWrongExpectedStreamRevision) {
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

	db, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	defer db.Close()

	streamID, _ := uuid.NewV4()
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := client.AppendToStreamOptions{
		ExpectedRevision: types.Any{},
	}

	_, err = db.AppendToStream(context, streamID.String(), opts, createTestEvent())

	if !errors.Is(err, client.ErrUnauthenticated) {
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

	db, err := client.NewClient(config)

	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	defer db.Close()

	streamID := uuid.Must(uuid.NewV4())
	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	opts := client.AppendToStreamOptions{
		ExpectedRevision: types.Any{},
	}

	_, err = db.AppendToStream(context, streamID.String(), opts, createTestEvent())

	assert.Nil(t, err, "error when writing an event")

	acl := types.Acl{}
	acl.AddReadRoles("admin")

	meta := types.StreamMetadata{}
	meta.SetMaxAge(2 * time.Second)
	meta.SetAcl(acl)

	result, err := db.SetStreamMetadata(context, streamID.String(), opts, meta)

	assert.Nil(t, err, "no error from writing stream metadata")
	assert.NotNil(t, result, "defined write result after writing metadata")

	metaActual, err := db.GetStreamMetadata(context, streamID.String(), client.ReadStreamEventsOptions{})

	assert.Nil(t, err, "no error when reading stream metadata")

	assert.Equal(t, meta, *metaActual, "matching metadata")
}

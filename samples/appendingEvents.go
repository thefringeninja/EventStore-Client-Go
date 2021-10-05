package samples

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gofrs/uuid"

	"github.com/EventStore/EventStore-Client-Go/client"
)

type TestEvent struct {
	Id            string
	ImportantData string
}

func AppendToStream(db *client.Client) {
	// region append-to-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	result, err := db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *client.Client) {
	// region append-duplicate-event
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	id := uuid.Must(uuid.NewV4())
	event := client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		EventID:     id,
		Data:        bytes,
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// endregion append-duplicate-event
}

func AppendWithNoStream(db *client.Client) {
	// region append-with-no-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	options := client.AppendToStreamOptions{
		ExpectedRevision: client.NoStream{},
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		Data:        bytes,
	})

	if err != nil {
		panic(err)
	}

	bytes, err = json.Marshal(TestEvent{
		Id:            "2",
		ImportantData: "some other value",
	})
	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *client.Client) {
	// region append-with-concurrency-check
	ropts := client.ReadStreamOptions{
		Direction: client.Backwards,
		From:      client.End{},
	}

	stream, err := db.ReadStream(context.Background(), "concurrency-stream", ropts, 1)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	lastEvent, err := stream.Recv()

	if err != nil {
		panic(err)
	}

	data := TestEvent{
		Id:            "1",
		ImportantData: "clientOne",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	aopts := client.AppendToStreamOptions{
		ExpectedRevision: client.Revision(lastEvent.OriginalEvent().EventNumber),
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		Data:        bytes,
	})

	data = TestEvent{
		Id:            "1",
		ImportantData: "clientTwo",
	}
	bytes, err = json.Marshal(data)
	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, client.ProposedEvent{
		ContentType: client.JsonContentType,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-concurrency-check
}

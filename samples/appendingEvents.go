package samples

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"log"

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

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	result, err := db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, event)
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *client.Client) {
	// region append-duplicate-event
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
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

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	options := client.AppendToStreamOptions{}
	options.SetExpectNoStream()

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, event)

	if err != nil {
		panic(err)
	}

	err = event.SetJsonData(TestEvent{
		Id:            "2",
		ImportantData: "some other value",
	})

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, event)
	// noregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *client.Client) {
	// region append-with-concurrency-check
	ropts := client.ReadStreamEventsOptions{}
	ropts.SetBackwards()
	ropts.SetFromEnd()

	stream, err := db.ReadStreamEvents(context.Background(), "concurrency-stream", ropts, 1)

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

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err = event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	aopts := client.AppendToStreamOptions{}
	aopts.SetExpectRevision(lastEvent.OriginalEvent().EventNumber)

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, event)

	data = TestEvent{
		Id:            "1",
		ImportantData: "clientTwo",
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, event)
	// endregion append-with-concurrency-check
}

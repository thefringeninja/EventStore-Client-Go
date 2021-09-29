package samples

import (
	"context"
	"log"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/options"
)

type TestEvent struct {
	id            string
	importantData string
}

func AppendToStream(db *client.Client) {
	// region append-to-stream
	data := TestEvent{
		id:            "1",
		importantData: "some value",
	}

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptions{}
	options.SetDefaults()

	result, err := db.AppendToStream(context.Background(), "some-stream", options, event)
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *client.Client) {
	// region append-duplicate-event
	data := TestEvent{
		id:            "1",
		importantData: "some value",
	}

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptions{}
	options.SetDefaults()

	_, err = db.AppendToStream(context.Background(), "some-stream", options, event)

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", options, event)

	if err != nil {
		panic(err)
	}

	// endregion append-duplicate-event
}

func AppendWithNoStream(db *client.Client) {
	// region append-with-no-stream
	data := TestEvent{
		id:            "1",
		importantData: "some value",
	}

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err := event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptions{}
	options.SetExpectNoStream()

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, event)

	if err != nil {
		panic(err)
	}

	err = event.SetJsonData(TestEvent{
		id:            "2",
		importantData: "some other value",
	})

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, event)
	// noregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *client.Client) {
	// region append-with-concurrency-check
	ropts := options.ReadStreamEventsOptions{}
	ropts.SetDefaults()
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
		id:            "1",
		importantData: "clientOne",
	}

	event := messages.ProposedEvent{}
	event.SetEventType("some-event")
	err = event.SetJsonData(data)

	if err != nil {
		panic(err)
	}

	aopts := options.AppendToStreamOptions{}
	aopts.SetExpectRevision(lastEvent.GetOriginalEvent().EventNumber)

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, event)

	data = TestEvent{
		id:            "1",
		importantData: "clientTwo",
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, event)
	// endregion append-with-concurrency-check
}

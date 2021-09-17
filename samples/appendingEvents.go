package samples

import (
	"context"
	"log"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
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

	event, err := messages.NewJsonProposedEvent("some-event", data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptionsDefault()

	result, err := db.AppendToStream(context.Background(), "some-stream", &options, event)
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *client.Client) {
	// region append-duplicate-event
	data := TestEvent{
		id:            "1",
		importantData: "some value",
	}

	event, err := messages.NewJsonProposedEvent("some-event", data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptionsDefault()

	_, err = db.AppendToStream(context.Background(), "some-stream", &options, event)

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", &options, event)

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

	event, err := messages.NewJsonProposedEvent("some-event", data)

	if err != nil {
		panic(err)
	}

	options := options.AppendToStreamOptionsDefault().ExpectedRevision(streamrevision.NoStream())

	_, err = db.AppendToStream(context.Background(), "same-event-stream", &options, event)

	if err != nil {
		panic(err)
	}

	data = TestEvent{
		id:            "2",
		importantData: "some other value",
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", &options, event)
	// noregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *client.Client) {
	// region append-with-concurrency-check
	ropts := options.ReadStreamEventsOptionsDefault().
		Backwards().
		Position(stream_position.End())

	stream, err := db.ReadStreamEvents(context.Background(), "concurrency-stream", &ropts, 1)

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

	event, err := messages.NewJsonProposedEvent("some-event", data)

	if err != nil {
		panic(err)
	}

	aopts := options.AppendToStreamOptionsDefault().ExpectedRevision(streamrevision.Exact(lastEvent.GetOriginalEvent().EventNumber))

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", &aopts, event)

	data = TestEvent{
		id:            "1",
		importantData: "clientTwo",
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", &aopts, event)
	// endregion append-with-concurrency-check
}

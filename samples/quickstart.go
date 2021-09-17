package samples

import (
	"context"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/options"
)

func Run() {
	// region createClient
	settings, err := client.ParseConnectionString("{connectionString}")

	if err != nil {
		panic(err)
	}

	db, err := client.NewClient(settings)

	// endregion createClient
	if err != nil {
		panic(err)
	}

	testEvent := TestEvent{
		id:            "some id",
		importantData: "I wrote my first event!",
	}

	data, err := messages.NewJsonProposedEvent("TestEvent", testEvent)

	if err != nil {
		panic(err)
	}

	aopts := options.AppendToStreamOptionsDefault()

	_, err = db.AppendToStream(context.Background(), "some-stream", &aopts, data)

	if err != nil {
		panic(err)
	}

	// region readStream
	ropts := options.ReadStreamEventsOptionsDefault()
	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", &ropts, 10)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		// Doing something productive with the event
		fmt.Println(event)
	}
	// endregion readStream
}

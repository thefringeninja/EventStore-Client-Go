package samples

import (
	"context"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
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

	event := messages.ProposedEvent{}
	event.SetEventType("TestEvent")
	err = event.SetJsonData(testEvent)

	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// region readStream
	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", client.ReadStreamEventsOptions{}, 10)

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

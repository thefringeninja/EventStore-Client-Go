package samples

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/types"
	"github.com/gofrs/uuid"

	"github.com/EventStore/EventStore-Client-Go/client"
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

	// region createEvent
	testEvent := TestEvent{
		Id:            uuid.Must(uuid.NewV4()).String(),
		ImportantData: "I wrote my first event!",
	}

	data, err := json.Marshal(testEvent)
	// endregion createEvent
	
	if err != nil {
		panic(err)
	}

	// region appendEvents
	_, err = db.AppendToStream(context.Background(), "some-stream", client.AppendToStreamOptions{}, types.ProposedEvent{
		ContentType: types.JsonContentType,
		EventType:   "TestEvent",
		Data:        data,
	})
	// endregion appendEvents

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

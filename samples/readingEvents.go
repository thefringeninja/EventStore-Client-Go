package samples

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/errors"
)

func ReadFromStream(db *client.Client) {
	// region read-from-stream
	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", client.ReadStreamEventsOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadFromStreamPosition(db *client.Client) {
	// region read-from-position
	ropts := client.ReadStreamEventsOptions{}
	ropts.SetFromRevision(10)

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", ropts, 20)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-position
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadFromStreamPositionCheck(db *client.Client) {
	// region checking-for-stream-presence
	ropts := client.ReadStreamEventsOptions{}
	ropts.SetFromRevision(10)

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if err == errors.ErrStreamNotFound {
			fmt.Print("Stream not found")
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion checking-for-stream-presence
}

func ReadStreamBackwards(db *client.Client) {
	// region reading-backwards
	ropts := client.ReadStreamEventsOptions{}
	ropts.SetBackwards()
	ropts.SetFromEnd()

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", ropts, 10)

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

		fmt.Printf("Event> %v", event)
	}
	// endregion reading-backwards
}

func ReadFromAllStream(db *client.Client) {
	// region read-from-all-stream
	stream, err := db.ReadAllEvents(context.Background(), client.ReadAllEventsOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream
	// region read-from-all-stream-iterate
	for {
		event, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-iterate
}

func IgnoreSystemEvents(db *client.Client) {
	// region ignore-system-events
	stream, err := db.ReadAllEvents(context.Background(), client.ReadAllEventsOptions{}, 100)

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

		fmt.Printf("Event> %v", event)

		if strings.HasPrefix(event.GetOriginalEvent().EventType, "$") {
			continue
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion ignore-system-events
}

func ReadFromAllBackwards(db *client.Client) {
	// region read-from-all-stream-backwards
	ropts := client.ReadAllEventsOptions{}
	ropts.SetFromEnd()
	ropts.SetBackwards()

	stream, err := db.ReadAllEvents(context.Background(), ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream-backwards
	// region read-from-all-stream-backwards-iterate
	for {
		event, err := stream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-backwards-iterate
}

func ReadFromStreamResolvingLinkToS(db *client.Client) {
	// region read-from-all-stream-resolving-link-tos
	ropts := client.ReadAllEventsOptions{}
	ropts.SetResolveLinks()

	stream, err := db.ReadAllEvents(context.Background(), ropts, 100)
	// endregion read-from-all-stream-resolving-link-tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

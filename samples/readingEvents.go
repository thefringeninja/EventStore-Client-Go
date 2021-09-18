package samples

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
)

func ReadFromStream(db *client.Client) {
	// region read-from-stream
	ropts := options.ReadStreamEventsOptionsDefault().
		Position(stream_position.Start()).
		Forwards()

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", &ropts, 100)

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
	ropts := options.ReadStreamEventsOptionsDefault().
		Position(stream_position.Revision(10))

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", &ropts, 20)

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
	ropts := options.ReadStreamEventsOptionsDefault().
		Position(stream_position.Revision(10))

	stream, err := db.ReadStreamEvents(context.Background(), "some-stream", &ropts, 100)

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

func ReadSreamBackwards(db *client.Client) {
	// region reading-backwards
	ropts := options.ReadStreamEventsOptionsDefault().
		Position(stream_position.End()).
		Backwards()

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

		fmt.Printf("Event> %v", event)
	}
	// endregion reading-backwards
}

func ReadFromAllStream(db *client.Client) {
	// region read-from-all-stream
	ropts := options.ReadAllEventsOptionsDefault().
		Position(stream_position.Start()).
		Forwards()

	stream, err := db.ReadAllEvents(context.Background(), &ropts, 100)

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
	ropts := options.ReadAllEventsOptionsDefault()

	stream, err := db.ReadAllEvents(context.Background(), &ropts, 100)

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
	ropts := options.ReadAllEventsOptionsDefault().
		Position(stream_position.End()).
		Backwards()

	stream, err := db.ReadAllEvents(context.Background(), &ropts, 100)

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
	ropts := options.ReadAllEventsOptionsDefault().ResolveLinks()
	stream, err := db.ReadAllEvents(context.Background(), &ropts, 100)
	// endregion read-from-all-stream-resolving-link-tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

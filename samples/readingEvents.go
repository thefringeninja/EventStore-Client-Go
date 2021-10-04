package samples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/types"
)

func ReadFromStream(db *client.Client) {
	// region read-from-stream
	stream, err := db.ReadStream(context.Background(), "some-stream", client.ReadStreamOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
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
	ropts := client.ReadStreamOptions{
		From: types.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 20)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-position
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadStreamOverridingUserCredentials(db *client.Client) {
	// region overriding-user-credentials
	options := client.ReadStreamOptions{
		From: types.Start{},
		Authenticated: &types.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	}
	stream, err := db.ReadStream(context.Background(), "some-stream", options, 100)
	// endregion overriding-user-credentials

	if err != nil {
		panic(err)
	}

	stream.Close()
}

func ReadFromStreamPositionCheck(db *client.Client) {
	// region checking-for-stream-presence
	ropts := client.ReadStreamOptions{
		From: types.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, client.ErrStreamNotFound) {
			fmt.Print("Stream not found")
		}

		if errors.Is(err, io.EOF) {
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
	ropts := client.ReadStreamOptions{
		Direction: types.Backwards,
		From:      types.End{},
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 10)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
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
	stream, err := db.ReadAll(context.Background(), client.ReadAllOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream
	// region read-from-all-stream-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
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
	stream, err := db.ReadAll(context.Background(), client.ReadAllOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)

		if strings.HasPrefix(event.OriginalEvent().EventType, "$") {
			continue
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion ignore-system-events
}

func ReadFromAllBackwards(db *client.Client) {
	// region read-from-all-stream-backwards
	ropts := client.ReadAllOptions{
		Direction: types.Backwards,
		From:      types.End{},
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream-backwards
	// region read-from-all-stream-backwards-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
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
	// region read-from-all-stream-resolving-link-Tos
	ropts := client.ReadAllOptions{
		ResolveLinkTos: true,
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-from-all-stream-resolving-link-Tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

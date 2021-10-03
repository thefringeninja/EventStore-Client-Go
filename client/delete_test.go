package client_test

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanDeleteStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	opts := client.DeleteStreamOptions{
		ExpectedRevision: types.Revision(1_999),
	}

	deleteResult, err := db.DeleteStream(context.Background(), "dataset20M-1800", opts)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)
}

func TestCanTombstoneStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	deleteResult, err := db.TombstoneStream(context.Background(), "dataset20M-1800", client.TombstoneStreamOptions{
		ExpectedRevision: types.Revision(1_999),
	})

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)

	_, err = db.AppendToStream(context.Background(), "dataset20M-1800", client.AppendToStreamOptions{}, createTestEvent())
	require.Error(t, err)
}

func TestDetectStreamDeleted(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	event := createTestEvent()

	_, err := db.AppendToStream(context.Background(), "foobar", client.AppendToStreamOptions{}, event)
	require.Nil(t, err)

	_, err = db.TombstoneStream(context.Background(), "foobar", client.TombstoneStreamOptions{})
	require.Nil(t, err)

	_, err = db.ReadStreamEvents(context.Background(), "foobar", client.ReadStreamEventsOptions{}, 1)
	var streamDeletedError *client.StreamDeletedError

	require.True(t, errors.As(err, &streamDeletedError))
}

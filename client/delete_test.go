package client_test

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/client"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanDeleteStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	opts := client.DeleteStreamOptions{}
	opts.SetExpectRevision(1_999)

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

	opts := client.TombstoneStreamOptions{}
	opts.SetExpectRevision(1_999)

	deleteResult, err := db.TombstoneStream(context.Background(), "dataset20M-1800", opts)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)

	_, err = db.AppendToStream(context.Background(), "dataset20M-1800", client.AppendToStreamOptions{}, createTestEvent())
	require.Error(t, err)
}

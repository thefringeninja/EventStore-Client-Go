package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanDeleteStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	opts := options.DeleteStreamOptions{}
	opts.SetDefaults()
	opts.SetExpectRevision(1_999)

	deleteResult, err := client.DeleteStream(context.Background(), "dataset20M-1800", opts)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)
}

func TestCanTombstoneStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	opts := options.TombstoneStreamOptions{}
	opts.SetDefaults()
	opts.SetExpectRevision(1_999)

	deleteResult, err := client.TombstoneStream(context.Background(), "dataset20M-1800", opts)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)

	opts2 := options.AppendToStreamOptions{}
	opts2.SetDefaults()

	_, err = client.AppendToStream(context.Background(), "dataset20M-1800", opts2, createTestEvent())
	require.Error(t, err)
}

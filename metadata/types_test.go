package metadata_test

import (
	"testing"
	"time"
	"encoding/json"

	"github.com/stretchr/testify/assert"
	"github.com/EventStore/EventStore-Client-Go/metadata"
)

func TestConsistentMetadataSerializationStreamAcl(t *testing.T) {
	acl := metadata.AclDefault().
		AddReadRoles("admin").
		AddWriteRoles("admin").
		AddDeleteRoles("admin").
		AddMetaReadRoles("admin").
		AddMetaWriteRoles("admin")

	expected := metadata.StreamMetadataDefault().
		MaxAge(2 * time.Second).
		CacheControl(15 * time.Second).
		TruncateBefore(1).
		MaxCount(12).
		Acl(acl).
		AddCustomProperty("foo", "bar")

	props, err := expected.ToMap()

	assert.NoError(t, err, "failed to generate a map")

	bytes, err := json.Marshal(props)

	assert.NoError(t, err, "failed to serialize in JSON")

	var outProps map[string]interface{}

	err = json.Unmarshal(bytes, &outProps)

	assert.NoError(t, err, "failed to deserializing props")

	meta, err := metadata.StreamMetadataFromMap(outProps)

	assert.NoError(t, err, "failed to parse metadata from props")

	assert.Equal(t, expected, meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationUserStreamAcl(t *testing.T) {
	expected := metadata.StreamMetadataDefault().
		MaxAge(2 * time.Second).
		CacheControl(15 * time.Second).
		TruncateBefore(1).
		MaxCount(12).
		Acl(metadata.UserStreamAcl).
		AddCustomProperty("foo", "bar")

	props, err := expected.ToMap()

	assert.NoError(t, err, "failed to generate a map")

	bytes, err := json.Marshal(props)

	assert.NoError(t, err, "failed to serialize in JSON")

	var outProps map[string]interface{}

	err = json.Unmarshal(bytes, &outProps)

	assert.NoError(t, err, "failed to deserializing props")

	meta, err := metadata.StreamMetadataFromMap(outProps)

	assert.NoError(t, err, "failed to parse metadata from props")

	assert.Equal(t, expected, meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationSystemStreamAcl(t *testing.T) {
	expected := metadata.StreamMetadataDefault().
		MaxAge(2 * time.Second).
		CacheControl(15 * time.Second).
		TruncateBefore(1).
		MaxCount(12).
		Acl(metadata.SystemStreamAcl).
		AddCustomProperty("foo", "bar")

	props, err := expected.ToMap()

	assert.NoError(t, err, "failed to generate a map")

	bytes, err := json.Marshal(props)

	assert.NoError(t, err, "failed to serialize in JSON")

	var outProps map[string]interface{}

	err = json.Unmarshal(bytes, &outProps)

	assert.NoError(t, err, "failed to deserializing props")

	meta, err := metadata.StreamMetadataFromMap(outProps)

	assert.NoError(t, err, "failed to parse metadata from props")

	assert.Equal(t, expected, meta, "consistency serialization failure")
}

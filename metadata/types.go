package metadata

import (
	"fmt"
	"strings"
	"time"
)

const (
	UserStreamAcl   = "$userStreamAcl"
	SystemStreamAcl = "$systemStreamAcl"
)

type Acl struct {
	readRoles      []string
	writeRoles     []string
	deleteRoles    []string
	metaReadRoles  []string
	metaWriteRoles []string
}

func AclDefault() Acl {
	return Acl{
		readRoles:      []string{},
		writeRoles:     []string{},
		deleteRoles:    []string{},
		metaReadRoles:  []string{},
		metaWriteRoles: []string{},
	}
}

func (acl Acl) AddReadRoles(roles ...string) Acl {
	acl.readRoles = append(acl.readRoles, roles...)
	return acl
}

func (acl Acl) AddWriteRoles(roles ...string) Acl {
	acl.writeRoles = append(acl.writeRoles, roles...)
	return acl
}

func (acl Acl) AddDeleteRoles(roles ...string) Acl {
	acl.deleteRoles = append(acl.deleteRoles, roles...)
	return acl
}

func (acl Acl) AddMetaWriteRoles(roles ...string) Acl {
	acl.metaWriteRoles = append(acl.metaWriteRoles, roles...)
	return acl
}

func (acl Acl) GetReadRoles() []string {
	return acl.readRoles
}

func (acl Acl) GetWriteRoles() []string {
	return acl.writeRoles
}

func (acl Acl) GetDeleteRoles() []string {
	return acl.deleteRoles
}

func (acl Acl) GetMetaReadRoles() []string {
	return acl.metaReadRoles
}

func (acl Acl) GetMetaWriteRoles() []string {
	return acl.metaWriteRoles
}

func (acl Acl) AddMetaReadRoles(roles ...string) Acl {
	acl.metaReadRoles = append(acl.metaReadRoles, roles...)
	return acl
}

type StreamMetadata struct {
	MaxCountValue       *uint64
	MaxAgeValue         *time.Duration
	TruncateBeforeValue *uint64
	CacheControlValue   *time.Duration
	AclValue            *interface{}
	PropertiesValue     map[string]interface{}
}

func StreamMetadataDefault() StreamMetadata {
	return StreamMetadata{
		PropertiesValue: make(map[string]interface{}),
	}
}

func (meta StreamMetadata) MaxCount(value uint64) StreamMetadata {
	meta.MaxCountValue = &value
	return meta
}

func (meta StreamMetadata) MaxAge(value time.Duration) StreamMetadata {
	meta.MaxAgeValue = &value
	return meta
}

func (meta StreamMetadata) TruncateBefore(value uint64) StreamMetadata {
	meta.TruncateBeforeValue = &value
	return meta
}

func (meta StreamMetadata) CacheControl(value time.Duration) StreamMetadata {
	meta.CacheControlValue = &value
	return meta
}

func (meta StreamMetadata) Acl(value interface{}) StreamMetadata {
	meta.AclValue = &value
	return meta
}

func (meta StreamMetadata) AddCustomProperty(name string, value interface{}) StreamMetadata {
	meta.PropertiesValue[name] = value
	return meta
}

func flattenRoles(props map[string]interface{}, key string, roles []string) {
	len_r := len(roles)

	if len_r == 0 {
		return
	}

	if len_r == 1 {
		props[key] = roles[0]
		return
	}

	props[key] = roles
}

func (acl Acl) ToMap() map[string]interface{} {
	props := make(map[string]interface{})

	flattenRoles(props, "$r", acl.readRoles)
	flattenRoles(props, "$w", acl.writeRoles)
	flattenRoles(props, "$d", acl.deleteRoles)
	flattenRoles(props, "$mr", acl.metaReadRoles)
	flattenRoles(props, "$mw", acl.metaWriteRoles)

	return props
}

func (meta StreamMetadata) ToMap() (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if meta.MaxCountValue != nil {
		props["$maxCount"] = *meta.MaxCountValue
	}

	if meta.MaxAgeValue != nil {
		props["$maxAge"] = *meta.MaxAgeValue
	}

	if meta.TruncateBeforeValue != nil {
		props["$tb"] = *meta.TruncateBeforeValue
	}

	if meta.CacheControlValue != nil {
		props["$cacheControl"] = (*meta.CacheControlValue).Milliseconds()
	}

	if meta.AclValue != nil {
		switch value := (*meta.AclValue).(type) {
		case string:
			if value != UserStreamAcl && value != SystemStreamAcl {
				return nil, fmt.Errorf("unsupported acl string value: %s", value)
			}

			props["$acl"] = value
		case Acl:
			props["$acl"] = value.ToMap()
		}
	}

	for key, value := range meta.PropertiesValue {
		// We ignore properties that can conflict with internal metatadata names.
		if strings.HasPrefix(key, "$") {
			continue
		}

		props[key] = value
	}

	return props, nil
}

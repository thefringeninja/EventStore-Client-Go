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
	maxCount         *uint64
	maxAge           *time.Duration
	truncateBefore   *uint64
	cacheControl     *time.Duration
	acl              interface{}
	customProperties map[string]interface{}
}

func StreamMetadataDefault() StreamMetadata {
	return StreamMetadata{
		customProperties: make(map[string]interface{}),
	}
}

func (meta StreamMetadata) MaxCount(value uint64) StreamMetadata {
	meta.maxCount = &value
	return meta
}

func (meta StreamMetadata) MaxAge(value time.Duration) StreamMetadata {
	meta.maxAge = &value
	return meta
}

func (meta StreamMetadata) TruncateBefore(value uint64) StreamMetadata {
	meta.truncateBefore = &value
	return meta
}

func (meta StreamMetadata) CacheControl(value time.Duration) StreamMetadata {
	meta.cacheControl = &value
	return meta
}

func (meta StreamMetadata) Acl(value interface{}) StreamMetadata {
	meta.acl = value
	return meta
}

func (meta StreamMetadata) AddCustomProperty(name string, value interface{}) StreamMetadata {
	meta.customProperties[name] = value
	return meta
}

func (meta StreamMetadata) GetMaxCount() *uint64 {
	return meta.maxCount
}

func (meta StreamMetadata) GetMaxAge() *time.Duration {
	return meta.maxAge
}

func (meta StreamMetadata) GetTruncateBefore() *uint64 {
	return meta.truncateBefore
}

func (meta StreamMetadata) GetCacheControl() *time.Duration {
	return meta.cacheControl
}

func (meta StreamMetadata) GetAcl() interface{} {
	return meta.acl
}

func (meta StreamMetadata) GetStreamAcl() *Acl {
	if acl, ok := meta.acl.(Acl); ok {
		return &acl
	}

	return nil
}

func (meta StreamMetadata) IsUserStreamAcl() bool {
	if acl, ok := meta.acl.(string); ok {
		return acl == UserStreamAcl
	}

	return false
}

func (meta StreamMetadata) IsSystemStreamAcl() bool {
	if acl, ok := meta.acl.(string); ok {
		return acl == SystemStreamAcl
	}

	return false
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

func collectRoles(value interface{}) ([]string, error) {

	switch roleValue := value.(type) {
	case string:
		return []string{roleValue}, nil
	case []string:
		return roleValue, nil
	default:
		return nil, fmt.Errorf("invalid acl role value: %v", roleValue)
	}
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

func AclFromMap(props map[string]interface{}) (*Acl, error) {
	acl := AclDefault()

	for key, value := range props {
		switch key {
		case "$r":
			roles, err := collectRoles(value)

			if err != nil {
				return nil, err
			}

			acl.readRoles = roles
		case "$w":
			roles, err := collectRoles(value)

			if err != nil {
				return nil, err
			}

			acl.writeRoles = roles
		case "$d":
			roles, err := collectRoles(value)

			if err != nil {
				return nil, err
			}

			acl.deleteRoles = roles
		case "$mr":
			roles, err := collectRoles(value)

			if err != nil {
				return nil, err
			}

			acl.metaReadRoles = roles
		case "$mw":
			roles, err := collectRoles(value)

			if err != nil {
				return nil, err
			}

			acl.metaWriteRoles = roles
		default:
			return nil, fmt.Errorf("unknown acl key: %v", key)
		}
	}

	return &acl, nil
}

func (meta StreamMetadata) ToMap() (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if meta.maxCount != nil {
		props["$maxCount"] = *meta.maxCount
	}

	if meta.maxAge != nil {
		props["$maxAge"] = *meta.maxAge
	}

	if meta.truncateBefore != nil {
		props["$tb"] = *meta.truncateBefore
	}

	if meta.cacheControl != nil {
		props["$cacheControl"] = (*meta.cacheControl).Milliseconds()
	}

	if meta.acl != nil {
		switch value := meta.acl.(type) {
		case string:
			if value != UserStreamAcl && value != SystemStreamAcl {
				return nil, fmt.Errorf("unsupported acl string value: %s", value)
			}

			props["$acl"] = value
		case Acl:
			props["$acl"] = value.ToMap()
		}
	}

	for key, value := range meta.customProperties {
		// We ignore properties that can conflict with internal metatadata names.
		if strings.HasPrefix(key, "$") {
			continue
		}

		props[key] = value
	}

	return props, nil
}

func StreamMetadataFromMap(props map[string]interface{}) (*StreamMetadata, error) {
	meta := StreamMetadataDefault()

	for key, value := range props {
		switch key {
		case "$maxCount":
			if i, ok := value.(uint64); ok {
				meta.maxCount = &i
				continue
			}

			return nil, fmt.Errorf("invalid $maxCount value: %v", value)
		case "$maxAge":
			if ms, ok := value.(uint64); ok {
				age := time.Duration(ms) * time.Millisecond
				meta.maxAge = &age
				continue
			}

			return nil, fmt.Errorf("invalid $maxAge value: %v", value)
		case "$tb":
			if i, ok := value.(uint64); ok {
				meta.truncateBefore = &i
				continue
			}

			return nil, fmt.Errorf("invalid $tb value: %v", value)
		case "$cacheControl":
			if ms, ok := value.(uint64); ok {
				age := time.Duration(ms) * time.Millisecond
				meta.cacheControl = &age
				continue
			}

			return nil, fmt.Errorf("invalid $cacheControl value: %v", value)
		case "$acl":
			switch aclValue := value.(type) {
			case string:
				if aclValue != UserStreamAcl && aclValue != SystemStreamAcl {
					return nil, fmt.Errorf("invalid string $acl value: %v", aclValue)
				}

				meta.acl = &value
			case map[string]interface{}:
				acl, err := AclFromMap(aclValue)

				if err != nil {
					return nil, err
				}

				meta.acl = acl
			default:
				return nil, fmt.Errorf("invalid $acl object value: %v", value)
			}

		default:
			meta.customProperties[key] = value
		}
	}

	return &meta, nil
}

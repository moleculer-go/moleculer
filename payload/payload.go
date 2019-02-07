package payload

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer"
)

// RawPayload is a payload implementation for raw types.
type RawPayload struct {
	source interface{}
}

func (rawPayload *RawPayload) Exists() bool {
	return rawPayload.source != nil
}

func (rawPayload *RawPayload) Int() int {
	return rawPayload.source.(int)
}

func (rawPayload *RawPayload) Int64() int64 {
	return rawPayload.source.(int64)
}

func (rawPayload *RawPayload) Bool() bool {
	return rawPayload.source.(bool)
}

func (rawPayload *RawPayload) Uint() uint64 {
	return rawPayload.source.(uint64)
}

func (rawPayload *RawPayload) Time() time.Time {
	return rawPayload.source.(time.Time)
}

func (rawPayload *RawPayload) Array() []moleculer.Payload {
	if transformer := getArrayTransformer(&rawPayload.source); transformer != nil {
		source := transformer.interfaceArray(&rawPayload.source)
		array := make([]moleculer.Payload, len(source))
		for index, item := range source {
			array[index] = &RawPayload{&item}
		}
		return array
	}
	return nil
}

func (rawPayload *RawPayload) ForEach(iterator func(key interface{}, value moleculer.Payload) bool) {
	if rawPayload.IsArray() {
		list := rawPayload.Array()
		for index, value := range list {
			if !iterator(index, value) {
				break
			}
		}
	} else if rawPayload.IsMap() {
		mapValue := rawPayload.Map()
		for key, value := range mapValue {
			if !iterator(key, value) {
				break
			}
		}
	} else {
		iterator(nil, rawPayload)
	}
}

func (rawPayload *RawPayload) IsArray() bool {
	transformer := getArrayTransformer(&rawPayload.source)
	return transformer != nil
}

func (rawPayload *RawPayload) IsMap() bool {
	transformer := getMapTransformer(&rawPayload.source)
	return transformer != nil
}

func (rawPayload *RawPayload) Float() float64 {
	return rawPayload.source.(float64)
}

func (rawPayload *RawPayload) Float32() float32 {
	return rawPayload.source.(float32)
}

func (rawPayload *RawPayload) String() string {
	return fmt.Sprintf("%v", rawPayload.source)
}

func (rawPayload *RawPayload) Map() map[string]moleculer.Payload {
	if transformer := getMapTransformer(&rawPayload.source); transformer != nil {
		source := transformer.asMap(&rawPayload.source)
		newMap := make(map[string]moleculer.Payload)
		for key, item := range source {
			newPayload := RawPayload{item}
			newMap[key] = &newPayload
		}
		return newMap
	}
	return nil
}

func (rawPayload *RawPayload) RawMap() map[string]interface{} {
	if transformer := getMapTransformer(&rawPayload.source); transformer != nil {
		return transformer.asMap(&rawPayload.source)
	}
	return nil
}

// mapGet try to get the value at the path assuming the source is a map
func (rawPayload *RawPayload) mapGet(path string) (interface{}, bool) {
	if transformer := getMapTransformer(&rawPayload.source); transformer != nil {
		return transformer.get(path, &rawPayload.source)
	}
	return nil, false
}

func (rawPayload *RawPayload) Get(path string) moleculer.Payload {
	if value, ok := rawPayload.mapGet(path); ok {
		return Create(value)
	}
	return Create(nil)
}

func (rawPayload *RawPayload) Value() interface{} {
	return rawPayload.source
}

func Create(source interface{}) moleculer.Payload {
	return &RawPayload{source}
}

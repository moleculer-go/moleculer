package serializer

import (
	"time"

	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JSONSerializer struct {
	logger *log.Entry
}

type ResultWrapper struct {
	result *gjson.Result
	logger *log.Entry
}

func CreateJSONSerializer(logger *log.Entry) JSONSerializer {
	return JSONSerializer{logger}
}

// mapToContext make sure all value types are compatible with the context fields.
func (serializer JSONSerializer) contextMap(values map[string]interface{}) map[string]interface{} {
	values["level"] = int(values["level"].(float64))
	if values["timeout"] != nil {
		values["timeout"] = int(values["timeout"].(float64))
	}
	return values
}

func (serializer JSONSerializer) BytesToMessage(bytes *[]byte) moleculer.Payload {
	result := gjson.ParseBytes(*bytes)
	message := ResultWrapper{&result, serializer.logger}
	return message
}

func (serializer JSONSerializer) MapToMessage(mapValue *map[string]interface{}) (moleculer.Payload, error) {
	json, err := sjson.Set("{root:false}", "root", mapValue)
	if err != nil {
		serializer.logger.Error("MapToMessage() Error when parsing the map: ", mapValue, " Error: ", err)
		return nil, err
	}
	result := gjson.Get(json, "root")
	message := ResultWrapper{&result, serializer.logger}
	return message, nil
}

func (serializer JSONSerializer) MessageToContextMap(message moleculer.Payload) map[string]interface{} {
	return serializer.contextMap(message.RawMap())
}

func (wrapper ResultWrapper) Get(path string) moleculer.Payload {
	result := wrapper.result.Get(path)
	message := ResultWrapper{&result, wrapper.logger}
	return message
}

func (wrapper ResultWrapper) Exists() bool {
	return wrapper.result.Exists()
}

func (wrapper ResultWrapper) Value() interface{} {
	return wrapper.result.Value()
}

func (wrapper ResultWrapper) Int() int {
	return int(wrapper.result.Int())
}

func (wrapper ResultWrapper) Int64() int64 {
	return wrapper.result.Int()
}

func (wrapper ResultWrapper) Uint() uint64 {
	return wrapper.result.Uint()
}

func (wrapper ResultWrapper) Time() time.Time {
	return wrapper.result.Time()
}

func (wrapper ResultWrapper) Array() []moleculer.Payload {
	source := wrapper.result.Array()
	array := make([]moleculer.Payload, len(source))
	for index, item := range source {
		array[index] = &ResultWrapper{&item, wrapper.logger}
	}
	return array
}

func (wrapper ResultWrapper) IsArray() bool {
	return wrapper.result.IsArray()
}

func (wrapper ResultWrapper) IsMap() bool {
	return wrapper.result.IsObject()
}

func (wrapper ResultWrapper) ForEach(iterator func(key interface{}, value moleculer.Payload) bool) {
	wrapper.result.ForEach(func(key, value gjson.Result) bool {
		return iterator(key.Value(), &ResultWrapper{&value, wrapper.logger})
	})
}

func (wrapper ResultWrapper) Bool() bool {
	return wrapper.result.Bool()
}

func (wrapper ResultWrapper) Float() float64 {
	return wrapper.result.Float()
}

func (wrapper ResultWrapper) Float32() float32 {
	return float32(wrapper.result.Float())
}

func (wrapper ResultWrapper) String() string {
	return wrapper.result.String()
}

func (wrapper ResultWrapper) RawMap() map[string]interface{} {
	mapValue, ok := wrapper.result.Value().(map[string]interface{})
	if !ok {
		wrapper.logger.Warn("RawMap() Could not convert result.Value() into a map[string]interface{} - result: ", wrapper.result)
		return nil
	}
	return mapValue
}

func (wrapper ResultWrapper) Map() map[string]moleculer.Payload {
	if source := wrapper.result.Map(); source != nil {
		newMap := make(map[string]moleculer.Payload)
		for key, item := range source {
			newMap[key] = &ResultWrapper{&item, wrapper.logger}
		}
		return newMap
	}
	return nil
}

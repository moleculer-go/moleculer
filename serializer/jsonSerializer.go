package serializer

import (
	. "github.com/moleculer-go/moleculer/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JSONSerializer struct {
}

type ResultWrapper struct {
	result *gjson.Result
}

type brokerInfoFunction func() *BrokerInfo

func CreateJSONSerializer() JSONSerializer {
	return JSONSerializer{}
}

// mapToContext make sure all value types are compatible with the context fields.
func (serializer JSONSerializer) contextMap(values map[string]interface{}) map[string]interface{} {
	values["level"] = int(values["level"].(float64))
	if values["timeout"] != nil {
		values["timeout"] = int(values["timeout"].(float64))
	}
	return values
}

func (serializer JSONSerializer) BytesToMessage(bytes *[]byte) TransitMessage {
	result := gjson.ParseBytes(*bytes)
	message := ResultWrapper{&result}
	return message
}

func (serializer JSONSerializer) MapToMessage(mapValue *map[string]interface{}) TransitMessage {
	json, err := sjson.Set("{root:false}", "root", mapValue)
	if err != nil {
		panic(err)
	}
	result := gjson.Get(json, "root")
	message := ResultWrapper{&result}
	return message
}

func (serializer JSONSerializer) MessageToContextMap(message *TransitMessage) map[string]interface{} {
	return serializer.contextMap((*message).AsMap())
}

func (wrapper ResultWrapper) Get(path string) TransitMessage {
	result := wrapper.result.Get(path)
	message := ResultWrapper{&result}
	return message
}

func (wrapper ResultWrapper) Exists() bool {
	return wrapper.result.Exists()
}

func (wrapper ResultWrapper) Value() interface{} {
	return wrapper.result.Value()
}

func (wrapper ResultWrapper) Int() int64 {
	return wrapper.result.Int()
}

func (wrapper ResultWrapper) Float() float64 {
	return wrapper.result.Float()
}

func (wrapper ResultWrapper) String() string {
	return wrapper.result.String()
}

func (wrapper ResultWrapper) AsMap() map[string]interface{} {
	mapValue, ok := wrapper.result.Value().(map[string]interface{})
	if !ok {
		//TODO: do what in this case ?
		return nil
	}
	return mapValue
}

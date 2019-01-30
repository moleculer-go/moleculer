package serializer

import (
	. "github.com/moleculer-go/moleculer/common"
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

type brokerInfoFunction func() *BrokerInfo

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

func (serializer JSONSerializer) BytesToMessage(bytes *[]byte) TransitMessage {
	result := gjson.ParseBytes(*bytes)
	message := ResultWrapper{&result, serializer.logger}
	return message
}

func (serializer JSONSerializer) MapToMessage(mapValue *map[string]interface{}) (TransitMessage, error) {
	json, err := sjson.Set("{root:false}", "root", mapValue)
	if err != nil {
		serializer.logger.Error("MapToMessage() Error when parsing the map: ", mapValue, " Error: ", err)
		return nil, err
	}
	result := gjson.Get(json, "root")
	message := ResultWrapper{&result, serializer.logger}
	return message, nil
}

func (serializer JSONSerializer) MessageToContextMap(message *TransitMessage) map[string]interface{} {
	return serializer.contextMap((*message).AsMap())
}

func (wrapper ResultWrapper) Get(path string) TransitMessage {
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
		wrapper.logger.Warn("AsMap() Could not convert result.Value() into a map[string]interface{} - result: ", wrapper.result)
		return nil
	}
	return mapValue
}

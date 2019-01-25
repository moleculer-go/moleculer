package serializer

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/context"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JSONSerializer struct {
}

type ResultWrapper struct {
	result *gjson.Result
}

func CreateJSONSerializer() JSONSerializer {
	return JSONSerializer{}
}

func mapToContext(contextMap map[string]interface{}) Context {
	id := contextMap["id"].(string)
	actionName := contextMap["action"].(string)
	params := contextMap["params"]
	level := int(contextMap["level"].(float64))
	sendMetrics := contextMap["metrics"].(bool)

	var timeout int
	var meta map[string]interface{}
	var parentID, requestID string
	if contextMap["timeout"] != nil {
		timeout = int(contextMap["timeout"].(float64))
	}
	if contextMap["meta"] != nil {
		meta = contextMap["meta"].(map[string]interface{})
	}
	if contextMap["parentID"] != nil {
		parentID = contextMap["parentID"].(string)
	}
	if contextMap["requestID"] != nil {
		requestID = contextMap["requestID"].(string)
	}

	context := CreateContext(id, actionName, params, meta, level, sendMetrics, timeout, parentID, requestID)

	return context
}

func (serializer JSONSerializer) BytesToMessage(bytes []byte) TransitMessage {
	result := gjson.ParseBytes(bytes)
	message := ResultWrapper{&result}
	return message
}

func (serializer JSONSerializer) MessageToContext(message TransitMessage) Context {
	contextMap := message.AsMap()
	context := mapToContext(contextMap)
	return context
}

func (serializer JSONSerializer) ContextToMessage(context Context) TransitMessage {
	contextMap := context.AsMap()
	json, err := sjson.Set("{root:false}", "root", contextMap)
	if err != nil {
		panic(err)
	}
	result := gjson.Get(json, "root")
	message := ResultWrapper{&result}
	return message
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

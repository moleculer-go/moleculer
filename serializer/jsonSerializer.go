package serializer

import (
	"errors"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JSONSerializer struct {
	logger *log.Entry
}

type JSONPayload struct {
	result gjson.Result
	logger *log.Entry
}

func CreateJSONSerializer(logger *log.Entry) JSONSerializer {
	return JSONSerializer{logger}
}

// mapToContext make sure all value types are compatible with the context fields.
func (serializer JSONSerializer) contextMap(values map[string]interface{}) map[string]interface{} {
	if values["level"] != nil {
		values["level"] = int(values["level"].(float64))
	}
	if values["timeout"] != nil {
		values["timeout"] = int(values["timeout"].(float64))
	}
	return values
}

func (serializer JSONSerializer) BytesToPayload(bytes *[]byte) moleculer.Payload {
	result := gjson.ParseBytes(*bytes)
	payload := JSONPayload{result, serializer.logger}
	return payload
}

var invalidTypes = []string{"func()"}

func validTypeForSerializing(vType string) bool {
	for _, item := range invalidTypes {
		if item == vType {
			return false
		}
	}
	return true
}

// cleanUpForSerialization clean the map from invalid values for serialization, example: functions.
func cleanUpForSerialization(values *map[string]interface{}) *map[string]interface{} {
	result := map[string]interface{}{}
	for key, value := range *values {
		vType := payload.GetValueType(&value)
		mTransformer := payload.MapTransformer(&value)
		if mTransformer != nil {
			value := mTransformer.AsMap(&value)
			temp := cleanUpForSerialization(&value)
			result[key] = temp
			continue
		}
		aTransformer := payload.ArrayTransformer(&value)
		if aTransformer != nil {
			iArray := aTransformer.InterfaceArray(&value)
			valueA := []interface{}{}
			for _, item := range iArray {
				mTransformer := payload.MapTransformer(&item)
				if mTransformer != nil {
					mValue := mTransformer.AsMap(&item)
					valueA = append(valueA, cleanUpForSerialization(&mValue))
					continue
				}
				if validTypeForSerializing(payload.GetValueType(&item)) {
					valueA = append(valueA, item)
				}
			}
			result[key] = valueA
			continue
		}
		if validTypeForSerializing(vType) {
			result[key] = value
		}
	}
	return &result
}

func (serializer JSONSerializer) MapToPayload(mapValue *map[string]interface{}) (moleculer.Payload, error) {
	mapValue = cleanUpForSerialization(mapValue)
	json, err := sjson.Set("{root:false}", "root", mapValue)
	if err != nil {
		serializer.logger.Error("MapToPayload() Error when parsing the map: ", mapValue, " Error: ", err)
		return nil, err
	}
	result := gjson.Get(json, "root")
	payload := JSONPayload{result, serializer.logger}
	return payload, nil
}

func (serializer JSONSerializer) PayloadToContextMap(message moleculer.Payload) map[string]interface{} {
	return serializer.contextMap(message.RawMap())
}

func (payload JSONPayload) Get(path string) moleculer.Payload {
	result := payload.result.Get(path)
	message := JSONPayload{result, payload.logger}
	return message
}

func (payload JSONPayload) Exists() bool {
	return payload.result.Exists()
}

func (payload JSONPayload) Value() interface{} {
	return payload.result.Value()
}

func (payload JSONPayload) Int() int {
	return int(payload.result.Int())
}

func (payload JSONPayload) Int64() int64 {
	return payload.result.Int()
}

func (payload JSONPayload) Uint() uint64 {
	return payload.result.Uint()
}

func (payload JSONPayload) Time() time.Time {
	return payload.result.Time()
}

func (payload JSONPayload) StringArray() []string {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]string, len(source))
		for index, item := range source {
			array[index] = item.String()
		}
		return array
	}
	return nil
}

func resultToArray(results []gjson.Result, allTheWay bool) []interface{} {
	list := make([]interface{}, len(results))
	for index, item := range results {
		var value interface{}
		if item.IsObject() {
			value = resultToMap(item, allTheWay)
		} else if item.IsArray() {
			value = resultToArray(item.Array(), allTheWay)
		} else {
			value = item.Value()
		}
		list[index] = value
	}
	return list
}

func resultToMap(result gjson.Result, allTheWay bool) map[string]interface{} {
	mvalues := make(map[string]interface{})
	result.ForEach(func(key, value gjson.Result) bool {
		if allTheWay && value.IsObject() {
			mvalues[key.String()] = resultToMap(value, allTheWay)
		} else if allTheWay && value.IsArray() {
			mvalues[key.String()] = resultToArray(value.Array(), allTheWay)
		} else {
			mvalues[key.String()] = value.Value()
		}
		return true
	})
	return mvalues
}

func (payload JSONPayload) MapArray() []map[string]interface{} {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]map[string]interface{}, len(source))
		for index, item := range source {
			array[index] = resultToMap(item, true)
		}
		return array
	}
	return nil
}

func (payload JSONPayload) ValueArray() []interface{} {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]interface{}, len(source))
		for index, item := range source {
			array[index] = item.Value()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) IntArray() []int {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]int, len(source))
		for index, item := range source {
			array[index] = int(item.Int())
		}
		return array
	}
	return nil
}

func (payload JSONPayload) Int64Array() []int64 {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]int64, len(source))
		for index, item := range source {
			array[index] = item.Int()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) UintArray() []uint64 {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]uint64, len(source))
		for index, item := range source {
			array[index] = item.Uint()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) Float32Array() []float32 {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]float32, len(source))
		for index, item := range source {
			array[index] = float32(item.Float())
		}
		return array
	}
	return nil
}

func (payload JSONPayload) FloatArray() []float64 {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]float64, len(source))
		for index, item := range source {
			array[index] = item.Float()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) BoolArray() []bool {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]bool, len(source))
		for index, item := range source {
			array[index] = item.Bool()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) TimeArray() []time.Time {
	if source := payload.result.Array(); source != nil {
		array := make([]time.Time, len(source))
		for index, item := range source {
			array[index] = item.Time()
		}
		return array
	}
	return nil
}

func (payload JSONPayload) Array() []moleculer.Payload {
	if payload.IsArray() {
		source := payload.result.Array()
		array := make([]moleculer.Payload, len(source))
		for index, item := range source {
			array[index] = JSONPayload{item, payload.logger}
		}
		return array
	}
	return nil
}

func (payload JSONPayload) IsArray() bool {
	return payload.result.IsArray()
}

func (payload JSONPayload) IsMap() bool {
	return payload.result.IsObject()
}

func (payload JSONPayload) ForEach(iterator func(key interface{}, value moleculer.Payload) bool) {
	payload.result.ForEach(func(key, value gjson.Result) bool {
		return iterator(key.Value(), &JSONPayload{value, payload.logger})
	})
}

func (payload JSONPayload) Bool() bool {
	return payload.result.Bool()
}

func (payload JSONPayload) Float() float64 {
	return payload.result.Float()
}

func (payload JSONPayload) Float32() float32 {
	return float32(payload.result.Float())
}

func (payload JSONPayload) IsError() bool {
	return payload.IsMap() && payload.Get("error").Exists()
}

func (payload JSONPayload) Error() error {
	if payload.IsError() {
		return errors.New(payload.Get("error").String())
	}
	return nil
}

func (payload JSONPayload) String() string {
	return payload.result.String()
}

func (payload JSONPayload) RawMap() map[string]interface{} {
	mapValue, ok := payload.result.Value().(map[string]interface{})
	if !ok {
		payload.logger.Warn("RawMap() Could not convert result.Value() into a map[string]interface{} - result: ", payload.result)
		return nil
	}
	return mapValue
}

func (payload JSONPayload) Map() map[string]moleculer.Payload {
	if source := payload.result.Map(); source != nil {
		newMap := make(map[string]moleculer.Payload)
		for key, item := range source {
			newMap[key] = &JSONPayload{item, payload.logger}
		}
		return newMap
	}
	return nil
}

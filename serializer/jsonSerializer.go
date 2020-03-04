package serializer

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.mongodb.org/mongo-driver/bson"
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

// ReaderToPayload transform an io.Reader into a Payload assusming the contes is a valid json :)
func (serializer JSONSerializer) ReaderToPayload(r io.Reader) moleculer.Payload {
	buf := bytes.Buffer{}
	buf.ReadFrom(r)
	json := buf.String()
	if !gjson.Valid(json) {
		return payload.New(errors.New("invalid json"))
	}
	result := gjson.Parse(json)
	payload := JSONPayload{result, serializer.logger}
	return payload
}

func (serializer JSONSerializer) PayloadToBytes(payload moleculer.Payload) []byte {
	return []byte(serializer.PayloadToString(payload))
}

func (serializer JSONSerializer) PayloadToString(payload moleculer.Payload) string {
	var err error
	jp, isJson := payload.(JSONPayload)
	if !isJson {
		if payload.IsArray() {
			jp, err = serializer.arrayToJsonPayload(payload.ValueArray())
			if err != nil {
				panic(err)
			}
			return jp.result.String()
		}
		rawMap := payload.RawMap()
		if payload.IsError() {
			rawMap = map[string]interface{}{"error": payload.Error().Error()}
		}
		if rawMap != nil && len(rawMap) > 0 {
			jp, err = serializer.mapToJsonPayload(&rawMap)
			if err != nil {
				panic(err)
			}
			return jp.result.String()
		}
		json, err := sjson.Set("{root:false}", "root", payload.Value())
		if err != nil {
			panic(err)
		}
		jp = JSONPayload{gjson.Get(json, "root"), serializer.logger}
		return jp.result.String()
	}
	return jp.result.String()
}

func (jpayload JSONPayload) Remove(fields ...string) moleculer.Payload {
	var err error
	json := jpayload.result.Raw
	for _, item := range fields {
		json, err = sjson.Delete(json, item)
		if err != nil {
			return payload.Error("Error serializng value into JSON. error: ", err.Error())
		}
	}
	return JSONPayload{gjson.Parse(json), jpayload.logger}
}

func (jpayload JSONPayload) AddItem(value interface{}) moleculer.Payload {
	if !jpayload.IsArray() {
		return payload.Error("payload.AddItem can only deal with lists/arrays.")
	}
	arr := jpayload.Array()
	arr = append(arr, payload.New(value))
	return payload.New(arr)
}

func (jpayload JSONPayload) Add(field string, value interface{}) moleculer.Payload {
	if !jpayload.IsMap() {
		return payload.Error("payload.Add can only deal with map payloads.")
	}
	var err error
	json := jpayload.result.Raw
	json, err = sjson.Set(json, field, value)
	if err != nil {
		return payload.Error("Error serializng value into JSON. error: ", err.Error())
	}
	return JSONPayload{gjson.Parse(json), jpayload.logger}
}

func (jpayload JSONPayload) AddMany(toAdd map[string]interface{}) moleculer.Payload {
	if !jpayload.IsMap() {
		return payload.Error("payload.Add can only deal with map payloads.")
	}
	var err error
	json := jpayload.result.Raw
	for key, value := range toAdd {
		json, err = sjson.Set(json, key, value)
		if err != nil {
			return payload.Error("Error serializng value into JSON. error: ", err.Error())
		}
	}
	return JSONPayload{gjson.Parse(json), jpayload.logger}
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
		btsArray, isBytesArray := value.([]byte)
		if isBytesArray {
			result[key] = string(btsArray)
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

func (serializer JSONSerializer) arrayToJsonPayload(list []interface{}) (JSONPayload, error) {
	json, err := sjson.Set("{root:false}", "root", list)
	if err != nil {
		serializer.logger.Error("arrayToJsonPayload() Error when parsing the map: ", list, " Error: ", err)
		return JSONPayload{}, err
	}
	return JSONPayload{gjson.Get(json, "root"), serializer.logger}, nil
}

func (serializer JSONSerializer) mapToJsonPayload(mapValue *map[string]interface{}) (JSONPayload, error) {
	mapValue = cleanUpForSerialization(mapValue)
	json, err := sjson.Set("{root:false}", "root", mapValue)
	if err != nil {
		serializer.logger.Error("mapToJsonPayload() Error when parsing the map: ", mapValue, " Error: ", err)
		return JSONPayload{}, err
	}
	return JSONPayload{gjson.Get(json, "root"), serializer.logger}, nil
}

func (serializer JSONSerializer) MapToPayload(mapValue *map[string]interface{}) (moleculer.Payload, error) {
	return serializer.mapToJsonPayload(mapValue)
}

func (serializer JSONSerializer) PayloadToContextMap(message moleculer.Payload) map[string]interface{} {
	return serializer.contextMap(message.RawMap())
}

func (jp JSONPayload) Get(path string, defaultValue ...interface{}) moleculer.Payload {
	result := jp.result.Get(path)
	if !result.Exists() && len(defaultValue) > 1 {
		return payload.New(defaultValue)
	} else if !result.Exists() && len(defaultValue) > 0 {
		return payload.New(defaultValue[0])
	}
	message := JSONPayload{result, jp.logger}
	return message
}

//Only return a payload containing only the field specified
func (p JSONPayload) Only(path string) moleculer.Payload {
	result := p.result.Get(path)
	if result.Exists() {
		return payload.Empty().Add(path, JSONPayload{result, p.logger})
	}
	return payload.New(nil)
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

func (jp JSONPayload) Len() int {
	if jp.IsArray() {
		return len(jp.result.Array())
	}
	return -1
}

func (jp JSONPayload) First() moleculer.Payload {
	if jp.IsArray() {
		return JSONPayload{jp.result.Array()[0], jp.logger}
	}
	return payload.New(nil)
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

func (jp JSONPayload) BsonArray() bson.A {
	if jp.IsArray() {
		ba := make(bson.A, jp.Len())
		for index, value := range jp.Array() {
			if value.IsMap() {
				ba[index] = value.Bson()
			} else if value.IsArray() {
				ba[index] = value.BsonArray()
			} else {
				ba[index] = value.Value()
			}
		}
		return ba
	}
	return nil
}

func (jp JSONPayload) Bson() bson.M {
	if jp.IsMap() {
		bm := bson.M{}
		for key, value := range jp.Map() {
			if value.IsMap() {
				bm[key] = value.Bson()
			} else if value.IsArray() {
				bm[key] = value.BsonArray()
			} else {
				bm[key] = value.Value()
			}
		}
		return bm
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

func (payload JSONPayload) ByteArray() []byte {
	return []byte(payload.result.Raw)
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

func (p JSONPayload) Sort(field string) moleculer.Payload {
	if !p.IsArray() {
		return p
	}
	ps := &payload.Sortable{field, p.Array()}
	sort.Sort(ps)
	return ps.Payload()
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

func orderedKeys(m map[string]moleculer.Payload) []string {
	keys := make([]string, len(m))
	i := 0
	for key := range m {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}

func (jp JSONPayload) StringIdented(ident string) string {
	return jp.String()
}

func (jp JSONPayload) String() string {
	if jp.IsMap() {
		ident := "  "
		m := jp.Map()

		out := "(len=" + strconv.Itoa(len(m)) + ") {\n"
		for _, key := range orderedKeys(m) {
			out = out + ident + `"` + key + `": ` + m[key].String() + "," + "\n"
		}
		if len(m) == 0 {
			out = out + "\n"
		}
		out = out + "}"
		return out
	}
	return jp.result.String()
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
		newMap := make(map[string]moleculer.Payload, len(source))
		for key, item := range source {
			newMap[key] = &JSONPayload{item, payload.logger}
		}
		return newMap
	}
	return nil
}

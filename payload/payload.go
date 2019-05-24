package payload

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/moleculer-go/moleculer"
)

// RawPayload is a payload implementation for raw types.
type RawPayload struct {
	source interface{}
}

func (p *RawPayload) Exists() bool {
	return p.source != nil
}

func (p *RawPayload) IsError() bool {
	_, isError := p.source.(error)
	return isError
}

func (p *RawPayload) Error() error {
	if p.IsError() {
		return p.source.(error)
	}
	return nil
}

func (p *RawPayload) Int() int {
	value, ok := p.source.(int)
	if !ok {
		if transformer := getNumberTransformer(&p.source); transformer != nil {
			value = transformer.toInt(&p.source)
		}
	}
	return value
}

func (p *RawPayload) Int64() int64 {
	value, ok := p.source.(int64)
	if !ok {
		if transformer := getNumberTransformer(&p.source); transformer != nil {
			value = transformer.toInt64(&p.source)
		}
	}
	return value
}

func (p *RawPayload) Bool() bool {
	value, ok := p.source.(bool)
	if !ok {
		value = strings.ToLower(fmt.Sprint(p.source)) == "true"
	}
	return value
}

func (p *RawPayload) Uint() uint64 {
	value, ok := p.source.(uint64)
	if !ok {
		if transformer := getNumberTransformer(&p.source); transformer != nil {
			value = transformer.toUint64(&p.source)
		}
	}
	return value
}

func (p *RawPayload) Time() time.Time {
	return p.source.(time.Time)
}

func (p *RawPayload) StringArray() []string {
	if source := p.Array(); source != nil {
		array := make([]string, len(source))
		for index, item := range source {
			array[index] = item.String()
		}
		return array
	}
	return nil
}

func (p *RawPayload) MapArray() []map[string]interface{} {
	if source := p.Array(); source != nil {
		array := make([]map[string]interface{}, len(source))
		for index, item := range source {
			array[index] = item.RawMap()
		}
		return array
	}
	return nil
}

func (p *RawPayload) ValueArray() []interface{} {
	if source := p.Array(); source != nil {
		array := make([]interface{}, len(source))
		for index, item := range source {
			array[index] = item.Value()
		}
		return array
	}
	return nil
}

func (p *RawPayload) IntArray() []int {
	if source := p.Array(); source != nil {
		array := make([]int, len(source))
		for index, item := range source {
			array[index] = item.Int()
		}
		return array
	}
	return nil
}

func (p *RawPayload) Int64Array() []int64 {
	if source := p.Array(); source != nil {
		array := make([]int64, len(source))
		for index, item := range source {
			array[index] = item.Int64()
		}
		return array
	}
	return nil
}

func (p *RawPayload) UintArray() []uint64 {
	if source := p.Array(); source != nil {
		array := make([]uint64, len(source))
		for index, item := range source {
			array[index] = item.Uint()
		}
		return array
	}
	return nil
}

func (p *RawPayload) Float32Array() []float32 {
	if source := p.Array(); source != nil {
		array := make([]float32, len(source))
		for index, item := range source {
			array[index] = item.Float32()
		}
		return array
	}
	return nil
}

func (p *RawPayload) FloatArray() []float64 {
	if source := p.Array(); source != nil {
		array := make([]float64, len(source))
		for index, item := range source {
			array[index] = item.Float()
		}
		return array
	}
	return nil
}

func (p *RawPayload) BoolArray() []bool {
	ba, ok := p.source.([]bool)
	if ok {
		return ba
	}
	if source := p.Array(); source != nil {
		array := make([]bool, len(source))
		for index, item := range source {
			array[index] = item.Bool()
		}
		return array
	}
	return nil
}

func (p *RawPayload) ByteArray() []byte {
	ba, ok := p.source.([]byte)
	if ok {
		return ba
	}
	return nil
}

func (p *RawPayload) TimeArray() []time.Time {
	if source := p.Array(); source != nil {
		array := make([]time.Time, len(source))
		for index, item := range source {
			array[index] = item.Time()
		}
		return array
	}
	return nil
}

func (p *RawPayload) Len() int {
	if transformer := ArrayTransformer(&p.source); transformer != nil {
		return transformer.ArrayLen(&p.source)
	}
	if transformer := MapTransformer(&p.source); transformer != nil {
		return transformer.Len(&p.source)
	}
	return 0
}

func (p *RawPayload) First() moleculer.Payload {
	if transformer := ArrayTransformer(&p.source); transformer != nil && transformer.ArrayLen(&p.source) > 0 {
		return New(transformer.First(&p.source))
	}
	return New(nil)
}

func (p *RawPayload) Array() []moleculer.Payload {
	if transformer := ArrayTransformer(&p.source); transformer != nil {
		source := transformer.InterfaceArray(&p.source)
		array := make([]moleculer.Payload, len(source))
		for index, item := range source {
			array[index] = New(item)
		}
		return array
	}
	return nil
}

func (p *RawPayload) ForEach(iterator func(key interface{}, value moleculer.Payload) bool) {
	if p.IsArray() {
		list := p.Array()
		for index, value := range list {
			if !iterator(index, value) {
				break
			}
		}
	} else if p.IsMap() {
		mapValue := p.Map()
		for key, value := range mapValue {
			if !iterator(key, value) {
				break
			}
		}
	} else {
		iterator(nil, p)
	}
}

func (p *RawPayload) IsArray() bool {
	transformer := ArrayTransformer(&p.source)
	return transformer != nil
}

func (p *RawPayload) IsMap() bool {
	transformer := MapTransformer(&p.source)
	return transformer != nil
}

func (p *RawPayload) Float() float64 {
	value, ok := p.source.(float64)
	if !ok {
		if transformer := getNumberTransformer(&p.source); transformer != nil {
			value = transformer.toFloat64(&p.source)
		}
	}
	return value
}

func (p *RawPayload) Float32() float32 {
	value, ok := p.source.(float32)
	if !ok {
		if transformer := getNumberTransformer(&p.source); transformer != nil {
			value = transformer.toFloat32(&p.source)
		}
	}
	return value
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

//mapToString takes in a map of payloads and return a string :)
func mapToString(m map[string]moleculer.Payload, ident string) string {
	out := "(len=" + strconv.Itoa(len(m)) + ") {\n"
	for _, key := range orderedKeys(m) {
		out = out + ident + `"` + key + `": ` + m[key].String() + ",\n"
	}
	if len(m) == 0 {
		out = out + "\n"
	}
	out = out + "}"
	return out
}

//arrayToString takes in a list of payloads and return a string :)
func arrayToString(arr []moleculer.Payload, ident string) string {
	out := "(array (len=" + strconv.Itoa(len(arr)) + ")) {\n"
	lines := make([]string, len(arr))
	for index, item := range arr {
		lines[index] = item.String()
	}
	sort.Strings(lines)
	for _, item := range lines {
		out = out + ident + item + ",\n"
	}
	if len(arr) == 0 {
		out = out + "\n"
	}
	out = out + "}"
	return out
}

type Stringer interface {
	String() string
}

func (p *RawPayload) String() string {
	s, isS := p.source.(string)
	if isS {
		return s
	}
	sr, isSr := p.source.(Stringer)
	if isSr {
		return sr.String()
	}
	return fmt.Sprint(p.source)
}

// func (p *RawPayload) StringIdented(ident string) string {
// 	if p.IsMap() {
// 		return mapToString(p.Map(), ident+"  ")
// 	}
// 	if p.IsArray() {
// 		return arrayToString(p.Array(), ident+"  ")
// 	}
// 	byteList, isBytes := p.source.([]byte)
// 	if isBytes {
// 		return string(byteList)
// 	}
// 	rawString, ok := p.source.(string)
// 	if ok {
// 		return rawString
// 	}
// 	return fmt.Sprintf("%v", p.source)

// }

func (p *RawPayload) Map() map[string]moleculer.Payload {
	if transformer := MapTransformer(&p.source); transformer != nil {
		source := transformer.AsMap(&p.source)
		newMap := make(map[string]moleculer.Payload, len(source))
		for key, item := range source {
			newPayload := RawPayload{item}
			newMap[key] = &newPayload
		}
		return newMap
	}
	return nil
}

func (p *RawPayload) RawMap() map[string]interface{} {
	if transformer := MapTransformer(&p.source); transformer != nil {
		return transformer.AsMap(&p.source)
	}
	return nil
}

// TODO refactor out as a transformer.. just not depend on bson.
func (p *RawPayload) Bson() bson.M {
	if GetValueType(&p.source) == "primitive.M" {
		return p.source.(bson.M)
	}
	if p.IsMap() {
		bm := bson.M{}
		p.ForEach(func(key interface{}, value moleculer.Payload) bool {
			skey := key.(string)
			if value.IsArray() {
				bm[skey] = value.BsonArray()
			} else if value.IsMap() {
				bm[skey] = value.Bson()
			} else {
				bm[skey] = value.Value()
			}
			return true
		})
		return bm
	}
	return nil
}

func (p *RawPayload) BsonArray() bson.A {
	if GetValueType(&p.source) == "[]primitive.A" {
		return p.source.(bson.A)
	}
	if p.IsArray() {
		ba := make(bson.A, p.Len())
		p.ForEach(func(index interface{}, value moleculer.Payload) bool {
			if value.IsMap() {
				ba[index.(int)] = value.Bson()
			} else if value.IsArray() {
				ba[index.(int)] = value.BsonArray()
			} else {
				ba[index.(int)] = value.Value()
			}
			return true
		})
		return ba
	}
	return nil
}

// mapGet try to get the value at the path assuming the source is a map
func (p *RawPayload) mapGet(path string) (interface{}, bool) {
	if transformer := MapTransformer(&p.source); transformer != nil {
		return transformer.get(path, &p.source)
	}
	return nil, false
}

func (p *RawPayload) Get(path string) moleculer.Payload {
	if value, ok := p.mapGet(path); ok {
		return New(value)
	}
	return New(nil)
}

func (p *RawPayload) Value() interface{} {
	return p.source
}

func match(key string, options []string) bool {
	for _, item := range options {
		if item == key {
			return true
		}
	}
	return false
}

type Sortable struct {
	Field string
	List  []moleculer.Payload
}

func (s *Sortable) Len() int {
	return len(s.List)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (s *Sortable) Less(i, j int) bool {
	vi := s.List[i].Get(s.Field)
	vj := s.List[j].Get(s.Field)
	return vi.String() < vj.String()
}

// Swap swaps the elements with indexes i and j.
func (s *Sortable) Swap(i, j int) {
	vi := s.List[i]
	vj := s.List[j]
	s.List[j] = vi
	s.List[i] = vj
}

func (s *Sortable) Payload() moleculer.Payload {
	return New(s.List)
}

func (p *RawPayload) Sort(field string) moleculer.Payload {
	if !p.IsArray() {
		return p
	}
	ps := &Sortable{field, p.Array()}
	sort.Sort(ps)
	return ps.Payload()
}

func (p *RawPayload) Remove(fields ...string) moleculer.Payload {
	if p.IsMap() {
		new := map[string]interface{}{}
		for key, value := range p.RawMap() {
			if !match(key, fields) {
				new[key] = value
			}
		}
		return New(new)
	}
	if p.IsArray() {
		arr := p.Array()
		new := make([]moleculer.Payload, len(arr))
		for index, item := range arr {
			new[index] = item.Remove(fields...)
		}
		return New(new)
	}
	return Error("payload.Remove can only deal with map and array payloads.")
}

func (p *RawPayload) AddItem(value interface{}) moleculer.Payload {
	if !p.IsArray() {
		return Error("payload.AddItem can only deal with lists/arrays.")
	}
	arr := p.Array()
	arr = append(arr, New(value))
	return New(arr)
}

//Add add the field:value pair to the existing values and return a new payload.
func (p *RawPayload) Add(field string, value interface{}) moleculer.Payload {
	if !p.IsMap() {
		return Error("payload.Add can only deal with map payloads.")
	}
	m := p.RawMap()
	m[field] = value
	return New(m)
}

//AddMany merge the maps with eh existing values and return a new payload.
func (p *RawPayload) AddMany(toAdd map[string]interface{}) moleculer.Payload {
	if !p.IsMap() {
		return Error("payload.Add can only deal with map payloads.")
	}
	m := p.RawMap()
	for key, value := range toAdd {
		m[key] = value
	}
	return New(m)
}

func Error(msgs ...interface{}) moleculer.Payload {
	return New(errors.New(fmt.Sprint(msgs...)))
}

var emptyList = &RawPayload{}

func EmptyList() moleculer.Payload {
	emptyList.source = []interface{}{}
	return emptyList
}

var emptyValue = &RawPayload{}

func Empty() moleculer.Payload {
	emptyValue.source = map[string]interface{}{}
	return emptyValue
}

func New(source interface{}) moleculer.Payload {
	pl, isPayload := source.(moleculer.Payload)
	if isPayload {
		return pl
	}
	return &RawPayload{source}
}

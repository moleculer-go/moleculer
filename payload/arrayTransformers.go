package payload

import (
	"reflect"
	"time"

	"github.com/moleculer-go/moleculer"
)

type interfaceArrayFunc func(source *interface{}) []interface{}
type arrayLenFunc func(source *interface{}) int
type firstFunc func(source *interface{}) interface{}

type arrayTransformer struct {
	name           string
	InterfaceArray interfaceArrayFunc
	First          firstFunc
	ArrayLen       arrayLenFunc
}

var arrayTransformers = []arrayTransformer{
	arrayTransformer{
		"[]interface {}",
		func(source *interface{}) []interface{} {
			return (*source).([]interface{})
		},
		func(source *interface{}) interface{} {
			return (*source).([]interface{})[0]
		},
		func(source *interface{}) int {
			return len((*source).([]interface{}))
		},
	},
	arrayTransformer{
		"[]string",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]string)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]string)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]string))
		},
	},
	arrayTransformer{
		"[]int",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]int)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]int)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]int))
		},
	},
	arrayTransformer{
		"[]bool",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]bool)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]bool)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]bool))
		},
	},
	arrayTransformer{
		"[]int64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]int64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]int64)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]int64))
		},
	},
	arrayTransformer{
		"[]float32",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]float32)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]float32)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]float32))
		},
	},
	arrayTransformer{
		"[]float64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]float64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]float64)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]float64))
		},
	},
	arrayTransformer{
		"[]uint64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]uint64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]uint64)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]uint64))
		},
	},
	arrayTransformer{
		"[]time.Time",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]time.Time)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]time.Time)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]time.Time))
		},
	},
	arrayTransformer{
		"[]map[string]interface {}",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]map[string]interface{})
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]map[string]interface{})[0]
		},
		func(source *interface{}) int {
			return len((*source).([]map[string]interface{}))
		},
	},
	arrayTransformer{
		"[]moleculer.Payload",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]moleculer.Payload)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value.Value()
			}
			return result
		},
		func(source *interface{}) interface{} {
			return (*source).([]moleculer.Payload)[0]
		},
		func(source *interface{}) int {
			return len((*source).([]moleculer.Payload))
		},
	},
}

func rawPayloadArrayTransformerFirst(source *interface{}) interface{} {
	return (*source).(*RawPayload).First()
}

func rawPayloadArrayTransformer(source *interface{}) []interface{} {
	sourceList := (*source).(*RawPayload)
	result := make([]interface{}, sourceList.Len())
	sourceList.ForEach(func(index interface{}, value moleculer.Payload) bool {
		result[index.(int)] = value.Value()
		return true
	})
	return result
}

func rawPayloadArrayTransformerLen(source *interface{}) int {
	sourceList := (*source).(*RawPayload)
	return sourceList.Len()
}

func reflectionArrayTransformerFirst(source *interface{}) interface{} {
	rv := reflect.ValueOf(*source)
	return rv.Index(0).Interface()
}

func reflectionArrayTransformer(source *interface{}) []interface{} {
	rv := reflect.ValueOf(*source)
	result := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i)
		value := item.Interface()
		if item.Kind() == reflect.Map {
			mt := MapTransformer(&value)
			result[i] = mt.AsMap(&value)
		} else if item.Kind() == reflect.Array || item.Kind() == reflect.Slice {
			at := ArrayTransformer(&value)
			result[i] = at.InterfaceArray(&value)
		} else {
			result[i] = value
		}
	}
	return result
}

func reflectionArraySize(source *interface{}) int {
	rv := reflect.ValueOf(*source)
	return rv.Len()
}

// ArrayTransformer : return the array/slice transformer for the specific map type
func ArrayTransformer(value *interface{}) *arrayTransformer {
	valueType := GetValueType(value)
	for _, transformer := range arrayTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	if valueType == "*payload.RawPayload" && (*value).(*RawPayload).IsArray() {
		transformer := arrayTransformer{
			"*payload.RawPayload",
			rawPayloadArrayTransformer,
			rawPayloadArrayTransformerFirst,
			rawPayloadArrayTransformerLen,
		}
		return &transformer
	}

	//try to use reflection
	rt := reflect.TypeOf(*value)
	if rt != nil && (rt.Kind() == reflect.Array || rt.Kind() == reflect.Slice) {
		return &arrayTransformer{"reflection", reflectionArrayTransformer, reflectionArrayTransformerFirst, reflectionArraySize}
	}
	//fmt.Println("ArrayTransformer() no transformer for  valueType -> ", valueType)
	return nil
}
